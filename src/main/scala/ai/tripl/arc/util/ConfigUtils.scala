package ai.tripl.arc.util

import java.net.URI
import java.net.InetAddress
import java.sql.DriverManager
import java.util.ServiceLoader
import java.util.{Map => JMap}

import com.fasterxml.jackson.databind.ObjectMapper

import scala.collection.JavaConverters._
import scala.util.Properties._
import com.typesafe.config._

import org.apache.commons.codec.digest.DigestUtils

import org.apache.hadoop.fs.GlobPattern

import org.apache.spark.SparkFiles
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.With
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.plugins.{DynamicConfigurationPlugin, LifecyclePlugin, PipelineStagePlugin}
import ai.tripl.arc.util.ControlUtils._
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.config._
import ai.tripl.arc.config.Error._

import scala.reflect.runtime.universe._

object ConfigUtils {
  def classAccessors[T: TypeTag]: List[String] = typeOf[T].members.collect {
    case m: MethodSymbol if m.isCaseAccessor => m.name.toString
  }.toList

  def paramsToOptions(params: Map[String, String], options: Seq[String]): Map[String, String] = {
      params.filter{ case (k,v) => options.contains(k) }
  }

  def parsePipeline(configUri: Option[String], commandLineArguments: collection.mutable.Map[String, String], arcContext: ARCContext)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): Either[List[Error], (ETLPipeline, ARCContext)] = {
    configUri match {
      case Some(uri) => parseConfig(Right(new URI(uri)), commandLineArguments, arcContext)
      case None => Left(ConfigError("file", None, s"No config defined as a command line argument --etl.config.uri or ETL_CONF_URI environment variable.") :: Nil)
     }
  }

  def parseConfig(uri: Either[String, URI], commandLineArguments: collection.mutable.Map[String, String], arcContext: ARCContext)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): Either[List[Error], (ETLPipeline, ARCContext)] = {
    val base = ConfigFactory.load()

    val etlConfString = uri match {
      case Left(str) => Right(str)
      case Right(uri) => getConfigString(uri, commandLineArguments, arcContext)
    }

    val uriString = uri match {
      case Left(str) => ""
      case Right(uri) => uri.toString
    }    

    etlConfString.rightFlatMap { str =>
      // calculate hash of raw string so that logs can be used to detect changes
      val etlConfStringHash = DigestUtils.md5Hex(str.getBytes)

      val etlConf = ConfigFactory.parseString(str, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))

      // convert to json string so that parameters can be correctly parsed
      val commandLineArgumentsJson = new ObjectMapper().writeValueAsString(commandLineArguments.asJava).replace("\\", "")
      val commandLineArgumentsConf = ConfigFactory.parseString(commandLineArgumentsJson, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))

      // try to read objects in the plugins.config path
      val resolvedConfigPlugins = resolveDynamicConfigPlugins(etlConf, base, arcContext)
      
      val pluginConfs: List[Config] = resolvedConfigPlugins.map( c => ConfigFactory.parseMap(c) )

      val resolvedConfig: Config = pluginConfs match {
        case Nil =>
          etlConf.resolveWith(commandLineArgumentsConf.withFallback(etlConf).withFallback(base)).resolve()
        case _ =>
          val pluginConf = pluginConfs.reduceRight[Config]{ case (c1, c2) => c1.withFallback(c2) }
          val pluginValues = pluginConf.root().unwrapped()
          logger.debug()
            .message("Found additional config values from plugins")
            .field("pluginConf", pluginValues)
            .log()           
          etlConf.resolveWith(commandLineArgumentsConf.withFallback(pluginConf).withFallback(etlConf).withFallback(base)).resolve()
      }

      // used the resolved config to find plugins.lifestyle and create objects and replace the context
      val ctx = ARCContext(
        jobId=arcContext.jobId, 
        jobName=arcContext.jobName, 
        environment=arcContext.environment, 
        environmentId=arcContext.environmentId, 
        configUri=arcContext.configUri, 
        isStreaming=arcContext.isStreaming, 
        ignoreEnvironments=arcContext.ignoreEnvironments, 
        dynamicConfigurationPlugins=arcContext.dynamicConfigurationPlugins,
        lifecyclePlugins=arcContext.lifecyclePlugins,
        enabledLifecyclePlugins=resolveLifecyclePlugins(resolvedConfig, arcContext),
        pipelineStagePlugins=arcContext.pipelineStagePlugins,
        udfPlugins=arcContext.udfPlugins
      )      

      readPipeline(resolvedConfig, etlConfStringHash, uriString, commandLineArguments, ctx)
    }
  }  

  def getConfigString(uri: URI, commandLineArguments: collection.mutable.Map[String, String], arcContext: ARCContext)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): Either[List[Error], String] = {
    uri.getScheme match {
      case "local" => {
        val filePath = new URI(SparkFiles.get(uri.getPath))
        val etlConfString = CloudUtils.getTextBlob(filePath)
        Right(etlConfString)
      }
      case "file" => {
        val etlConfString = CloudUtils.getTextBlob(uri)
        Right(etlConfString)
      }
      case "classpath" => {
        val path = s"/${uri.getHost}${uri.getPath}"
        val etlConfString = using(getClass.getResourceAsStream(path)) { is =>
          scala.io.Source.fromInputStream(is).mkString
        }
        Right(etlConfString)
      }      
      // databricks file system
      case "dbfs" => {
        val etlConfString = CloudUtils.getTextBlob(uri)
        Right(etlConfString)
      }    
      // amazon s3
      case "s3a" => {
        val s3aAccessKey: Option[String] = commandLineArguments.get("etl.config.fs.s3a.access.key").orElse(envOrNone("ETL_CONF_S3A_ACCESS_KEY"))
        val s3aSecretKey: Option[String] = commandLineArguments.get("etl.config.fs.s3a.secret.key").orElse(envOrNone("ETL_CONF_S3A_SECRET_KEY"))
        val s3aEndpoint: Option[String] = commandLineArguments.get("etl.config.fs.s3a.endpoint").orElse(envOrNone("ETL_CONF_S3A_ENDPOINT"))
        val s3aConnectionSSLEnabled: Option[String] = commandLineArguments.get("etl.config.fs.s3a.connection.ssl.enabled").orElse(envOrNone("ETL_CONF_S3A_CONNECTION_SSL_ENABLED"))        

        val accessKey = s3aAccessKey match {
          case Some(value) => value
          case None => throw new IllegalArgumentException(s"AWS Access Key not provided for: ${uri}. Set etl.config.fs.s3a.access.key property or ETL_CONF_S3A_ACCESS_KEY environment variable.")
        }
        val secretKey = s3aSecretKey match {
          case Some(value) => value
          case None => throw new IllegalArgumentException(s"AWS Secret Key not provided for: ${uri}. Set etl.config.fs.s3a.secret.key property or ETL_CONF_S3A_SECRET_KEY environment variable.")
        }
        val connectionSSLEnabled = s3aConnectionSSLEnabled match {
          case Some(value) => {
            try {
              Option(value.toBoolean)
            } catch {
              case e: Exception => throw new IllegalArgumentException(s"AWS SSL configuration incorrect for: ${uri}. Ensure etl.config.fs.s3a.connection.ssl.enabled or ETL_CONF_S3A_CONNECTION_SSL_ENABLED environment variables are boolean.")
            }
          }
          case None => None
        }        

        CloudUtils.setHadoopConfiguration(Some(Authentication.AmazonAccessKey(accessKey, secretKey, s3aEndpoint, connectionSSLEnabled)))
        val etlConfString = CloudUtils.getTextBlob(uri)
        Right(etlConfString)
      }
      // azure blob
      case "wasb" | "wasbs" => {
        val azureAccountName: Option[String] = commandLineArguments.get("etl.config.fs.azure.account.name").orElse(envOrNone("ETL_CONF_AZURE_ACCOUNT_NAME"))
        val azureAccountKey: Option[String] = commandLineArguments.get("etl.config.fs.azure.account.key").orElse(envOrNone("ETL_CONF_AZURE_ACCOUNT_KEY"))

        val accountName = azureAccountName match {
          case Some(value) => value
          case None => throw new IllegalArgumentException(s"Azure Account Name not provided for: ${uri}. Set etl.config.fs.azure.account.name property or ETL_CONF_AZURE_ACCOUNT_NAME environment variable.")
        }
        val accountKey = azureAccountKey match {
          case Some(value) => value
          case None => throw new IllegalArgumentException(s"Azure Account Key not provided for: ${uri}. Set etl.config.fs.azure.account.key property or ETL_CONF_AZURE_ACCOUNT_KEY environment variable.")
        }

        CloudUtils.setHadoopConfiguration(Some(Authentication.AzureSharedKey(accountName, accountKey)))
        val etlConfString = CloudUtils.getTextBlob(uri)
        Right(etlConfString)
      }
      // azure data lake storage
      case "adl" => {
        val adlClientID: Option[String] = commandLineArguments.get("etl.config.fs.adl.oauth2.client.id").orElse(envOrNone("ETL_CONF_ADL_OAUTH2_CLIENT_ID"))
        val adlRefreshToken: Option[String] = commandLineArguments.get("etl.config.fs.adl.oauth2.refresh.token").orElse(envOrNone("ETL_CONF_ADL_OAUTH2_REFRESH_TOKEN"))

        val clientID = adlClientID match {
          case Some(value) => value
          case None => throw new IllegalArgumentException(s"Azure Data Lake Storage Client ID not provided for: ${uri}. Set etl.config.fs.adl.oauth2.client.id or ETL_CONF_ADL_OAUTH2_CLIENT_ID environment variable.")
        }
        val refreshToken = adlRefreshToken match {
          case Some(value) => value
          case None => throw new IllegalArgumentException(s"Azure Data Lake Storage Refresh Token not provided for: ${uri}. Set etl.config.fs.adl.oauth2.refresh.token property or ETL_CONF_ADL_OAUTH2_REFRESH_TOKEN environment variable.")
        }

        CloudUtils.setHadoopConfiguration(Some(Authentication.AzureDataLakeStorageToken(clientID, refreshToken)))
        val etlConfString = CloudUtils.getTextBlob(uri)
        Right(etlConfString)
      }
      // azure data lake storage gen 2
      case "abfs" | "abfss" => {
        val dfAccountName: Option[String] = commandLineArguments.get("etl.config.fs.dfs.account.name").orElse(envOrNone("ETL_CONF_DFS_ACCOUNT_NAME"))
        val dfAccessKey: Option[String] = commandLineArguments.get("etl.config.fs.dfs.access.key").orElse(envOrNone("ETL_CONF_DFS_ACCESS_KEY"))

        val accountName = dfAccountName match {
          case Some(value) => value
          case None => throw new IllegalArgumentException(s"Azure DLS Account Name not provided for: ${uri}. Set etl.config.fs.dfs.account.name property or ETL_CONF_DFS_ACCOUNT_NAME environment variable.")
        }
        val accountKey = dfAccessKey match {
          case Some(value) => value
          case None => throw new IllegalArgumentException(s"Azure DLS Access Key not provided for: ${uri}. Set etl.config.fs.dfs.access.key property or ETL_CONF_DFS_ACCESS_KEY environment variable.")
        }

        CloudUtils.setHadoopConfiguration(Some(Authentication.AzureDataLakeStorageGen2AccountKey(accountName, accountKey)))
        val etlConfString = CloudUtils.getTextBlob(uri)
        Right(etlConfString)
      }      
      // google cloud
      case "gs" => {
        val gsProjectID: Option[String] = commandLineArguments.get("etl.config.fs.gs.project.id").orElse(envOrNone("ETL_CONF_GOOGLE_CLOUD_PROJECT_ID"))
        val gsKeyfilePath: Option[String] = commandLineArguments.get("etl.config.fs.google.cloud.auth.service.account.json.keyfile").orElse(envOrNone("ETL_CONF_GOOGLE_CLOUD_AUTH_SERVICE_ACCOUNT_JSON_KEYFILE"))

        val projectID = gsProjectID match {
          case Some(value) => value
          case None => throw new IllegalArgumentException(s"Google Cloud Project ID not provided for: ${uri}. Set etl.config.fs.gs.project.id or ETL_CONF_GOOGLE_CLOUD_PROJECT_ID environment variable.")
        }
        val keyFilePath = gsKeyfilePath match {
          case Some(value) => value
          case None => throw new IllegalArgumentException(s"Google Cloud KeyFile Path not provided for: ${uri}. Set etl.config.fs.google.cloud.auth.service.account.json.keyfile property or ETL_CONF_GOOGLE_CLOUD_AUTH_SERVICE_ACCOUNT_JSON_KEYFILE environment variable.")
        }

        CloudUtils.setHadoopConfiguration(Some(Authentication.GoogleCloudStorageKeyFile(projectID, keyFilePath)))
        val etlConfString = CloudUtils.getTextBlob(uri)
        Right(etlConfString)
      }
      case _ => {
        Left(ConfigError("file", None, "make sure url scheme is defined e.g. file://${pwd}") :: Nil)
      }
    }
  }

  def resolveDynamicConfigPlugins(c: Config, base: Config, arcContext: ARCContext)(implicit logger: ai.tripl.arc.util.log.logger.Logger): List[JMap[String, Object]] = {
    if (c.hasPath("plugins.config")) {
      val plugins =
        (for (p <- c.getObjectList("plugins.config").asScala) yield {
          val plugin = p.toConfig.withFallback(base).resolve()
          if (plugin.hasPath("type")) {
            val _type =  plugin.getString("type")

            val environments = if (plugin.hasPath("environments")) plugin.getStringList("environments").asScala.toList else Nil
            logger.trace()
              .field("event", "validateConfig")
              .field("type", _type)              
              .field("message", "skipping plugin due to environment configuration")       
              .field("environment", arcContext.environment)               
              .list("environments", environments.asJava)               
              .log()   

            if (arcContext.ignoreEnvironments || environments.contains(arcContext.environment)) {
              val params = ai.tripl.arc.config.ConfigUtils.readMap("params", plugin)
              DynamicConfigurationPlugin.resolveAndExecutePlugin(_type, params).map(_ :: Nil)
            } else {
              None
            }
          } else {
            None
          }
        }).toList
      plugins.flatMap( p => p.getOrElse(Nil) )
    } else {
      Nil
    }
  }

  def resolveLifecyclePlugins(c: Config, arcContext: ARCContext)(implicit logger: ai.tripl.arc.util.log.logger.Logger): List[LifecyclePlugin] = {
    if (c.hasPath("plugins.lifecycle")) {
      val plugins =
        (for (p <- c.getObjectList("plugins.lifecycle").asScala) yield {
          val plugin = p.toConfig
          if (plugin.hasPath("type")) {
            val _type =  plugin.getString("type")

            val environments = if (plugin.hasPath("environments")) plugin.getStringList("environments").asScala.toList else Nil
            logger.trace()
              .field("event", "validateConfig")
              .field("type", _type)              
              .field("message", "skipping plugin due to environment configuration")       
              .field("environment", arcContext.environment)               
              .list("environments", environments.asJava)               
              .log()   

            if (arcContext.ignoreEnvironments || environments.contains(arcContext.environment)) {
              val params = ai.tripl.arc.config.ConfigUtils.readMap("params", plugin)
              LifecyclePlugin.resolve(_type, params).map(_ :: Nil)
            } else {
              None
            }
          } else {
            None
          }
        }).toList
      plugins.flatMap( p => p.getOrElse(Nil) )
    } else {
      Nil
    }
  }  


  private def textContentForURI(uri: URI, uriKey: String, authentication: Either[Errors, Option[Authentication]])
                               (implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): Either[Errors, String] = {
    uri.getScheme match {
      case "classpath" =>
        val path = s"/${uri.getHost}${uri.getPath}"
        using(getClass.getResourceAsStream(path)) { is =>
          val text = scala.io.Source.fromInputStream(is).mkString
          Right(text)
        }
      case _ =>
        authentication.right.map(auth => CloudUtils.setHadoopConfiguration(auth))
        getBlob(uriKey, uri)
    }
  }

  private def getBlob(path: String, uri: URI)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): Either[Errors, String] = {
    def err(lineNumber: Option[Int], msg: String): Either[Errors, String] = Left(ConfigError(path, lineNumber, msg) :: Nil)

    try {
      val textFile = CloudUtils.getTextBlob(uri)
      if (textFile.length == 0) {
        err(Some(c.getValue(path).origin.lineNumber()), s"""File at ${uri.toString} is empty.""")
      } else {
        Right(textFile)
      }
    } catch {
      case e: Exception => err(Some(c.getValue(path).origin.lineNumber()), e.getMessage)
    }
  }  

  private def getModel(path: String, uri: URI)(implicit spark: SparkSession, c: Config): Either[Errors, Either[PipelineModel, CrossValidatorModel]] = {
    def err(lineNumber: Option[Int], msg: String): Either[Errors, Either[PipelineModel, CrossValidatorModel]] = Left(ConfigError(path, lineNumber, msg) :: Nil)

    try {
      Right(Left(PipelineModel.load(uri.toString)))
    } catch {
      case e: Exception => {
        try{
         Right(Right(CrossValidatorModel.load(uri.toString)))
        } catch {
          case e: Exception => err(Some(c.getValue(path).origin.lineNumber()), e.getMessage)
        }
      }
    }
  }    

  def getExtractColumns(parsedURI: Either[Errors, Option[URI]], uriKey: String, authentication: Either[Errors, Option[Authentication]])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): Either[Errors, List[ExtractColumn]] = {
    /*val schema: Either[Errors, Option[String]] = parsedURI.rightFlatMap {
      case Some(uri) =>
        textContentForURI(uri, uriKey, authentication).rightFlatMap(text => Right(Option(text)))
      case None => Right(None)
    }

    schema.rightFlatMap { sch =>
      val cols = sch.map{ s => MetadataSchema.parseJsonMetadata(s) }.getOrElse(Right(Nil))

      cols match {
        case Left(errs) => Left(errs.map( e => ConfigError("metadata error", None, Error.pipelineSimpleErrorMsg(e.errors)) ))
        case Right(extractColumns) => Right(extractColumns)
      }
    }*/
    ???
  }

  private def getJDBCDriver(path: String, uri: String)(implicit c: Config): Either[Errors, java.sql.Driver] = {
    def err(lineNumber: Option[Int], msg: String): Either[Errors, java.sql.Driver] = Left(ConfigError(path, lineNumber, msg) :: Nil)

    // without this line tests fail as drivers have not been registered yet
    val drivers = DriverManager.getDrivers.asScala.toList.map(driver => s"""'${driver.toString}'""")

    try {
      Right(DriverManager.getDriver(uri))
    } catch {
      case e: Exception => {
        err(Some(c.getValue(path).origin.lineNumber()), s"""Invalid driver for ('$uri'). Available JDBC drivers: ${drivers.mkString("[", ", ", "]")}.""")
      }
    }
  }  

  // validateSQL uses the parsePlan method to verify if the sql command is parseable/valid. it will not check table existence.
  private def validateSQL(path: String, sql: String)(implicit spark: SparkSession, c: Config): Either[Errors, String] = {
    def err(lineNumber: Option[Int], msg: String): Either[Errors, String] = Left(ConfigError(path, lineNumber, msg) :: Nil)

    try {
      val parser = spark.sessionState.sqlParser
      parser.parsePlan(sql)
      Right(sql)
    } catch {
      case e: Exception => err(Some(c.getValue(path).origin.lineNumber()), e.getMessage)
    }
  }  

  // getRelations finds referenced tables within sql statements
  private def getRelations(sql: String)(implicit spark: SparkSession): List[String] = {
    val parser = spark.sessionState.sqlParser
    val plan = parser.parsePlan(sql)
    val relations = plan.collect { case r: UnresolvedRelation => r.tableName }
    relations.distinct.toList
  }  

  // getRelations finds referenced tables and aliases created within sql CTE statements
  private def getCteRelations(sql: String)(implicit spark: SparkSession): List[(String, List[String])] = {
    val parser = spark.sessionState.sqlParser
    val plan = parser.parsePlan(sql)
    plan.collect { case r: With => r.cteRelations.collect { case (alias, sa: SubqueryAlias) => (alias, sa.collect { case r: UnresolvedRelation => r.tableName }.toList)}}.flatten.toList
  }      

  private def validateAzureCosmosDBConfig(path: String, map:  Map[String, String])(implicit spark: SparkSession, c: Config): Either[Errors, com.microsoft.azure.cosmosdb.spark.config.Config] = {
    def err(lineNumber: Option[Int], msg: String): Either[Errors, com.microsoft.azure.cosmosdb.spark.config.Config] = Left(ConfigError(path, lineNumber, msg) :: Nil)

    try {
      Right(com.microsoft.azure.cosmosdb.spark.config.Config(map))
    } catch {
      case e: Exception => err(Some(c.getValue(path).origin.lineNumber()), e.getMessage)
    }
  }    

  // // extract
  // def readAvroExtract(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "inputView" :: "outputView" :: "authentication" :: "contiguousIndex" :: "numPartitions" :: "partitionBy" :: "persist" :: "schemaURI" :: "schemaView" :: "params" :: "basePath" :: "avroSchemaURI" :: "inputField" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)

  //   val description = getOptionalValue[String]("description")

  //   val input = if(c.hasPath("inputView")) getValue[String]("inputView") else getValue[String]("inputURI")
  //   val parsedGlob = if (!c.hasPath("inputView")) {
  //     input.rightFlatMap(glob => parseGlob("inputURI", glob))
  //   } else {
  //     Right("")
  //   }    

  //   val outputView = getValue[String]("outputView")
  //   val persist = getValue[Boolean]("persist", default = Some(false))
  //   val numPartitions = getOptionalValue[Int]("numPartitions")
  //   val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
  //   val authentication = readAuthentication("authentication")
  //   val contiguousIndex = getValue[Boolean]("contiguousIndex", default = Some(true))

  //   val uriKey = "schemaURI"
  //   val stringURI = getOptionalValue[String](uriKey)
  //   val parsedURI: Either[Errors, Option[URI]] = stringURI.rightFlatMap(optURI => 
  //     optURI match { 
  //       case Some(uri) => parseURI(uriKey, uri).rightFlatMap(parsedURI => Right(Option(parsedURI)))
  //       case None => Right(None)
  //     }
  //   )
  //   val extractColumns = if(!c.hasPath("schemaView")) getExtractColumns(parsedURI, uriKey, authentication) else Right(List.empty)
  //   val schemaView = if(c.hasPath("schemaView")) getValue[String]("schemaView") else Right("")
  //   val basePath = getOptionalValue[String]("basePath")
  //   val inputField = getOptionalValue[String]("inputField")

  //   val avroSchemaURI = getOptionalValue[String]("avroSchemaURI")
  //   val avroSchema: Either[Errors, Option[org.apache.avro.Schema]] = avroSchemaURI.rightFlatMap(optAvroSchemaURI => 
  //     optAvroSchemaURI match { 
  //       case Some(uri) => {
  //         parseURI("avroSchemaURI", uri)
  //         .rightFlatMap(uri => textContentForURI(uri, "avroSchemaURI", Right(None) ))
  //         .rightFlatMap(schemaString => parseAvroSchema("avroSchemaURI", schemaString))
  //       }
  //       case None => Right(None)
  //     }
  //   )    

  //   (name, description, extractColumns, schemaView, input, parsedGlob, outputView, persist, numPartitions, partitionBy, authentication, contiguousIndex, invalidKeys, basePath, inputField, avroSchemaURI, avroSchema) match {
  //     case (Right(n), Right(d), Right(cols), Right(sv), Right(in), Right(pg), Right(ov), Right(p), Right(np), Right(pb), Right(auth), Right(ci), Right(_), Right(bp), Right(ipf), Right(_), Right(avsc)) =>
  //       val input = if(c.hasPath("inputView")) Left(in) else Right(pg)
  //       val schema = if(c.hasPath("schemaView")) Left(sv) else Right(cols)

  //       var outputGraph = graph.addVertex(Vertex(idx, ov))

  //       (Right(AvroExtract(n, d, schema, ov, input, auth, params, p, np, pb, ci, bp, avsc, ipf)), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, extractColumns, schemaView, input, parsedGlob, outputView, persist, numPartitions, partitionBy, authentication, contiguousIndex, extractColumns, invalidKeys, basePath, inputField, avroSchemaURI, avroSchema).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }  

  // def readAzureCosmosDBExtract(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "authentication" :: "outputView" :: "numPartitions" :: "partitionBy" :: "persist" :: "params" :: "config" :: "schemaView" :: "schemaURI" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)

  //   val description = getOptionalValue[String]("description")

  //   val config = readMap("config", c)
  //   val configValid = validateAzureCosmosDBConfig("config", config)
  //   val authentication = readAuthentication("authentication")

  //   val uriKey = "schemaURI"
  //   val stringURI = getOptionalValue[String](uriKey)
  //   val parsedURI: Either[Errors, Option[URI]] = stringURI.rightFlatMap(optURI => 
  //     optURI match { 
  //       case Some(uri) => parseURI(uriKey, uri).rightFlatMap(parsedURI => Right(Option(parsedURI)))
  //       case None => Right(None)
  //     }
  //   )
  //   val extractColumns = if(!c.hasPath("schemaView")) getExtractColumns(parsedURI, uriKey, authentication) else Right(List.empty)
  //   val schemaView = if(c.hasPath("schemaView")) getValue[String]("schemaView") else Right("")

  //   val outputView = getValue[String]("outputView")
  //   val persist = getValue[Boolean]("persist", default = Some(false))
  //   val numPartitions = getOptionalValue[Int]("numPartitions")
  //   val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))

  //   (name, description, extractColumns, schemaView, outputView, persist, numPartitions, partitionBy, authentication, invalidKeys, configValid) match {
  //     case (Right(n), Right(d), Right(cols), Right(sv), Right(ov), Right(p), Right(np), Right(pb), Right(auth), Right(_), Right(_)) =>
  //     val schema = if(c.hasPath("schemaView")) Left(sv) else Right(cols)
  //     var outputGraph = graph.addVertex(Vertex(idx, ov))

  //     (Right(AzureCosmosDBExtract(n, d, schema, ov, params, p, np, pb, config)), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, extractColumns, schemaView, outputView, persist, numPartitions, partitionBy, authentication, invalidKeys, configValid).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }    

  // def readBytesExtract(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "inputURI" :: "outputView" :: "authentication" :: "contiguousIndex" :: "numPartitions" :: "persist" :: "params" :: "failMode" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)    

  //   val description = getOptionalValue[String]("description")

  //   val inputURI = getOptionalValue[String]("inputURI")
  //   val parsedGlob: Either[Errors, Option[String]] = inputURI.rightFlatMap {
  //     case Some(glob) =>
  //       val parsedGlob: Either[Errors, String] = parseGlob("inputURI", glob)
  //       parsedGlob.rightFlatMap(g => Right(Option(g)))
  //     case None =>
  //       val noValue: Either[Errors, Option[String]] = Right(None)
  //       noValue
  //   }
  //   val outputView = getValue[String]("outputView")
  //   val persist = getValue[Boolean]("persist", default = Some(false))
  //   val numPartitions = getOptionalValue[Int]("numPartitions")
  //   val authentication = readAuthentication("authentication")
  //   val contiguousIndex = getValue[Boolean]("contiguousIndex", default = Some(true))
  //   val inputView = getOptionalValue[String]("inputView")
  //   val failMode = getValue[String]("failMode", default = Some("failfast"), validValues = "permissive" :: "failfast" :: Nil) |> parseFailMode("failMode") _

  //   (name, description, parsedGlob, inputView, outputView, persist, numPartitions, authentication, contiguousIndex, invalidKeys, failMode) match {
  //     case (Right(n), Right(d), Right(pg), Right(iv), Right(ov), Right(p), Right(np), Right(auth), Right(ci), Right(_), Right(fm)) =>
  //       val validInput = (pg, iv) match {
  //         case (Some(_), None) => true
  //         case (None, Some(_)) => true
  //         case _ => false
  //       }

  //       if (validInput) {
  //         val input = if(c.hasPath("inputView")) Left(iv.get) else Right(pg.get)

  //         // add the vertices
  //         var outputGraph = graph.addVertex(Vertex(idx, ov))

  //         (Right(BytesExtract(n, d, ov, input, auth, params, p, np, ci, fm)), outputGraph)
  //       } else {
  //         val inputError = ConfigError("inputURI:inputView", Some(c.getValue("inputURI").origin.lineNumber()), "Either inputURI and inputView must be defined but only one can be defined at the same time") :: Nil
  //         val stageName = stringOrDefault(name, "unnamed stage")
  //         val err = StageError(idx, stageName, c.origin.lineNumber, inputError)
  //         (Left(err :: Nil), graph)
  //       }
  //     case _ =>
  //       val allErrors: Errors = List(name, description, inputURI, inputView, outputView, persist, numPartitions, authentication, invalidKeys, failMode).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }

  // def readDatabricksDeltaExtract(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "outputView" :: "numPartitions" :: "partitionBy" :: "persist" :: "params" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)

  //   val description = getOptionalValue[String]("description")

  //   val inputURI = getValue[String]("inputURI")
  //   val parsedGlob = inputURI.rightFlatMap(glob => parseGlob("inputURI", glob))
  //   val outputView = getValue[String]("outputView")
  //   val persist = getValue[Boolean]("persist", default = Some(false))
  //   val numPartitions = getOptionalValue[Int]("numPartitions")
  //   val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))

  //   (name, description, inputURI, parsedGlob, outputView, persist, numPartitions, partitionBy, invalidKeys) match {
  //     case (Right(n), Right(d), Right(in), Right(pg), Right(ov), Right(p), Right(np), Right(pb), Right(_)) => 
  //       var outputGraph = graph.addVertex(Vertex(idx, ov))

  //       (Right(DatabricksDeltaExtract(n, d, ov, pg, params, p, np, pb)), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, inputURI, parsedGlob, outputView, persist, numPartitions, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }  

  

  // def readElasticsearchExtract(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments"  :: "input" :: "outputView"  :: "numPartitions" :: "partitionBy" :: "persist" :: "params" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)    

  //   val description = getOptionalValue[String]("description")

  //   val input = getValue[String]("input")
  //   val outputView = getValue[String]("outputView")
  //   val persist = getValue[Boolean]("persist", default = Some(false))
  //   val numPartitions = getOptionalValue[Int]("numPartitions")
  //   val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))

  //   (name, description, input, outputView, persist, numPartitions, partitionBy, invalidKeys) match {
  //     case (Right(n), Right(d), Right(in), Right(ov), Right(p), Right(np), Right(pb), Right(_)) => 
  //       var outputGraph = graph.addVertex(Vertex(idx, ov))
  //       (Right(ElasticsearchExtract(n, d, in, ov, params, p, np, pb)), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, input, outputView, persist, numPartitions, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }   

  // def readHTTPExtract(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "inputURI" :: "outputView" :: "body" :: "headers" :: "method" :: "numPartitions" :: "partitionBy" :: "persist" :: "validStatusCodes" :: "params" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)

  //   val description = getOptionalValue[String]("description")

  //   val input = if(c.hasPath("inputView")) getValue[String]("inputView") else getValue[String]("inputURI")
  //   val parsedURI = if (!c.hasPath("inputView")) {
  //     input.rightFlatMap(uri => parseURI("inputURI", uri))
  //   } else {
  //     Right(new URI(""))
  //   }

  //   val headers = readMap("headers", c)
  //   val validStatusCodes = getValue[IntList]("validStatusCodes", default = Some(200 :: 201 :: 202 :: Nil))

  //   val outputView = getValue[String]("outputView")
  //   val persist = getValue[Boolean]("persist", default = Some(false))
  //   val numPartitions = getOptionalValue[Int]("numPartitions")
  //   val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
  //   val contiguousIndex = getValue[Boolean]("contiguousIndex", default = Some(true))

  //   val method = getValue[String]("method", default = Some("GET"), validValues = "GET" :: "POST" :: Nil)

  //   val body = getOptionalValue[String]("body")

  //   (name, description, input, parsedURI, outputView, persist, numPartitions, method, body, partitionBy, validStatusCodes, invalidKeys) match {
  //     case (Right(n), Right(d), Right(in), Right(pu), Right(ov), Right(p), Right(np), Right(m), Right(b), Right(pb), Right(vsc), Right(_)) => 
  //       val inp = if(c.hasPath("inputView")) Left(in) else Right(pu)
  //       var outputGraph = graph.addVertex(Vertex(idx, ov))

  //       (Right(HTTPExtract(n, d, inp, m, headers, b, vsc, ov, params, p, np, pb)), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, input, parsedURI, outputView, persist, numPartitions, method, body, partitionBy, validStatusCodes, invalidKeys).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // } 

  // def readImageExtract(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "outputView" :: "authentication" :: "dropInvalid" :: "numPartitions" :: "partitionBy" :: "persist" :: "params" :: "basePath" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)    

  //   val description = getOptionalValue[String]("description")

  //   val inputURI = getValue[String]("inputURI")
  //   val parsedGlob = inputURI.rightFlatMap(glob => parseGlob("inputURI", glob))
  //   val outputView = getValue[String]("outputView")
  //   val persist = getValue[Boolean]("persist", default = Some(false))
  //   val numPartitions = getOptionalValue[Int]("numPartitions")
  //   val partitionBy = if (c.hasPath("partitionBy")) c.getStringList("partitionBy").asScala.toList else Nil
  //   val authentication = readAuthentication("authentication")
  //   val dropInvalid = getValue[Boolean]("dropInvalid", default = Some(true))
  //   val basePath = getOptionalValue[String]("basePath")

  //   (name, description, inputURI, parsedGlob, outputView, persist, numPartitions, authentication, dropInvalid, invalidKeys, basePath) match {
  //     case (Right(n), Right(d), Right(in), Right(pg), Right(ov), Right(p), Right(np), Right(auth), Right(di), Right(_), Right(bp)) => 
  //       var outputGraph = graph.addVertex(Vertex(idx, ov))
  //       (Right(ImageExtract(n, d, ov, pg, auth, params, p, np, partitionBy, di, bp)), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, inputURI, parsedGlob, outputView, persist, numPartitions, authentication, dropInvalid, invalidKeys, basePath).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }   

  // def readJDBCExtract(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "jdbcURL" :: "tableName" :: "outputView" :: "authentication" :: "contiguousIndex" :: "fetchsize" :: "numPartitions" :: "params" :: "partitionBy" :: "partitionColumn" :: "persist" :: "predicates" :: "schemaURI" :: "schemaView" :: "params" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)  

  //   val description = getOptionalValue[String]("description")

  //   val outputView = getValue[String]("outputView")
  //   val persist = getValue[Boolean]("persist", default = Some(false))
  //   val jdbcURL = getValue[String]("jdbcURL")
  //   val driver = jdbcURL.rightFlatMap(uri => getJDBCDriver("jdbcURL", uri))
  //   val tableName = getValue[String]("tableName")
  //   val numPartitions = getOptionalValue[Int]("numPartitions")
  //   val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
  //   val fetchsize = getOptionalValue[Int]("fetchsize")
  //   val customSchema = getOptionalValue[String]("customSchema")
  //   val partitionColumn = getOptionalValue[String]("partitionColumn")
  //   val predicates = if (c.hasPath("predicates")) c.getStringList("predicates").asScala.toList else Nil

  //   val authentication = readAuthentication("authentication")
  //   val uriKey = "schemaURI"
  //   val stringURI = getOptionalValue[String](uriKey)
  //   val parsedURI: Either[Errors, Option[URI]] = stringURI.rightFlatMap(optURI => 
  //     optURI match { 
  //       case Some(uri) => parseURI(uriKey, uri).rightFlatMap(parsedURI => Right(Option(parsedURI)))
  //       case None => Right(None)
  //     }
  //   )
  //   val extractColumns = if(!c.hasPath("schemaView")) getExtractColumns(parsedURI, uriKey, authentication) else Right(List.empty)
  //   val schemaView = if(c.hasPath("schemaView")) getValue[String]("schemaView") else Right("")

  //   (name, description, extractColumns, schemaView, outputView, persist, jdbcURL, driver, tableName, numPartitions, fetchsize, customSchema, partitionColumn, partitionBy, invalidKeys) match {
  //     case (Right(n), Right(desc), Right(cols), Right(sv), Right(ov), Right(p), Right(ju), Right(d), Right(tn), Right(np), Right(fs), Right(cs), Right(pc), Right(pb), Right(_)) => 
  //       val schema = if(c.hasPath("schemaView")) Left(sv) else Right(cols)
  //       var outputGraph = graph.addVertex(Vertex(idx, ov))
  //       (Right(JDBCExtract(n, desc, schema, ov, ju, tn, np, fs, cs, d, pc, params, p, pb, predicates)), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, outputView, schemaView, persist, jdbcURL, driver, tableName, numPartitions, fetchsize, customSchema, extractColumns, partitionColumn, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }   

  // def readJSONExtract(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "inputURI" :: "outputView" :: "authentication" :: "contiguousIndex" :: "multiLine" :: "numPartitions" :: "partitionBy" :: "persist" :: "schemaURI" :: "schemaView" :: "params" :: "inputField" :: "basePath" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)    

  //   val description = getOptionalValue[String]("description")

  //   val input = if(c.hasPath("inputView")) getValue[String]("inputView") else getValue[String]("inputURI")
  //   val parsedGlob = if (!c.hasPath("inputView")) {
  //     input.rightFlatMap(glob => parseGlob("inputURI", glob))
  //   } else {
  //     Right("")
  //   }

  //   val outputView = getValue[String]("outputView")
  //   val persist = getValue[Boolean]("persist", default = Some(false))
  //   val numPartitions = getOptionalValue[Int]("numPartitions")
  //   val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
  //   val multiLine = getOptionalValue[Boolean]("multiLine")
  //   val authentication = readAuthentication("authentication")
  //   val contiguousIndex = getValue[Boolean]("contiguousIndex", default = Some(true))

  //   val uriKey = "schemaURI"
  //   val stringURI = getOptionalValue[String]("schemaURI")
  //   val parsedURI: Either[Errors, Option[URI]] = stringURI.rightFlatMap(optURI => 
  //     optURI match { 
  //       case Some(uri) => parseURI(uriKey, uri).rightFlatMap(parsedURI => Right(Option(parsedURI)))
  //       case None => Right(None)
  //     }
  //   )

  //   val extractColumns = if(!c.hasPath("schemaView")) getExtractColumns(parsedURI, uriKey, authentication) else Right(List.empty)
  //   val schemaView = if(c.hasPath("schemaView")) getValue[String]("schemaView") else Right("")

  //   val inputField = getOptionalValue[String]("inputField")
  //   val basePath = getOptionalValue[String]("basePath")

  //   (name, description, extractColumns, schemaView, input, parsedGlob, outputView, persist, numPartitions, multiLine, authentication, contiguousIndex, partitionBy, invalidKeys, inputField, basePath) match {
  //     case (Right(n), Right(d), Right(cols), Right(sv), Right(in), Right(pg), Right(ov), Right(p), Right(np), Right(ml), Right(auth), Right(ci), Right(pb), Right(_), Right(ipf), Right(bp)) => 
  //       val input = if(c.hasPath("inputView")) Left(in) else Right(pg)
  //       val schema = if(c.hasPath("schemaView")) Left(sv) else Right(cols)

  //       val json = JSON()
  //       val multiLine = ml match {
  //         case Some(b: Boolean) => b
  //         case _ => json.multiLine
  //       }
  //       var outputGraph = graph.addVertex(Vertex(idx, ov))
  //       (Right(JSONExtract(n, d, schema, ov, input, JSON(multiLine=multiLine), auth, params, p, np, pb, ci, ipf, bp)), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, input, schemaView, parsedGlob, outputView, persist, numPartitions, multiLine, authentication, contiguousIndex, extractColumns, partitionBy, invalidKeys, inputField, basePath).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }

  // def readKafkaExtract(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "outputView" :: "bootstrapServers" :: "topic" :: "groupID" :: "autoCommit" :: "maxPollRecords" :: "numPartitions" :: "partitionBy" :: "persist" :: "timeout" :: "params" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)    

  //   val description = getOptionalValue[String]("description")

  //   val outputView = getValue[String]("outputView")
  //   val topic = getValue[String]("topic")
  //   val bootstrapServers = getValue[String]("bootstrapServers")
  //   val groupID = getValue[String]("groupID")

  //   val persist = getValue[Boolean]("persist", default = Some(false))
  //   val numPartitions = getOptionalValue[Int]("numPartitions")
  //   val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))

  //   val maxPollRecords = getValue[Int]("maxPollRecords", default = Some(10000))
  //   val timeout = getValue[Long]("timeout", default = Some(10000L))
  //   val autoCommit = getValue[Boolean]("autoCommit", default = Some(false))

  //   (name, description, outputView, topic, bootstrapServers, groupID, persist, numPartitions, maxPollRecords, timeout, autoCommit, partitionBy, invalidKeys) match {
  //     case (Right(n), Right(d), Right(ov), Right(t), Right(bs), Right(g), Right(p), Right(np), Right(mpr), Right(time), Right(ac), Right(pb), Right(_)) => 
  //       var outputGraph = graph.addVertex(Vertex(idx, ov))
  //       (Right(KafkaExtract(n, d, ov, t, bs, g, mpr, time, ac, params, p, np, pb)), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, outputView, topic, bootstrapServers, groupID, persist, numPartitions, maxPollRecords, timeout, autoCommit, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }  

  // def readORCExtract(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "outputView" :: "authentication" :: "contiguousIndex" :: "numPartitions" :: "partitionBy" :: "persist" :: "schemaURI" :: "schemaView" :: "params" :: "basePath" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)

  //   val description = getOptionalValue[String]("description")

  //   val inputURI = getValue[String]("inputURI")
  //   val parsedGlob = inputURI.rightFlatMap(glob => parseGlob("inputURI", glob))
  //   val outputView = getValue[String]("outputView")
  //   val persist = getValue[Boolean]("persist", default = Some(false))
  //   val numPartitions = getOptionalValue[Int]("numPartitions")
  //   val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
  //   val authentication = readAuthentication("authentication")
  //   val contiguousIndex = getValue[Boolean]("contiguousIndex", default = Some(true))

  //   val uriKey = "schemaURI"
  //   val stringURI = getOptionalValue[String](uriKey)
  //   val parsedURI: Either[Errors, Option[URI]] = stringURI.rightFlatMap(optURI => 
  //     optURI match { 
  //       case Some(uri) => parseURI(uriKey, uri).rightFlatMap(parsedURI => Right(Option(parsedURI)))
  //       case None => Right(None)
  //     }
  //   )
  //   val extractColumns = if(!c.hasPath("schemaView")) getExtractColumns(parsedURI, uriKey, authentication) else Right(List.empty)
  //   val schemaView = if(c.hasPath("schemaView")) getValue[String]("schemaView") else Right("")
  //   val basePath = getOptionalValue[String]("basePath")

  //   (name, description, extractColumns, schemaView, inputURI, parsedGlob, outputView, persist, numPartitions, authentication, contiguousIndex, invalidKeys, partitionBy, basePath) match {
  //     case (Right(n), Right(d), Right(cols), Right(sv), Right(in), Right(pg), Right(ov), Right(p), Right(np), Right(auth), Right(ci), Right(_), Right(pb), Right(bp)) => 
  //       val schema = if(c.hasPath("schemaView")) Left(sv) else Right(cols)
  //       var outputGraph = graph.addVertex(Vertex(idx, ov))
  //       (Right(ORCExtract(n, d, schema, ov, pg, auth, params, p, np, pb, ci, bp)), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, inputURI, schemaView, parsedGlob, outputView, persist, numPartitions, authentication, contiguousIndex, extractColumns, invalidKeys, partitionBy, basePath).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }  

  // def readRateExtract(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "outputView" :: "rowsPerSecond" :: "rampUpTime" :: "numPartitions" :: "params" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)

  //   val description = getOptionalValue[String]("description")

  //   val outputView = getValue[String]("outputView")
  //   val rowsPerSecond = getValue[Int]("rowsPerSecond", default = Some(1))
  //   val rampUpTime = getValue[Int]("rampUpTime", default = Some(1))
  //   val numPartitions = getValue[Int]("numPartitions", default = Some(spark.sparkContext.defaultParallelism))

  //   (name, description, outputView, rowsPerSecond, rampUpTime, numPartitions) match {
  //     case (Right(n), Right(d), Right(ov), Right(rps), Right(rut), Right(np)) => 
  //       var outputGraph = graph.addVertex(Vertex(idx, ov))
  //       (Right(RateExtract(n, d, ov, params, rps, rut, np)), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, outputView, rowsPerSecond, rampUpTime, numPartitions).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }  

  // def readTextExtract(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "outputView" :: "authentication" :: "contiguousIndex" :: "multiLine" :: "numPartitions" :: "persist" :: "schemaURI" :: "schemaView" :: "params" :: "basePath" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)

  //   val description = getOptionalValue[String]("description")

  //   val input = getValue[String]("inputURI")
  //   val parsedGlob = input.rightFlatMap(glob => parseGlob("inputURI", glob))

  //   val outputView = getValue[String]("outputView")
  //   val persist = getValue[Boolean]("persist", default = Some(false))
  //   val numPartitions = getOptionalValue[Int]("numPartitions")
  //   val multiLine = getValue[Boolean]("multiLine", default = Some(false))
  //   val authentication = readAuthentication("authentication")
  //   val contiguousIndex = getValue[Boolean]("contiguousIndex", default = Some(true))

  //   val uriKey = "schemaURI"
  //   val stringURI = getOptionalValue[String](uriKey)
  //   val parsedURI: Either[Errors, Option[URI]] = stringURI.rightFlatMap(optURI => 
  //     optURI match { 
  //       case Some(uri) => parseURI(uriKey, uri).rightFlatMap(parsedURI => Right(Option(parsedURI)))
  //       case None => Right(None)
  //     }
  //   )

  //   val extractColumns = if(!c.hasPath("schemaView")) getExtractColumns(parsedURI, uriKey, authentication) else Right(List.empty)
  //   val basePath = getOptionalValue[String]("basePath")

  //   (name, description, extractColumns, input, parsedGlob, outputView, persist, numPartitions, multiLine, authentication, contiguousIndex, invalidKeys, basePath) match {
  //     case (Right(n), Right(d), Right(cols), Right(in), Right(pg), Right(ov), Right(p), Right(np), Right(ml), Right(auth), Right(ci), Right(_), Right(bp)) => 
  //       var outputGraph = graph.addVertex(Vertex(idx, ov))
  //       (Right(TextExtract(n, d, Right(cols), ov, in, auth, params, p, np, ci, ml, bp)), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, input, parsedGlob, outputView, persist, numPartitions, multiLine, authentication, contiguousIndex, extractColumns, invalidKeys, basePath).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }

  // def readXMLExtract(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "inputView" :: "outputView" :: "authentication" :: "contiguousIndex" :: "numPartitions" :: "partitionBy" :: "persist" :: "schemaURI" :: "schemaView" :: "params" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)

  //   val description = getOptionalValue[String]("description")

  //   val input = if(c.hasPath("inputView")) getValue[String]("inputView") else getValue[String]("inputURI")
  //   val parsedGlob = if (!c.hasPath("inputView")) {
  //     input.rightFlatMap(glob => parseGlob("inputURI", glob))
  //   } else {
  //     Right("")
  //   }

  //   val outputView = getValue[String]("outputView")
  //   val persist = getValue[Boolean]("persist", default = Some(false))
  //   val numPartitions = getOptionalValue[Int]("numPartitions")
  //   val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
  //   val authentication = readAuthentication("authentication")
  //   val contiguousIndex = getValue[Boolean]("contiguousIndex", default = Some(true))

  //   val uriKey = "schemaURI"
  //   val stringURI = getOptionalValue[String](uriKey)
  //   val parsedURI: Either[Errors, Option[URI]] = stringURI.rightFlatMap(optURI => 
  //     optURI match { 
  //       case Some(uri) => parseURI(uriKey, uri).rightFlatMap(parsedURI => Right(Option(parsedURI)))
  //       case None => Right(None)
  //     }
  //   )
  //   val extractColumns = if(!c.hasPath("schemaView")) getExtractColumns(parsedURI, uriKey, authentication) else Right(List.empty)
  //   val schemaView = if(c.hasPath("schemaView")) getValue[String]("schemaView") else Right("")  

  //   (name, description, extractColumns, schemaView, input, parsedGlob, outputView, persist, numPartitions, authentication, contiguousIndex, partitionBy, invalidKeys) match {
  //     case (Right(n), Right(d), Right(cols), Right(sv), Right(in), Right(pg), Right(ov), Right(p), Right(np), Right(auth), Right(ci), Right(pb), Right(_)) => 
  //       val input = if(c.hasPath("inputView")) Left(in) else Right(pg)
  //       val schema = if(c.hasPath("schemaView")) Left(sv) else Right(cols)
  //       var outputGraph = graph.addVertex(Vertex(idx, ov))

  //       (Right(XMLExtract(n, d, schema, ov, input, auth, params, p, np, pb, ci)), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, input, schemaView, parsedGlob, outputView, persist, numPartitions, authentication, contiguousIndex, extractColumns, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }


  // // transform
  // def readDiffTransform(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config, ctx: ARCContext): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputLeftView" :: "inputRightView" :: "outputIntersectionView" :: "outputLeftView" :: "outputRightView" :: "persist" :: "params" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)    

  //   val description = getOptionalValue[String]("description")

  //   val inputLeftView = getValue[String]("inputLeftView") |>  graph.vertexExists("inputLeftView") _
  //   val inputRightView = getValue[String]("inputRightView") |>  graph.vertexExists("inputRightView") _
  //   val outputIntersectionView = getOptionalValue[String]("outputIntersectionView")
  //   val outputLeftView = getOptionalValue[String]("outputLeftView")
  //   val outputRightView = getOptionalValue[String]("outputRightView")
  //   val persist = getValue[Boolean]("persist", default = Some(false))

  //   (name, description, inputLeftView, inputRightView, outputIntersectionView, outputLeftView, outputRightView, persist, invalidKeys) match {
  //     case (Right(n), Right(d), Right(ilv), Right(irv), Right(oiv), Right(olv), Right(orv), Right(p), Right(_)) => 
  //       // add the vertices
  //       var outputGraph = graph
  //       outputGraph = if (oiv.isEmpty) outputGraph else outputGraph.addVertex(Vertex(idx, oiv.get))
  //       outputGraph = if (olv.isEmpty) outputGraph else outputGraph.addVertex(Vertex(idx, olv.get)) 
  //       outputGraph = if (orv.isEmpty) outputGraph else outputGraph.addVertex(Vertex(idx, orv.get))

  //       (Right(DiffTransform(n, d, ilv, irv, oiv, olv, orv, params, p)), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, inputLeftView, inputRightView, outputIntersectionView, outputLeftView, outputRightView, persist, invalidKeys).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // } 

  // def readHTTPTransform(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputView" :: "uri" :: "headers" :: "inputField" :: "persist" :: "validStatusCodes" :: "params" :: "batchSize" :: "delimiter" :: "numPartitions" :: "partitionBy" :: "failMode" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)    

  //   val description = getOptionalValue[String]("description")

  //   val httpUriKey = "uri"
  //   val inputURI = getValue[String](httpUriKey)
  //   val inputField = getValue[String]("inputField", default = Some("value"))
  //   val parsedHttpURI = inputURI.rightFlatMap(uri => parseURI(httpUriKey, uri))
  //   val headers = readMap("headers", c)
  //   val inputView = getValue[String]("inputView")
  //   val outputView = getValue[String]("outputView")
  //   val persist = getValue[Boolean]("persist", default = Some(false))
  //   val validStatusCodes = getValue[IntList]("validStatusCodes", default = Some(200 :: 201 :: 202 :: Nil))
  //   val batchSize = getValue[Int]("batchSize", default = Some(1))
  //   val delimiter = getValue[String]("delimiter", default = Some("\n"))
  //   val numPartitions = getOptionalValue[Int]("numPartitions")
  //   val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))    
  //   val failMode = getValue[String]("failMode", default = Some("failfast"), validValues = "permissive" :: "failfast" :: Nil) |> parseFailMode("failMode") _

  //   (name, description, inputView, outputView, parsedHttpURI, persist, inputField, validStatusCodes, invalidKeys, batchSize, delimiter, numPartitions, partitionBy, failMode) match {
  //     case (Right(n), Right(d), Right(iv), Right(ov), Right(uri), Right(p), Right(ifld), Right(vsc), Right(_), Right(bs), Right(delim), Right(np), Right(pb), Right(fm)) => 
        
  //       var outputGraph = graph
  //       // add the vertices
  //       outputGraph = outputGraph.addVertex(Vertex(idx, ov))
  //       // add the edges
  //       outputGraph = outputGraph.addEdge(iv, ov)

  //       (Right(HTTPTransform(n, d, uri, headers, vsc, iv, ov, ifld, params, p, bs, delim, np, pb, fm)), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, inputView, outputView, parsedHttpURI, persist, inputField, validStatusCodes, invalidKeys, batchSize, delimiter, numPartitions, partitionBy, failMode).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }  

  // def readJSONTransform(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputView" :: "persist" :: "params" :: "numPartitions" :: "partitionBy" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)  

  //   val description = getOptionalValue[String]("description")

  //   val inputView = getValue[String]("inputView")
  //   val outputView = getValue[String]("outputView")
  //   val persist = getValue[Boolean]("persist", default = Some(false))
  //   val numPartitions = getOptionalValue[Int]("numPartitions")
  //   val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))  

  //   (name, description, inputView, outputView, persist, invalidKeys, numPartitions, partitionBy) match {
  //     case (Right(n), Right(d), Right(iv), Right(ov), Right(p), Right(_), Right(np), Right(pb)) => 
  //       var outputGraph = graph
  //       // add the vertices
  //       outputGraph = outputGraph.addVertex(Vertex(idx, ov))
  //       // add the edges
  //       outputGraph = outputGraph.addEdge(iv, ov)

  //       (Right(JSONTransform(n, d, iv, ov, params, p, np, pb)), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, inputView, outputView, persist, invalidKeys, numPartitions, partitionBy).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }  

  // def readMetadataFilterTransform(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "inputView" :: "outputView" :: "authentication" :: "persist" :: "sqlParams" :: "params" :: "numPartitions" :: "partitionBy" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)  

  //   val description = getOptionalValue[String]("description")

  //   val uriKey = "inputURI"
  //   val inputURI = getValue[String](uriKey)
  //   val parsedURI = inputURI.rightFlatMap(uri => parseURI(uriKey, uri))
  //   val authentication = readAuthentication("authentication")  
  //   val inputSQL = parsedURI.rightFlatMap { uri =>
  //       authentication.right.map(auth => CloudUtils.setHadoopConfiguration(auth))  
  //       getBlob(uriKey, uri)
  //   }
  //   val inputView = getValue[String]("inputView")
  //   val outputView = getValue[String]("outputView")
  //   val persist = getValue[Boolean]("persist", default = Some(false))
  //   val sqlParams = readMap("sqlParams", c)
  //   val numPartitions = getOptionalValue[Int]("numPartitions")
  //   val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))    

  //   // try to verify if sql is technically valid against HQL dialect (will not check dependencies)
  //   val validSQL = inputSQL.rightFlatMap { sql =>
  //     validateSQL(uriKey, SQLUtils.injectParameters(sql, sqlParams, false))
  //   }

  //   (name, description, parsedURI, inputSQL, validSQL, inputView, outputView, persist, invalidKeys, numPartitions, partitionBy) match {
  //     case (Right(n), Right(d), Right(uri), Right(sql), Right(vsql), Right(iv), Right(ov), Right(p), Right(_), Right(np), Right(pb)) => 
  //       var outputGraph = graph
  //       // add the vertices
  //       outputGraph = outputGraph.addVertex(Vertex(idx, ov))
  //       // add the edges
  //       outputGraph = outputGraph.addEdge(iv, ov)

  //       // pass the unreplaced input sql not the 'valid sql' as the paramenters will be replaced when the stage is executed for testing
  //       (Right(MetadataFilterTransform(n, d, iv, uri, sql, ov, params, sqlParams, p, np, pb)), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, inputURI, parsedURI, inputSQL, validSQL, inputView, outputView, persist, invalidKeys, numPartitions, partitionBy).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }   

  // def readMLTransform(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "inputView" :: "outputView" :: "authentication" :: "persist" :: "params" :: "numPartitions" :: "partitionBy" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)  

  //   val description = getOptionalValue[String]("description")

  //   val uriKey = "inputURI"
  //   val inputURI = getValue[String](uriKey)
  //   val parsedURI = inputURI.rightFlatMap(uri => parseURI(uriKey, uri))
  //   val authentication = readAuthentication("authentication")

  //   val inputModel = parsedURI.rightFlatMap { uri =>
  //       authentication.right.map(auth => CloudUtils.setHadoopConfiguration(auth))
  //       getModel(uriKey, uri)
  //   }

  //   val inputView = getValue[String]("inputView")
  //   val outputView = getValue[String]("outputView")
  //   val persist = getValue[Boolean]("persist", default = Some(false))
  //   val numPartitions = getOptionalValue[Int]("numPartitions")
  //   val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))        

  //   (name, description, inputURI, inputModel, inputView, outputView, persist, invalidKeys, numPartitions, partitionBy) match {
  //     case (Right(n), Right(d), Right(in), Right(mod), Right(iv), Right(ov), Right(p), Right(_), Right(np), Right(pb)) => 
  //       val uri = new URI(in)

  //       var outputGraph = graph
  //       // add the vertices
  //       outputGraph = outputGraph.addVertex(Vertex(idx, ov))
  //       // add the edges
  //       outputGraph = outputGraph.addEdge(iv, ov)

  //       (Right(MLTransform(n, d, uri, mod, iv, ov, params, p, np, pb)), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, inputURI, parsedURI, inputModel, inputView, outputView, persist, invalidKeys, numPartitions, partitionBy).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }

  // def readSQLTransform(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config, ctx: ARCContext): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._


  //   var outputGraph = graph

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "outputView" :: "authentication" :: "persist" :: "sqlParams" :: "params" :: "numPartitions" :: "partitionBy" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)  

  //   val description = getOptionalValue[String]("description")

  //   val uriKey = "inputURI"
  //   val inputURI = getValue[String](uriKey)
  //   val parsedURI = inputURI.rightFlatMap(uri => parseURI(uriKey, uri))
  //   val authentication = readAuthentication("authentication")  
  //   val inputSQL = parsedURI.rightFlatMap{ uri => textContentForURI(uri, uriKey, authentication) }
  //   val outputView = getValue[String]("outputView")
  //   val persist = getValue[Boolean]("persist", default = Some(false))
  //   val sqlParams = readMap("sqlParams", c)
  //   val numPartitions = getOptionalValue[Int]("numPartitions")
  //   val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))        

  //   // try to verify if sql is technically valid against HQL dialect (will not check dependencies)
  //   val validSQL = inputSQL.rightFlatMap { sql =>
  //     validateSQL(uriKey, SQLUtils.injectParameters(sql, sqlParams, false))
  //   }

  //   // tables exist
  //   val tableExistence: Either[Errors, String] = validSQL.rightFlatMap { sql =>

  //     // first ensure that all the tables referenced by the cte exist and add them to the graph
  //     val cteRelations = getCteRelations(sql)
  //     val (cteErrors, _) = cteRelations.map { case (alias, relations) => 
  //       // ensure all the relations inside the cte exist
  //       val errors = relations.map { relation => 
  //         val relEither = outputGraph.vertexExists("inputURI")(relation)
  //         relEither match {
  //           case Right(r) => {
  //             // add the CTE alias
  //             outputGraph = outputGraph.addVertex(Vertex(idx, alias))
  //             // add an edge
  //             outputGraph = outputGraph.addEdge(r, alias)
  //           }
  //           case Left(_) =>
  //         }
  //         relEither
  //       }
  //       .foldLeft[(Errors, String)]( (Nil, "") ){ case ( (errs, ret), table ) => 
  //         table match {
  //           case Left(err) => (err ::: errs, "")
  //           case _ => (errs, "")
  //         }
  //       }    

  //       errors
  //     }.foldLeft[(Errors, String)]( (Nil, "") ){ case ( (errs, ret), table ) => 
  //       table match {
  //         case (err, _) => (err ::: errs, "")
  //         case _ => (errs, "")
  //       }
  //     }  
 
  //     // if any cte tables missing do not evaluate dependency
  //     if (!cteErrors.isEmpty) {
  //       Left(cteErrors.reverse)
  //     } else {
  //       // if no cte errors
  //       // then ensure that all the relations in the non-cte part refer to tables that exist
  //       val relations = getRelations(sql)
  //       val (errors, _) = relations.map { relation => 
  //         outputGraph.vertexExists("inputURI")(relation)
  //       }
  //       .foldLeft[(Errors, String)]( (Nil, "") ){ case ( (errs, ret), table ) => 
  //         table match {
  //           case Left(err) => (err ::: errs, "")
  //           case _ => (errs, "")
  //         }
  //       }        

  //       errors match {
  //         case Nil => Right("")
  //         case _ => Left(errors.reverse)
  //       }        
  //     }
  //   }

  //   (name, description, parsedURI, inputSQL, validSQL, outputView, persist, invalidKeys, numPartitions, partitionBy, tableExistence) match {
  //     case (Right(n), Right(d), Right(uri), Right(sql), Right(vsql), Right(ov), Right(p), Right(_), Right(np), Right(pb), Right(te)) => 

  //       if (vsql.toLowerCase() contains "now") {
  //         logger.warn()
  //           .field("event", "validateConfig")
  //           .field("name", n)
  //           .field("type", "SQLTransform")              
  //           .field("message", "sql contains NOW() function which may produce non-deterministic results")       
  //           .log()   
  //       } 

  //       if (vsql.toLowerCase() contains "current_date") {
  //         logger.warn()
  //           .field("event", "validateConfig")
  //           .field("name", n)
  //           .field("type", "SQLTransform")              
  //           .field("message", "sql contains CURRENT_DATE() function which may produce non-deterministic results")       
  //           .log()   
  //       }

  //       if (vsql.toLowerCase() contains "current_timestamp") {
  //         logger.warn()
  //           .field("event", "validateConfig")
  //           .field("name", n)
  //           .field("type", "SQLTransform")              
  //           .field("message", "sql contains CURRENT_TIMESTAMP() function which may produce non-deterministic results")       
  //           .log()   
  //       }        

  //       outputGraph = outputGraph.addVertex(Vertex(idx, ov))
  //       // add the edges
  //       // add input/output edges by resolving dependent tables
  //       val relations = getRelations(sql)
  //       relations.foreach { iv =>
  //         outputGraph = outputGraph.addEdge(iv, ov)
  //       }

  //       // pass the unreplaced input sql not the 'valid sql' as the paramenters will be replaced when the stage is executed for testing
  //       (Right(SQLTransform(n, d, uri, sql, ov, params, sqlParams, p, np, pb)), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, inputURI, parsedURI, inputSQL, validSQL, outputView, persist, invalidKeys, numPartitions, partitionBy, tableExistence).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // } 

  // def readTensorFlowServingTransform(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputView" :: "uri" :: "batchSize" :: "inputField" :: "params"  :: "persist" :: "responseType" :: "signatureName" :: "numPartitions" :: "partitionBy" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)  

  //   val description = getOptionalValue[String]("description")

  //   val inputView = getValue[String]("inputView")
  //   val outputView = getValue[String]("outputView")
  //   val inputURI = getValue[String]("uri")
  //   val inputField = getValue[String]("inputField", default = Some("value"))
  //   val parsedURI = inputURI.rightFlatMap(uri => parseURI("uri", uri))
  //   val signatureName = getOptionalValue[String]("signatureName")
  //   val batchSize = getValue[Int]("batchsize", default = Some(1))
  //   val persist = getValue[Boolean]("persist", default = Some(false))
  //   val responseType = getValue[String]("responseType", default = Some("object"), validValues = "integer" :: "double" :: "object" :: Nil) |> parseResponseType("responseType") _
  //   val numPartitions = getOptionalValue[Int]("numPartitions")
  //   val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))        

  //   (name, description, inputView, outputView, inputURI, parsedURI, signatureName, responseType, batchSize, persist, inputField, invalidKeys, numPartitions, partitionBy) match {
  //     case (Right(n), Right(d), Right(iv), Right(ov), Right(uri), Right(puri), Right(sn), Right(rt), Right(bs), Right(p), Right(ifld), Right(_), Right(np), Right(pb)) => 
  //       var outputGraph = graph
  //       // add the vertices
  //       outputGraph = outputGraph.addVertex(Vertex(idx, ov))
  //       // add the edges
  //       outputGraph = outputGraph.addEdge(iv, ov)

  //       (Right(TensorFlowServingTransform(n, d, iv, ov, puri, sn, rt, bs, ifld, params, p, np, pb)), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, inputView, outputView, inputURI, parsedURI, signatureName, responseType, batchSize, persist, inputField, invalidKeys, numPartitions, partitionBy).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }

  // def readTypingTransform(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "inputView" :: "outputView" :: "authentication" :: "failMode" :: "persist" :: "params" :: "numPartitions" :: "partitionBy" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)  

  //   val description = getOptionalValue[String]("description")

  //   val uriKey = "inputURI"
  //   val inputURI = getValue[String](uriKey)
  //   val parsedURI = inputURI.rightFlatMap(uri => parseURI(uriKey, uri)).rightFlatMap(uri => Right(Option(uri)))
  //   val authentication = readAuthentication("authentication")  

  //   val extractColumns = if(!c.hasPath("schemaView")) getExtractColumns(parsedURI, uriKey, authentication) else Right(List.empty)
  //   val schemaView = if(c.hasPath("schemaView")) getValue[String]("schemaView") else Right("")

  //   val inputView = getValue[String]("inputView")
  //   val outputView = getValue[String]("outputView")
  //   val persist = getValue[Boolean]("persist", default = Some(false))

  //   val failMode = getValue[String]("failMode", default = Some("permissive"), validValues = "permissive" :: "failfast" :: Nil) |> parseFailMode("failMode") _
  //   val numPartitions = getOptionalValue[Int]("numPartitions")
  //   val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))        

  //   (name, description, extractColumns, schemaView, inputView, outputView, persist, failMode, invalidKeys, numPartitions, partitionBy) match {
  //     case (Right(n), Right(d), Right(cols), Right(sv), Right(iv), Right(ov), Right(p), Right(fm), Right(_), Right(np), Right(pb)) => 
  //       val schema = if(c.hasPath("schemaView")) Left(sv) else Right(cols)

  //       var outputGraph = graph
  //       // add the vertices
  //       outputGraph = outputGraph.addVertex(Vertex(idx, ov))
  //       // add the edges
  //       outputGraph = outputGraph.addEdge(iv, ov)

  //       (Right(TypingTransform(n, d, schema, iv, ov, params, p, fm, np, pb)), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, inputURI, parsedURI, extractColumns, schemaView, inputView, outputView, persist, authentication, failMode, invalidKeys, numPartitions, partitionBy).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }   

  // // load

  // def readAvroLoad(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputURI" :: "authentication" :: "numPartitions" :: "partitionBy" :: "saveMode" :: "params" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)  

  //   val description = getOptionalValue[String]("description")

  //   val inputView = getValue[String]("inputView")
  //   val outputURI = getValue[String]("outputURI")
  //   val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
  //   val numPartitions = getOptionalValue[Int]("numPartitions")
  //   val authentication = readAuthentication("authentication")  
  //   val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _

  //   (name, description, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys) match {
  //     case (Right(n), Right(d), Right(iv), Right(out), Right(np), Right(auth), Right(sm), Right(pb), Right(_)) => 
  //       val uri = new URI(out)
  //       val load = AvroLoad(n, d, iv, uri, pb, np, auth, sm, params)

  //       val ov = s"$idx:${load.getType}"
  //       var outputGraph = graph
  //       // add the vertices
  //       outputGraph = outputGraph.addVertex(Vertex(idx, ov))
  //       // add the edges
  //       outputGraph = outputGraph.addEdge(iv, ov)

  //       (Right(load), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }    

  // def readAzureEventHubsLoad(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "namespaceName" :: "eventHubName" :: "sharedAccessSignatureKeyName" :: "sharedAccessSignatureKey" :: "numPartitions" :: "retryCount" :: "retryMaxBackoff" :: "retryMinBackoff" :: "params" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)  

  //   val description = getOptionalValue[String]("description")

  //   val inputView = getValue[String]("inputView")
  //   val namespaceName = getValue[String]("namespaceName")
  //   val eventHubName = getValue[String]("eventHubName")
  //   val sharedAccessSignatureKeyName = getValue[String]("sharedAccessSignatureKeyName")
  //   val sharedAccessSignatureKey = getValue[String]("sharedAccessSignatureKey")
  //   val numPartitions = getOptionalValue[Int]("numPartitions")
  //   val retryMinBackoff = getValue[Long]("retryMinBackoff", default = Some(0)) // DEFAULT_RETRY_MIN_BACKOFF = 0
  //   val retryMaxBackoff = getValue[Long]("retryMaxBackoff", default = Some(30)) // DEFAULT_RETRY_MAX_BACKOFF = 30
  //   val retryCount = getValue[Int]("retryCount", default = Some(10)) // DEFAULT_MAX_RETRY_COUNT = 10

  //   (name, description, inputView, namespaceName, eventHubName, sharedAccessSignatureKeyName, sharedAccessSignatureKey, numPartitions, retryMinBackoff, retryMaxBackoff, retryCount, invalidKeys) match {
  //     case (Right(n), Right(d), Right(iv), Right(nn), Right(ehn), Right(saskn), Right(sask), Right(np), Right(rmin), Right(rmax), Right(rcount), Right(_)) => 
  //       val load = AzureEventHubsLoad(n, d, iv, nn, ehn, saskn, sask, np, rmin, rmax, rcount, params)

  //       val ov = s"$idx:${load.getType}"
  //       var outputGraph = graph
  //       // add the vertices
  //       outputGraph = outputGraph.addVertex(Vertex(idx, ov))
  //       // add the edges
  //       outputGraph = outputGraph.addEdge(iv, ov)

  //       (Right(load), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, inputView, namespaceName, eventHubName, sharedAccessSignatureKeyName, sharedAccessSignatureKey, numPartitions, retryMinBackoff, retryMaxBackoff, retryCount, invalidKeys).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }    

  // def readConsoleLoad(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputMode" :: "params" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)  

  //   val description = getOptionalValue[String]("description")

  //   val inputView = getValue[String]("inputView")
  //   val outputMode = getValue[String]("outputMode", default = Some("Append"), validValues = "Append" :: "Complete" :: "Update" :: Nil) |> parseOutputModeType("outputMode") _

  //   (name, description, inputView, outputMode, invalidKeys) match {
  //     case (Right(n), Right(d), Right(iv), Right(om), Right(_)) => 
  //       val load = ConsoleLoad(n, d, iv, om, params)

  //       val ov = s"$idx:${load.getType}"
  //       var outputGraph = graph
  //       // add the vertices
  //       outputGraph = outputGraph.addVertex(Vertex(idx, ov))
  //       // add the edges
  //       outputGraph = outputGraph.addEdge(iv, ov)

  //       (Right(load), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, inputView, outputMode, invalidKeys).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }    

  // def readDatabricksDeltaLoad(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputURI" :: "numPartitions" :: "partitionBy" :: "saveMode" :: "params" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)  

  //   val description = getOptionalValue[String]("description")

  //   val inputView = getValue[String]("inputView")
  //   val outputURI = getValue[String]("outputURI")
  //   val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
  //   val numPartitions = getOptionalValue[Int]("numPartitions")
  //   val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _

  //   (name, description, inputView, outputURI, numPartitions, saveMode, partitionBy, invalidKeys) match {
  //     case (Right(n), Right(d), Right(iv), Right(out), Right(np), Right(sm), Right(pb), Right(_)) => 
  //       val load = DatabricksDeltaLoad(n, d, iv, new URI(out), pb, np, sm, params)

  //       val ov = s"$idx:${load.getType}"
  //       var outputGraph = graph
  //       // add the vertices
  //       outputGraph = outputGraph.addVertex(Vertex(idx, ov))
  //       // add the edges
  //       outputGraph = outputGraph.addEdge(iv, ov)

  //       (Right(load), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, inputView, outputURI, numPartitions, saveMode, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }   

  // def readDatabricksSQLDWLoad(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "jdbcURL" :: "tempDir" :: "dbTable" :: "query" :: "forwardSparkAzureStorageCredentials" :: "tableOptions" :: "maxStrLength" :: "authentication" :: "params" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)     

  //   val description = getOptionalValue[String]("description")

  //   val inputView = getValue[String]("inputView")
  //   val jdbcURL = getValue[String]("jdbcURL")
  //   val driver = jdbcURL.rightFlatMap(uri => getJDBCDriver("jdbcURL", uri))
  //   val tempDir = getValue[String]("tempDir")
  //   val dbTable = getValue[String]("dbTable")
  //   val forwardSparkAzureStorageCredentials = getValue[Boolean]("forwardSparkAzureStorageCredentials", default = Some(true))
  //   val tableOptions = getOptionalValue[String]("tableOptions")
  //   val maxStrLength = getValue[Int]("maxStrLength", default = Some(256))
  //   val authentication = readAuthentication("authentication") |> validateAzureSharedKey("authentication") _

  //   (name, description, inputView, jdbcURL, driver, tempDir, dbTable, forwardSparkAzureStorageCredentials, tableOptions, maxStrLength, authentication, invalidKeys) match {
  //     case (Right(name), Right(description), Right(inputView), Right(jdbcURL), Right(driver), Right(tempDir), Right(dbTable), Right(forwardSparkAzureStorageCredentials), Right(tableOptions), Right(maxStrLength), Right(authentication), Right(invalidKeys)) => 
  //       val load = DatabricksSQLDWLoad(name, description, inputView, jdbcURL, driver, tempDir, dbTable, forwardSparkAzureStorageCredentials, tableOptions, maxStrLength, authentication, params)

  //       val ov = s"$idx:${load.getType}"
  //       var outputGraph = graph
  //       // add the vertices
  //       outputGraph = outputGraph.addVertex(Vertex(idx, ov))
  //       // add the edges
  //       outputGraph = outputGraph.addEdge(inputView, ov)

  //       (Right(load), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, inputView, jdbcURL, driver, tempDir, dbTable, forwardSparkAzureStorageCredentials, tableOptions, maxStrLength, authentication, invalidKeys).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }   

  // def readDelimitedLoad(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputURI" :: "authentication" :: "delimiter" :: "header" :: "numPartitions" :: "partitionBy" :: "quote" :: "saveMode" :: "params"  :: "customDelimiter" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)  

  //   val description = getOptionalValue[String]("description")

  //   val inputView = getValue[String]("inputView")
  //   val outputURI = getValue[String]("outputURI")
  //   val partitionBy = if (c.hasPath("partitionBy")) c.getStringList("partitionBy").asScala.toList else Nil    
  //   val numPartitions = getOptionalValue[Int]("numPartitions")
  //   val authentication = readAuthentication("authentication")  
  //   val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _

  //   val delimiter = getValue[String]("delimiter", default = Some("Comma"), validValues = "Comma" :: "Pipe" :: "DefaultHive" :: "Custom" :: Nil) |> parseDelimiter("delimiter") _
  //   val quote = getValue[String]("quote", default =  Some("DoubleQuote"), validValues = "DoubleQuote" :: "SingleQuote" :: "None" :: Nil) |> parseQuote("quote") _
  //   val header = getValue[Boolean]("header", Some(false))   

  //   val customDelimiter = delimiter match {
  //     case Right(Delimiter.Custom) => {
  //       getValue[String]("customDelimiter")
  //     }
  //     case _ => Right("")
  //   }     

  //   (name, description, inputView, outputURI, numPartitions, authentication, saveMode, delimiter, quote, header, invalidKeys, customDelimiter) match {
  //     case (Right(n), Right(desc), Right(iv), Right(out),  Right(np), Right(auth), Right(sm), Right(d), Right(q), Right(h), Right(_), Right(cd)) => 
  //       val uri = new URI(out)
  //       val load = DelimitedLoad(n, desc, iv, uri, Delimited(header=h, sep=d, quote=q, customDelimiter=cd), partitionBy, np, auth, sm, params)

  //       val ov = s"$idx:${load.getType}"
  //       var outputGraph = graph
  //       // add the vertices
  //       outputGraph = outputGraph.addVertex(Vertex(idx, ov))
  //       // add the edges
  //       outputGraph = outputGraph.addEdge(iv, ov)

  //       (Right(load), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, inputView, outputURI, authentication, numPartitions, saveMode, delimiter, quote, header, invalidKeys, customDelimiter).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }    

  // def readElasticsearchLoad(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "output" :: "numPartitions" :: "partitionBy" :: "saveMode" :: "params" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)  

  //   val description = getOptionalValue[String]("description")

  //   val inputView = getValue[String]("inputView")
  //   val output = getValue[String]("output")
  //   val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
  //   val numPartitions = getOptionalValue[Int]("numPartitions")
  //   val authentication = readAuthentication("authentication")  
  //   val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _

  //   (name, description, inputView, output, numPartitions, saveMode, partitionBy, invalidKeys) match {
  //     case (Right(n), Right(d), Right(iv), Right(out), Right(np), Right(sm), Right(pb), Right(_)) => 
  //       val load = ElasticsearchLoad(n, d, iv, out, pb, np, sm, params)

  //       val ov = s"$idx:${load.getType}"
  //       var outputGraph = graph
  //       // add the vertices
  //       outputGraph = outputGraph.addVertex(Vertex(idx, ov))
  //       // add the edges
  //       outputGraph = outputGraph.addEdge(iv, ov)

  //       (Right(load), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, inputView, output, numPartitions, saveMode, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }      
  
  // def readHTTPLoad(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputURI" :: "headers" :: "validStatusCodes" :: "params" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)     

  //   val description = getOptionalValue[String]("description")

  //   val inputView = getValue[String]("inputView")
  //   val inputURI = getValue[String]("outputURI")
  //   val parsedURI = inputURI.rightFlatMap(uri => parseURI("outputURI", uri))
  //   val headers = readMap("headers", c)
  //   val validStatusCodes = getValue[IntList]("validStatusCodes", default = Some(200 :: 201 :: 202 :: Nil))

  //   (name, description, inputURI, parsedURI, inputView, invalidKeys, validStatusCodes) match {
  //     case (Right(n), Right(d), Right(iuri), Right(uri), Right(iv), Right(_), Right(vsc)) => 
  //       val load = HTTPLoad(n, d, iv, uri, headers, vsc, params)

  //       val ov = s"$idx:${load.getType}"
  //       var outputGraph = graph
  //       // add the vertices
  //       outputGraph = outputGraph.addVertex(Vertex(idx, ov))
  //       // add the edges
  //       outputGraph = outputGraph.addEdge(iv, ov)

  //       (Right(load), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, parsedURI, inputView, invalidKeys, validStatusCodes).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }  

  // def readJDBCLoad(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "jdbcURL" :: "tableName" :: "params" :: "batchsize" :: "bulkload" :: "createTableColumnTypes" :: "createTableOptions" :: "isolationLevel" :: "numPartitions" :: "saveMode" :: "tablock" :: "truncate" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)     

  //   val description = getOptionalValue[String]("description")

  //   val inputView = getValue[String]("inputView")
  //   val jdbcURL = getValue[String]("jdbcURL")
  //   val driver = jdbcURL.rightFlatMap(uri => getJDBCDriver("jdbcURL", uri))
  //   val tableName = getValue[String]("tableName")
  //   val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
  //   val numPartitions = getOptionalValue[Int]("numPartitions")
  //   val isolationLevel = getValue[String]("isolationLevel", default = Some("READ_UNCOMMITTED"), validValues = "NONE" :: "READ_COMMITTED" :: "READ_UNCOMMITTED" :: "REPEATABLE_READ" :: "SERIALIZABLE" :: Nil) |> parseIsolationLevel("isolationLevel") _
  //   val batchsize = getValue[Int]("batchsize", default = Some(1000))
  //   val truncate = getValue[Boolean]("truncate", default = Some(false))
  //   val createTableOptions = getOptionalValue[String]("createTableOptions")
  //   val createTableColumnTypes = getOptionalValue[String]("createTableColumnTypes")
  //   val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _
  //   val bulkload = getValue[Boolean]("bulkload", default = Some(false))
  //   val tablock = getValue[Boolean]("tablock", default = Some(true))

  //   (name, description, inputView, jdbcURL, driver, tableName, numPartitions, isolationLevel, batchsize, truncate, createTableOptions, createTableColumnTypes, saveMode, bulkload, tablock, partitionBy, invalidKeys) match {
  //     case (Right(n), Right(desc), Right(iv), Right(ju), Right(d), Right(tn), Right(np), Right(il), Right(bs), Right(t), Right(cto), Right(ctct), Right(sm), Right(bl), Right(tl), Right(pb), Right(_)) => 
  //       val load = JDBCLoad(n, desc, iv, ju, tn, pb, np, il, bs, t, cto, ctct, sm, d, bl, tl, params)

  //       val ov = s"$idx:${load.getType}"
  //       var outputGraph = graph
  //       // add the vertices
  //       outputGraph = outputGraph.addVertex(Vertex(idx, ov))
  //       // add the edges
  //       outputGraph = outputGraph.addEdge(iv, ov)

  //       (Right(load), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, inputView, jdbcURL, driver, tableName, numPartitions, isolationLevel, batchsize, truncate, createTableOptions, createTableColumnTypes, saveMode, bulkload, tablock, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }      

  // def readJSONLoad(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputURI" :: "authentication" :: "numPartitions" :: "partitionBy" :: "saveMode" :: "params" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)      

  //   val description = getOptionalValue[String]("description")

  //   val inputView = getValue[String]("inputView")
  //   val outputURI = getValue[String]("outputURI")
  //   val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
  //   val numPartitions = getOptionalValue[Int]("numPartitions")
  //   val authentication = readAuthentication("authentication")  
  //   val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _

  //   (name, description, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys) match {
  //     case (Right(n), Right(d), Right(iv), Right(out), Right(np), Right(auth), Right(sm), Right(pb), Right(_)) => 
  //       val load = JSONLoad(n, d, iv, new URI(out), pb, np, auth, sm, params)

  //       val ov = s"$idx:${load.getType}"
  //       var outputGraph = graph
  //       // add the vertices
  //       outputGraph = outputGraph.addVertex(Vertex(idx, ov))
  //       // add the edges
  //       outputGraph = outputGraph.addEdge(iv, ov)

  //       (Right(load), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }

  // def readKafkaLoad(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "bootstrapServers" :: "topic" :: "acks" :: "batchSize" :: "numPartitions" :: "retries" :: "params" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)     

  //   val description = getOptionalValue[String]("description")

  //   val inputView = getValue[String]("inputView")
  //   val bootstrapServers = getValue[String]("bootstrapServers")
  //   val topic = getValue[String]("topic")
  //   val acks = getValue[Int]("acks", default = Some(1))
  //   val retries = getValue[Int]("retries", default = Some(0))
  //   val batchSize = getValue[Int]("batchSize", default = Some(16384))

  //   val numPartitions = getOptionalValue[Int]("numPartitions")

  //   (name, description, inputView, topic, bootstrapServers, acks, retries, batchSize, numPartitions, invalidKeys) match {
  //     case (Right(n), Right(d), Right(iv), Right(t), Right(bss), Right(a), Right(r), Right(bs), Right(np), Right(_)) => 
  //       val load = KafkaLoad(n, d, iv, t, bss, a, np, r, bs, params)

  //       val ov = s"$idx:${load.getType}"
  //       var outputGraph = graph
  //       // add the vertices
  //       outputGraph = outputGraph.addVertex(Vertex(idx, ov))
  //       // add the edges
  //       outputGraph = outputGraph.addEdge(iv, ov)

  //       (Right(load), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, inputView, topic, bootstrapServers, acks, retries, batchSize, numPartitions, invalidKeys).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }   

  // def readORCLoad(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputURI" :: "authentication" :: "numPartitions" :: "partitionBy" :: "saveMode" :: "params" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)     

  //   val description = getOptionalValue[String]("description")

  //   val inputView = getValue[String]("inputView")
  //   val outputURI = getValue[String]("outputURI")
  //   val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
  //   val numPartitions = getOptionalValue[Int]("numPartitions")
  //   val authentication = readAuthentication("authentication")  
  //   val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _

  //   (name, description, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys) match {
  //     case (Right(n), Right(d), Right(iv), Right(out), Right(np), Right(auth), Right(sm), Right(pb), Right(_)) => 
  //       val load = ORCLoad(n, d, iv, new URI(out), pb, np, auth, sm, params)

  //       val ov = s"$idx:${load.getType}"
  //       var outputGraph = graph
  //       // add the vertices
  //       outputGraph = outputGraph.addVertex(Vertex(idx, ov))
  //       // add the edges
  //       outputGraph = outputGraph.addEdge(iv, ov)

  //       (Right(load), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }  

  // def readParquetLoad(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config, ctx: ARCContext): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputURI" :: "authentication" :: "numPartitions" :: "partitionBy" :: "saveMode" :: "params" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)  

  //   val description = getOptionalValue[String]("description")

  //   val inputView = getValue[String]("inputView") |> graph.vertexExists("inputView") _
  //   val outputURI = getValue[String]("outputURI") |> validateURI("outputURI") _
  //   val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
  //   val numPartitions = getOptionalValue[Int]("numPartitions")
  //   val authentication = readAuthentication("authentication")  
  //   val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _

  //   (name, description, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys) match {
  //     case (Right(n), Right(d), Right(iv), Right(out), Right(np), Right(auth), Right(sm), Right(pb), Right(_)) => 
  //       val load = ParquetLoad(n, d, iv, out, pb, np, auth, sm, params)

  //       val ov = s"$idx:${load.getType}"
  //       var outputGraph = graph
  //       // add the vertices
  //       outputGraph = outputGraph.addVertex(Vertex(idx, ov))
  //       // add the edges
  //       outputGraph = outputGraph.addEdge(iv, ov)

  //       (Right(load), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }  

  // def readTextLoad(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config, ctx: ARCContext): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputURI" :: "authentication" :: "numPartitions" :: "partitionBy" :: "saveMode" :: "params" :: "singleFile" :: "prefix" :: "separator" :: "suffix" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)  

  //   val description = getOptionalValue[String]("description")

  //   val inputView = getValue[String]("inputView") |> graph.vertexExists("inputView") _
  //   val outputURI = getValue[String]("outputURI") |> validateURI("outputURI") _
  //   val numPartitions = getOptionalValue[Int]("numPartitions")
  //   val authentication = readAuthentication("authentication")  
  //   val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _

  //   val singleFile = getValue[Boolean]("singleFile", default = Some(false))
  //   val prefix = getValue[String]("prefix", default = Some(""))
  //   val separator = getValue[String]("separator", default = Some(""))
  //   val suffix = getValue[String]("suffix", default = Some(""))

  //   (name, description, inputView, outputURI, numPartitions, authentication, saveMode, invalidKeys, singleFile, prefix, separator, suffix) match {
  //     case (Right(n), Right(d), Right(iv), Right(out), Right(np), Right(auth), Right(sm), Right(_), Right(sf), Right(pre), Right(sep), Right(suf)) => 
  //       val load = TextLoad(n, d, iv, out, np, auth, sm, params, sf, pre, sep, suf)

  //       val ov = s"$idx:${load.getType}"
  //       var outputGraph = graph
  //       // add the vertices
  //       outputGraph = outputGraph.addVertex(Vertex(idx, ov))
  //       // add the edges
  //       outputGraph = outputGraph.addEdge(iv, ov)

  //       (Right(load), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, inputView, outputURI, numPartitions, authentication, saveMode, invalidKeys, singleFile, prefix, separator, suffix).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // } 

  // def readXMLLoad(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputURI" :: "authentication" :: "numPartitions" :: "partitionBy" :: "saveMode" :: "params" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)  

  //   val description = getOptionalValue[String]("description")

  //   val inputView = getValue[String]("inputView")
  //   val outputURI = getValue[String]("outputURI")
  //   val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
  //   val numPartitions = getOptionalValue[Int]("numPartitions")
  //   val authentication = readAuthentication("authentication")  
  //   val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _

  //   (name, description, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys) match {
  //     case (Right(n), Right(d), Right(iv), Right(out), Right(np), Right(auth), Right(sm), Right(pb), Right(_)) => 
  //       val load = XMLLoad(n, d, iv, new URI(out), pb, np, auth, sm, params)

  //       val ov = s"$idx:${load.getType}"
  //       var outputGraph = graph
  //       // add the vertices
  //       outputGraph = outputGraph.addVertex(Vertex(idx, ov))
  //       // add the edges
  //       outputGraph = outputGraph.addEdge(iv, ov)

  //       (Right(load), outputGraph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }  

  // // execute
  // def readHTTPExecute(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "uri" :: "headers" :: "payloads" :: "validStatusCodes" :: "params" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)  

  //   val description = getOptionalValue[String]("description")

  //   val uri = getValue[String]("uri")
  //   val headers = readMap("headers", c)
  //   val payloads = readMap("payloads", c)
  //   val validStatusCodes = getValue[IntList]("validStatusCodes", default = Some(200 :: 201 :: 202 :: Nil))

  //   (name, description, uri, validStatusCodes, invalidKeys) match {
  //     case (Right(n), Right(d), Right(u), Right(vsc), Right(_)) => 
  //       (Right(HTTPExecute(n, d, new URI(u), headers, payloads, vsc, params)), graph)
  //     case _ =>
  //       val allErrors: Errors = List(uri, description, validStatusCodes, invalidKeys).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }

  // def readJDBCExecute(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "jdbcURL" :: "authentication" :: "params" :: "password" :: "sqlParams" :: "user" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)  

  //   val description = getOptionalValue[String]("description")

  //   val authentication = readAuthentication("authentication")  

  //   val uriKey = "inputURI"
  //   val inputURI = getValue[String](uriKey)
  //   val parsedURI = inputURI.rightFlatMap(uri => parseURI(uriKey, uri))
  //   val inputSQL = parsedURI.rightFlatMap { uri =>
  //       authentication.right.map(auth => CloudUtils.setHadoopConfiguration(auth))    
  //       getBlob(uriKey, uri)
  //   }

  //   val jdbcURL = getValue[String]("jdbcURL")
  //   val driver = jdbcURL.rightFlatMap(uri => getJDBCDriver("jdbcURL", uri))
  //   val user = getOptionalValue[String]("user")
  //   val password = getOptionalValue[String]("password")

  //   val sqlParams = readMap("sqlParams", c)    

  //   (name, description, inputURI, inputSQL, jdbcURL, user, password, driver, invalidKeys) match {
  //     case (Right(n), Right(desc), Right(in), Right(sql), Right(url), Right(u), Right(p), Right(d), Right(_)) => 
  //       (Right(JDBCExecute(n, desc, new URI(in), url, u, p, sql, sqlParams, params)), graph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, inputURI, parsedURI, inputSQL, jdbcURL, user, password, driver, invalidKeys).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }

  // def readKafkaCommitExecute(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "bootstrapServers" :: "groupID" :: "params" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)  

  //   val description = getOptionalValue[String]("description")

  //   val inputView = getValue[String]("inputView")
  //   val bootstrapServers = getValue[String]("bootstrapServers")
  //   val groupID = getValue[String]("groupID")

  //   (name, description, inputView, bootstrapServers, groupID, invalidKeys) match {
  //     case (Right(n), Right(d), Right(iv), Right(bs), Right(g), Right(_)) => 
  //       (Right(KafkaCommitExecute(n, d, iv, bs, g, params)), graph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, inputView, bootstrapServers, groupID, invalidKeys).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }    

  // def readPipelineExecute(idx: Int, config: Config, argsMap: collection.mutable.Map[String, String], arcContext: ARCContext)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): Either[List[StageError], PipelineStage] = {
  //   import ConfigReader._

  //   implicit val c: Config = config

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "uri" :: "authentication" :: "params" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)      

  //   val description = getOptionalValue[String]("description")

  //   val name = getValue[String]("name")
  //   val uri = getValue[String]("uri")
  //   val authentication = readAuthentication("authentication")  
  //   authentication.right.map(auth => CloudUtils.setHadoopConfiguration(auth))    

  //   (name, description, uri, invalidKeys) match {
  //     case (Right(n), Right(d), Right(u), Right(_)) => 
  //       val uri = new URI(u)
  //       val subPipeline = parseConfig(Right(uri), argsMap, arcContext)
  //       subPipeline match {
  //         case Right(etl) => Right(PipelineExecute(n, d, uri, etl._1))
  //         case Left(errors) => {
  //           val stageErrors = errors.collect { case s: StageError => s }
  //           Left(stageErrors)
  //         }
  //       }
  //     case _ =>
  //       val allErrors: Errors = List(uri, description, invalidKeys).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       Left(err :: Nil)
  //   }
  // }

  // // validate
  // def readEqualityValidate(idx: Int, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "leftView" :: "rightView" :: "params" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)    

  //   val description = getOptionalValue[String]("description")

  //   val leftView = getValue[String]("leftView")
  //   val rightView = getValue[String]("rightView")

  //   (name, description, leftView, rightView, invalidKeys) match {
  //     case (Right(n), Right(d), Right(l), Right(r), Right(_)) => 
  //       (Right(EqualityValidate(n, d, l, r, params)), graph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, leftView, rightView, invalidKeys).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }  
  
  // def readSQLValidate(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
  //   import ConfigReader._

  //   val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "authentication" :: "sqlParams" :: "params" :: Nil
  //   val invalidKeys = checkValidKeys(c)(expectedKeys)    

  //   val description = getOptionalValue[String]("description")

  //   val authentication = readAuthentication("authentication")  

  //   val uriKey = "inputURI"
  //   val inputURI = getValue[String](uriKey)
  //   val parsedURI = inputURI.rightFlatMap(uri => parseURI(uriKey, uri))
  //   val inputSQL = parsedURI.rightFlatMap{ uri => textContentForURI(uri, uriKey, authentication) }

  //   val sqlParams = readMap("sqlParams", c) 

  //   // try to verify if sql is technically valid against HQL dialect (will not check dependencies)
  //   val validSQL = inputSQL.rightFlatMap { sql =>
  //     validateSQL(uriKey, SQLUtils.injectParameters(sql, sqlParams, false))
  //   }    

  //   (name, description, parsedURI, inputSQL, validSQL, invalidKeys) match {
  //     case (Right(n), Right(d), Right(uri), Right(sql), Right(vsql), Right(_)) => 
  //       // pass the unreplaced input sql not the 'valid sql' as the paramenters will be replaced when the stage is executed for testing
  //       (Right(SQLValidate(n, d, uri, sql, sqlParams, params)), graph)
  //     case _ =>
  //       val allErrors: Errors = List(name, description, parsedURI, inputSQL, validSQL, invalidKeys).collect{ case Left(errs) => errs }.flatten
  //       val stageName = stringOrDefault(name, "unnamed stage")
  //       val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
  //       (Left(err :: Nil), graph)
  //   }
  // }

  def readPipelineStage(index: Int, stageType: String, config: Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[StageError], PipelineStage] = {
    implicit val c: Config = config

    // match on either full class name or just the simple name AND version or not
    val splitStageType = stageType.split(":", 2)
    val hasPackage = splitStageType(0) contains "."
    val hasVersion = splitStageType.length > 1

    val nameFilteredPlugins = if (hasPackage) {
      arcContext.pipelineStagePlugins.filter(plugin => plugin.getClass.getName == splitStageType(0))
    } else {
      arcContext.pipelineStagePlugins.filter(plugin => plugin.getClass.getSimpleName == splitStageType(0))
    }
    val filteredPlugins = if (hasVersion) {
      nameFilteredPlugins.filter(plugin => plugin.version == splitStageType(1))
    } else {
      nameFilteredPlugins
    }
    
    // return clean error messages if missing or duplicate
    val availablePluginsMessage = s"""Available plugins: ${arcContext.pipelineStagePlugins.map(c => s"${c.getClass.getName}:${c.version}").mkString("[",",","]")}."""
    if (filteredPlugins.length == 0) {
      val versionMessage = if (hasVersion) s"name:version" else "name"
      Left(StageError(index, stageType, config.origin.lineNumber, ConfigError("stages", Some(config.origin.lineNumber), s"No plugins found with ${versionMessage} ${stageType}. ${availablePluginsMessage}") :: Nil) :: Nil)
    } else if (filteredPlugins.length > 1) {
      Left(StageError(index, stageType, config.origin.lineNumber, ConfigError("stages", Some(config.origin.lineNumber), s"Multiple plugins found with name ${splitStageType(0)}. ${availablePluginsMessage}") :: Nil) :: Nil)
    } else {
      filteredPlugins(0).createStage(index, config)
    }
  }

  def readPipeline(c: Config, configMD5: String, uri: String, commandLineArguments: collection.mutable.Map[String, String], arcContext: ARCContext)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): Either[List[Error], (ETLPipeline, ARCContext)] = {
    import ConfigReader._

    val startTime = System.currentTimeMillis() 
    val configStages = c.getObjectList("stages")

    logger.info()
      .field("event", "validateConfig")
      .field("uri", uri)        
      .field("content-md5", configMD5)
      .log()    

    val (stages, errors) = configStages.asScala.zipWithIndex.foldLeft[(List[PipelineStage], List[StageError])]( (Nil, Nil) ) { case ( (stages, errs), (stage, index) ) =>
      import ConfigReader._
      val config = stage.toConfig

      implicit val c = config
      implicit val ctx = arcContext

      val stageType = getValue[String]("type")
      val environments = if (config.hasPath("environments")) config.getStringList("environments").asScala.toList else Nil

      // skip stage if not in environment
      if (!arcContext.ignoreEnvironments && !environments.contains(arcContext.environment)) {
        logger.trace()
          .field("event", "validateConfig")
          .field("type", stageType.right.getOrElse("unknown"))
          .field("stageIndex", index)
          .field("message", "skipping stage due to environment configuration")       
          .field("skipStage", true)
          .field("environment", arcContext.environment)               
          .list("environments", environments.asJava)               
          .log()    
        
        (stages, errs)
      } else {
        logger.trace()
          .field("event", "validateConfig")
          .field("type", stageType.right.getOrElse("unknown"))              
          .field("stageIndex", index)
          .field("skipStage", false)
          .field("environment", arcContext.environment)               
          .list("environments", environments.asJava)               
          .log()   

        val stageOrError: Either[List[StageError], PipelineStage] = stageType match {
          case Left(_) => Left(StageError(index, "unknown", stage.origin.lineNumber, ConfigError("stages", Some(stage.origin.lineNumber), s"Unknown stage type: '${stageType}'") :: Nil) :: Nil)
          case Right(stageType) => readPipelineStage(index, stageType, config)
        }

        stageOrError match {
          // case Right(PipelineExecute(_, _, _, subPipeline)) => (subPipeline.stages.reverse ::: stages, errs)
          case Right(s) => (s :: stages, errs)
          case Left(stageErrors) => (stages, stageErrors ::: errs)
        }
      }
    }

    val errorsOrPipeline = errors match {
      case Nil => {
        val stagesReversed = stages.reverse

        Right(ETLPipeline(stagesReversed), arcContext)
      }
      case _ => Left(errors.reverse)
    }

    logger.info()
      .field("event", "exit")
      .field("type", "readPipeline")        
      .field("duration", System.currentTimeMillis() - startTime)
      .log()  

    errorsOrPipeline
  }
}
