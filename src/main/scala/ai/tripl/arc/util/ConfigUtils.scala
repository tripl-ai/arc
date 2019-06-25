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
    arcContext.environment match {
      case Some(_) => {
        configUri match {
          case Some(uri) => parseConfig(Right(new URI(uri)), commandLineArguments, arcContext)
          case None => Left(ConfigError("file", None, s"No config defined as a command line argument --etl.config.uri or ETL_CONF_URI environment variable.") :: Nil)
        }
      }  
      case None => Left(ConfigError("file", None, s"No environment defined as a command line argument --etl.config.environment or ETL_CONF_ENVIRONMENT environment variable.") :: Nil)
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
              .field("environment", arcContext.environment.get)               
              .list("environments", environments.asJava)               
              .log()   

            if (arcContext.ignoreEnvironments || environments.contains(arcContext.environment.get)) {
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
              .field("environment", arcContext.environment.get)               
              .list("environments", environments.asJava)               
              .log()   

            if (arcContext.ignoreEnvironments || environments.contains(arcContext.environment.get)) {
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
      if (!arcContext.ignoreEnvironments && !environments.contains(arcContext.environment.get)) {
        logger.trace()
          .field("event", "validateConfig")
          .field("type", stageType.right.getOrElse("unknown"))
          .field("stageIndex", index)
          .field("message", "skipping stage due to environment configuration")       
          .field("skipStage", true)
          .field("environment", arcContext.environment.get)               
          .list("environments", environments.asJava)               
          .log()    
        
        (stages, errs)
      } else {
        logger.trace()
          .field("event", "validateConfig")
          .field("type", stageType.right.getOrElse("unknown"))              
          .field("stageIndex", index)
          .field("skipStage", false)
          .field("environment", arcContext.environment.get)               
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
