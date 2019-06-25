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
