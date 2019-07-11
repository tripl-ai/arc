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

  def parsePipeline(configUri: Option[String], arcContext: ARCContext)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): Either[List[Error], (ETLPipeline, ARCContext)] = {
    arcContext.environment match {
      case Some(_) => {
        configUri match {
          case Some(uri) => parseConfig(Right(new URI(uri)), arcContext)
          case None => Left(ConfigError("file", None, s"No config defined as a command line argument --etl.config.uri or ETL_CONF_URI environment variable.") :: Nil)
        }
      }  
      case None => Left(ConfigError("file", None, s"No environment defined as a command line argument --etl.config.environment or ETL_CONF_ENVIRONMENT environment variable.") :: Nil)
    }
  }

  def parseConfig(uri: Either[String, URI], arcContext: ARCContext)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): Either[List[Error], (ETLPipeline, ARCContext)] = {
    val base = ConfigFactory.load()

    val etlConfString = uri match {
      case Left(str) => Right(str)
      case Right(uri) => getConfigString(uri, arcContext)
    }

    etlConfString.rightFlatMap { str =>
      // calculate hash of raw string so that logs can be used to detect changes
      val etlConfStringHash = DigestUtils.md5Hex(str.getBytes)
      val uriString = uri match {
        case Left(str) => ""
        case Right(uri) => uri.toString
      }    

      logger.info()
        .field("event", "validateConfig")
        .field("uri", uriString)        
        .field("content-md5", etlConfStringHash)
        .log()         

      val etlConf = ConfigFactory.parseString(str, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))

      // convert to json string so that parameters can be correctly parsed
      val commandLineArgumentsJson = new ObjectMapper().writeValueAsString(arcContext.commandLineArguments.asJava).replace("\\", "")
      val commandLineArgumentsConf = ConfigFactory.parseString(commandLineArgumentsJson, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))

      // try to read objects in the plugins.config path. these must resolve before trying to read anything else
      val dynamicConfigsOrErrors = resolveConfigPlugins(etlConf, "plugins.config", arcContext.dynamicConfigurationPlugins)(spark, logger, arcContext)
      dynamicConfigsOrErrors match {
        case Left(errors) => Left(errors)
        case Right(dynamicConfigs) => {

          val resolvedConfig = dynamicConfigs match {
            case Nil =>
              etlConf.resolveWith(commandLineArgumentsConf.withFallback(etlConf).withFallback(base)).resolve()
            case _ =>
              val dynamicConfigsConf = dynamicConfigs.reduceRight[Config]{ case (c1, c2) => c1.withFallback(c2) }
              etlConf.resolveWith(commandLineArgumentsConf.withFallback(dynamicConfigsConf).withFallback(etlConf).withFallback(base)).resolve()
          }

          // use resolved config to parse other plugins
          val lifecyclePluginsOrErrors = resolveConfigPlugins(resolvedConfig, "plugins.lifecycle", arcContext.lifecyclePlugins)(spark, logger, arcContext)
          val pipelinePluginsOrErrors = resolveConfigPlugins(resolvedConfig, "stages", arcContext.pipelineStagePlugins)(spark, logger, arcContext)


          (lifecyclePluginsOrErrors, pipelinePluginsOrErrors) match {
            case (Left(lifecycleErrors), Left(pipelineErrors)) => Left(lifecycleErrors.reverse ::: pipelineErrors.reverse)
            case (Right(_), Left(pipelineErrors)) => Left(pipelineErrors.reverse)
            case (Left(lifecycleErrors), Right(_)) => Left(lifecycleErrors.reverse)
            case (Right(lifecycleInstances), Right(pipelineInstances)) => {

              // flatten any PipelineExecuteStage stages
              val flatPipelineInstances: List[PipelineStage] = pipelineInstances.flatMap { 
                instance => {
                  instance match {
                    case ai.tripl.arc.execute.PipelineExecuteStage(_, _, _, _, pipeline) => pipeline.stages
                    case stage: PipelineStage => List(stage)
                  }
                }
              }

              // used the resolved config to add registered lifecyclePlugins to context
              val ctx = ARCContext(
                jobId=arcContext.jobId, 
                jobName=arcContext.jobName, 
                environment=arcContext.environment, 
                environmentId=arcContext.environmentId, 
                configUri=arcContext.configUri, 
                isStreaming=arcContext.isStreaming, 
                ignoreEnvironments=arcContext.ignoreEnvironments, 
                storageLevel=arcContext.storageLevel,
                immutableViews=arcContext.immutableViews,
                commandLineArguments=arcContext.commandLineArguments,
                dynamicConfigurationPlugins=arcContext.dynamicConfigurationPlugins,
                lifecyclePlugins=arcContext.lifecyclePlugins,
                activeLifecyclePlugins=lifecycleInstances,
                pipelineStagePlugins=arcContext.pipelineStagePlugins,
                udfPlugins=arcContext.udfPlugins,
                userData=arcContext.userData
              ) 

              Right((ETLPipeline(flatPipelineInstances), ctx))
            }
          }
        }
      }
    }
  }  

  def getConfigString(uri: URI, arcContext: ARCContext)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): Either[List[Error], String] = {
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
        val s3aAccessKey: Option[String] = arcContext.commandLineArguments.get("etl.config.fs.s3a.access.key").orElse(envOrNone("ETL_CONF_S3A_ACCESS_KEY"))
        val s3aSecretKey: Option[String] = arcContext.commandLineArguments.get("etl.config.fs.s3a.secret.key").orElse(envOrNone("ETL_CONF_S3A_SECRET_KEY"))
        val s3aEndpoint: Option[String] = arcContext.commandLineArguments.get("etl.config.fs.s3a.endpoint").orElse(envOrNone("ETL_CONF_S3A_ENDPOINT"))
        val s3aConnectionSSLEnabled: Option[String] = arcContext.commandLineArguments.get("etl.config.fs.s3a.connection.ssl.enabled").orElse(envOrNone("ETL_CONF_S3A_CONNECTION_SSL_ENABLED"))        

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
        val azureAccountName: Option[String] = arcContext.commandLineArguments.get("etl.config.fs.azure.account.name").orElse(envOrNone("ETL_CONF_AZURE_ACCOUNT_NAME"))
        val azureAccountKey: Option[String] = arcContext.commandLineArguments.get("etl.config.fs.azure.account.key").orElse(envOrNone("ETL_CONF_AZURE_ACCOUNT_KEY"))

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
        val adlClientID: Option[String] = arcContext.commandLineArguments.get("etl.config.fs.adl.oauth2.client.id").orElse(envOrNone("ETL_CONF_ADL_OAUTH2_CLIENT_ID"))
        val adlRefreshToken: Option[String] = arcContext.commandLineArguments.get("etl.config.fs.adl.oauth2.refresh.token").orElse(envOrNone("ETL_CONF_ADL_OAUTH2_REFRESH_TOKEN"))

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
        val dfAccountName: Option[String] = arcContext.commandLineArguments.get("etl.config.fs.dfs.account.name").orElse(envOrNone("ETL_CONF_DFS_ACCOUNT_NAME"))
        val dfAccessKey: Option[String] = arcContext.commandLineArguments.get("etl.config.fs.dfs.access.key").orElse(envOrNone("ETL_CONF_DFS_ACCESS_KEY"))

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
        val gsProjectID: Option[String] = arcContext.commandLineArguments.get("etl.config.fs.gs.project.id").orElse(envOrNone("ETL_CONF_GOOGLE_CLOUD_PROJECT_ID"))
        val gsKeyfilePath: Option[String] = arcContext.commandLineArguments.get("etl.config.fs.google.cloud.auth.service.account.json.keyfile").orElse(envOrNone("ETL_CONF_GOOGLE_CLOUD_AUTH_SERVICE_ACCOUNT_JSON_KEYFILE"))

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

  // resolveConfigPlugins reads a list of objects at a specific path in the config and attempts to instantiate them
  def resolveConfigPlugins[T](c: Config, path: String, plugins: List[ConfigPlugin[T]])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[Error], List[T]] = {
    if (c.hasPath(path)) {

      // check valid type
      val objectList = try {
        c.getObjectList(path)
      } catch {
        case e: com.typesafe.config.ConfigException.WrongType => return Left(StageError(0, path, c.origin.lineNumber, ConfigError(path, Some(c.origin.lineNumber), s"Expected ${path} to be a List of Objects.") :: Nil) :: Nil)
        case e: Exception => return Left(StageError(0, path, c.origin.lineNumber, ConfigError(path, Some(c.origin.lineNumber), e.getMessage) :: Nil) :: Nil)
      }

      val (errors, instances) = objectList.asScala.zipWithIndex.foldLeft[(List[StageError], List[T])]( (Nil, Nil) ) { case ( (errors, instances), (plugin, index) ) =>
        import ConfigReader._
        val config = plugin.toConfig

        implicit val c = config

        val pluginType = getValue[String]("type")
        val environments = if (config.hasPath("environments")) config.getStringList("environments").asScala.toList else Nil

        // skip stage if not in environment
        if (!arcContext.ignoreEnvironments && !environments.contains(arcContext.environment.get)) {
          logger.trace()
            .field("event", "validateConfig")
            .field("type", pluginType.right.getOrElse("unknown"))
            .field("stageIndex", index)
            .field("environment", arcContext.environment.get)               
            .list("environments", environments.asJava)   
            .field("message", "skipping stage due to environment configuration")       
            .field("skipPlugin", true)
            .log()    
          
          (errors, instances)
        } else {
          val instanceOrError: Either[List[StageError], T] = pluginType match {
            case Left(errors) => Left(StageError(index, path, plugin.origin.lineNumber, errors) :: Nil)
            case Right(pluginType) => resolvePlugin(index, pluginType, config, plugins)
          }

          instanceOrError match {
            case Left(error) => (error ::: errors, instances)
            case Right(instance) => (errors, instance :: instances)
          }
        }
      }

      errors match {
        case Nil => Right(instances.reverse)
        case _ => Left(errors.reverse)
      }
    } else {
      Right(Nil)
    }
  }

  // resolvePlugin searches a provided list of plugins for a name/version combination
  // it then validates only a single plugin exists and if so calls the instantiate method
  def resolvePlugin[T](index: Int, name: String, config: Config, plugins: List[ConfigPlugin[T]])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[StageError], T] = {
    // match on either full class name or just the simple name AND version or not
    val splitPlugin = name.split(":", 2)
    val hasPackage = splitPlugin(0) contains "."
    val hasVersion = splitPlugin.length > 1

    val nameFilteredPlugins = if (hasPackage) {
      plugins.filter(plugin => plugin.getClass.getName == splitPlugin(0))
    } else {
      plugins.filter(plugin => plugin.getClass.getSimpleName == splitPlugin(0))
    }
    val filteredPlugins = if (hasVersion) {
      nameFilteredPlugins.filter(plugin => plugin.version == splitPlugin(1))
    } else {
      nameFilteredPlugins
    }

    val availablePluginsMessage = s"""Available plugins: ${plugins.map(c => s"${c.getClass.getName}:${c.version}").mkString("[",",","]")}."""
    val versionMessage = if (hasVersion) s"name:version" else "name"

    // return clean error messages if missing or duplicate
    if (filteredPlugins.length == 0) {
      Left(StageError(index, name, config.origin.lineNumber, ConfigError("stages", Some(config.origin.lineNumber), s"No plugins found with ${versionMessage} ${name}. ${availablePluginsMessage}") :: Nil) :: Nil)
    } else if (filteredPlugins.length > 1) {
      Left(StageError(index, name, config.origin.lineNumber, ConfigError("stages", Some(config.origin.lineNumber), s"Multiple plugins found with name ${splitPlugin(0)}. ${availablePluginsMessage}") :: Nil) :: Nil)
    } else {
      filteredPlugins.head.instantiate(index, config)
    }
  }

}
