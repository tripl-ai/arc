package au.com.agl.arc.util

import java.net.URI
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

import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.plugins.{DynamicConfigurationPlugin, PipelineStagePlugin}
import au.com.agl.arc.util.ControlUtils._
import au.com.agl.arc.util.EitherUtils._

import scala.reflect.runtime.universe._

object ConfigUtils {
  def classAccessors[T: TypeTag]: List[String] = typeOf[T].members.collect {
    case m: MethodSymbol if m.isCaseAccessor => m.name.toString
  }.toList

  def paramsToOptions(params: Map[String, String], options: Seq[String]): Map[String, String] = {
      params.filter{ case (k,v) => options.contains(k) }
  }

  def parsePipeline(configUri: Option[String], argsMap: collection.mutable.Map[String, String], graph: Graph, arcContext: ARCContext)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Either[List[Error], (ETLPipeline, Graph)] = {
    configUri match {
      case Some(uri) => parseConfig(Right(new URI(uri)), argsMap, graph, arcContext)
      case None => Left(ConfigError("file", None, s"No config defined as a command line argument --etl.config.uri or ETL_CONF_URI environment variable.") :: Nil)
     }
  }

  def getConfigString(uri: URI, argsMap: collection.mutable.Map[String, String], arcContext: ARCContext)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Either[List[Error], String] = {
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
        val s3aEndpoint: Option[String] = argsMap.get("etl.config.fs.s3a.endpoint").orElse(envOrNone("ETL_CONF_S3A_ENDPOINT"))
        val s3aConnectionSSLEnabled: Option[String] = argsMap.get("etl.config.fs.s3a.connection.ssl.enabled").orElse(envOrNone("ETL_CONF_S3A_CONNECTION_SSL_ENABLED"))
        val s3aAccessKey: Option[String] = argsMap.get("etl.config.fs.s3a.access.key").orElse(envOrNone("ETL_CONF_S3A_ACCESS_KEY"))
        val s3aSecretKey: Option[String] = argsMap.get("etl.config.fs.s3a.secret.key").orElse(envOrNone("ETL_CONF_S3A_SECRET_KEY"))

        val endpoint = s3aEndpoint match {
          case Some(value) => value
          case None => throw new IllegalArgumentException(s"AWS Endpoint not provided to for: ${uri}. Set etl.config.fs.s3a.endpoint property or ETL_CONF_S3A_ENDPOINT environment variable.")
        }
        val connectionSSLEnabled = s3aConnectionSSLEnabled match {
          case Some(value) => value
          case None => throw new IllegalArgumentException(s"AWS Connection SSL Enabled not provided for: ${uri}. Set etl.config.fs.s3a.connection.ssl.enabled property or ETL_CONF_S3A_CONNECTION_SSL_ENABLED environment variable.")
        }
        val accessKey = s3aAccessKey match {
          case Some(value) => value
          case None => throw new IllegalArgumentException(s"AWS Access Key not provided for: ${uri}. Set etl.config.fs.s3a.access.key property or ETL_CONF_S3A_ACCESS_KEY environment variable.")
        }
        val secretKey = s3aSecretKey match {
          case Some(value) => value
          case None => throw new IllegalArgumentException(s"AWS Secret Key not provided for: ${uri}. Set etl.config.fs.s3a.secret.key property or ETL_CONF_S3A_SECRET_KEY environment variable.")
        }

        val config = Map("fs_s3a_endpoint" -> endpoint, "fs_s3a_connection_ssl_enabled" -> connectionSSLEnabled, "fs_s3a_access_key" -> accessKey, "fs_s3a_secret_key" -> secretKey)

        CloudUtils.setHadoopConfiguration(Some(Authentication.AmazonAccessKey(accessKey, secretKey)))
        val etlConfString = CloudUtils.getTextBlob(uri)
        Right(etlConfString)
      }
      // azure blob
      case "wasb" | "wasbs" => {
        val azureAccountName: Option[String] = argsMap.get("etl.config.fs.azure.account.name").orElse(envOrNone("ETL_CONF_AZURE_ACCOUNT_NAME"))
        val azureAccountKey: Option[String] = argsMap.get("etl.config.fs.azure.account.key").orElse(envOrNone("ETL_CONF_AZURE_ACCOUNT_KEY"))

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
        val adlClientID: Option[String] = argsMap.get("etl.config.fs.adl.oauth2.client.id").orElse(envOrNone("ETL_CONF_ADL_OAUTH2_CLIENT_ID"))
        val adlRefreshToken: Option[String] = argsMap.get("etl.config.fs.adl.oauth2.refresh.token").orElse(envOrNone("ETL_CONF_ADL_OAUTH2_REFRESH_TOKEN"))

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
        val dfAccountName: Option[String] = argsMap.get("etl.config.fs.dfs.account.name").orElse(envOrNone("ETL_CONF_DFS_ACCOUNT_NAME"))
        val dfAccessKey: Option[String] = argsMap.get("etl.config.fs.dfs.access.key").orElse(envOrNone("ETL_CONF_DFS_ACCESS_KEY"))

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
        val gsProjectID: Option[String] = argsMap.get("etl.config.fs.gs.project.id").orElse(envOrNone("ETL_CONF_GOOGLE_CLOUD_PROJECT_ID"))
        val gsKeyfilePath: Option[String] = argsMap.get("etl.config.fs.google.cloud.auth.service.account.json.keyfile").orElse(envOrNone("ETL_CONF_GOOGLE_CLOUD_AUTH_SERVICE_ACCOUNT_JSON_KEYFILE"))

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

  def parseConfig(uri: Either[String, URI], argsMap: collection.mutable.Map[String, String], graph: Graph, arcContext: ARCContext)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Either[List[Error], (ETLPipeline, Graph)] = {
    val base = ConfigFactory.load()

    val etlConfString = uri match {
      case Left(str) => Right(str)
      case Right(uri) => getConfigString(uri, argsMap, arcContext)
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
      val argsMapJson = new ObjectMapper().writeValueAsString(argsMap.asJava).replace("\\", "")
      val argsMapConf = ConfigFactory.parseString(argsMapJson, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))

      // try to read objects in the plugins.config path
      val resolvedConfigPlugins = resolveConfigPlugins(etlConf, base)
      
      val pluginConfs: List[Config] = resolvedConfigPlugins.map( c => ConfigFactory.parseMap(c) )

      val c = pluginConfs match {
        case Nil =>
          etlConf.resolveWith(argsMapConf.withFallback(etlConf).withFallback(base)).resolve()
        case _ =>
          val pluginConf = pluginConfs.reduceRight[Config]{ case (c1, c2) => c1.withFallback(c2) }
          val pluginValues = pluginConf.root().unwrapped()
          logger.debug()
            .message("Found additional config values from plugins")
            .field("pluginConf", pluginValues)
            .log()           
          etlConf.resolveWith(argsMapConf.withFallback(pluginConf).withFallback(etlConf).withFallback(base)).resolve()
      }

      readPipeline(c, etlConfStringHash, uriString, argsMap, graph, arcContext)
    }
  }

  def resolveConfigPlugins(c: Config, base: Config)(implicit logger: au.com.agl.arc.util.log.logger.Logger): List[JMap[String, Object]] = {
    if (c.hasPath("plugins.config")) {
      val plugins =
        (for (p <- c.getObjectList("plugins.config").asScala) yield {
          val plugin = p.toConfig.withFallback(base).resolve()
          
          if (plugin.hasPath("type")) {
            val name = plugin.getString("type")
            val params = readMap("params", plugin)
            DynamicConfigurationPlugin.resolveAndExecutePlugin(name, params).map(_ :: Nil)
          } else {
            None
          }
        }).toList
      plugins.flatMap( p => p.getOrElse(Nil) )
    } else {
      Nil
    }
  }

  def readMap(path: String, c: Config): Map[String, String] = {
    if (c.hasPath(path)) {
      val params = c.getConfig(path).entrySet
      (for (e <- params.asScala) yield {
        val k = e.getKey
        val v = e.getValue
        
        // append string value to map
        k -> v.unwrapped.toString
      }).toMap
    } else {
      Map.empty
    }
  }

  def readAuthentication(path: String)(implicit c: Config): Either[Errors, Option[Authentication]] = {
  
    def err(lineNumber: Option[Int], msg: String): Either[Errors, Option[Authentication]] = Left(ConfigError(path, lineNumber, msg) :: Nil)

    try {
      if (c.hasPath(path)) {
        val authentication = readMap("authentication", c)
        if (authentication.isEmpty) {
          Right(None)
        } else {
          authentication.get("method") match {
            case Some("AzureSharedKey") => {
              val accountName = authentication.get("accountName") match {
                case Some(v) => v
                case None => throw new Exception(s"Authentication method 'AzureSharedKey' requires 'accountName' parameter.")
              } 
              if (accountName.contains("fs.azure")) {
                throw new Exception(s"Authentication method 'AzureSharedKey' 'accountName' should be just the account name not 'fs.azure.account.key...''.")
              }
              val signature = authentication.get("signature") match {
                case Some(v) => v
                case None => throw new Exception(s"Authentication method 'AzureSharedKey' requires 'signature' parameter.")
              }            
              Right(Some(Authentication.AzureSharedKey(accountName, signature)))
            }
            case Some("AzureSharedAccessSignature") => {
              val accountName = authentication.get("accountName") match {
                case Some(v) => v
                case None => throw new Exception(s"Authentication method 'AzureSharedAccessSignature' requires 'accountName' parameter.")
              } 
              if (accountName.contains("fs.azure")) {
                throw new Exception(s"Authentication method 'AzureSharedAccessSignature' 'accountName' should be just the account name not 'fs.azure.account.key...''.")
              }              
              val container = authentication.get("container") match {
                case Some(v) => v
                case None => throw new Exception(s"Authentication method 'AzureSharedAccessSignature' requires 'container' parameter.")
              } 
              if (accountName.contains("fs.azure")) {
                throw new Exception(s"Authentication method 'AzureSharedAccessSignature' 'container' should be just the container name not 'fs.azure.account.key...''.")
              }                
              val token = authentication.get("token") match {
                case Some(v) => v
                case None => throw new Exception(s"Authentication method 'AzureSharedAccessSignature' requires 'container' parameter.")
              }                 
              Right(Some(Authentication.AzureSharedAccessSignature(accountName, container, token)))
            } 
            case Some("AzureDataLakeStorageToken") => {
              val clientID = authentication.get("clientID") match {
                case Some(v) => v
                case None => throw new Exception(s"Authentication method 'AzureDataLakeStorageToken' requires 'clientID' parameter.")
              }
              val refreshToken = authentication.get("refreshToken") match {
                case Some(v) => v
                case None => throw new Exception(s"Authentication method 'AzureDataLakeStorageToken' requires 'refreshToken' parameter.")
              }
              Right(Some(Authentication.AzureDataLakeStorageToken(clientID, refreshToken)))
            }          
            case Some("AzureDataLakeStorageGen2AccountKey") => {
              val accountName = authentication.get("accountName") match {
                case Some(v) => v
                case None => throw new Exception(s"Authentication method 'AzureDataLakeStorageGen2AccountKey' requires 'accountName' parameter.")
              }
              val accessKey = authentication.get("accessKey") match {
                case Some(v) => v
                case None => throw new Exception(s"Authentication method 'AzureDataLakeStorageGen2AccountKey' requires 'accessKey' parameter.")
              }
              Right(Some(Authentication.AzureDataLakeStorageGen2AccountKey(accountName, accessKey)))
            }   
            case Some("AzureDataLakeStorageGen2OAuth") => {
              val clientID = authentication.get("clientID") match {
                case Some(v) => v
                case None => throw new Exception(s"Authentication method 'AzureDataLakeStorageGen2OAuth' requires 'clientID' parameter.")
              }
              val secret = authentication.get("secret") match {
                case Some(v) => v
                case None => throw new Exception(s"Authentication method 'AzureDataLakeStorageGen2OAuth' requires 'secret' parameter.")
              }
              val directoryID = authentication.get("directoryID") match {
                case Some(v) => v
                case None => throw new Exception(s"Authentication method 'AzureDataLakeStorageGen2OAuth' requires 'directoryID' parameter.")
              }              
              Right(Some(Authentication.AzureDataLakeStorageGen2OAuth(clientID, secret, directoryID)))
            }                            
            case Some("AmazonAccessKey") => {
              val accessKeyID = authentication.get("accessKeyID") match {
                case Some(v) => v
                case None => throw new Exception(s"Authentication method 'AmazonAccessKey' requires 'accessKeyID' parameter.")
              } 
              val secretAccessKey = authentication.get("secretAccessKey") match {
                case Some(v) => v
                case None => throw new Exception(s"Authentication method 'AmazonAccessKey' requires 'secretAccessKey' parameter.")
              }       
              Right(Some(Authentication.AmazonAccessKey(accessKeyID, secretAccessKey)))
            }
            case Some("GoogleCloudStorageKeyFile") => {
              val projectID = authentication.get("projectID") match {
                case Some(v) => v
                case None => throw new Exception(s"Authentication method 'GoogleCloudStorageKeyFile' requires 'projectID' parameter.")
              } 
              val keyFilePath = authentication.get("keyFilePath") match {
                case Some(v) => v
                case None => throw new Exception(s"Authentication method 'GoogleCloudStorageKeyFile' requires 'keyFilePath' parameter.")
              }       
              Right(Some(Authentication.GoogleCloudStorageKeyFile(projectID, keyFilePath)))
            }                                 
            case _ =>  throw new Exception(s"""Unable to parse authentication method: '${authentication.get("method").getOrElse("")}'""")
          }
        }
      } else {
        Right(None)
      }
    } catch {
      case e: Exception => err(Some(c.getValue(path).origin.lineNumber()), s"Unable to read config value: ${e.getMessage}")
    }
  }

  def parseSaveMode(path: String)(delim: String)(implicit c: Config): Either[Errors, SaveMode] = {
    delim.toLowerCase.trim match {
      case "append" => Right(SaveMode.Append)
      case "errorifexists" => Right(SaveMode.ErrorIfExists)
      case "ignore" => Right(SaveMode.Ignore)
      case "overwrite" => Right(SaveMode.Overwrite)
      case _ => Left(ConfigError(path, None, s"invalid state please raise issue.") :: Nil)
    }
  }  

  def parseOutputModeType(path: String)(delim: String)(implicit c: Config): Either[Errors, OutputModeType] = {
    delim.toLowerCase.trim match {
      case "append" => Right(OutputModeTypeAppend)
      case "complete" => Right(OutputModeTypeComplete)
      case "update" => Right(OutputModeTypeUpdate)
      case _ => Left(ConfigError(path, None, s"invalid state please raise issue.") :: Nil)
    }
  }    

  def parseFailMode(path: String)(delim: String)(implicit c: Config): Either[Errors, FailModeType] = {
    delim.toLowerCase.trim match {
      case "permissive" => Right(FailModeTypePermissive)
      case "failfast" => Right(FailModeTypeFailFast)
      case _ => Left(ConfigError(path, None, s"invalid state please raise issue.") :: Nil)
    }
  }  

  def parseDataType(path: String)(datatype: String)(implicit c: Config): Either[Errors, DataType] = {
    datatype.toLowerCase.trim match {
      case "string" => Right(StringType)
      case "binary" => Right(BinaryType)
      case _ => Left(ConfigError(path, None, s"invalid state please raise issue.") :: Nil)
    }
  }     

  def parseResponseType(path: String)(delim: String)(implicit c: Config): Either[Errors, ResponseType] = {
    delim.toLowerCase.trim match {
      case "integer" => Right(IntegerResponse)
      case "double" => Right(DoubleResponse)
      case "object" => Right(StringResponse)
      case _ => Left(ConfigError(path, None, s"invalid state please raise issue.") :: Nil)
    }
  }

  def parseDelimiter(path: String)(delim: String)(implicit c: Config): Either[Errors, Delimiter] = {
    delim.toLowerCase.trim match {
      case "comma" => Right(Delimiter.Comma)
      case "defaulthive" => Right(Delimiter.DefaultHive)
      case "pipe" => Right(Delimiter.Pipe)
      case "custom" => Right(Delimiter.Custom)
      case _ => Left(ConfigError(path, None, s"invalid state please raise issue.") :: Nil)
    }
  }

  def parseEncoding(path: String)(encoding: String)(implicit c: Config): Either[Errors, EncodingType] = {
    encoding.toLowerCase.trim match {
      case "base64" => Right(EncodingTypeBase64)
      case "hexadecimal" => Right(EncodingTypeHexadecimal)
      case _ => Left(ConfigError(path, None, s"invalid state please raise issue.") :: Nil)
    }
  }  

  def parseQuote(path: String)(quote: String)(implicit c: Config): Either[Errors, QuoteCharacter] = {
    quote.toLowerCase.trim match {
      case "doublequote" => Right(QuoteCharacter.DoubleQuote)
      case "singlequote" => Right(QuoteCharacter.SingleQuote)
      case "none" => Right(QuoteCharacter.Disabled)
      case _ => Left(ConfigError(path, None, s"invalid state please raise issue.") :: Nil)
    }
  }  

  def parseIsolationLevel(path: String)(quote: String)(implicit c: Config): Either[Errors, IsolationLevelType] = {
    quote.toLowerCase.trim match {
      case "none" => Right(IsolationLevelNone)
      case "read_committed" => Right(IsolationLevelReadCommitted)
      case "read_uncommitted" => Right(IsolationLevelReadUncommitted)
      case "repeatable_read" => Right(IsolationLevelRepeatableRead)
      case "serializable" => Right(IsolationLevelSerializable)
      case _ => Left(ConfigError(path, None, s"invalid state please raise issue.") :: Nil)
    }
  }  

  def validateAzureSharedKey(path: String)(authentication: Option[Authentication])(implicit c: Config): Either[Errors, Option[Authentication]] = {
    authentication match {
      case Some(auth) => {
        auth match {
          case _: Authentication.AzureSharedKey => Right(Some(auth))
          case _ => Left(ConfigError(path, Some(c.getValue(path).origin.lineNumber), s"Authentication method must be 'AzureSharedKey'.") :: Nil)
        }
      }
      case None => Right(None)
    }
  }    

  def validateURI(path: String)(uri: String)(implicit c: Config): Either[Errors, URI] = {
    try {
      Right(new URI(uri))
    } catch {
      case e: Exception => Left(ConfigError(path, Some(c.getValue(path).origin.lineNumber), e.getMessage) :: Nil)
    }
  }    

  sealed trait Error

  object Error {

    def errToString(err: Error): String = {
      err match {
        case StageError(idx, stage, lineNumber, configErrors) => {
          s"""Stage: $idx '${stage}' (starting on line ${lineNumber}):\n${configErrors.map(e => "  - " + errToString(e)).mkString("\n")}"""
        }
          
        case ConfigError(attribute, lineNumber, message) => {
          lineNumber match {
            case Some(ln) => s"""${attribute} (Line ${ln}): $message"""
            case None => s"""${attribute}: $message"""
          }
        }
      }
    }

    def errToSimpleString(err: Error): String = {
      err match {
        case StageError(_, stage, lineNumber, configErrors) => {
          s"""${configErrors.map(e => "- " + errToSimpleString(e)).mkString("\n")}"""
        }
          
        case ConfigError(attribute, lineNumber, message) => {
          lineNumber match {
            case Some(ln) => s"""${attribute} (Line ${ln}): $message"""
            case None => s"""${attribute}: $message"""
          }
        }
      }
    }


    def errorsToJSON(err: Error): java.util.HashMap[String, Object] = {
      err match {
        case StageError(idx, stage, lineNumber, configErrors) => {  
          val stageErrorMap = new java.util.HashMap[String, Object]()
          stageErrorMap.put("stageIndex", Integer.valueOf(idx))
          stageErrorMap.put("stage", stage)
          stageErrorMap.put("lineNumber", Integer.valueOf(lineNumber))
          stageErrorMap.put("errors", configErrors.map(configError => errorsToJSON(configError)).asJava)
          stageErrorMap
        }
        case ConfigError(attribute, lineNumber, message) => {
          val configErrorMap = new java.util.HashMap[String, Object]()
          lineNumber match {
            case Some(ln) => {
              configErrorMap.put("attribute", attribute)
              configErrorMap.put("lineNumber", Integer.valueOf(ln))
              configErrorMap.put("message", message)
            }
            case None => {
              configErrorMap.put("attribute", attribute)
              configErrorMap.put("message", message)          
            }
          } 
          configErrorMap      
        }
      }
    }

    def pipelineErrorMsg(errors: List[Error]): String = {
      errors.map(e => s"${ConfigUtils.Error.errToString(e)}").mkString("\n")
    }

    def pipelineSimpleErrorMsg(errors: List[Error]): String = {
      errors.map(e => s"${ConfigUtils.Error.errToSimpleString(e)}").mkString("\n")
    }    

    def pipelineErrorJSON(errors: List[Error]): java.util.List[java.util.HashMap[String, Object]] = {
      errors.map(e => errorsToJSON(e)).asJava
    }    

  }

  type Errors =  List[ConfigError]

  case class ConfigError(path: String, lineNumber: Option[Int], message: String) extends Error

  case class StageError(idx: Int, stage: String, lineNumber: Int, errors: Errors) extends Error

  object ConfigError {

    def err(path: String, lineNumber: Option[Int], message: String): Errors = ConfigError(path, lineNumber, message) :: Nil

  }

  trait ConfigReader[A] {

    def getValue(path: String, c: Config, default: Option[A] = None, validValues: Seq[A] = Seq.empty): Either[Errors, A] =
      ConfigReader.getConfigValue[A](path, c, expectedType, default, validValues){ read(path, c) }

    def getOptionalValue(path: String, c: Config, default: Option[A], validValues: Seq[A] = Seq.empty): Either[Errors, Option[A]] =
      ConfigReader.getOptionalConfigValue(path, c, expectedType, default, validValues){ read(path, c) }

    def expectedType: String

    def read(path: String, c: Config): A

  }
  
  object ConfigReader {

    def getConfigValue[A](path: String, c: Config, expectedType: String,
                          default: Option[A] = None, validValues: Seq[A] = Seq.empty)(read: => A): Either[Errors, A] = {
    
      def err(lineNumber: Option[Int], msg: String): Either[Errors, A] = Left(ConfigError(path, lineNumber, msg) :: Nil)

      try {
        if (c.hasPath(path)) {
          val value = read
          if (!validValues.isEmpty) {
            if (validValues.contains(value)) {
              Right(value)
            } else {
              err(Some(c.getValue(path).origin.lineNumber()), s"""Invalid value. Valid values are ${validValues.map(value => s"'${value.toString}'").mkString("[",",","]")}.""")
            }
          } else {
            Right(read)
          }
        } else {
          default match {
            case Some(value) => {
              if (!validValues.isEmpty) {
                if (validValues.contains(value)) {
                  Right(value)
                } else {
                  err(None, s"""Invalid default value '$value'. Valid values are ${validValues.map(value => s"'${value.toString}'").mkString("[",",","]")}.""")
                }
              } else {
                Right(value)
              }
            }
            case None => err(None, s"""Missing required attribute '$path'.""")
          }
        }
      } catch {
        case wt: ConfigException.WrongType => err(Some(c.getValue(path).origin.lineNumber()), s"Wrong type, expected: '$expectedType'.")
        case e: Exception => err(Some(c.getValue(path).origin.lineNumber()), s"Unable to read value: ${e.getMessage}")
      }

    }

    def getOptionalConfigValue[A](path: String, c: Config, expectedType: String,
                                     default: Option[A] = None, validValues: Seq[A] = Seq.empty)(read: => A): Either[Errors, Option[A]] = {
      if (c.hasPath(path)) {
        val value = getConfigValue(path, c, expectedType, None, validValues)(read)
        value match {
          case Right(cv) => Right(Option(cv))
          case Left(l) => Left(l) // matching works around typing error
        }
      } else {
        Right(default)
      }
    }

    implicit object StringConfigReader extends ConfigReader[String] {

      val expectedType = "string"

      def read(path: String, c: Config): String = c.getString(path)

    }

    implicit object StringListConfigReader extends ConfigReader[StringList] {

      val expectedType = "string array"

      def read(path: String, c: Config): StringList = c.getStringList(path).asScala.toList

    }

    implicit object IntListConfigReader extends ConfigReader[IntList] {

      val expectedType = "integer array"

      def read(path: String, c: Config): IntList = c.getIntList(path).asScala.map(f => f.toInt).toList

    }    

    implicit object BooleanConfigReader extends ConfigReader[Boolean] {

      val expectedType = "boolean"

      def read(path: String, c: Config): Boolean = c.getBoolean(path)

    }

    implicit object IntConfigReader extends ConfigReader[Int] {

      val expectedType = "int"

      def read(path: String, c: Config): Int = c.getInt(path)

    }

    implicit object LongConfigReader extends ConfigReader[Long] {

      val expectedType = "long"

      def read(path: String, c: Config): Long = c.getLong(path)

    }

    def getValue[A](path: String, default: Option[A] = None, validValues: Seq[A] = Seq.empty)(implicit c: Config, reader: ConfigReader[A]): Either[Errors, A] = {
      reader.getValue(path, c, default, validValues)
    }

    def getOptionalValue[A](path: String, default: Option[A] = None, validValues: Seq[A] = Seq.empty)(implicit c: Config, reader: ConfigReader[A]): Either[Errors, Option[A]] = {
      reader.getOptionalValue(path, c, default, validValues)
    }

  }

  type StringConfigValue = Either[Errors, String]

  type StringList = List[String]

  type IntList = List[Int]

  def stringOrDefault(sv: StringConfigValue, default: String): String = {
    sv match {
      case Right(v) => v
      case Left(err) => default
    }
  }

  case class Vertex(stageId: Int, name: String)

  case class Edge(source: Vertex, target: Vertex)

  case class Graph(vertices: List[Vertex], edges: List[Edge], containsPipelineStagePlugin: Boolean) {

    def addVertex(vertex: Vertex): Graph = {
      // a node is a distinct combination of (stageId,name)
      if (!vertices.exists { v => v.stageId == vertex.stageId && v.name == vertex.name }) {
        Graph(vertex :: vertices, edges, containsPipelineStagePlugin)
      } else {
        this
      }
    }

    def addEdge(source: String, target: String): Graph = {
      // because of createOrReplaceTempView the same 'name' can be used in multiple stages with a higher stageId
      // this logic picks the highest stage to create the edge
      if (vertices.exists { v => v.name == source } && vertices.exists { v => v.name == target }) {
        val sourceStageId = vertices.filter { v => v.name == source }.map(_.stageId).reduce(_ max _)
        val targetStageId = vertices.filter { v => v.name == target }.map(_.stageId).reduce(_ max _)

        Graph(vertices, Edge(Vertex(sourceStageId, source), Vertex(targetStageId, target)) :: edges, containsPipelineStagePlugin)
      } else {
        this
      }
    }

    def vertexExists(path: String)(vertexName: String)(implicit spark: SparkSession, c: Config): Either[Errors, String] = {
      // if at least one PipelineStagePlugin exists in the job then disable vertexExists check due to opaque nature of code in plugins
      if (containsPipelineStagePlugin) {
        Right(vertexName)
      } else {
        // either the vertex was added by previous stage or has been already been registered in a hive metastore
        if (vertices.exists {v => v.name == vertexName } || spark.catalog.tableExists(vertexName)) {
          Right(vertexName)
        } else {
          val possibleKeys = levenshteinDistance(vertices.map(_.name), vertexName)(4)

          if (!possibleKeys.isEmpty) {
            Left(ConfigError(path, Option(c.getValue(path).origin.lineNumber()), s"""view '${vertexName}' does not exist by this stage of the job. Perhaps you meant one of: ${possibleKeys.map(field => s"'${field}'").mkString("[",", ","]")}.""") :: Nil)
          } else {
            Left(ConfigError(path, Option(c.getValue(path).origin.lineNumber()), s"""view '${vertexName}' does not exist by this stage of the job.""") :: Nil)
          }      
        }
      }
    }

  }

  def levenshteinDistance(keys: Seq[String], input: String)(limit: Int): Seq[String] = {
    val inputUTF8 = UTF8String.fromString(input)
    for {
      k <- keys
      v = inputUTF8.levenshteinDistance(UTF8String.fromString(k)) if v < limit
    } yield k
  }   

  def checkValidKeys(c: Config)(expectedKeys: => Seq[String]): Either[Errors, String] = {

    val diffKeys = c.root().keySet.asScala.toSeq.diff(expectedKeys).toList

    if (diffKeys.isEmpty) {
      Right("")
    } else {
      Left(diffKeys.map(key => {
        val possibleKeys = levenshteinDistance(expectedKeys, key)(4)
        if (!possibleKeys.isEmpty) {
          // if the value has been substituted line number is -1. can use description to detect it.
          // description looks like: 'merge of String: 11,env var ETL_JOB_ROOT'
          if (c.getValue(key).origin.description.contains("merge of")) {
            ConfigError(key, None, s"""Invalid attribute '${key}'. Perhaps you meant one of: ${possibleKeys.map(field => s"'${field}'").mkString("[",", ","]")}.""")
          } else {
            ConfigError(key, Some(c.getValue(key).origin.lineNumber()), s"""Invalid attribute '${key}'. Perhaps you meant one of: ${possibleKeys.map(field => s"'${field}'").mkString("[",", ","]")}.""")
          }
        } else {
          ConfigError(key, Some(c.getValue(key).origin.lineNumber()), s"""Invalid attribute '${key}'.""")
        }
      }))
    }
  }

  // this method will get the keys from a case class via reflection so could be used for key comparison
  // it does not work well if there is mapping between the hocon config and the case class fields (like the delimited object)
  def checkValidKeysReflection[T: TypeTag](c: Config): Seq[String] = {
    val expectedKeys = classAccessors[T]
    c.root().keySet.asScala.toSeq.diff(expectedKeys)
  }

  private def parseURI(path: String, uri: String)(implicit c: Config): Either[Errors, URI] = {
    def err(lineNumber: Option[Int], msg: String): Either[Errors, URI] = Left(ConfigError(path, lineNumber, msg) :: Nil)

    try {
      // try to parse uri
      Right(new URI(uri))
    } catch {
      case e: Exception => err(Some(c.getValue(path).origin.lineNumber()), e.getMessage)
    }
  }

  private def parseAvroSchema(path: String, schemaString: String)(implicit c: Config): Either[Errors, Option[org.apache.avro.Schema]] = {
    def err(lineNumber: Option[Int], msg: String): Either[Errors, Option[org.apache.avro.Schema]] = Left(ConfigError(path, lineNumber, msg) :: Nil)
    try {
      // if schema contains backslash it might have come from the kafka schema registry therefore try to get the data out of the returned object
      // this is hacky but needs to behave well with a registry response
      if (schemaString.contains(""""schema":""") && schemaString.contains("\\")) {
        val objectMapper = new ObjectMapper()
        val metaTree = objectMapper.readTree(schemaString)
        Right(Option(new org.apache.avro.Schema.Parser().parse(metaTree.get("schema").asText)))
      } else {
        // try to parse schema
        Right(Option(new org.apache.avro.Schema.Parser().parse(schemaString)))
      }
    } catch {
      case e: Exception => err(Some(c.getValue(path).origin.lineNumber()), e.getMessage)
    }
  }

  private def parseGlob(path: String, glob: String)(implicit c: Config): Either[Errors, String] = {
    def err(lineNumber: Option[Int], msg: String): Either[Errors, String] = Left(ConfigError(path, lineNumber, msg) :: Nil)

    try {
      // try to compile glob which will fail with bad characters
      GlobPattern.compile(glob)
      Right(glob)
    } catch {
      case e: Exception => err(Some(c.getValue(path).origin.lineNumber()), e.getMessage)
    }
  }

  private def textContentForURI(uri: URI, uriKey: String, authentication: Either[Errors, Option[Authentication]])
                               (implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): Either[Errors, String] = {
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

  private def getBlob(path: String, uri: URI)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): Either[Errors, String] = {
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

  private def getExtractColumns(parsedURI: Either[Errors, Option[URI]], uriKey: String, authentication: Either[Errors, Option[Authentication]])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): Either[Errors, List[ExtractColumn]] = {
    val schema: Either[Errors, Option[String]] = parsedURI.rightFlatMap {
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

  private def validateAzureCosmosDBConfig(path: String, map:  Map[String, String])(implicit spark: SparkSession, c: Config): Either[Errors, com.microsoft.azure.cosmosdb.spark.config.Config] = {
    def err(lineNumber: Option[Int], msg: String): Either[Errors, com.microsoft.azure.cosmosdb.spark.config.Config] = Left(ConfigError(path, lineNumber, msg) :: Nil)

    try {
      Right(com.microsoft.azure.cosmosdb.spark.config.Config(map))
    } catch {
      case e: Exception => err(Some(c.getValue(path).origin.lineNumber()), e.getMessage)
    }
  }    

  // extract
  def readAvroExtract(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "inputView" :: "outputView" :: "authentication" :: "contiguousIndex" :: "numPartitions" :: "partitionBy" :: "persist" :: "schemaURI" :: "schemaView" :: "params" :: "basePath" :: "avroSchemaURI" :: "inputField" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    val description = getOptionalValue[String]("description")

    val input = if(c.hasPath("inputView")) getValue[String]("inputView") else getValue[String]("inputURI")
    val parsedGlob = if (!c.hasPath("inputView")) {
      input.rightFlatMap(glob => parseGlob("inputURI", glob))
    } else {
      Right("")
    }    

    val outputView = getValue[String]("outputView")
    val persist = getValue[Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val authentication = readAuthentication("authentication")
    val contiguousIndex = getValue[Boolean]("contiguousIndex", default = Some(true))

    val uriKey = "schemaURI"
    val stringURI = getOptionalValue[String](uriKey)
    val parsedURI: Either[Errors, Option[URI]] = stringURI.rightFlatMap(optURI => 
      optURI match { 
        case Some(uri) => parseURI(uriKey, uri).rightFlatMap(parsedURI => Right(Option(parsedURI)))
        case None => Right(None)
      }
    )
    val extractColumns = if(!c.hasPath("schemaView")) getExtractColumns(parsedURI, uriKey, authentication) else Right(List.empty)
    val schemaView = if(c.hasPath("schemaView")) getValue[String]("schemaView") else Right("")
    val basePath = getOptionalValue[String]("basePath")
    val inputField = getOptionalValue[String]("inputField")

    val avroSchemaURI = getOptionalValue[String]("avroSchemaURI")
    val avroSchema: Either[Errors, Option[org.apache.avro.Schema]] = avroSchemaURI.rightFlatMap(optAvroSchemaURI => 
      optAvroSchemaURI match { 
        case Some(uri) => {
          parseURI("avroSchemaURI", uri)
          .rightFlatMap(uri => textContentForURI(uri, "avroSchemaURI", Right(None) ))
          .rightFlatMap(schemaString => parseAvroSchema("avroSchemaURI", schemaString))
        }
        case None => Right(None)
      }
    )    

    (name, description, extractColumns, schemaView, input, parsedGlob, outputView, persist, numPartitions, partitionBy, authentication, contiguousIndex, invalidKeys, basePath, inputField, avroSchemaURI, avroSchema) match {
      case (Right(n), Right(d), Right(cols), Right(sv), Right(in), Right(pg), Right(ov), Right(p), Right(np), Right(pb), Right(auth), Right(ci), Right(_), Right(bp), Right(ipf), Right(_), Right(avsc)) =>
        val input = if(c.hasPath("inputView")) Left(in) else Right(pg)
        val schema = if(c.hasPath("schemaView")) Left(sv) else Right(cols)

        var outputGraph = graph.addVertex(Vertex(idx, ov))

        (Right(AvroExtract(n, d, schema, ov, input, auth, params, p, np, pb, ci, bp, avsc, ipf)), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, input, schemaView, parsedGlob, outputView, persist, numPartitions, partitionBy, authentication, contiguousIndex, extractColumns, invalidKeys, basePath, inputField, avroSchemaURI, avroSchema).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }  

  def readAzureCosmosDBExtract(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "authentication" :: "outputView" :: "numPartitions" :: "partitionBy" :: "persist" :: "params" :: "config" :: "schemaView" :: "schemaURI" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    val description = getOptionalValue[String]("description")

    val config = readMap("config", c)
    val configValid = validateAzureCosmosDBConfig("config", config)
    val authentication = readAuthentication("authentication")

    val uriKey = "schemaURI"
    val stringURI = getOptionalValue[String](uriKey)
    val parsedURI: Either[Errors, Option[URI]] = stringURI.rightFlatMap(optURI => 
      optURI match { 
        case Some(uri) => parseURI(uriKey, uri).rightFlatMap(parsedURI => Right(Option(parsedURI)))
        case None => Right(None)
      }
    )
    val extractColumns = if(!c.hasPath("schemaView")) getExtractColumns(parsedURI, uriKey, authentication) else Right(List.empty)
    val schemaView = if(c.hasPath("schemaView")) getValue[String]("schemaView") else Right("")

    val outputView = getValue[String]("outputView")
    val persist = getValue[Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))

    (name, description, extractColumns, schemaView, outputView, persist, numPartitions, partitionBy, authentication, invalidKeys, configValid) match {
      case (Right(n), Right(d), Right(cols), Right(sv), Right(ov), Right(p), Right(np), Right(pb), Right(auth), Right(_), Right(_)) =>
      val schema = if(c.hasPath("schemaView")) Left(sv) else Right(cols)
      var outputGraph = graph.addVertex(Vertex(idx, ov))

      (Right(AzureCosmosDBExtract(n, d, schema, ov, params, p, np, pb, config)), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, extractColumns, schemaView, outputView, persist, numPartitions, partitionBy, authentication, invalidKeys, configValid).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }    

  def readBytesExtract(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "inputURI" :: "outputView" :: "authentication" :: "contiguousIndex" :: "numPartitions" :: "persist" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)    

    val description = getOptionalValue[String]("description")

    val inputURI = getOptionalValue[String]("inputURI")
    val parsedGlob: Either[Errors, Option[String]] = inputURI.rightFlatMap {
      case Some(glob) =>
        val parsedGlob: Either[Errors, String] = parseGlob("inputURI", glob)
        parsedGlob.rightFlatMap(g => Right(Option(g)))
      case None =>
        val noValue: Either[Errors, Option[String]] = Right(None)
        noValue
    }
    val outputView = getValue[String]("outputView")
    val persist = getValue[Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val authentication = readAuthentication("authentication")
    val contiguousIndex = getValue[Boolean]("contiguousIndex", default = Some(true))
    val inputView = getOptionalValue[String]("inputView")

    (name, description, parsedGlob, inputView, outputView, persist, numPartitions, authentication, contiguousIndex, invalidKeys) match {
      case (Right(n), Right(d), Right(pg), Right(iv), Right(ov), Right(p), Right(np), Right(auth), Right(ci), Right(_)) =>

        val validInput = (pg, iv) match {
          case (Some(_), None) => true
          case (None, Some(_)) => true
          case _ => false
        }

        if (validInput) {
          val input = if(c.hasPath("inputView")) Left(iv.get) else Right(pg.get)

          // add the vertices
          var outputGraph = graph.addVertex(Vertex(idx, ov))

          (Right(BytesExtract(n, d, ov, input, auth, params, p, np, ci)), outputGraph)
        } else {
          val inputError = ConfigError("inputURI:inputView", Some(c.getValue("inputURI").origin.lineNumber()), "Either inputURI and inputView must be defined but only one can be defined at the same time") :: Nil
          val stageName = stringOrDefault(name, "unnamed stage")
          val err = StageError(idx, stageName, c.origin.lineNumber, inputError)
          (Left(err :: Nil), graph)
        }
      case _ =>
        val allErrors: Errors = List(name, description, inputURI, inputView, outputView, persist, numPartitions, authentication, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }

  def readDatabricksDeltaExtract(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "outputView" :: "numPartitions" :: "partitionBy" :: "persist" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    val description = getOptionalValue[String]("description")

    val inputURI = getValue[String]("inputURI")
    val parsedGlob = inputURI.rightFlatMap(glob => parseGlob("inputURI", glob))
    val outputView = getValue[String]("outputView")
    val persist = getValue[Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))

    (name, description, inputURI, parsedGlob, outputView, persist, numPartitions, partitionBy, invalidKeys) match {
      case (Right(n), Right(d), Right(in), Right(pg), Right(ov), Right(p), Right(np), Right(pb), Right(_)) => 
        var outputGraph = graph.addVertex(Vertex(idx, ov))

        (Right(DatabricksDeltaExtract(n, d, ov, pg, params, p, np, pb)), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, inputURI, parsedGlob, outputView, persist, numPartitions, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }  

  def readDelimitedExtract(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._
    import au.com.agl.arc.extract.DelimitedExtract._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "inputURI" :: "outputView" :: "delimiter" :: "quote" :: "header" :: "authentication" :: "contiguousIndex" :: "numPartitions" :: "params" :: "partitionBy" :: "persist" :: "schemaURI" :: "schemaView" :: "customDelimiter" :: "inputField" :: "basePath" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    val description = getOptionalValue[String]("description")


    val inputView = if(c.hasPath("inputView")) getValue[String]("inputView") |> graph.vertexExists("inputView") _ else Right("")
    val inputURI = if (!c.hasPath("inputView")) {
      getValue[String]("inputURI").rightFlatMap(glob => parseGlob("inputURI", glob))
    } else {
      Right("")
    }

    val outputView = getValue[String]("outputView")
    val persist = getValue[Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val authentication = readAuthentication("authentication")
    val contiguousIndex = getValue[Boolean]("contiguousIndex", default = Some(true))

    val uriKey = "schemaURI"
    val stringURI = getOptionalValue[String](uriKey)
    val parsedURI: Either[Errors, Option[URI]] = stringURI.rightFlatMap(optURI =>
      optURI match {
        case Some(uri) => parseURI(uriKey, uri).rightFlatMap(parsedURI => Right(Option(parsedURI)))
        case None => Right(None)
      }
    )
    val extractColumns = if(!c.hasPath("schemaView")) getExtractColumns(parsedURI, uriKey, authentication) else Right(List.empty)
    val schemaView = if(c.hasPath("schemaView")) getValue[String]("schemaView") else Right("")

    val delimiter = getValue[String]("delimiter", default = Some("Comma"), validValues = "Comma" :: "Pipe" :: "DefaultHive" :: "Custom" :: Nil) |> parseDelimiter("delimiter") _
    val quote = getValue[String]("quote", default =  Some("DoubleQuote"), validValues = "DoubleQuote" :: "SingleQuote" :: "None" :: Nil) |> parseQuote("quote") _
    val header = getValue[Boolean]("header", Some(false))

    val customDelimiter = delimiter match {
      case Right(Delimiter.Custom) => {
        getValue[String]("customDelimiter")
      }
      case _ => Right("")
    }

    val inputField = getOptionalValue[String]("inputField")
    val basePath = getOptionalValue[String]("basePath")

    (name, description, inputView, inputURI, extractColumns, schemaView, outputView, persist, numPartitions, partitionBy, header, authentication, contiguousIndex, delimiter, quote, invalidKeys, customDelimiter, inputField, basePath) match {
      case (Right(n), Right(d), Right(iv), Right(uri), Right(cols), Right(sv), Right(ov), Right(p), Right(np), Right(pb), Right(head), Right(auth), Right(ci), Right(delim), Right(q), Right(_), Right(cd), Right(ipf), Right(bp)) =>
        var outputGraph = graph.addVertex(Vertex(idx, ov))

        val input = if(c.hasPath("inputView")) {
          outputGraph = outputGraph.addEdge(iv, ov)
          Left(iv) 
        } else {
          Right(uri)
        }
        val schema = if(c.hasPath("schemaView")) Left(sv) else Right(cols)
        val extract = DelimitedExtract(n, d, schema, ov, input, Delimited(header=head, sep=delim, quote=q, customDelimiter=cd), auth, params, p, np, pb, ci, ipf, bp)
        (Right(extract), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, inputURI, extractColumns, outputView, persist, numPartitions, partitionBy, header, authentication, contiguousIndex, delimiter, quote, invalidKeys, customDelimiter, inputField, basePath).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }  

  def readElasticsearchExtract(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments"  :: "input" :: "outputView"  :: "numPartitions" :: "partitionBy" :: "persist" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)    

    val description = getOptionalValue[String]("description")

    val input = getValue[String]("input")
    val outputView = getValue[String]("outputView")
    val persist = getValue[Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))

    (name, description, input, outputView, persist, numPartitions, partitionBy, invalidKeys) match {
      case (Right(n), Right(d), Right(in), Right(ov), Right(p), Right(np), Right(pb), Right(_)) => 
        var outputGraph = graph.addVertex(Vertex(idx, ov))
        (Right(ElasticsearchExtract(n, d, in, ov, params, p, np, pb)), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, input, outputView, persist, numPartitions, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }   

  def readHTTPExtract(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "inputURI" :: "outputView" :: "body" :: "headers" :: "method" :: "numPartitions" :: "partitionBy" :: "persist" :: "validStatusCodes" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    val description = getOptionalValue[String]("description")

    val input = if(c.hasPath("inputView")) getValue[String]("inputView") else getValue[String]("inputURI")
    val parsedURI = if (!c.hasPath("inputView")) {
      input.rightFlatMap(uri => parseURI("inputURI", uri))
    } else {
      Right(new URI(""))
    }

    val headers = readMap("headers", c)
    val validStatusCodes = getValue[IntList]("validStatusCodes", default = Some(200 :: 201 :: 202 :: Nil))

    val outputView = getValue[String]("outputView")
    val persist = getValue[Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val contiguousIndex = getValue[Boolean]("contiguousIndex", default = Some(true))

    val method = getValue[String]("method", default = Some("GET"), validValues = "GET" :: "POST" :: Nil)

    val body = getOptionalValue[String]("body")

    (name, description, input, parsedURI, outputView, persist, numPartitions, method, body, partitionBy, validStatusCodes, invalidKeys) match {
      case (Right(n), Right(d), Right(in), Right(pu), Right(ov), Right(p), Right(np), Right(m), Right(b), Right(pb), Right(vsc), Right(_)) => 
        val inp = if(c.hasPath("inputView")) Left(in) else Right(pu)
        var outputGraph = graph.addVertex(Vertex(idx, ov))

        (Right(HTTPExtract(n, d, inp, m, headers, b, vsc, ov, params, p, np, pb)), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, input, parsedURI, outputView, persist, numPartitions, method, body, partitionBy, validStatusCodes, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  } 

  def readImageExtract(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "outputView" :: "authentication" :: "dropInvalid" :: "numPartitions" :: "partitionBy" :: "persist" :: "params" :: "basePath" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)    

    val description = getOptionalValue[String]("description")

    val inputURI = getValue[String]("inputURI")
    val parsedGlob = inputURI.rightFlatMap(glob => parseGlob("inputURI", glob))
    val outputView = getValue[String]("outputView")
    val persist = getValue[Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = if (c.hasPath("partitionBy")) c.getStringList("partitionBy").asScala.toList else Nil
    val authentication = readAuthentication("authentication")
    val dropInvalid = getValue[Boolean]("dropInvalid", default = Some(true))
    val basePath = getOptionalValue[String]("basePath")

    (name, description, inputURI, parsedGlob, outputView, persist, numPartitions, authentication, dropInvalid, invalidKeys, basePath) match {
      case (Right(n), Right(d), Right(in), Right(pg), Right(ov), Right(p), Right(np), Right(auth), Right(di), Right(_), Right(bp)) => 
        var outputGraph = graph.addVertex(Vertex(idx, ov))
        (Right(ImageExtract(n, d, ov, pg, auth, params, p, np, partitionBy, di, bp)), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, inputURI, parsedGlob, outputView, persist, numPartitions, authentication, dropInvalid, invalidKeys, basePath).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }   

  def readJDBCExtract(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "jdbcURL" :: "tableName" :: "outputView" :: "authentication" :: "contiguousIndex" :: "fetchsize" :: "numPartitions" :: "params" :: "partitionBy" :: "partitionColumn" :: "persist" :: "predicates" :: "schemaURI" :: "schemaView" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    val description = getOptionalValue[String]("description")

    val outputView = getValue[String]("outputView")
    val persist = getValue[Boolean]("persist", default = Some(false))
    val jdbcURL = getValue[String]("jdbcURL")
    val driver = jdbcURL.rightFlatMap(uri => getJDBCDriver("jdbcURL", uri))
    val tableName = getValue[String]("tableName")
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val fetchsize = getOptionalValue[Int]("fetchsize")
    val customSchema = getOptionalValue[String]("customSchema")
    val partitionColumn = getOptionalValue[String]("partitionColumn")
    val predicates = if (c.hasPath("predicates")) c.getStringList("predicates").asScala.toList else Nil

    val authentication = readAuthentication("authentication")
    val uriKey = "schemaURI"
    val stringURI = getOptionalValue[String](uriKey)
    val parsedURI: Either[Errors, Option[URI]] = stringURI.rightFlatMap(optURI => 
      optURI match { 
        case Some(uri) => parseURI(uriKey, uri).rightFlatMap(parsedURI => Right(Option(parsedURI)))
        case None => Right(None)
      }
    )
    val extractColumns = if(!c.hasPath("schemaView")) getExtractColumns(parsedURI, uriKey, authentication) else Right(List.empty)
    val schemaView = if(c.hasPath("schemaView")) getValue[String]("schemaView") else Right("")

    (name, description, extractColumns, schemaView, outputView, persist, jdbcURL, driver, tableName, numPartitions, fetchsize, customSchema, partitionColumn, partitionBy, invalidKeys) match {
      case (Right(n), Right(desc), Right(cols), Right(sv), Right(ov), Right(p), Right(ju), Right(d), Right(tn), Right(np), Right(fs), Right(cs), Right(pc), Right(pb), Right(_)) => 
        val schema = if(c.hasPath("schemaView")) Left(sv) else Right(cols)
        var outputGraph = graph.addVertex(Vertex(idx, ov))
        (Right(JDBCExtract(n, desc, schema, ov, ju, tn, np, fs, cs, d, pc, params, p, pb, predicates)), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, outputView, schemaView, persist, jdbcURL, driver, tableName, numPartitions, fetchsize, customSchema, extractColumns, partitionColumn, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }   

  def readJSONExtract(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "inputURI" :: "outputView" :: "authentication" :: "contiguousIndex" :: "multiLine" :: "numPartitions" :: "partitionBy" :: "persist" :: "schemaURI" :: "schemaView" :: "params" :: "inputField" :: "basePath" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)    

    val description = getOptionalValue[String]("description")

    val input = if(c.hasPath("inputView")) getValue[String]("inputView") else getValue[String]("inputURI")
    val parsedGlob = if (!c.hasPath("inputView")) {
      input.rightFlatMap(glob => parseGlob("inputURI", glob))
    } else {
      Right("")
    }

    val outputView = getValue[String]("outputView")
    val persist = getValue[Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val multiLine = getOptionalValue[Boolean]("multiLine")
    val authentication = readAuthentication("authentication")
    val contiguousIndex = getValue[Boolean]("contiguousIndex", default = Some(true))

    val uriKey = "schemaURI"
    val stringURI = getOptionalValue[String]("schemaURI")
    val parsedURI: Either[Errors, Option[URI]] = stringURI.rightFlatMap(optURI => 
      optURI match { 
        case Some(uri) => parseURI(uriKey, uri).rightFlatMap(parsedURI => Right(Option(parsedURI)))
        case None => Right(None)
      }
    )

    val extractColumns = if(!c.hasPath("schemaView")) getExtractColumns(parsedURI, uriKey, authentication) else Right(List.empty)
    val schemaView = if(c.hasPath("schemaView")) getValue[String]("schemaView") else Right("")

    val inputField = getOptionalValue[String]("inputField")
    val basePath = getOptionalValue[String]("basePath")

    (name, description, extractColumns, schemaView, input, parsedGlob, outputView, persist, numPartitions, multiLine, authentication, contiguousIndex, partitionBy, invalidKeys, inputField, basePath) match {
      case (Right(n), Right(d), Right(cols), Right(sv), Right(in), Right(pg), Right(ov), Right(p), Right(np), Right(ml), Right(auth), Right(ci), Right(pb), Right(_), Right(ipf), Right(bp)) => 
        val input = if(c.hasPath("inputView")) Left(in) else Right(pg)
        val schema = if(c.hasPath("schemaView")) Left(sv) else Right(cols)

        val json = JSON()
        val multiLine = ml match {
          case Some(b: Boolean) => b
          case _ => json.multiLine
        }
        var outputGraph = graph.addVertex(Vertex(idx, ov))
        (Right(JSONExtract(n, d, schema, ov, input, JSON(multiLine=multiLine), auth, params, p, np, pb, ci, ipf, bp)), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, input, schemaView, parsedGlob, outputView, persist, numPartitions, multiLine, authentication, contiguousIndex, extractColumns, partitionBy, invalidKeys, inputField, basePath).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }

  def readKafkaExtract(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "outputView" :: "bootstrapServers" :: "topic" :: "groupID" :: "autoCommit" :: "maxPollRecords" :: "numPartitions" :: "partitionBy" :: "persist" :: "timeout" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)    

    val description = getOptionalValue[String]("description")

    val outputView = getValue[String]("outputView")
    val topic = getValue[String]("topic")
    val bootstrapServers = getValue[String]("bootstrapServers")
    val groupID = getValue[String]("groupID")

    val persist = getValue[Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))

    val maxPollRecords = getValue[Int]("maxPollRecords", default = Some(10000))
    val timeout = getValue[Long]("timeout", default = Some(10000L))
    val autoCommit = getValue[Boolean]("autoCommit", default = Some(false))

    (name, description, outputView, topic, bootstrapServers, groupID, persist, numPartitions, maxPollRecords, timeout, autoCommit, partitionBy, invalidKeys) match {
      case (Right(n), Right(d), Right(ov), Right(t), Right(bs), Right(g), Right(p), Right(np), Right(mpr), Right(time), Right(ac), Right(pb), Right(_)) => 
        var outputGraph = graph.addVertex(Vertex(idx, ov))
        (Right(KafkaExtract(n, d, ov, t, bs, g, mpr, time, ac, params, p, np, pb)), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, outputView, topic, bootstrapServers, groupID, persist, numPartitions, maxPollRecords, timeout, autoCommit, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }  

  def readORCExtract(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "outputView" :: "authentication" :: "contiguousIndex" :: "numPartitions" :: "partitionBy" :: "persist" :: "schemaURI" :: "schemaView" :: "params" :: "basePath" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    val description = getOptionalValue[String]("description")

    val inputURI = getValue[String]("inputURI")
    val parsedGlob = inputURI.rightFlatMap(glob => parseGlob("inputURI", glob))
    val outputView = getValue[String]("outputView")
    val persist = getValue[Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val authentication = readAuthentication("authentication")
    val contiguousIndex = getValue[Boolean]("contiguousIndex", default = Some(true))

    val uriKey = "schemaURI"
    val stringURI = getOptionalValue[String](uriKey)
    val parsedURI: Either[Errors, Option[URI]] = stringURI.rightFlatMap(optURI => 
      optURI match { 
        case Some(uri) => parseURI(uriKey, uri).rightFlatMap(parsedURI => Right(Option(parsedURI)))
        case None => Right(None)
      }
    )
    val extractColumns = if(!c.hasPath("schemaView")) getExtractColumns(parsedURI, uriKey, authentication) else Right(List.empty)
    val schemaView = if(c.hasPath("schemaView")) getValue[String]("schemaView") else Right("")
    val basePath = getOptionalValue[String]("basePath")

    (name, description, extractColumns, schemaView, inputURI, parsedGlob, outputView, persist, numPartitions, authentication, contiguousIndex, invalidKeys, partitionBy, basePath) match {
      case (Right(n), Right(d), Right(cols), Right(sv), Right(in), Right(pg), Right(ov), Right(p), Right(np), Right(auth), Right(ci), Right(_), Right(pb), Right(bp)) => 
        val schema = if(c.hasPath("schemaView")) Left(sv) else Right(cols)
        var outputGraph = graph.addVertex(Vertex(idx, ov))
        (Right(ORCExtract(n, d, schema, ov, pg, auth, params, p, np, pb, ci, bp)), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, inputURI, schemaView, parsedGlob, outputView, persist, numPartitions, authentication, contiguousIndex, extractColumns, invalidKeys, partitionBy, basePath).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }  

  def readParquetExtract(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "outputView" :: "authentication" :: "contiguousIndex" :: "numPartitions" :: "partitionBy" :: "persist" :: "schemaURI" :: "schemaView" :: "params" :: "basePath" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    val description = getOptionalValue[String]("description")

    val inputURI = getValue[String]("inputURI")
    val parsedGlob = inputURI.rightFlatMap(glob => parseGlob("inputURI", glob))
    val outputView = getValue[String]("outputView")
    val persist = getValue[Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val authentication = readAuthentication("authentication")
    val contiguousIndex = getValue[Boolean]("contiguousIndex", default = Some(true))

    val uriKey = "schemaURI"
    val stringURI = getOptionalValue[String](uriKey)
    val parsedURI: Either[Errors, Option[URI]] = stringURI.rightFlatMap(optURI => 
      optURI match { 
        case Some(uri) => parseURI(uriKey, uri).rightFlatMap(parsedURI => Right(Option(parsedURI)))
        case None => Right(None)
      }
    )
    val extractColumns = if(!c.hasPath("schemaView")) getExtractColumns(parsedURI, uriKey, authentication) else Right(List.empty)
    val schemaView = if(c.hasPath("schemaView")) getValue[String]("schemaView") else Right("")  
    val basePath = getOptionalValue[String]("basePath")

    (name, description, extractColumns, schemaView, inputURI, parsedGlob, outputView, persist, numPartitions, authentication, contiguousIndex, partitionBy, invalidKeys, basePath) match {
      case (Right(n), Right(d), Right(cols), Right(sv), Right(in), Right(pg), Right(ov), Right(p), Right(np), Right(auth), Right(ci), Right(pb), Right(_), Right(bp)) => 
        val schema = if(c.hasPath("schemaView")) Left(sv) else Right(cols)
        var outputGraph = graph.addVertex(Vertex(idx, ov))
        (Right(ParquetExtract(n, d, schema, ov, pg, auth, params, p, np, pb, ci, bp)), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, inputURI, schemaView, parsedGlob, outputView, persist, numPartitions, authentication, contiguousIndex, extractColumns, partitionBy, invalidKeys, basePath).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }

  def readRateExtract(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "outputView" :: "rowsPerSecond" :: "rampUpTime" :: "numPartitions" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    val description = getOptionalValue[String]("description")

    val outputView = getValue[String]("outputView")
    val rowsPerSecond = getValue[Int]("rowsPerSecond", default = Some(1))
    val rampUpTime = getValue[Int]("rampUpTime", default = Some(1))
    val numPartitions = getValue[Int]("numPartitions", default = Some(spark.sparkContext.defaultParallelism))

    (name, description, outputView, rowsPerSecond, rampUpTime, numPartitions) match {
      case (Right(n), Right(d), Right(ov), Right(rps), Right(rut), Right(np)) => 
        var outputGraph = graph.addVertex(Vertex(idx, ov))
        (Right(RateExtract(n, d, ov, params, rps, rut, np)), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, outputView, rowsPerSecond, rampUpTime, numPartitions).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }  

  def readTextExtract(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "outputView" :: "authentication" :: "contiguousIndex" :: "multiLine" :: "numPartitions" :: "persist" :: "schemaURI" :: "schemaView" :: "params" :: "basePath" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    val description = getOptionalValue[String]("description")

    val input = getValue[String]("inputURI")
    val parsedGlob = input.rightFlatMap(glob => parseGlob("inputURI", glob))

    val outputView = getValue[String]("outputView")
    val persist = getValue[Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val multiLine = getValue[Boolean]("multiLine", default = Some(false))
    val authentication = readAuthentication("authentication")
    val contiguousIndex = getValue[Boolean]("contiguousIndex", default = Some(true))

    val uriKey = "schemaURI"
    val stringURI = getOptionalValue[String](uriKey)
    val parsedURI: Either[Errors, Option[URI]] = stringURI.rightFlatMap(optURI => 
      optURI match { 
        case Some(uri) => parseURI(uriKey, uri).rightFlatMap(parsedURI => Right(Option(parsedURI)))
        case None => Right(None)
      }
    )

    val extractColumns = if(!c.hasPath("schemaView")) getExtractColumns(parsedURI, uriKey, authentication) else Right(List.empty)
    val basePath = getOptionalValue[String]("basePath")

    (name, description, extractColumns, input, parsedGlob, outputView, persist, numPartitions, multiLine, authentication, contiguousIndex, invalidKeys, basePath) match {
      case (Right(n), Right(d), Right(cols), Right(in), Right(pg), Right(ov), Right(p), Right(np), Right(ml), Right(auth), Right(ci), Right(_), Right(bp)) => 
        var outputGraph = graph.addVertex(Vertex(idx, ov))
        (Right(TextExtract(n, d, Right(cols), ov, in, auth, params, p, np, ci, ml, bp)), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, input, parsedGlob, outputView, persist, numPartitions, multiLine, authentication, contiguousIndex, extractColumns, invalidKeys, basePath).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }

  def readXMLExtract(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "inputView" :: "outputView" :: "authentication" :: "contiguousIndex" :: "numPartitions" :: "partitionBy" :: "persist" :: "schemaURI" :: "schemaView" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    val description = getOptionalValue[String]("description")

    val input = if(c.hasPath("inputView")) getValue[String]("inputView") else getValue[String]("inputURI")
    val parsedGlob = if (!c.hasPath("inputView")) {
      input.rightFlatMap(glob => parseGlob("inputURI", glob))
    } else {
      Right("")
    }

    val outputView = getValue[String]("outputView")
    val persist = getValue[Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val authentication = readAuthentication("authentication")
    val contiguousIndex = getValue[Boolean]("contiguousIndex", default = Some(true))

    val uriKey = "schemaURI"
    val stringURI = getOptionalValue[String](uriKey)
    val parsedURI: Either[Errors, Option[URI]] = stringURI.rightFlatMap(optURI => 
      optURI match { 
        case Some(uri) => parseURI(uriKey, uri).rightFlatMap(parsedURI => Right(Option(parsedURI)))
        case None => Right(None)
      }
    )
    val extractColumns = if(!c.hasPath("schemaView")) getExtractColumns(parsedURI, uriKey, authentication) else Right(List.empty)
    val schemaView = if(c.hasPath("schemaView")) getValue[String]("schemaView") else Right("")  

    (name, description, extractColumns, schemaView, input, parsedGlob, outputView, persist, numPartitions, authentication, contiguousIndex, partitionBy, invalidKeys) match {
      case (Right(n), Right(d), Right(cols), Right(sv), Right(in), Right(pg), Right(ov), Right(p), Right(np), Right(auth), Right(ci), Right(pb), Right(_)) => 
        val input = if(c.hasPath("inputView")) Left(in) else Right(pg)
        val schema = if(c.hasPath("schemaView")) Left(sv) else Right(cols)
        var outputGraph = graph.addVertex(Vertex(idx, ov))

        (Right(XMLExtract(n, d, schema, ov, input, auth, params, p, np, pb, ci)), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, input, schemaView, parsedGlob, outputView, persist, numPartitions, authentication, contiguousIndex, extractColumns, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }


  // transform
  def readDiffTransform(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputLeftView" :: "inputRightView" :: "outputIntersectionView" :: "outputLeftView" :: "outputRightView" :: "persist" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)    

    val description = getOptionalValue[String]("description")

    val inputLeftView = getValue[String]("inputLeftView") |>  graph.vertexExists("inputLeftView") _
    val inputRightView = getValue[String]("inputRightView") |>  graph.vertexExists("inputRightView") _
    val outputIntersectionView = getOptionalValue[String]("outputIntersectionView")
    val outputLeftView = getOptionalValue[String]("outputLeftView")
    val outputRightView = getOptionalValue[String]("outputRightView")
    val persist = getValue[Boolean]("persist", default = Some(false))

    (name, description, inputLeftView, inputRightView, outputIntersectionView, outputLeftView, outputRightView, persist, invalidKeys) match {
      case (Right(n), Right(d), Right(ilv), Right(irv), Right(oiv), Right(olv), Right(orv), Right(p), Right(_)) => 
        // add the vertices
        var outputGraph = graph
        outputGraph = if (oiv.isEmpty) outputGraph else outputGraph.addVertex(Vertex(idx, oiv.get))
        outputGraph = if (olv.isEmpty) outputGraph else outputGraph.addVertex(Vertex(idx, olv.get)) 
        outputGraph = if (orv.isEmpty) outputGraph else outputGraph.addVertex(Vertex(idx, orv.get))

        (Right(DiffTransform(n, d, ilv, irv, oiv, olv, orv, params, p)), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, inputLeftView, inputRightView, outputIntersectionView, outputLeftView, outputRightView, persist, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  } 

  def readHTTPTransform(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputView" :: "uri" :: "headers" :: "inputField" :: "persist" :: "validStatusCodes" :: "params" :: "batchSize" :: "delimiter" :: "numPartitions" :: "partitionBy" :: "failMode" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)    

    val description = getOptionalValue[String]("description")

    val httpUriKey = "uri"
    val inputURI = getValue[String](httpUriKey)
    val inputField = getValue[String]("inputField", default = Some("value"))
    val parsedHttpURI = inputURI.rightFlatMap(uri => parseURI(httpUriKey, uri))
    val headers = readMap("headers", c)
    val inputView = getValue[String]("inputView")
    val outputView = getValue[String]("outputView")
    val persist = getValue[Boolean]("persist", default = Some(false))
    val validStatusCodes = getValue[IntList]("validStatusCodes", default = Some(200 :: 201 :: 202 :: Nil))
    val batchSize = getValue[Int]("batchSize", default = Some(1))
    val delimiter = getValue[String]("delimiter", default = Some("\n"))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))    
    val failMode = getValue[String]("failMode", default = Some("failfast"), validValues = "permissive" :: "failfast" :: Nil) |> parseFailMode("failMode") _

    (name, description, inputView, outputView, parsedHttpURI, persist, inputField, validStatusCodes, invalidKeys, batchSize, delimiter, numPartitions, partitionBy, failMode) match {
      case (Right(n), Right(d), Right(iv), Right(ov), Right(uri), Right(p), Right(ifld), Right(vsc), Right(_), Right(bs), Right(delim), Right(np), Right(pb), Right(fm)) => 
        
        var outputGraph = graph
        // add the vertices
        outputGraph = outputGraph.addVertex(Vertex(idx, ov))
        // add the edges
        outputGraph = outputGraph.addEdge(iv, ov)

        (Right(HTTPTransform(n, d, uri, headers, vsc, iv, ov, ifld, params, p, bs, delim, np, pb, fm)), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, outputView, parsedHttpURI, persist, inputField, validStatusCodes, invalidKeys, batchSize, delimiter, numPartitions, partitionBy, failMode).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }  

  def readJSONTransform(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputView" :: "persist" :: "params" :: "numPartitions" :: "partitionBy" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    val description = getOptionalValue[String]("description")

    val inputView = getValue[String]("inputView")
    val outputView = getValue[String]("outputView")
    val persist = getValue[Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))  

    (name, description, inputView, outputView, persist, invalidKeys, numPartitions, partitionBy) match {
      case (Right(n), Right(d), Right(iv), Right(ov), Right(p), Right(_), Right(np), Right(pb)) => 
        var outputGraph = graph
        // add the vertices
        outputGraph = outputGraph.addVertex(Vertex(idx, ov))
        // add the edges
        outputGraph = outputGraph.addEdge(iv, ov)

        (Right(JSONTransform(n, d, iv, ov, params, p, np, pb)), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, outputView, persist, invalidKeys, numPartitions, partitionBy).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }  

  def readMetadataFilterTransform(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "inputView" :: "outputView" :: "authentication" :: "persist" :: "sqlParams" :: "params" :: "numPartitions" :: "partitionBy" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    val description = getOptionalValue[String]("description")

    val uriKey = "inputURI"
    val inputURI = getValue[String](uriKey)
    val parsedURI = inputURI.rightFlatMap(uri => parseURI(uriKey, uri))
    val authentication = readAuthentication("authentication")  
    val inputSQL = parsedURI.rightFlatMap { uri =>
        authentication.right.map(auth => CloudUtils.setHadoopConfiguration(auth))  
        getBlob(uriKey, uri)
    }
    val inputView = getValue[String]("inputView")
    val outputView = getValue[String]("outputView")
    val persist = getValue[Boolean]("persist", default = Some(false))
    val sqlParams = readMap("sqlParams", c)
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))    

    // try to verify if sql is technically valid against HQL dialect (will not check dependencies)
    val validSQL = inputSQL.rightFlatMap { sql =>
      validateSQL(uriKey, SQLUtils.injectParameters(sql, sqlParams))
    }

    (name, description, parsedURI, inputSQL, validSQL, inputView, outputView, persist, invalidKeys, numPartitions, partitionBy) match {
      case (Right(n), Right(d), Right(uri), Right(sql), Right(vsql), Right(iv), Right(ov), Right(p), Right(_), Right(np), Right(pb)) => 
        var outputGraph = graph
        // add the vertices
        outputGraph = outputGraph.addVertex(Vertex(idx, ov))
        // add the edges
        outputGraph = outputGraph.addEdge(iv, ov)

        // pass the unreplaced input sql not the 'valid sql' as the paramenters will be replaced when the stage is executed for testing
        (Right(MetadataFilterTransform(n, d, iv, uri, sql, ov, params, sqlParams, p, np, pb)), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, inputURI, parsedURI, inputSQL, validSQL, inputView, outputView, persist, invalidKeys, numPartitions, partitionBy).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }   

  def readMLTransform(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "inputView" :: "outputView" :: "authentication" :: "persist" :: "params" :: "numPartitions" :: "partitionBy" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    val description = getOptionalValue[String]("description")

    val uriKey = "inputURI"
    val inputURI = getValue[String](uriKey)
    val parsedURI = inputURI.rightFlatMap(uri => parseURI(uriKey, uri))
    val authentication = readAuthentication("authentication")

    val inputModel = parsedURI.rightFlatMap { uri =>
        authentication.right.map(auth => CloudUtils.setHadoopConfiguration(auth))
        getModel(uriKey, uri)
    }

    val inputView = getValue[String]("inputView")
    val outputView = getValue[String]("outputView")
    val persist = getValue[Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))        

    (name, description, inputURI, inputModel, inputView, outputView, persist, invalidKeys, numPartitions, partitionBy) match {
      case (Right(n), Right(d), Right(in), Right(mod), Right(iv), Right(ov), Right(p), Right(_), Right(np), Right(pb)) => 
        val uri = new URI(in)

        var outputGraph = graph
        // add the vertices
        outputGraph = outputGraph.addVertex(Vertex(idx, ov))
        // add the edges
        outputGraph = outputGraph.addEdge(iv, ov)

        (Right(MLTransform(n, d, uri, mod, iv, ov, params, p, np, pb)), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, inputURI, parsedURI, inputModel, inputView, outputView, persist, invalidKeys, numPartitions, partitionBy).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }

  def readSQLTransform(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "outputView" :: "authentication" :: "persist" :: "sqlParams" :: "params" :: "numPartitions" :: "partitionBy" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    val description = getOptionalValue[String]("description")

    val uriKey = "inputURI"
    val inputURI = getValue[String](uriKey)
    val parsedURI = inputURI.rightFlatMap(uri => parseURI(uriKey, uri))
    val authentication = readAuthentication("authentication")  
    val inputSQL = parsedURI.rightFlatMap{ uri => textContentForURI(uri, uriKey, authentication) }
    val outputView = getValue[String]("outputView")
    val persist = getValue[Boolean]("persist", default = Some(false))
    val sqlParams = readMap("sqlParams", c)
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))        

    // try to verify if sql is technically valid against HQL dialect (will not check dependencies)
    val validSQL = inputSQL.rightFlatMap { sql =>
      validateSQL(uriKey, SQLUtils.injectParameters(sql, sqlParams))
    }

    // tables exist
    val tableExistence: Either[Errors, String] = validSQL.rightFlatMap { sql =>
      val parser = spark.sessionState.sqlParser
      val plan = parser.parsePlan(sql)
      val relations = plan.collect { case r: UnresolvedRelation => r.tableName }

      val (errors, _) = relations.map { relation => 
         graph.vertexExists("inputURI")(relation)
      }
      .foldLeft[(Errors, String)]( (Nil, "") ){ case ( (errs, ret), table ) => 
        table match {
          case Left(err) => (err ::: errs, "")
          case _ => (errs, "")
        }
      }

      errors match {
        case Nil => Right("")
        case _ => Left(errors.reverse)
      }
    }

    (name, description, parsedURI, inputSQL, validSQL, outputView, persist, invalidKeys, numPartitions, partitionBy, tableExistence) match {
      case (Right(n), Right(d), Right(uri), Right(sql), Right(vsql), Right(ov), Right(p), Right(_), Right(np), Right(pb), Right(te)) => 

        if (vsql.toLowerCase() contains "now") {
          logger.warn()
            .field("event", "validateConfig")
            .field("name", n)
            .field("type", "SQLTransform")              
            .field("message", "sql contains NOW() function which may produce non-deterministic results")       
            .log()   
        } 

        if (vsql.toLowerCase() contains "current_date") {
          logger.warn()
            .field("event", "validateConfig")
            .field("name", n)
            .field("type", "SQLTransform")              
            .field("message", "sql contains CURRENT_DATE() function which may produce non-deterministic results")       
            .log()   
        }

        if (vsql.toLowerCase() contains "current_timestamp") {
          logger.warn()
            .field("event", "validateConfig")
            .field("name", n)
            .field("type", "SQLTransform")              
            .field("message", "sql contains CURRENT_TIMESTAMP() function which may produce non-deterministic results")       
            .log()   
        }        

        var outputGraph = graph
        // add the vertices
        outputGraph = outputGraph.addVertex(Vertex(idx, ov))
        // add the edges
        // add input/output edges by resolving dependent tables
        val parser = spark.sessionState.sqlParser
        val plan = parser.parsePlan(vsql)        
        plan.collect { case r: UnresolvedRelation => r.tableName }.toList.foreach { iv =>
          outputGraph = outputGraph.addEdge(iv, ov)
        }

        // pass the unreplaced input sql not the 'valid sql' as the paramenters will be replaced when the stage is executed for testing
        (Right(SQLTransform(n, d, uri, sql, ov, params, sqlParams, p, np, pb)), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, inputURI, parsedURI, inputSQL, validSQL, outputView, persist, invalidKeys, numPartitions, partitionBy, tableExistence).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  } 

  def readTensorFlowServingTransform(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputView" :: "uri" :: "batchSize" :: "inputField" :: "params"  :: "persist" :: "responseType" :: "signatureName" :: "numPartitions" :: "partitionBy" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    val description = getOptionalValue[String]("description")

    val inputView = getValue[String]("inputView")
    val outputView = getValue[String]("outputView")
    val inputURI = getValue[String]("uri")
    val inputField = getValue[String]("inputField", default = Some("value"))
    val parsedURI = inputURI.rightFlatMap(uri => parseURI("uri", uri))
    val signatureName = getOptionalValue[String]("signatureName")
    val batchSize = getValue[Int]("batchsize", default = Some(1))
    val persist = getValue[Boolean]("persist", default = Some(false))
    val responseType = getValue[String]("responseType", default = Some("object"), validValues = "integer" :: "double" :: "object" :: Nil) |> parseResponseType("responseType") _
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))        

    (name, description, inputView, outputView, inputURI, parsedURI, signatureName, responseType, batchSize, persist, inputField, invalidKeys, numPartitions, partitionBy) match {
      case (Right(n), Right(d), Right(iv), Right(ov), Right(uri), Right(puri), Right(sn), Right(rt), Right(bs), Right(p), Right(ifld), Right(_), Right(np), Right(pb)) => 
        var outputGraph = graph
        // add the vertices
        outputGraph = outputGraph.addVertex(Vertex(idx, ov))
        // add the edges
        outputGraph = outputGraph.addEdge(iv, ov)

        (Right(TensorFlowServingTransform(n, d, iv, ov, puri, sn, rt, bs, ifld, params, p, np, pb)), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, outputView, inputURI, parsedURI, signatureName, responseType, batchSize, persist, inputField, invalidKeys, numPartitions, partitionBy).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }

  def readTypingTransform(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "inputView" :: "outputView" :: "authentication" :: "failMode" :: "persist" :: "params" :: "numPartitions" :: "partitionBy" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    val description = getOptionalValue[String]("description")

    val uriKey = "inputURI"
    val inputURI = getValue[String](uriKey)
    val parsedURI = inputURI.rightFlatMap(uri => parseURI(uriKey, uri)).rightFlatMap(uri => Right(Option(uri)))
    val authentication = readAuthentication("authentication")  

    val extractColumns = if(!c.hasPath("schemaView")) getExtractColumns(parsedURI, uriKey, authentication) else Right(List.empty)
    val schemaView = if(c.hasPath("schemaView")) getValue[String]("schemaView") else Right("")

    val inputView = getValue[String]("inputView")
    val outputView = getValue[String]("outputView")
    val persist = getValue[Boolean]("persist", default = Some(false))

    val failMode = getValue[String]("failMode", default = Some("permissive"), validValues = "permissive" :: "failfast" :: Nil) |> parseFailMode("failMode") _
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))        

    (name, description, extractColumns, schemaView, inputView, outputView, persist, failMode, invalidKeys, numPartitions, partitionBy) match {
      case (Right(n), Right(d), Right(cols), Right(sv), Right(iv), Right(ov), Right(p), Right(fm), Right(_), Right(np), Right(pb)) => 
        val schema = if(c.hasPath("schemaView")) Left(sv) else Right(cols)

        var outputGraph = graph
        // add the vertices
        outputGraph = outputGraph.addVertex(Vertex(idx, ov))
        // add the edges
        outputGraph = outputGraph.addEdge(iv, ov)

        (Right(TypingTransform(n, d, schema, iv, ov, params, p, fm, np, pb)), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, inputURI, parsedURI, extractColumns, schemaView, inputView, outputView, persist, authentication, failMode, invalidKeys, numPartitions, partitionBy).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }   

  // load

  def readAvroLoad(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputURI" :: "authentication" :: "numPartitions" :: "partitionBy" :: "saveMode" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    val description = getOptionalValue[String]("description")

    val inputView = getValue[String]("inputView")
    val outputURI = getValue[String]("outputURI")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val authentication = readAuthentication("authentication")  
    val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _

    (name, description, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys) match {
      case (Right(n), Right(d), Right(iv), Right(out), Right(np), Right(auth), Right(sm), Right(pb), Right(_)) => 
        val uri = new URI(out)
        val load = AvroLoad(n, d, iv, uri, pb, np, auth, sm, params)

        val ov = s"$idx:${load.getType}"
        var outputGraph = graph
        // add the vertices
        outputGraph = outputGraph.addVertex(Vertex(idx, ov))
        // add the edges
        outputGraph = outputGraph.addEdge(iv, ov)

        (Right(load), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }    

  def readAzureEventHubsLoad(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "namespaceName" :: "eventHubName" :: "sharedAccessSignatureKeyName" :: "sharedAccessSignatureKey" :: "numPartitions" :: "retryCount" :: "retryMaxBackoff" :: "retryMinBackoff" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    val description = getOptionalValue[String]("description")

    val inputView = getValue[String]("inputView")
    val namespaceName = getValue[String]("namespaceName")
    val eventHubName = getValue[String]("eventHubName")
    val sharedAccessSignatureKeyName = getValue[String]("sharedAccessSignatureKeyName")
    val sharedAccessSignatureKey = getValue[String]("sharedAccessSignatureKey")
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val retryMinBackoff = getValue[Long]("retryMinBackoff", default = Some(0)) // DEFAULT_RETRY_MIN_BACKOFF = 0
    val retryMaxBackoff = getValue[Long]("retryMaxBackoff", default = Some(30)) // DEFAULT_RETRY_MAX_BACKOFF = 30
    val retryCount = getValue[Int]("retryCount", default = Some(10)) // DEFAULT_MAX_RETRY_COUNT = 10

    (name, description, inputView, namespaceName, eventHubName, sharedAccessSignatureKeyName, sharedAccessSignatureKey, numPartitions, retryMinBackoff, retryMaxBackoff, retryCount, invalidKeys) match {
      case (Right(n), Right(d), Right(iv), Right(nn), Right(ehn), Right(saskn), Right(sask), Right(np), Right(rmin), Right(rmax), Right(rcount), Right(_)) => 
        val load = AzureEventHubsLoad(n, d, iv, nn, ehn, saskn, sask, np, rmin, rmax, rcount, params)

        val ov = s"$idx:${load.getType}"
        var outputGraph = graph
        // add the vertices
        outputGraph = outputGraph.addVertex(Vertex(idx, ov))
        // add the edges
        outputGraph = outputGraph.addEdge(iv, ov)

        (Right(load), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, namespaceName, eventHubName, sharedAccessSignatureKeyName, sharedAccessSignatureKey, numPartitions, retryMinBackoff, retryMaxBackoff, retryCount, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }    

  def readConsoleLoad(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputMode" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    val description = getOptionalValue[String]("description")

    val inputView = getValue[String]("inputView")
    val outputMode = getValue[String]("outputMode", default = Some("Append"), validValues = "Append" :: "Complete" :: "Update" :: Nil) |> parseOutputModeType("outputMode") _

    (name, description, inputView, outputMode, invalidKeys) match {
      case (Right(n), Right(d), Right(iv), Right(om), Right(_)) => 
        val load = ConsoleLoad(n, d, iv, om, params)

        val ov = s"$idx:${load.getType}"
        var outputGraph = graph
        // add the vertices
        outputGraph = outputGraph.addVertex(Vertex(idx, ov))
        // add the edges
        outputGraph = outputGraph.addEdge(iv, ov)

        (Right(load), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, outputMode, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }    

  def readDatabricksDeltaLoad(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputURI" :: "numPartitions" :: "partitionBy" :: "saveMode" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    val description = getOptionalValue[String]("description")

    val inputView = getValue[String]("inputView")
    val outputURI = getValue[String]("outputURI")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _

    (name, description, inputView, outputURI, numPartitions, saveMode, partitionBy, invalidKeys) match {
      case (Right(n), Right(d), Right(iv), Right(out), Right(np), Right(sm), Right(pb), Right(_)) => 
        val load = DatabricksDeltaLoad(n, d, iv, new URI(out), pb, np, sm, params)

        val ov = s"$idx:${load.getType}"
        var outputGraph = graph
        // add the vertices
        outputGraph = outputGraph.addVertex(Vertex(idx, ov))
        // add the edges
        outputGraph = outputGraph.addEdge(iv, ov)

        (Right(load), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, outputURI, numPartitions, saveMode, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }   

  def readDatabricksSQLDWLoad(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "jdbcURL" :: "tempDir" :: "dbTable" :: "query" :: "forwardSparkAzureStorageCredentials" :: "tableOptions" :: "maxStrLength" :: "authentication" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)     

    val description = getOptionalValue[String]("description")

    val inputView = getValue[String]("inputView")
    val jdbcURL = getValue[String]("jdbcURL")
    val driver = jdbcURL.rightFlatMap(uri => getJDBCDriver("jdbcURL", uri))
    val tempDir = getValue[String]("tempDir")
    val dbTable = getValue[String]("dbTable")
    val forwardSparkAzureStorageCredentials = getValue[Boolean]("forwardSparkAzureStorageCredentials", default = Some(true))
    val tableOptions = getOptionalValue[String]("tableOptions")
    val maxStrLength = getValue[Int]("maxStrLength", default = Some(256))
    val authentication = readAuthentication("authentication") |> validateAzureSharedKey("authentication") _

    (name, description, inputView, jdbcURL, driver, tempDir, dbTable, forwardSparkAzureStorageCredentials, tableOptions, maxStrLength, authentication, invalidKeys) match {
      case (Right(name), Right(description), Right(inputView), Right(jdbcURL), Right(driver), Right(tempDir), Right(dbTable), Right(forwardSparkAzureStorageCredentials), Right(tableOptions), Right(maxStrLength), Right(authentication), Right(invalidKeys)) => 
        val load = DatabricksSQLDWLoad(name, description, inputView, jdbcURL, driver, tempDir, dbTable, forwardSparkAzureStorageCredentials, tableOptions, maxStrLength, authentication, params)

        val ov = s"$idx:${load.getType}"
        var outputGraph = graph
        // add the vertices
        outputGraph = outputGraph.addVertex(Vertex(idx, ov))
        // add the edges
        outputGraph = outputGraph.addEdge(inputView, ov)

        (Right(load), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, jdbcURL, driver, tempDir, dbTable, forwardSparkAzureStorageCredentials, tableOptions, maxStrLength, authentication, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }   

  def readDelimitedLoad(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputURI" :: "authentication" :: "delimiter" :: "header" :: "numPartitions" :: "partitionBy" :: "quote" :: "saveMode" :: "params"  :: "customDelimiter" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    val description = getOptionalValue[String]("description")

    val inputView = getValue[String]("inputView")
    val outputURI = getValue[String]("outputURI")
    val partitionBy = if (c.hasPath("partitionBy")) c.getStringList("partitionBy").asScala.toList else Nil    
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val authentication = readAuthentication("authentication")  
    val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _

    val delimiter = getValue[String]("delimiter", default = Some("Comma"), validValues = "Comma" :: "Pipe" :: "DefaultHive" :: "Custom" :: Nil) |> parseDelimiter("delimiter") _
    val quote = getValue[String]("quote", default =  Some("DoubleQuote"), validValues = "DoubleQuote" :: "SingleQuote" :: "None" :: Nil) |> parseQuote("quote") _
    val header = getValue[Boolean]("header", Some(false))   

    val customDelimiter = delimiter match {
      case Right(Delimiter.Custom) => {
        getValue[String]("customDelimiter")
      }
      case _ => Right("")
    }     

    (name, description, inputView, outputURI, numPartitions, authentication, saveMode, delimiter, quote, header, invalidKeys, customDelimiter) match {
      case (Right(n), Right(desc), Right(iv), Right(out),  Right(np), Right(auth), Right(sm), Right(d), Right(q), Right(h), Right(_), Right(cd)) => 
        val uri = new URI(out)
        val load = DelimitedLoad(n, desc, iv, uri, Delimited(header=h, sep=d, quote=q, customDelimiter=cd), partitionBy, np, auth, sm, params)

        val ov = s"$idx:${load.getType}"
        var outputGraph = graph
        // add the vertices
        outputGraph = outputGraph.addVertex(Vertex(idx, ov))
        // add the edges
        outputGraph = outputGraph.addEdge(iv, ov)

        (Right(load), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, outputURI, authentication, numPartitions, saveMode, delimiter, quote, header, invalidKeys, customDelimiter).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }    

  def readElasticsearchLoad(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "output" :: "numPartitions" :: "partitionBy" :: "saveMode" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    val description = getOptionalValue[String]("description")

    val inputView = getValue[String]("inputView")
    val output = getValue[String]("output")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val authentication = readAuthentication("authentication")  
    val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _

    (name, description, inputView, output, numPartitions, saveMode, partitionBy, invalidKeys) match {
      case (Right(n), Right(d), Right(iv), Right(out), Right(np), Right(sm), Right(pb), Right(_)) => 
        val load = ElasticsearchLoad(n, d, iv, out, pb, np, sm, params)

        val ov = s"$idx:${load.getType}"
        var outputGraph = graph
        // add the vertices
        outputGraph = outputGraph.addVertex(Vertex(idx, ov))
        // add the edges
        outputGraph = outputGraph.addEdge(iv, ov)

        (Right(load), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, output, numPartitions, saveMode, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }      
  
  def readHTTPLoad(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputURI" :: "headers" :: "validStatusCodes" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)     

    val description = getOptionalValue[String]("description")

    val inputView = getValue[String]("inputView")
    val inputURI = getValue[String]("outputURI")
    val parsedURI = inputURI.rightFlatMap(uri => parseURI("outputURI", uri))
    val headers = readMap("headers", c)
    val validStatusCodes = getValue[IntList]("validStatusCodes", default = Some(200 :: 201 :: 202 :: Nil))

    (name, description, inputURI, parsedURI, inputView, invalidKeys, validStatusCodes) match {
      case (Right(n), Right(d), Right(iuri), Right(uri), Right(iv), Right(_), Right(vsc)) => 
        val load = HTTPLoad(n, d, iv, uri, headers, vsc, params)

        val ov = s"$idx:${load.getType}"
        var outputGraph = graph
        // add the vertices
        outputGraph = outputGraph.addVertex(Vertex(idx, ov))
        // add the edges
        outputGraph = outputGraph.addEdge(iv, ov)

        (Right(load), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, parsedURI, inputView, invalidKeys, validStatusCodes).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }  

  def readJDBCLoad(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "jdbcURL" :: "tableName" :: "params" :: "batchsize" :: "bulkload" :: "createTableColumnTypes" :: "createTableOptions" :: "isolationLevel" :: "numPartitions" :: "saveMode" :: "tablock" :: "truncate" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)     

    val description = getOptionalValue[String]("description")

    val inputView = getValue[String]("inputView")
    val jdbcURL = getValue[String]("jdbcURL")
    val driver = jdbcURL.rightFlatMap(uri => getJDBCDriver("jdbcURL", uri))
    val tableName = getValue[String]("tableName")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val isolationLevel = getValue[String]("isolationLevel", default = Some("READ_UNCOMMITTED"), validValues = "NONE" :: "READ_COMMITTED" :: "READ_UNCOMMITTED" :: "REPEATABLE_READ" :: "SERIALIZABLE" :: Nil) |> parseIsolationLevel("isolationLevel") _
    val batchsize = getValue[Int]("batchsize", default = Some(1000))
    val truncate = getValue[Boolean]("truncate", default = Some(false))
    val createTableOptions = getOptionalValue[String]("createTableOptions")
    val createTableColumnTypes = getOptionalValue[String]("createTableColumnTypes")
    val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _
    val bulkload = getValue[Boolean]("bulkload", default = Some(false))
    val tablock = getValue[Boolean]("tablock", default = Some(true))

    (name, description, inputView, jdbcURL, driver, tableName, numPartitions, isolationLevel, batchsize, truncate, createTableOptions, createTableColumnTypes, saveMode, bulkload, tablock, partitionBy, invalidKeys) match {
      case (Right(n), Right(desc), Right(iv), Right(ju), Right(d), Right(tn), Right(np), Right(il), Right(bs), Right(t), Right(cto), Right(ctct), Right(sm), Right(bl), Right(tl), Right(pb), Right(_)) => 
        val load = JDBCLoad(n, desc, iv, ju, tn, pb, np, il, bs, t, cto, ctct, sm, d, bl, tl, params)

        val ov = s"$idx:${load.getType}"
        var outputGraph = graph
        // add the vertices
        outputGraph = outputGraph.addVertex(Vertex(idx, ov))
        // add the edges
        outputGraph = outputGraph.addEdge(iv, ov)

        (Right(load), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, jdbcURL, driver, tableName, numPartitions, isolationLevel, batchsize, truncate, createTableOptions, createTableColumnTypes, saveMode, bulkload, tablock, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }      

  def readJSONLoad(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputURI" :: "authentication" :: "numPartitions" :: "partitionBy" :: "saveMode" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)      

    val description = getOptionalValue[String]("description")

    val inputView = getValue[String]("inputView")
    val outputURI = getValue[String]("outputURI")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val authentication = readAuthentication("authentication")  
    val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _

    (name, description, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys) match {
      case (Right(n), Right(d), Right(iv), Right(out), Right(np), Right(auth), Right(sm), Right(pb), Right(_)) => 
        val load = JSONLoad(n, d, iv, new URI(out), pb, np, auth, sm, params)

        val ov = s"$idx:${load.getType}"
        var outputGraph = graph
        // add the vertices
        outputGraph = outputGraph.addVertex(Vertex(idx, ov))
        // add the edges
        outputGraph = outputGraph.addEdge(iv, ov)

        (Right(load), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }

  def readKafkaLoad(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "bootstrapServers" :: "topic" :: "acks" :: "batchSize" :: "numPartitions" :: "retries" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)     

    val description = getOptionalValue[String]("description")

    val inputView = getValue[String]("inputView")
    val bootstrapServers = getValue[String]("bootstrapServers")
    val topic = getValue[String]("topic")
    val acks = getValue[Int]("acks", default = Some(1))
    val retries = getValue[Int]("retries", default = Some(0))
    val batchSize = getValue[Int]("batchSize", default = Some(16384))

    val numPartitions = getOptionalValue[Int]("numPartitions")

    (name, description, inputView, topic, bootstrapServers, acks, retries, batchSize, numPartitions, invalidKeys) match {
      case (Right(n), Right(d), Right(iv), Right(t), Right(bss), Right(a), Right(r), Right(bs), Right(np), Right(_)) => 
        val load = KafkaLoad(n, d, iv, t, bss, a, np, r, bs, params)

        val ov = s"$idx:${load.getType}"
        var outputGraph = graph
        // add the vertices
        outputGraph = outputGraph.addVertex(Vertex(idx, ov))
        // add the edges
        outputGraph = outputGraph.addEdge(iv, ov)

        (Right(load), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, topic, bootstrapServers, acks, retries, batchSize, numPartitions, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }   

  def readORCLoad(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputURI" :: "authentication" :: "numPartitions" :: "partitionBy" :: "saveMode" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)     

    val description = getOptionalValue[String]("description")

    val inputView = getValue[String]("inputView")
    val outputURI = getValue[String]("outputURI")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val authentication = readAuthentication("authentication")  
    val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _

    (name, description, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys) match {
      case (Right(n), Right(d), Right(iv), Right(out), Right(np), Right(auth), Right(sm), Right(pb), Right(_)) => 
        val load = ORCLoad(n, d, iv, new URI(out), pb, np, auth, sm, params)

        val ov = s"$idx:${load.getType}"
        var outputGraph = graph
        // add the vertices
        outputGraph = outputGraph.addVertex(Vertex(idx, ov))
        // add the edges
        outputGraph = outputGraph.addEdge(iv, ov)

        (Right(load), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }  

  def readParquetLoad(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputURI" :: "authentication" :: "numPartitions" :: "partitionBy" :: "saveMode" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    val description = getOptionalValue[String]("description")

    val inputView = getValue[String]("inputView") |> graph.vertexExists("inputView") _
    val outputURI = getValue[String]("outputURI") |> validateURI("outputURI") _
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val authentication = readAuthentication("authentication")  
    val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _

    (name, description, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys) match {
      case (Right(n), Right(d), Right(iv), Right(out), Right(np), Right(auth), Right(sm), Right(pb), Right(_)) => 
        val load = ParquetLoad(n, d, iv, out, pb, np, auth, sm, params)

        val ov = s"$idx:${load.getType}"
        var outputGraph = graph
        // add the vertices
        outputGraph = outputGraph.addVertex(Vertex(idx, ov))
        // add the edges
        outputGraph = outputGraph.addEdge(iv, ov)

        (Right(load), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }  

  def readTextLoad(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputURI" :: "authentication" :: "numPartitions" :: "partitionBy" :: "saveMode" :: "params" :: "singleFile" :: "prefix" :: "separator" :: "suffix" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    val description = getOptionalValue[String]("description")

    val inputView = getValue[String]("inputView") |> graph.vertexExists("inputView") _
    val outputURI = getValue[String]("outputURI") |> validateURI("outputURI") _
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val authentication = readAuthentication("authentication")  
    val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _

    val singleFile = getValue[Boolean]("singleFile", default = Some(false))
    val prefix = getValue[String]("prefix", default = Some(""))
    val separator = getValue[String]("separator", default = Some(""))
    val suffix = getValue[String]("suffix", default = Some(""))

    (name, description, inputView, outputURI, numPartitions, authentication, saveMode, invalidKeys, singleFile, prefix, separator, suffix) match {
      case (Right(n), Right(d), Right(iv), Right(out), Right(np), Right(auth), Right(sm), Right(_), Right(sf), Right(pre), Right(sep), Right(suf)) => 
        val load = TextLoad(n, d, iv, out, np, auth, sm, params, sf, pre, sep, suf)

        val ov = s"$idx:${load.getType}"
        var outputGraph = graph
        // add the vertices
        outputGraph = outputGraph.addVertex(Vertex(idx, ov))
        // add the edges
        outputGraph = outputGraph.addEdge(iv, ov)

        (Right(load), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, outputURI, numPartitions, authentication, saveMode, invalidKeys, singleFile, prefix, separator, suffix).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  } 

  def readXMLLoad(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputURI" :: "authentication" :: "numPartitions" :: "partitionBy" :: "saveMode" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    val description = getOptionalValue[String]("description")

    val inputView = getValue[String]("inputView")
    val outputURI = getValue[String]("outputURI")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val authentication = readAuthentication("authentication")  
    val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _

    (name, description, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys) match {
      case (Right(n), Right(d), Right(iv), Right(out), Right(np), Right(auth), Right(sm), Right(pb), Right(_)) => 
        val load = XMLLoad(n, d, iv, new URI(out), pb, np, auth, sm, params)

        val ov = s"$idx:${load.getType}"
        var outputGraph = graph
        // add the vertices
        outputGraph = outputGraph.addVertex(Vertex(idx, ov))
        // add the edges
        outputGraph = outputGraph.addEdge(iv, ov)

        (Right(load), outputGraph)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }  

  // execute

  def readHTTPExecute(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "uri" :: "headers" :: "payloads" :: "validStatusCodes" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    val description = getOptionalValue[String]("description")

    val uri = getValue[String]("uri")
    val headers = readMap("headers", c)
    val payloads = readMap("payloads", c)
    val validStatusCodes = getValue[IntList]("validStatusCodes", default = Some(200 :: 201 :: 202 :: Nil))

    (name, description, uri, validStatusCodes, invalidKeys) match {
      case (Right(n), Right(d), Right(u), Right(vsc), Right(_)) => 
        (Right(HTTPExecute(n, d, new URI(u), headers, payloads, vsc, params)), graph)
      case _ =>
        val allErrors: Errors = List(uri, description, validStatusCodes, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }

  def readJDBCExecute(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "jdbcURL" :: "authentication" :: "params" :: "password" :: "sqlParams" :: "user" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    val description = getOptionalValue[String]("description")

    val authentication = readAuthentication("authentication")  

    val uriKey = "inputURI"
    val inputURI = getValue[String](uriKey)
    val parsedURI = inputURI.rightFlatMap(uri => parseURI(uriKey, uri))
    val inputSQL = parsedURI.rightFlatMap { uri =>
        authentication.right.map(auth => CloudUtils.setHadoopConfiguration(auth))    
        getBlob(uriKey, uri)
    }

    val jdbcURL = getValue[String]("jdbcURL")
    val driver = jdbcURL.rightFlatMap(uri => getJDBCDriver("jdbcURL", uri))
    val user = getOptionalValue[String]("user")
    val password = getOptionalValue[String]("password")

    val sqlParams = readMap("sqlParams", c)    

    (name, description, inputURI, inputSQL, jdbcURL, user, password, driver, invalidKeys) match {
      case (Right(n), Right(desc), Right(in), Right(sql), Right(url), Right(u), Right(p), Right(d), Right(_)) => 
        (Right(JDBCExecute(n, desc, new URI(in), url, u, p, sql, sqlParams, params)), graph)
      case _ =>
        val allErrors: Errors = List(name, description, inputURI, parsedURI, inputSQL, jdbcURL, user, password, driver, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }

  def readKafkaCommitExecute(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "bootstrapServers" :: "groupID" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    val description = getOptionalValue[String]("description")

    val inputView = getValue[String]("inputView")
    val bootstrapServers = getValue[String]("bootstrapServers")
    val groupID = getValue[String]("groupID")

    (name, description, inputView, bootstrapServers, groupID, invalidKeys) match {
      case (Right(n), Right(d), Right(iv), Right(bs), Right(g), Right(_)) => 
        (Right(KafkaCommitExecute(n, d, iv, bs, g, params)), graph)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, bootstrapServers, groupID, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }    

  def readPipelineExecute(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String], argsMap: collection.mutable.Map[String, String], arcContext: ARCContext)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "uri" :: "authentication" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)      

    val description = getOptionalValue[String]("description")

    val uri = getValue[String]("uri")
    val authentication = readAuthentication("authentication")  
    authentication.right.map(auth => CloudUtils.setHadoopConfiguration(auth))    

    (name, description, uri, invalidKeys) match {
      case (Right(n), Right(d), Right(u), Right(_)) => 
        val uri = new URI(u)
        val subPipeline = parseConfig(Right(uri), argsMap, graph, arcContext)
        subPipeline match {
          case Right(etl) => (Right(PipelineExecute(n, d, uri, etl._1)), graph)
          case Left(errors) => {
            val stageErrors = errors.collect { case s: StageError => s }
            (Left(stageErrors), graph)
          }
        }
      case _ =>
        val allErrors: Errors = List(uri, description, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }

  // validate

  def readEqualityValidate(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "leftView" :: "rightView" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)    

    val description = getOptionalValue[String]("description")

    val leftView = getValue[String]("leftView")
    val rightView = getValue[String]("rightView")

    (name, description, leftView, rightView, invalidKeys) match {
      case (Right(n), Right(d), Right(l), Right(r), Right(_)) => 
        (Right(EqualityValidate(n, d, l, r, params)), graph)
      case _ =>
        val allErrors: Errors = List(name, description, leftView, rightView, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }  
  
  def readSQLValidate(idx: Int, graph: Graph, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "authentication" :: "sqlParams" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)    

    val description = getOptionalValue[String]("description")

    val authentication = readAuthentication("authentication")  

    val uriKey = "inputURI"
    val inputURI = getValue[String](uriKey)
    val parsedURI = inputURI.rightFlatMap(uri => parseURI(uriKey, uri))
    val inputSQL = parsedURI.rightFlatMap{ uri => textContentForURI(uri, uriKey, authentication) }

    val sqlParams = readMap("sqlParams", c) 

    // try to verify if sql is technically valid against HQL dialect (will not check dependencies)
    val validSQL = inputSQL.rightFlatMap { sql =>
      validateSQL(uriKey, SQLUtils.injectParameters(sql, sqlParams))
    }    

    (name, description, parsedURI, inputSQL, validSQL, invalidKeys) match {
      case (Right(n), Right(d), Right(uri), Right(sql), Right(vsql), Right(_)) => 
        // pass the unreplaced input sql not the 'valid sql' as the paramenters will be replaced when the stage is executed for testing
        (Right(SQLValidate(n, d, uri, sql, sqlParams, params)), graph)
      case _ =>
        val allErrors: Errors = List(name, description, parsedURI, inputSQL, validSQL, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(idx, stageName, c.origin.lineNumber, allErrors)
        (Left(err :: Nil), graph)
    }
  }

  def readCustomStage(idx: Int, graph: Graph, stageType: String, name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): (Either[List[StageError], PipelineStage], Graph) = {
    val loader = Utils.getContextOrSparkClassLoader
    val serviceLoader = ServiceLoader.load(classOf[PipelineStagePlugin], loader)

    val customStage = serviceLoader.iterator().asScala.find( _.getClass.getName == stageType)

    // set containsPipelineStagePlugin true
    var outputGraph = Graph(graph.vertices, graph.edges, true)

    customStage match {
      case Some(cs) =>
        // validate stage
        name match {
          case Right(n) =>
            (Right(CustomStage(n, params, cs)), outputGraph)
          case Left(e) =>
            val err = StageError(idx, s"unnamed stage: $stageType", c.origin.lineNumber, e)
            (Left(err :: Nil), graph)
        }
      case None =>
        (Left(StageError(idx, "unknown", c.origin.lineNumber, ConfigError("stages", Some(c.origin.lineNumber), s"Unknown stage type: '${stageType}'") :: Nil) :: Nil), graph)
    }
  }

  def readPipeline(c: Config, configMD5: String, uri: String, argsMap: collection.mutable.Map[String, String], graph: Graph, arcContext: ARCContext)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Either[List[Error], (ETLPipeline, Graph)] = {
    import ConfigReader._

    val startTime = System.currentTimeMillis() 
    val configStages = c.getObjectList("stages")

    logger.info()
      .field("event", "validateConfig")
      .field("uri", uri)        
      .field("content-md5", configMD5)
      .log()    

    val (stages, errors, dependencyGraph) = configStages.asScala.zipWithIndex.foldLeft[(List[PipelineStage], List[StageError], Graph)]( (Nil, Nil, graph) ) { case ( (stages, errs, graph), (stage, idx) ) =>

      implicit val s = stage.toConfig
      val name: StringConfigValue = getValue[String]("name")
      val params = readMap("params", s)
      val environments = if (s.hasPath("environments")) s.getStringList("environments").asScala.toList else Nil
      val _type = getValue[String]("type")

      // deprecation message to override empty environments
      val depricationEnvironments = environments match {
        case Nil => {
          logger.warn()
            .field("event", "deprecation")
            .field("name", name.right.getOrElse("unnamed stage"))
            .field("type", _type.right.getOrElse("unknown"))                
            .field("message", "'environments' key will be required in next release. defaulting to ['prd', 'ppd', 'tst', 'dev']")        
            .log()

          List("prd", "ppd", "tst", "dev")
        }
        case _ => environments
      }

      // skip stage if not in environment
      if (!arcContext.ignoreEnvironments && !depricationEnvironments.contains(arcContext.environment)) {
        logger.trace()
          .field("event", "validateConfig")
          .field("name", name.right.getOrElse("unnamed stage"))
          .field("type", _type.right.getOrElse("unknown"))              
          .field("message", "skipping stage due to environment configuration")       
          .field("skipStage", true)
          .field("environment", arcContext.environment)               
          .list("environments", depricationEnvironments.asJava)               
          .log()    
        
        (stages, errs, graph)
      } else {
        logger.trace()
          .field("event", "validateConfig")
          .field("name", name.right.getOrElse("unnamed stage"))
          .field("type", _type.right.getOrElse("unknown"))              
          .field("skipStage", false)
          .field("environment", arcContext.environment)               
          .list("environments", depricationEnvironments.asJava)               
          .log()   

        val (stageOrError: Either[List[StageError], PipelineStage], newGraph: Graph) = _type match {

          case Right("AvroExtract") => readAvroExtract(idx, graph, name, params)
          case Right("AzureCosmosDBExtract") => readAzureCosmosDBExtract(idx, graph, name, params)
          case Right("BytesExtract") => readBytesExtract(idx, graph, name, params)
          case Right("DatabricksDeltaExtract") => readDatabricksDeltaExtract(idx, graph, name, params)
          case Right("DelimitedExtract") => readDelimitedExtract(idx, graph, name, params)
          case Right("ElasticsearchExtract") => readElasticsearchExtract(idx, graph, name, params)
          case Right("HTTPExtract") => readHTTPExtract(idx, graph, name, params)
          case Right("ImageExtract") => readImageExtract(idx, graph, name, params)
          case Right("JDBCExtract") => readJDBCExtract(idx, graph, name, params)
          case Right("JSONExtract") => readJSONExtract(idx, graph, name, params)
          case Right("KafkaExtract") => readKafkaExtract(idx, graph, name, params)
          case Right("ORCExtract") => readORCExtract(idx, graph, name, params)
          case Right("ParquetExtract") => readParquetExtract(idx, graph, name, params)
          case Right("RateExtract") => readRateExtract(idx, graph, name, params)
          case Right("TextExtract") => readTextExtract(idx, graph, name, params)
          case Right("XMLExtract") => readXMLExtract(idx, graph, name, params)

          case Right("DiffTransform") => readDiffTransform(idx, graph, name, params)
          case Right("HTTPTransform") => readHTTPTransform(idx, graph, name, params)
          case Right("JSONTransform") => readJSONTransform(idx, graph, name, params)
          case Right("MetadataFilterTransform") => readMetadataFilterTransform(idx, graph, name, params)
          case Right("MLTransform") => readMLTransform(idx, graph, name, params)
          case Right("SQLTransform") => readSQLTransform(idx, graph, name, params)
          case Right("TensorFlowServingTransform") => readTensorFlowServingTransform(idx, graph, name, params)
          case Right("TypingTransform") => readTypingTransform(idx, graph, name, params)

          case Right("AvroLoad") => readAvroLoad(idx, graph, name, params)
          case Right("AzureEventHubsLoad") => readAzureEventHubsLoad(idx, graph, name, params)
          case Right("ConsoleLoad") => readConsoleLoad(idx, graph, name, params)
          case Right("DatabricksDeltaLoad") => readDatabricksDeltaLoad(idx, graph, name, params)
          case Right("DatabricksSQLDWLoad") => readDatabricksSQLDWLoad(idx, graph, name, params)
          case Right("DelimitedLoad") => readDelimitedLoad(idx, graph, name, params)
          case Right("ElasticsearchLoad") => readElasticsearchLoad(idx, graph, name, params)
          case Right("HTTPLoad") => readHTTPLoad(idx, graph, name, params)
          case Right("JDBCLoad") => readJDBCLoad(idx, graph, name, params)
          case Right("JSONLoad") => readJSONLoad(idx, graph, name, params)
          case Right("KafkaLoad") => readKafkaLoad(idx, graph, name, params)
          case Right("ORCLoad") => readORCLoad(idx, graph, name, params)
          case Right("ParquetLoad") => readParquetLoad(idx, graph, name, params)
          case Right("TextLoad") => readTextLoad(idx, graph, name, params)
          case Right("XMLLoad") => readXMLLoad(idx, graph, name, params)

          case Right("HTTPExecute") => readHTTPExecute(idx, graph, name, params)
          case Right("JDBCExecute") => readJDBCExecute(idx, graph, name, params)
          case Right("KafkaCommitExecute") => readKafkaCommitExecute(idx, graph, name, params)
          case Right("PipelineExecute") => readPipelineExecute(idx, graph, name, params, argsMap, arcContext)

          case Right("EqualityValidate") => readEqualityValidate(idx, graph, name, params)
          case Right("SQLValidate") => readSQLValidate(idx, graph, name, params)

          case Right(stageType) => readCustomStage(idx, graph, stageType, name, params)
          case _ => (Left(StageError(idx, "unknown", s.origin.lineNumber, ConfigError("stages", Some(c.origin.lineNumber), s"Unknown stage type: '${_type}'") :: Nil) :: Nil), graph)
        }

        stageOrError match {
          case Right(PipelineExecute(_, _, _, subPipeline)) => (subPipeline.stages.reverse ::: stages, errs, newGraph)
          case Right(s) => (s :: stages, errs, newGraph)
          case Left(stageErrors) => (stages, stageErrors ::: errs, newGraph)
        }
      }
    }

    val errorsOrPipeline = errors match {
      case Nil => {
        val stagesReversed = stages.reverse
        val dependencyGraphReversed = Graph(dependencyGraph.vertices.reverse, dependencyGraph.edges.reverse, dependencyGraph.containsPipelineStagePlugin)

        // print optimisation opportunity messages
        dependencyGraphReversed.vertices.foreach { case (vertex) =>
          val edgesFrom = dependencyGraphReversed.edges.filter { edge => edge.source == vertex }
          if (edgesFrom.length > 1) {
            val stage = stagesReversed(vertex.stageId)

            stage match {
              case s: PersistableExtract => {
                if (!s.persist) {
                  logger.info()
                    .field("event", "validateConfig")
                    .field("type", "optimization")
                    .field("message", s"output of stage ${vertex.stageId} '${s.name}' used in multiple (${edgesFrom.length}) downstream stages and 'persist' is false. consider setting 'persist' to true to improve job performance")
                    .log()
                }
              }
              case s: PersistableTransform => {
                if (!s.persist) {
                  logger.info()
                    .field("event", "validateConfig")
                    .field("type", "optimization")
                    .field("message", s"output of stage ${vertex.stageId} '${s.name}' used in multiple (${edgesFrom.length}) downstream stages and 'persist' is false. consider setting 'persist' to true to improve job performance")
                    .log()
                }
              }
              case _ =>
            }
          }
        }

        Right(ETLPipeline(stagesReversed), dependencyGraphReversed)
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
