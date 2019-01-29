package au.com.agl.arc.util

import java.net.URI
import java.sql.DriverManager
import java.util.ServiceLoader

import scala.collection.JavaConverters._
import scala.util.Properties._
import com.typesafe.config._
import org.apache.hadoop.fs.GlobPattern
import org.apache.spark.SparkFiles
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql._
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

  def parsePipeline(configUri: Option[String], argsMap: collection.mutable.Map[String, String], arcContext: ARCContext)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Either[List[Error], ETLPipeline] = {
    configUri match {
      case Some(uri) => parseConfig(new URI(uri), argsMap, arcContext)
      case None => Left(ConfigError("file", None, s"No config defined as a command line argument --etl.config.uri or ETL_CONF_URI environment variable.") :: Nil)
     }
  }

  def getConfigString(uri: URI, argsMap: collection.mutable.Map[String, String], arcContext: ARCContext)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Either[List[Error], String] = {
    uri.getScheme match {
      case "local" => {
        val filePath = SparkFiles.get(uri.getPath)
        val etlConfString = CloudUtils.getTextBlob(filePath)
        Right(etlConfString)
      }
      case "file" => {
        val etlConfString = CloudUtils.getTextBlob(uri.toString)
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
        val etlConfString = CloudUtils.getTextBlob(uri.toString)
        Right(etlConfString)
      }
      // azure blob
      case "wasbs" => {
        val azureAccountName: Option[String] = argsMap.get("etl.config.fs.azure.account.name").orElse(envOrNone("ETL_CONF_AZURE_ACCOUNT_NAME"))
        val azureAccountKey: Option[String] = argsMap.get("etl.config.fs.azure.account.key").orElse(envOrNone("ETL_CONF_AZURE_ACCOUNT_KEY"))

        val accountName = azureAccountName match {
          case Some(value) => value
          case None => throw new IllegalArgumentException(s"Azure Account Name not provided for: ${uri}. Set etl.config.fs.azure.account.nameproperty or ETL_CONF_AZURE_ACCOUNT_NAME environment variable.")
        }
        val accountKey = azureAccountKey match {
          case Some(value) => value
          case None => throw new IllegalArgumentException(s"Azure Account Key not provided for: ${uri}. Set etl.config.fs.azure.account.key property or ETL_CONF_AZURE_ACCOUNT_KEY environment variable.")
        }

        CloudUtils.setHadoopConfiguration(Some(Authentication.AzureSharedKey(accountName, accountKey)))
        val etlConfString = CloudUtils.getTextBlob(uri.toString)
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
        val etlConfString = CloudUtils.getTextBlob(uri.toString)
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
        val etlConfString = CloudUtils.getTextBlob(uri.toString)
        Right(etlConfString)
      }
      case "classpath" => {
        val path = s"/${uri.getHost}${uri.getPath}"
        val etlConfString = using(getClass.getResourceAsStream(path)) { is =>
          scala.io.Source.fromInputStream(is).mkString
        }
        Right(etlConfString)
      }
      case _ => {
        Left(ConfigError("file", None, "make sure url scheme is defined e.g. file://${pwd}") :: Nil)
      }
    }
  }

  def parseConfig(uri: URI, argsMap: collection.mutable.Map[String, String], arcContext: ARCContext)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Either[List[Error], ETLPipeline] = {
    val base = ConfigFactory.load()

    val etlConfString = getConfigString(uri, argsMap, arcContext)

    etlConfString.rightFlatMap { str =>
      val etlConf = ConfigFactory.parseString(str, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))

      val pluginConfs: List[Config] = configPlugins(etlConf).map( c => ConfigFactory.parseMap(c.values()) )

      val config = etlConf.withFallback(base)

      val c = pluginConfs match {
        case Nil =>
          config.resolve()
        case _ =>
          val pluginConf = pluginConfs.reduceRight[Config]{ case (c1, c2) => c1.withFallback(c2) }
          val pluginValues = pluginConf.root().unwrapped()
          logger.info().message("Found additional config values from plugins").field("pluginConf", pluginValues).log()
          config.resolveWith(pluginConf).resolve()
      }

      readPipeline(c, uri, argsMap, arcContext)
    }
  }

  def configPlugins(c: Config): List[DynamicConfigurationPlugin] = {
    if (c.hasPath("plugins.config")) {
      val plugins =
        (for (p <- c.getStringList("plugins.config").asScala) yield {
          DynamicConfigurationPlugin.pluginForName(p).map(_ :: Nil)
        }).toList
      plugins.flatMap( p => p.getOrElse(Nil))
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

  sealed trait Error

  object Error {

    def errToString(err: Error): String = {
      err match {
        case StageError(stage, lineNumber, configErrors) => {
          s"""Stage '${stage}' (Line ${lineNumber}):\n${configErrors.map(e => "  - " + errToString(e)).mkString("\n")}"""
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
        case StageError(stage, lineNumber, configErrors) => {
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
        case StageError(stage, lineNumber, configErrors) => {  
          val stageErrorMap = new java.util.HashMap[String, Object]()
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

  case class StageError(stage: String, lineNumber: Int, errors: Errors) extends Error

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
      val textFile = CloudUtils.getTextBlob(uri.toString)
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


  // extract
  def readAvroExtract(name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "inputURI" :: "outputView" :: "authentication" :: "contiguousIndex" :: "numPartitions" :: "partitionBy" :: "persist" :: "schemaURI" :: "schemaView" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)

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

    (name, extractColumns, schemaView, inputURI, parsedGlob, outputView, persist, numPartitions, partitionBy, authentication, contiguousIndex, invalidKeys) match {
      case (Right(n), Right(cols), Right(sv), Right(in), Right(pg), Right(ov), Right(p), Right(np), Right(pb), Right(auth), Right(ci), Right(_)) =>
        val schema = if(c.hasPath("schemaView")) Left(sv) else Right(cols)

        Right(AvroExtract(n, schema, ov, pg, auth, params, p, np, pb, ci))
      case _ =>
        val allErrors: Errors = List(name, inputURI, schemaView, parsedGlob, outputView, persist, numPartitions, partitionBy, authentication, contiguousIndex, extractColumns, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }  

  def readBytesExtract(name: StringConfigValue, params: Map[String, String])(implicit c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "inputView" :: "inputURI" :: "outputView" :: "authentication" :: "contiguousIndex" :: "numPartitions" :: "persist" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)    

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

    (name, parsedGlob, inputView, outputView, persist, numPartitions, authentication, contiguousIndex, invalidKeys) match {
      case (Right(n), Right(pg), Right(iv), Right(ov), Right(p), Right(np), Right(auth), Right(ci), Right(_)) =>

        val validInput = (pg, iv) match {
          case (Some(_), None) => true
          case (None, Some(_)) => true
          case _ => false
        }

        if (validInput) {
          val input = if(c.hasPath("inputView")) Left(iv.get) else Right(pg.get)

          Right(BytesExtract(n, ov, input, auth, params, p, np, ci))
        } else {
          val inputError = ConfigError("inputURI:inputView", Some(c.getValue("inputURI").origin.lineNumber()), "Either inputURI and inputView must be defined but only one can be defined at the same time") :: Nil
          val stageName = stringOrDefault(name, "unnamed stage")
          val err = StageError(stageName, c.origin.lineNumber, inputError)
          Left(err :: Nil)
        }
      case _ =>
        val allErrors: Errors = List(name, inputURI, inputView, outputView, persist, numPartitions, authentication, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }


  def readDelimitedExtract(name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._
    import au.com.agl.arc.extract.DelimitedExtract._

    val expectedKeys = "type" :: "name" :: "environments" :: "inputView" :: "inputURI" :: "outputView" :: "delimiter" :: "quote" :: "header" :: "authentication" :: "contiguousIndex" :: "numPartitions" :: "params" :: "partitionBy" :: "persist" :: "schemaURI" :: "schemaView" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)

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

    val delimiter = getValue[String]("delimiter", default = Some("Comma"), validValues = "Comma" :: "Pipe" :: "DefaultHive" :: Nil) |> parseDelimiter("delimiter") _
    val quote = getValue[String]("quote", default =  Some("DoubleQuote"), validValues = "DoubleQuote" :: "SingleQuote" :: "None" :: Nil) |> parseQuote("quote") _
    val header = getValue[Boolean]("header", Some(false))

    (name, input, parsedGlob, extractColumns, schemaView, outputView, persist, numPartitions, partitionBy, header, authentication, contiguousIndex, delimiter, quote, invalidKeys) match {
      case (Right(n), Right(in), Right(pg), Right(cols), Right(sv), Right(ov), Right(p), Right(np), Right(pb), Right(head), Right(auth), Right(ci), Right(delim), Right(q), Right(_)) =>
        val input = if(c.hasPath("inputView")) Left(in) else Right(pg)
        val schema = if(c.hasPath("schemaView")) Left(sv) else Right(cols)
        val extract = DelimitedExtract(n, schema, ov, input, Delimited(header=head, sep=delim, quote=q), auth, params, p, np, pb, ci)
        Right(extract)
      case _ =>
        val allErrors: Errors = List(name, parsedGlob, extractColumns, outputView, persist, numPartitions, partitionBy, header, authentication, contiguousIndex, delimiter, quote, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }  

  def readHTTPExtract(name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "inputView" :: "inputURI" :: "outputView" :: "body" :: "headers" :: "method" :: "numPartitions" :: "partitionBy" :: "persist" :: "validStatusCodes" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)

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

    (name, input, parsedURI, outputView, persist, numPartitions, method, body, partitionBy, validStatusCodes, invalidKeys) match {
      case (Right(n), Right(in), Right(pu), Right(ov), Right(p), Right(np), Right(m), Right(b), Right(pb), Right(vsc), Right(_)) => 
        val inp = if(c.hasPath("inputView")) Left(in) else Right(pu)
        Right(HTTPExtract(n, inp, m, headers, b, vsc, ov, params, p, np, pb))
      case _ =>
        val allErrors: Errors = List(name, input, parsedURI, outputView, persist, numPartitions, method, body, partitionBy, validStatusCodes, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  } 

  def readImageExtract(name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "inputURI" :: "outputView" :: "authentication" :: "dropInvalid" :: "numPartitions" :: "partitionBy" :: "persist" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)    

    val inputURI = getValue[String]("inputURI")
    val parsedGlob = inputURI.rightFlatMap(glob => parseGlob("inputURI", glob))
    val outputView = getValue[String]("outputView")
    val persist = getValue[Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = if (c.hasPath("partitionBy")) c.getStringList("partitionBy").asScala.toList else Nil
    val authentication = readAuthentication("authentication")
    val dropInvalid = getValue[Boolean]("dropInvalid", default = Some(true))

    (name, inputURI, parsedGlob, outputView, persist, numPartitions, authentication, dropInvalid, invalidKeys) match {
      case (Right(n), Right(in), Right(pg), Right(ov), Right(p), Right(np), Right(auth), Right(di), Right(_)) => 
        Right(ImageExtract(n, ov, pg, auth, params, p, np, partitionBy, di))
      case _ =>
        val allErrors: Errors = List(name, inputURI, parsedGlob, outputView, persist, numPartitions, authentication, dropInvalid, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }   

  def readJDBCExtract(name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "jdbcURL" :: "tableName" :: "outputView" :: "authentication" :: "contiguousIndex" :: "fetchsize" :: "numPartitions" :: "params" :: "partitionBy" :: "partitionColumn" :: "persist" :: "predicates" :: "schemaURI" :: "schemaView" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

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

    (name, extractColumns, schemaView, outputView, persist, jdbcURL, driver, tableName, numPartitions, fetchsize, customSchema, partitionColumn, partitionBy, invalidKeys) match {
      case (Right(n), Right(cols), Right(sv), Right(ov), Right(p), Right(ju), Right(d), Right(tn), Right(np), Right(fs), Right(cs), Right(pc), Right(pb), Right(_)) => 
        val schema = if(c.hasPath("schemaView")) Left(sv) else Right(cols)
        Right(JDBCExtract(n, schema, ov, ju, tn, np, fs, cs, d, pc, params, p, pb, predicates))
      case _ =>
        val allErrors: Errors = List(name, outputView, schemaView, persist, jdbcURL, driver, tableName, numPartitions, fetchsize, customSchema, extractColumns, partitionColumn, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }   

  def readJSONExtract(name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "inputView" :: "inputURI" :: "outputView" :: "authentication" :: "contiguousIndex" :: "multiLine" :: "numPartitions" :: "partitionBy" :: "persist" :: "schemaURI" :: "schemaView" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)    

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

    (name, extractColumns, schemaView, input, parsedGlob, outputView, persist, numPartitions, multiLine, authentication, contiguousIndex, partitionBy, invalidKeys) match {
      case (Right(n), Right(cols), Right(sv), Right(in), Right(pg), Right(ov), Right(p), Right(np), Right(ml), Right(auth), Right(ci), Right(pb), Right(_)) => 
        val input = if(c.hasPath("inputView")) Left(in) else Right(pg)
        val schema = if(c.hasPath("schemaView")) Left(sv) else Right(cols)

        val json = JSON()
        val multiLine = ml match {
          case Some(b: Boolean) => b
          case _ => json.multiLine
        }
        Right(JSONExtract(n, schema, ov, input, JSON(multiLine=multiLine), auth, params, p, np, pb, ci))
      case _ =>
        val allErrors: Errors = List(name, input, schemaView, parsedGlob, outputView, persist, numPartitions, multiLine, authentication, contiguousIndex, extractColumns, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

  def readKafkaExtract(name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "outputView" :: "bootstrapServers" :: "topic" :: "groupID" :: "autoCommit" :: "maxPollRecords" :: "numPartitions" :: "partitionBy" :: "persist" :: "timeout" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)    

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

    (name, outputView, topic, bootstrapServers, groupID, persist, numPartitions, maxPollRecords, timeout, autoCommit, partitionBy, invalidKeys) match {
      case (Right(n), Right(ov), Right(t), Right(bs), Right(g), Right(p), Right(np), Right(mpr), Right(time), Right(ac), Right(pb), Right(_)) => 
        Right(KafkaExtract(n, ov, t, bs, g, mpr, time, ac, params, p, np, pb))
      case _ =>
        val allErrors: Errors = List(name, outputView, topic, bootstrapServers, groupID, persist, numPartitions, maxPollRecords, timeout, autoCommit, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }  

  def readORCExtract(name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "inputURI" :: "outputView" :: "authentication" :: "contiguousIndex" :: "numPartitions" :: "partitionBy" :: "persist" :: "schemaURI" :: "schemaView" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)

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

    (name, extractColumns, schemaView, inputURI, parsedGlob, outputView, persist, numPartitions, authentication, contiguousIndex, invalidKeys, partitionBy) match {
      case (Right(n), Right(cols), Right(sv), Right(in), Right(pg), Right(ov), Right(p), Right(np), Right(auth), Right(ci), Right(_), Right(pb)) => 
        val schema = if(c.hasPath("schemaView")) Left(sv) else Right(cols)
        Right(ORCExtract(n, schema, ov, pg, auth, params, p, np, pb, ci))
      case _ =>
        val allErrors: Errors = List(name, inputURI, schemaView, parsedGlob, outputView, persist, numPartitions, authentication, contiguousIndex, extractColumns, invalidKeys, partitionBy).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }  

  def readParquetExtract(name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "inputURI" :: "outputView" :: "authentication" :: "contiguousIndex" :: "numPartitions" :: "partitionBy" :: "persist" :: "schemaURI" :: "schemaView" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)

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

    (name, extractColumns, schemaView, inputURI, parsedGlob, outputView, persist, numPartitions, authentication, contiguousIndex, partitionBy, invalidKeys) match {
      case (Right(n), Right(cols), Right(sv), Right(in), Right(pg), Right(ov), Right(p), Right(np), Right(auth), Right(ci), Right(pb), Right(_)) => 
        val schema = if(c.hasPath("schemaView")) Left(sv) else Right(cols)
        Right(ParquetExtract(n, schema, ov, pg, auth, params, p, np, pb, ci))
      case _ =>
        val allErrors: Errors = List(name, inputURI, schemaView, parsedGlob, outputView, persist, numPartitions, authentication, contiguousIndex, extractColumns, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

  def readRateExtract(name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "outputView" :: "rowsPerSecond" :: "rampUpTime" :: "numPartitions" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    val outputView = getValue[String]("outputView")
    val rowsPerSecond = getValue[Int]("rowsPerSecond", default = Some(1))
    val rampUpTime = getValue[Int]("rampUpTime", default = Some(1))
    val numPartitions = getValue[Int]("numPartitions", default = Some(spark.sparkContext.defaultParallelism))

    (name, outputView, rowsPerSecond, rampUpTime, numPartitions) match {
      case (Right(n), Right(ov), Right(rps), Right(rut), Right(np)) => 
        Right(RateExtract(n, ov, params, rps, rut, np))
      case _ =>
        val allErrors: Errors = List(name, outputView, rowsPerSecond, rampUpTime, numPartitions).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }  

  def readTextExtract(name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "inputURI" :: "outputView" :: "authentication" :: "contiguousIndex" :: "multiLine" :: "numPartitions" :: "persist" :: "schemaURI" :: "schemaView" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)

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

    (name, extractColumns, input, parsedGlob, outputView, persist, numPartitions, multiLine, authentication, contiguousIndex, invalidKeys) match {
      case (Right(n), Right(cols), Right(in), Right(pg), Right(ov), Right(p), Right(np), Right(ml), Right(auth), Right(ci), Right(_)) => 
        Right(TextExtract(n, Right(cols), ov, in, auth, params, p, np, ci, ml))
      case _ =>
        val allErrors: Errors = List(name, input, parsedGlob, outputView, persist, numPartitions, multiLine, authentication, contiguousIndex, extractColumns, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

  def readXMLExtract(name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "inputURI" :: "inputView" :: "outputView" :: "authentication" :: "contiguousIndex" :: "numPartitions" :: "partitionBy" :: "persist" :: "schemaURI" :: "schemaView" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)

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

    (name, extractColumns, schemaView, input, parsedGlob, outputView, persist, numPartitions, authentication, contiguousIndex, partitionBy, invalidKeys) match {
      case (Right(n), Right(cols), Right(sv), Right(in), Right(pg), Right(ov), Right(p), Right(np), Right(auth), Right(ci), Right(pb), Right(_)) => 
        val input = if(c.hasPath("inputView")) Left(in) else Right(pg)
        val schema = if(c.hasPath("schemaView")) Left(sv) else Right(cols)

        Right(XMLExtract(n, schema, ov, input, auth, params, p, np, pb, ci))
      case _ =>
        val allErrors: Errors = List(name, input, schemaView, parsedGlob, outputView, persist, numPartitions, authentication, contiguousIndex, extractColumns, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }


  // transform
  def readDiffTransform(name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "inputLeftView" :: "inputRightView" :: "outputIntersectionView" :: "outputLeftView" :: "outputRightView" :: "persist" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)    

    val inputLeftView = getValue[String]("inputLeftView")
    val inputRightView = getValue[String]("inputRightView")
    val outputIntersectionView = getOptionalValue[String]("outputIntersectionView")
    val outputLeftView = getOptionalValue[String]("outputLeftView")
    val outputRightView = getOptionalValue[String]("outputRightView")
    val persist = getValue[Boolean]("persist", default = Some(false))

    (name, inputLeftView, inputRightView, outputIntersectionView, outputLeftView, outputRightView, persist, invalidKeys) match {
      case (Right(n), Right(ilv), Right(irv), Right(oiv), Right(olv), Right(orv), Right(p), Right(_)) => 
        Right(DiffTransform(n, ilv, irv, oiv, olv, orv, params, p))
      case _ =>
        val allErrors: Errors = List(name, inputLeftView, inputRightView, outputIntersectionView, outputLeftView, outputRightView, persist, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  } 

  def readHTTPTransform(name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "inputView" :: "outputView" :: "uri" :: "headers" :: "inputField" :: "persist" :: "validStatusCodes" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)    

    val httpUriKey = "uri"
    val inputURI = getValue[String](httpUriKey)
    val inputField = getValue[String]("inputField", default = Some("value"))
    val parsedHttpURI = inputURI.rightFlatMap(uri => parseURI(httpUriKey, uri))
    val headers = readMap("headers", c)
    val inputView = getValue[String]("inputView")
    val outputView = getValue[String]("outputView")
    val persist = getValue[Boolean]("persist", default = Some(false))
    val validStatusCodes = getValue[IntList]("validStatusCodes", default = Some(200 :: 201 :: 202 :: Nil))

    (name, inputView, outputView, parsedHttpURI, persist, inputField, validStatusCodes, invalidKeys) match {
      case (Right(n), Right(iv), Right(ov), Right(uri), Right(p), Right(ifld), Right(vsc), Right(_)) => 
        Right(HTTPTransform(n, uri, headers, vsc, iv, ov, ifld, params, p))
      case _ =>
        val allErrors: Errors = List(name, inputView, outputView, parsedHttpURI, persist, inputField, validStatusCodes, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }  

  def readJSONTransform(name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "inputView" :: "outputView" :: "persist" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    val inputView = getValue[String]("inputView")
    val outputView = getValue[String]("outputView")
    val persist = getValue[Boolean]("persist", default = Some(false))

    (name, inputView, outputView, persist, invalidKeys) match {
      case (Right(n), Right(iv), Right(ov), Right(p), Right(_)) => 
        Right(JSONTransform(n, iv, ov, params, p))
      case _ =>
        val allErrors: Errors = List(name, inputView, outputView, persist, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }  

  def readMetadataFilterTransform(name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "inputURI" :: "inputView" :: "outputView" :: "authentication" :: "persist" :: "sqlParams" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

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

    // try to verify if sql is technically valid against HQL dialect (will not check dependencies)
    val validSQL = inputSQL.rightFlatMap { sql =>
      validateSQL(uriKey, SQLUtils.injectParameters(sql, sqlParams))
    }

    (name, parsedURI, inputSQL, validSQL, inputView, outputView, persist, invalidKeys) match {
      case (Right(n), Right(uri), Right(isql), Right(vsql), Right(iv), Right(ov), Right(p), Right(_)) => 
        Right(MetadataFilterTransform(n, iv, uri, vsql, ov, params, sqlParams, p))
      case _ =>
        val allErrors: Errors = List(name, inputURI, parsedURI, inputSQL, validSQL, inputView, outputView, persist, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }   

  def readMLTransform(name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "inputURI" :: "inputView" :: "outputView" :: "authentication" :: "persist" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

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

    (name, inputURI, inputModel, inputView, outputView, persist, invalidKeys) match {
      case (Right(n), Right(in), Right(mod), Right(iv), Right(ov), Right(p), Right(_)) => 
        val uri = new URI(in)
        Right(MLTransform(n, uri, mod, iv, ov, params, p))
      case _ =>
        val allErrors: Errors = List(name, inputURI, parsedURI, inputModel, inputView, outputView, persist, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

  def readSQLTransform(name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "inputURI" :: "outputView" :: "authentication" :: "persist" :: "sqlParams" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    val uriKey = "inputURI"
    val inputURI = getValue[String](uriKey)
    val parsedURI = inputURI.rightFlatMap(uri => parseURI(uriKey, uri))
    val authentication = readAuthentication("authentication")  
    val inputSQL = parsedURI.rightFlatMap{ uri => textContentForURI(uri, uriKey, authentication) }
    val outputView = getValue[String]("outputView")
    val persist = getValue[Boolean]("persist", default = Some(false))
    val sqlParams = readMap("sqlParams", c)

    // try to verify if sql is technically valid against HQL dialect (will not check dependencies)
    val validSQL = inputSQL.rightFlatMap { sql =>
      validateSQL(uriKey, SQLUtils.injectParameters(sql, sqlParams))
    }

    (name, parsedURI, inputSQL, validSQL, outputView, persist, invalidKeys) match {
      case (Right(n), Right(uri), Right(isql), Right(vsql), Right(ov), Right(p), Right(_)) => 

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

        Right(SQLTransform(n, uri, vsql, ov, params, sqlParams, p))
      case _ =>
        val allErrors: Errors = List(name, inputURI, parsedURI, inputSQL, validSQL, outputView, persist, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  } 

  def readTensorFlowServingTransform(name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "inputView" :: "outputView" :: "uri" :: "batchSize" :: "inputField" :: "params"  :: "persist" :: "responseType" :: "signatureName" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    val inputView = getValue[String]("inputView")
    val outputView = getValue[String]("outputView")
    val inputURI = getValue[String]("uri")
    val inputField = getValue[String]("inputField", default = Some("value"))
    val parsedURI = inputURI.rightFlatMap(uri => parseURI("uri", uri))
    val signatureName = getOptionalValue[String]("signatureName")
    val batchSize = getValue[Int]("batchsize", default = Some(1))
    val persist = getValue[Boolean]("persist", default = Some(false))
    val responseType = getValue[String]("responseType", default = Some("object"), validValues = "integer" :: "double" :: "object" :: Nil) |> parseResponseType("responseType") _

    (name, inputView, outputView, inputURI, parsedURI, signatureName, responseType, batchSize, persist, inputField, invalidKeys) match {
      case (Right(n), Right(iv), Right(ov), Right(uri), Right(puri), Right(sn), Right(rt), Right(bs), Right(p), Right(ifld), Right(_)) => 
        Right(TensorFlowServingTransform(n, iv, ov, puri, sn, rt, bs, ifld, params, p))
      case _ =>
        val allErrors: Errors = List(name, inputView, outputView, inputURI, parsedURI, signatureName, responseType, batchSize, persist, inputField, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

  def readTypingTransform(name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "inputURI" :: "inputView" :: "outputView" :: "authentication" :: "failMode" :: "persist" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

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

    (name, extractColumns, schemaView, inputView, outputView, persist, failMode, invalidKeys) match {
      case (Right(n), Right(cols), Right(sv), Right(iv), Right(ov), Right(p), Right(fm), Right(_)) => 
        val schema = if(c.hasPath("schemaView")) Left(sv) else Right(cols)

        Right(TypingTransform(n, schema, iv, ov, params, p, fm))
      case _ =>
        val allErrors: Errors = List(name, inputURI, parsedURI, extractColumns, schemaView, inputView, outputView, persist, authentication, failMode, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }   

  // load

  def readAvroLoad(name: StringConfigValue, params: Map[String, String])(implicit c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "inputView" :: "outputURI" :: "authentication" :: "numPartitions" :: "partitionBy" :: "saveMode" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    val inputView = getValue[String]("inputView")
    val outputURI = getValue[String]("outputURI")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val authentication = readAuthentication("authentication")  
    val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _

    (name, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys) match {
      case (Right(n), Right(iv), Right(out), Right(np), Right(auth), Right(sm), Right(pb), Right(_)) => 
        val uri = new URI(out)
        Right(AvroLoad(n, iv, uri, pb, np, auth, sm, params))
      case _ =>
        val allErrors: Errors = List(name, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }    

  def readAzureEventHubsLoad(name: StringConfigValue, params: Map[String, String])(implicit c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "inputView" :: "namespaceName" :: "eventHubName" :: "sharedAccessSignatureKeyName" :: "sharedAccessSignatureKey" :: "numPartitions" :: "retryCount" :: "retryMaxBackoff" :: "retryMinBackoff" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    val inputView = getValue[String]("inputView")
    val namespaceName = getValue[String]("namespaceName")
    val eventHubName = getValue[String]("eventHubName")
    val sharedAccessSignatureKeyName = getValue[String]("sharedAccessSignatureKeyName")
    val sharedAccessSignatureKey = getValue[String]("sharedAccessSignatureKey")
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val retryMinBackoff = getValue[Long]("retryMinBackoff", default = Some(0)) // DEFAULT_RETRY_MIN_BACKOFF = 0
    val retryMaxBackoff = getValue[Long]("retryMaxBackoff", default = Some(30)) // DEFAULT_RETRY_MAX_BACKOFF = 30
    val retryCount = getValue[Int]("retryCount", default = Some(10)) // DEFAULT_MAX_RETRY_COUNT = 10

    (name, inputView, namespaceName, eventHubName, sharedAccessSignatureKeyName, sharedAccessSignatureKey, numPartitions, retryMinBackoff, retryMaxBackoff, retryCount, invalidKeys) match {
      case (Right(n), Right(iv), Right(nn), Right(ehn), Right(saskn), Right(sask), Right(np), Right(rmin), Right(rmax), Right(rcount), Right(_)) => 
        Right(AzureEventHubsLoad(n, iv, nn, ehn, saskn, sask, np, rmin, rmax, rcount, params))
      case _ =>
        val allErrors: Errors = List(name, inputView, namespaceName, eventHubName, sharedAccessSignatureKeyName, sharedAccessSignatureKey, numPartitions, retryMinBackoff, retryMaxBackoff, retryCount, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }    

  def readConsoleLoad(name: StringConfigValue, params: Map[String, String])(implicit c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "inputView" :: "outputMode" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    val inputView = getValue[String]("inputView")
    val outputMode = getValue[String]("outputMode", default = Some("Append"), validValues = "Append" :: "Complete" :: "Update" :: Nil) |> parseOutputModeType("outputMode") _

    (name, inputView, outputMode, invalidKeys) match {
      case (Right(n), Right(iv), Right(om), Right(_)) => 
        Right(ConsoleLoad(n, iv, om, params))
      case _ =>
        val allErrors: Errors = List(name, inputView, outputMode, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }    

  def readDelimitedLoad(name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "inputView" :: "outputURI" :: "authentication" :: "delimiter" :: "header" :: "numPartitions" :: "partitionBy" :: "quote" :: "saveMode" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    val inputView = getValue[String]("inputView")
    val outputURI = getValue[String]("outputURI")
    val partitionBy = if (c.hasPath("partitionBy")) c.getStringList("partitionBy").asScala.toList else Nil    
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val authentication = readAuthentication("authentication")  
    val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _

    val delimiter = getValue[String]("delimiter", default = Some("Comma"), validValues = "Comma" :: "Pipe" :: "DefaultHive" :: Nil) |> parseDelimiter("delimiter") _
    val quote = getValue[String]("quote", default =  Some("DoubleQuote"), validValues = "DoubleQuote" :: "SingleQuote" :: "None" :: Nil) |> parseQuote("quote") _
    val header = getValue[Boolean]("header", Some(false))    

    (name, inputView, outputURI, numPartitions, authentication, saveMode, delimiter, quote, header, invalidKeys) match {
      case (Right(n), Right(in), Right(out),  Right(np), Right(auth), Right(sm), Right(d), Right(q), Right(h), Right(_)) => 
        val uri = new URI(out)
        val load = DelimitedLoad(n, in, uri, Delimited(header=h, sep=d, quote=q), partitionBy, np, auth, sm, params)
        Right(load)
      case _ =>
        val allErrors: Errors = List(name, inputView, outputURI, authentication, numPartitions, saveMode, delimiter, quote, header, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }    
  
  def readHTTPLoad(name: StringConfigValue, params: Map[String, String])(implicit c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "inputView" :: "outputURI" :: "headers" :: "validStatusCodes" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)     

    val inputView = getValue[String]("inputView")
    val inputURI = getValue[String]("outputURI")
    val parsedURI = inputURI.rightFlatMap(uri => parseURI("outputURI", uri))
    val headers = readMap("headers", c)
    val validStatusCodes = if (c.hasPath("validStatusCodes")) Some(c.getIntList("validStatusCodes").asScala.map(f => f.toInt).toList) else None

    (name, parsedURI, inputView, invalidKeys) match {
      case (Right(n), Right(uri), Right(iv), Right(_)) => 
        Right(HTTPLoad(n, iv, uri, headers, validStatusCodes, params))
      case _ =>
        val allErrors: Errors = List(name, parsedURI, inputView, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }  

  def readJDBCLoad(name: StringConfigValue, params: Map[String, String])(implicit c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "inputView" :: "jdbcURL" :: "tableName" :: "params" :: "batchsize" :: "bulkload" :: "createTableColumnTypes" :: "createTableOptions" :: "isolationLevel" :: "numPartitions" :: "saveMode" :: "tablock" :: "truncate" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)     

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

    (name, inputView, jdbcURL, driver, tableName, numPartitions, isolationLevel, batchsize, truncate, createTableOptions, createTableColumnTypes, saveMode, bulkload, tablock, partitionBy, invalidKeys) match {
      case (Right(n), Right(iv), Right(ju), Right(d), Right(tn), Right(np), Right(il), Right(bs), Right(t), Right(cto), Right(ctct), Right(sm), Right(bl), Right(tl), Right(pb), Right(_)) => 
        Right(JDBCLoad(n, iv, ju, tn, pb, np, il, bs, t, cto, ctct, sm, d, bl, tl, params))
      case _ =>
        val allErrors: Errors = List(name, inputView, jdbcURL, driver, tableName, numPartitions, isolationLevel, batchsize, truncate, createTableOptions, createTableColumnTypes, saveMode, bulkload, tablock, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }      

  def readJSONLoad(name: StringConfigValue, params: Map[String, String])(implicit c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "inputView" :: "outputURI" :: "authentication" :: "numPartitions" :: "partitionBy" :: "saveMode" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)      

    val inputView = getValue[String]("inputView")
    val outputURI = getValue[String]("outputURI")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val authentication = readAuthentication("authentication")  
    val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _

    (name, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys) match {
      case (Right(n), Right(iv), Right(out), Right(np), Right(auth), Right(sm), Right(pb), Right(_)) => 
        val uri = new URI(out)
        Right(JSONLoad(n, iv, uri, pb, np, auth, sm, params))
      case _ =>
        val allErrors: Errors = List(name, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

  def readKafkaLoad(name: StringConfigValue, params: Map[String, String])(implicit c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "inputView" :: "bootstrapServers" :: "topic" :: "acks" :: "batchSize" :: "numPartitions" :: "retries" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)     

    val inputView = getValue[String]("inputView")
    val bootstrapServers = getValue[String]("bootstrapServers")
    val topic = getValue[String]("topic")
    val acks = getValue[Int]("acks", default = Some(1))
    val retries = getValue[Int]("retries", default = Some(0))
    val batchSize = getValue[Int]("batchSize", default = Some(16384))

    val numPartitions = getOptionalValue[Int]("numPartitions")

    (name, inputView, topic, bootstrapServers, acks, retries, batchSize, numPartitions, invalidKeys) match {
      case (Right(n), Right(iv), Right(t), Right(bss), Right(a), Right(r), Right(bs), Right(np), Right(_)) => 
        Right(KafkaLoad(n, iv, t, bss, a, np, r, bs, params))
      case _ =>
        val allErrors: Errors = List(name, inputView, topic, bootstrapServers, acks, retries, batchSize, numPartitions, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }   

  def readORCLoad(name: StringConfigValue, params: Map[String, String])(implicit c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "inputView" :: "outputURI" :: "authentication" :: "numPartitions" :: "partitionBy" :: "saveMode" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)     

    val inputView = getValue[String]("inputView")
    val outputURI = getValue[String]("outputURI")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val authentication = readAuthentication("authentication")  
    val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _

    (name, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys) match {
      case (Right(n), Right(iv), Right(out), Right(np), Right(auth), Right(sm), Right(pb), Right(_)) => 
        val uri = new URI(out)
        Right(ORCLoad(n, iv, uri, pb, np, auth, sm, params))
      case _ =>
        val allErrors: Errors = List(name, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }  

  def readParquetLoad(name: StringConfigValue, params: Map[String, String])(implicit c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "inputView" :: "outputURI" :: "authentication" :: "numPartitions" :: "partitionBy" :: "saveMode" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    val inputView = getValue[String]("inputView")
    val outputURI = getValue[String]("outputURI")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val authentication = readAuthentication("authentication")  
    val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _

    (name, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys) match {
      case (Right(n), Right(iv), Right(out), Right(np), Right(auth), Right(sm), Right(pb), Right(_)) => 
        val uri = new URI(out)
        Right(ParquetLoad(n, iv, uri, pb, np, auth, sm, params))
      case _ =>
        val allErrors: Errors = List(name, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }  

  def readXMLLoad(name: StringConfigValue, params: Map[String, String])(implicit c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "inputView" :: "outputURI" :: "authentication" :: "numPartitions" :: "partitionBy" :: "saveMode" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    val inputView = getValue[String]("inputView")
    val outputURI = getValue[String]("outputURI")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val authentication = readAuthentication("authentication")  
    val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _

    (name, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys) match {
      case (Right(n), Right(iv), Right(out), Right(np), Right(auth), Right(sm), Right(pb), Right(_)) => 
        val uri = new URI(out)
        Right(XMLLoad(n, iv, uri, pb, np, auth, sm, params))
      case _ =>
        val allErrors: Errors = List(name, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }  

  // execute

  def readHTTPExecute(name: StringConfigValue, params: Map[String, String])(implicit logger: au.com.agl.arc.util.log.logger.Logger, c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "uri" :: "headers" :: "payloads" :: "validStatusCodes" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    val uri = getValue[String]("uri")
    val headers = readMap("headers", c)
    val payloads = readMap("payloads", c)
    val validStatusCodes = getValue[IntList]("validStatusCodes", default = Some(200 :: 201 :: 202 :: Nil))

    (name, uri, validStatusCodes, invalidKeys) match {
      case (Right(n), Right(u), Right(vsc), Right(_)) => 
        val uri = new URI(u)
        Right(HTTPExecute(n, uri, headers, payloads, vsc, params))
      case _ =>
        val allErrors: Errors = List(uri, validStatusCodes, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

  def readJDBCExecute(name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "inputURI" :: "jdbcURL" :: "authentication" :: "params" :: "password" :: "sqlParams" :: "user" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

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

    (name, inputURI, inputSQL, jdbcURL, user, password, driver, invalidKeys) match {
      case (Right(n), Right(in), Right(sql), Right(url), Right(u), Right(p), Right(d), Right(_)) => 
        val sqlFileUri = new URI(in)
        
        Right(JDBCExecute(n, sqlFileUri, url, u, p, sql, sqlParams, params))
      case _ =>
        val allErrors: Errors = List(name, inputURI, parsedURI, inputSQL, jdbcURL, user, password, driver, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

  def readKafkaCommitExecute(name: StringConfigValue, params: Map[String, String])(implicit c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "inputView" :: "bootstrapServers" :: "groupID" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    val inputView = getValue[String]("inputView")
    val bootstrapServers = getValue[String]("bootstrapServers")
    val groupID = getValue[String]("groupID")

    (name, inputView, bootstrapServers, groupID, invalidKeys) match {
      case (Right(n), Right(iv), Right(bs), Right(g), Right(_)) => 
        Right(KafkaCommitExecute(n, iv, bs, g, params))
      case _ =>
        val allErrors: Errors = List(name, inputView, bootstrapServers, groupID, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }    

  def readPipelineExecute(name: StringConfigValue, params: Map[String, String], argsMap: collection.mutable.Map[String, String], arcContext: ARCContext)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "uri" :: "authentication" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)      

    val uri = getValue[String]("uri")
    val authentication = readAuthentication("authentication")  
    authentication.right.map(auth => CloudUtils.setHadoopConfiguration(auth))    

    (name, uri, invalidKeys) match {
      case (Right(n), Right(u), Right(_)) => 
        val uri = new URI(u)
        val subPipeline = parseConfig(uri, argsMap, arcContext)
        subPipeline match {
          case Right(etl) => Right(PipelineExecute(n, uri, etl))
          case Left(errors) => {
            val stageErrors = errors.collect { case s: StageError => s }
            Left(stageErrors)
          }
        }
      case _ =>
        val allErrors: Errors = List(uri, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

  // validate

  def readEqualityValidate(name: StringConfigValue, params: Map[String, String])(implicit logger: au.com.agl.arc.util.log.logger.Logger, c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "leftView" :: "rightView" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)    

    val leftView = getValue[String]("leftView")
    val rightView = getValue[String]("rightView")

    (name, leftView, rightView, invalidKeys) match {
      case (Right(n), Right(l), Right(r), Right(_)) => 
        Right(EqualityValidate(n, l, r, params))
      case _ =>
        val allErrors: Errors = List(name, leftView, rightView, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }  
  
  def readSQLValidate(name: StringConfigValue, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, c: Config): Either[List[StageError], PipelineStage] = {
    import ConfigReader._

    val expectedKeys = "type" :: "name" :: "environments" :: "inputURI" :: "authentication" :: "sqlParams" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)    

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

    (name, parsedURI, inputSQL, validSQL, invalidKeys) match {
      case (Right(n), Right(uri), Right(sql), Right(vsql), Right(_)) => 
        Right(SQLValidate(n, uri, sql, sqlParams, params))
      case _ =>
        val allErrors: Errors = List(name, parsedURI, inputSQL, validSQL, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

  def readCustomStage(stageType: String, name: StringConfigValue, params: Map[String, String])(implicit c: Config): Either[List[StageError], PipelineStage] = {
    val loader = Utils.getContextOrSparkClassLoader
    val serviceLoader = ServiceLoader.load(classOf[PipelineStagePlugin], loader)

    val customStage = serviceLoader.iterator().asScala.find( _.getClass.getName == stageType)

    customStage match {
      case Some(cs) =>
        // validate stage
        name match {
          case Right(n) =>
            Right(CustomStage(n, params, cs))
          case Left(e) =>
            val err = StageError(s"unnamed stage: $stageType", c.origin.lineNumber, e)
            Left(err :: Nil)
        }
      case None =>
        Left(StageError("unknown", c.origin.lineNumber, ConfigError("stages", Some(c.origin.lineNumber), s"Unknown stage type: '${stageType}'") :: Nil) :: Nil)
    }
  }

  def readPipeline(c: Config, uri: URI, argsMap: collection.mutable.Map[String, String], arcContext: ARCContext)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Either[List[Error], ETLPipeline] = {
    import ConfigReader._

    val startTime = System.currentTimeMillis() 
    val configStages = c.getObjectList("stages")

    logger.info()
      .field("event", "validateConfig")
      .field("uri", uri.toString)        
      .log()    

    val pipelineStages: List[Either[List[StageError], PipelineStage]] =
      (for (stage <- configStages.asScala) yield {
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
          
          None
        } else {
            logger.trace()
              .field("event", "validateConfig")
              .field("name", name.right.getOrElse("unnamed stage"))
              .field("type", _type.right.getOrElse("unknown"))              
              .field("skipStage", false)
              .field("environment", arcContext.environment)               
              .list("environments", depricationEnvironments.asJava)               
              .log()   

          _type match {

            case Right("AvroExtract") => Option(readAvroExtract(name, params))
            case Right("BytesExtract") => Option(readBytesExtract(name, params))
            case Right("DelimitedExtract") => Option(readDelimitedExtract(name, params))
            case Right("HTTPExtract") => Option(readHTTPExtract(name, params))
            case Right("ImageExtract") => Option(readImageExtract(name, params))
            case Right("JDBCExtract") => Option(readJDBCExtract(name, params))
            case Right("JSONExtract") => Option(readJSONExtract(name, params))
            case Right("KafkaExtract") => Option(readKafkaExtract(name, params))
            case Right("ORCExtract") => Option(readORCExtract(name, params))
            case Right("ParquetExtract") => Option(readParquetExtract(name, params))
            case Right("RateExtract") => Option(readRateExtract(name, params))
            case Right("TextExtract") => Option(readTextExtract(name, params))
            case Right("XMLExtract") => Option(readXMLExtract(name, params))

            case Right("DiffTransform") => Option(readDiffTransform(name, params))
            case Right("HTTPTransform") => Option(readHTTPTransform(name, params))
            case Right("JSONTransform") => Option(readJSONTransform(name, params))
            case Right("MetadataFilterTransform") => Option(readMetadataFilterTransform(name, params))
            case Right("MLTransform") => Option(readMLTransform(name, params))
            case Right("SQLTransform") => Option(readSQLTransform(name, params))
            case Right("TensorFlowServingTransform") => Option(readTensorFlowServingTransform(name, params))
            case Right("TypingTransform") => Option(readTypingTransform(name, params))

            case Right("AvroLoad") => Option(readAvroLoad(name, params))
            case Right("AzureEventHubsLoad") => Option(readAzureEventHubsLoad(name, params))
            case Right("ConsoleLoad") => Option(readConsoleLoad(name, params))
            case Right("DelimitedLoad") => Option(readDelimitedLoad(name, params))
            case Right("HTTPLoad") => Option(readHTTPLoad(name, params))
            case Right("JDBCLoad") => Option(readJDBCLoad(name, params))
            case Right("JSONLoad") => Option(readJSONLoad(name, params))
            case Right("KafkaLoad") => Option(readKafkaLoad(name, params))
            case Right("ORCLoad") => Option(readORCLoad(name, params))
            case Right("ParquetLoad") => Option(readParquetLoad(name, params))
            case Right("XMLLoad") => Option(readXMLLoad(name, params))

            case Right("HTTPExecute") => Option(readHTTPExecute(name, params))
            case Right("JDBCExecute") => Option(readJDBCExecute(name, params))
            case Right("KafkaCommitExecute") => Option(readKafkaCommitExecute(name, params))
            case Right("PipelineExecute") => Option(readPipelineExecute(name, params, argsMap, arcContext))

            case Right("EqualityValidate") => Option(readEqualityValidate(name, params))
            case Right("SQLValidate") => Option(readSQLValidate(name, params))

            case Right(stageType) => Option(readCustomStage(stageType, name, params))
            case _ => Option(Left(StageError("unknown", s.origin.lineNumber, ConfigError("stages", Some(c.origin.lineNumber), s"Unknown stage type: '${_type}'") :: Nil) :: Nil))
          }
        }
      }).flatten.toList

    val (stages, errors) = pipelineStages.foldLeft[(List[PipelineStage], List[StageError])]( (Nil, Nil) ) { case ( (stages, errs), stageOrError ) =>
      stageOrError match {
        case Right(PipelineExecute(_, _, subPipeline)) => (subPipeline.stages.reverse ::: stages, errs)
        case Right(s) => (s :: stages, errs)
        case Left(stageErrors) => (stages, stageErrors ::: errs)
      }
    }

    logger.info()
      .field("event", "exit")
      .field("type", "readPipeline")        
      .field("duration", System.currentTimeMillis() - startTime)
      .log()      

    errors match {
      case Nil => Right(ETLPipeline(stages.reverse))
      case _ => Left(errors.reverse)
    }
  }

}
