package ai.tripl.arc.config

import java.net.URI
import java.net.InetAddress
import java.sql.DriverManager

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.GlobPattern
import org.apache.spark.unsafe.types.UTF8String

import com.typesafe.config._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.util.CloudUtils
import ai.tripl.arc.util.ControlUtils._
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.ExtractUtils
import ai.tripl.arc.util.SQLUtils
import ai.tripl.arc.util.MetadataSchema


import Error._

object ConfigUtils {

  def readMap(path: String, c: Config): Map[String, String] = {
    if (c.hasPath(path)) {
      val params = c.getConfig(path).entrySet
      (for (e <- params.asScala) yield {

        // regex replaceall finds leading or trailing double quote(") characters
        // typesafe config will emit them if the key string contains dot (.) characters because they could be used as a reference
        val k = e.getKey.replaceAll("^\"|\"$", "")
        val v = e.getValue.unwrapped.toString
        
        // append string value to map
        k -> v
      }).toMap
    } else {
      Map.empty
    }
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

  def levenshteinDistance(keys: Seq[String], input: String)(limit: Int): Seq[String] = {
    val inputUTF8 = UTF8String.fromString(input)
    for {
      k <- keys
      v = inputUTF8.levenshteinDistance(UTF8String.fromString(k)) if v < limit
    } yield k
  } 

  def parseGlob(path: String)(glob: String)(implicit c: Config): Either[Errors, String] = {
    def err(lineNumber: Option[Int], msg: String): Either[Errors, String] = Left(ConfigError(path, lineNumber, msg) :: Nil)

    try {
      // try to compile glob which will fail with bad characters
      GlobPattern.compile(glob)
      Right(glob)
    } catch {
      case e: Exception => err(Some(c.getValue(path).origin.lineNumber()), e.getMessage)
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
              val endpoint = authentication.get("endpoint") match {
                case Some(v) => {
                  // try to resolve URI
                  val uri = new URI(v)
                  val inetAddress = InetAddress.getByName(uri.getHost)
                  val replacedURI = new URI(uri.getScheme, uri.getUserInfo, inetAddress.getHostAddress, uri.getPort, uri.getPath, uri.getQuery, uri.getFragment)
                  Option(replacedURI.toString)
                }
                case None => None
              }
              val sslEnabled = authentication.get("sslEnabled") match {
                case Some(v) => {
                  try {
                    Option(v.toBoolean)
                  } catch {
                    case e: Exception => throw new IllegalArgumentException(s"Authentication method 'AmazonAccessKey' expects 'sslEnabled' parameter to be boolean.")
                  }
                }                
                case None => None
              }          
              Right(Some(Authentication.AmazonAccessKey(accessKeyID, secretAccessKey, endpoint, sslEnabled)))
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

  def parseURI(path: String)(uri: String)(implicit c: Config): Either[Errors, URI] = {
    def err(lineNumber: Option[Int], msg: String): Either[Errors, URI] = Left(ConfigError(path, lineNumber, msg) :: Nil)

    try {
      // try to parse uri
      Right(new URI(uri))
    } catch {
      case e: Exception => err(Some(c.getValue(path).origin.lineNumber()), e.getMessage)
    }
  }

  def getExtractColumns(uriKey: String, authentication: Either[Errors, Option[Authentication]])(uri: URI)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): Either[Errors, List[ExtractColumn]] = {
    val schema = textContentForURI(uri, uriKey, authentication).rightFlatMap(text => Right(Option(text)))

    schema.rightFlatMap { sch =>
      val cols = sch.map{ s => MetadataSchema.parseJsonMetadata(s) }.getOrElse(Right(Nil))

      cols match {
        case Left(errs) => Left(errs.map( e => ConfigError("metadata error", None, Error.pipelineSimpleErrorMsg(e.errors)) ))
        case Right(extractColumns) => Right(extractColumns)
      }
    }
  }

  def textContentForURI(uri: URI, uriKey: String, authentication: Either[Errors, Option[Authentication]])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): Either[Errors, String] = {
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

  def getBlob(path: String, uri: URI)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): Either[Errors, String] = {
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

  def parseEncoding(path: String)(encoding: String)(implicit c: Config): Either[Errors, EncodingType] = {
    encoding.toLowerCase.trim match {
      case "base64" => Right(EncodingTypeBase64)
      case "hexadecimal" => Right(EncodingTypeHexadecimal)
      case _ => Left(ConfigError(path, None, s"invalid state please raise issue.") :: Nil)
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

  def parseDelimiter(path: String)(delim: String)(implicit c: Config): Either[Errors, Delimiter] = {
    delim.toLowerCase.trim match {
      case "comma" => Right(Delimiter.Comma)
      case "defaulthive" => Right(Delimiter.DefaultHive)
      case "pipe" => Right(Delimiter.Pipe)
      case "custom" => Right(Delimiter.Custom)
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
  
  def getJDBCDriver(path: String, uri: String)(implicit c: Config): Either[Errors, java.sql.Driver] = {
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

  // inject params inline
  def injectSQLParams(path: String, sqlParams: Map[String, String], allowMissing: Boolean)(sql: String)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): Either[Errors, String] = {
    def err(lineNumber: Option[Int], msg: String): Either[Errors, String] = Left(ConfigError(path, lineNumber, msg) :: Nil)
    
    try {
      Right(SQLUtils.injectParameters(sql, sqlParams, allowMissing))
    } catch {
      case e: Exception => err(Some(c.getValue(path).origin.lineNumber()), e.getMessage)
    }
  }    

  // validateSQL uses the parsePlan method to verify if the sql command is parseable/valid. it will not check table existence.
  def validateSQL(path: String)(sql: String)(implicit spark: SparkSession, c: Config): Either[Errors, String] = {
    def err(lineNumber: Option[Int], msg: String): Either[Errors, String] = Left(ConfigError(path, lineNumber, msg) :: Nil)

    try {
      val parser = spark.sessionState.sqlParser
      parser.parsePlan(sql)
      Right(sql)
    } catch {
      case e: Exception => err(Some(c.getValue(path).origin.lineNumber()), e.getMessage)
    }
  }    
}