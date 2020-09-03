package ai.tripl.arc.config

import java.net.URI
import java.net.InetAddress
import java.sql.DriverManager

import scala.collection.JavaConverters._
import scala.util.Properties._
import scala.util.{Try,Success,Failure}

import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.hadoop.fs.GlobPattern
import org.apache.spark.unsafe.types.UTF8String

import com.typesafe.config._

import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.util.CloudUtils
import ai.tripl.arc.util.ControlUtils._
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.SQLUtils
import ai.tripl.arc.util.ArcSchema

import Error._

object ConfigUtils {

  def getConfigString(uri: URI)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[Error], String] = {

    val isLocalMaster = spark.sparkContext.master.toLowerCase.startsWith("local")

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
      case "http" | "https" => {
        val etlConfString = CloudUtils.getTextBlob(uri)
        Right(etlConfString)
      }
      // databricks file system
      case "dbfs" => {
        val etlConfString = CloudUtils.getTextBlob(uri)
        Right(etlConfString)
      }
      // amazon s3
      // for local master throw error as some providers (Amazon EMR) redirect s3:// to use the s3a:// driver transparently
      case "s3" | "s3n" if isLocalMaster =>
        throw new Exception("s3:// and s3n:// are no longer supported. Please use s3a:// instead.")
      case "s3" | "s3a" => {

        val s3aBucket = uri.getAuthority
        val s3aAccessKey: Option[String] = arcContext.commandLineArguments.get("etl.config.fs.s3a.access.key").orElse(envOrNone("ETL_CONF_S3A_ACCESS_KEY"))
        val s3aSecretKey: Option[String] = arcContext.commandLineArguments.get("etl.config.fs.s3a.secret.key").orElse(envOrNone("ETL_CONF_S3A_SECRET_KEY"))
        val s3aEndpoint: Option[String] = arcContext.commandLineArguments.get("etl.config.fs.s3a.endpoint").orElse(envOrNone("ETL_CONF_S3A_ENDPOINT"))
        val s3aConnectionSSLEnabled: Option[String] = arcContext.commandLineArguments.get("etl.config.fs.s3a.connection.ssl.enabled").orElse(envOrNone("ETL_CONF_S3A_CONNECTION_SSL_ENABLED"))

        val s3aEncType: Option[AmazonS3EncryptionType] = arcContext.commandLineArguments.get("etl.config.fs.s3a.encryption.algorithm").orElse(envOrNone("ETL_CONF_S3A_ENCRYPTION_ALGORITHM")).flatMap(AmazonS3EncryptionType.fromString(_))
        val s3aKmsId: Option[String] = arcContext.commandLineArguments.get("etl.config.fs.s3a.kms.arn").orElse(envOrNone("ETL_CONF_S3A_KMS_ARN"))
        val s3aCustomKey: Option[String] = arcContext.commandLineArguments.get("etl.config.fs.s3a.custom.key").orElse(envOrNone("ETL_CONF_S3A_CUSTOM_KEY"))

        // try access key then anonmymous then default to iam
        (s3aAccessKey, s3aSecretKey, s3aEndpoint, s3aConnectionSSLEnabled, s3aEncType, s3aKmsId, s3aCustomKey) match {
          case (Some(accessKey), _, _, _, _, _, _) => {
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
            CloudUtils.setHadoopConfiguration(Some(Authentication.AmazonAccessKey(Option(s3aBucket), accessKey, secretKey, s3aEndpoint, connectionSSLEnabled)))
          }
          case (None, _, _, _, Some(AmazonS3EncryptionType.SSE_S3), None, None) =>
            CloudUtils.setHadoopConfiguration(Some(Authentication.AmazonIAM(Option(s3aBucket), s3aEncType, s3aKmsId, None)))
          case (None, _, _, _, Some(AmazonS3EncryptionType.SSE_KMS), None, None) =>
            CloudUtils.setHadoopConfiguration(Some(Authentication.AmazonIAM(Option(s3aBucket), s3aEncType, s3aKmsId, None)))
          case (None, _, _, _, Some(AmazonS3EncryptionType.SSE_C), None, None) =>
            CloudUtils.setHadoopConfiguration(Some(Authentication.AmazonIAM(Option(s3aBucket), s3aEncType, None, s3aCustomKey)))
          case _ =>
            CloudUtils.setHadoopConfiguration(Some(Authentication.AmazonIAM(Option(s3aBucket), None, None, None)))
        }

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

        (gsProjectID, gsKeyfilePath) match {
          case (Some(projectID), Some(keyFilePath)) =>
            CloudUtils.setHadoopConfiguration(Some(Authentication.GoogleCloudStorageKeyFile(projectID, keyFilePath)))
          case _ =>
        }

        val etlConfString = CloudUtils.getTextBlob(uri)
        Right(etlConfString)
      }
      case _ => {
        Left(ConfigError("file", None, "make sure url scheme is defined e.g. file://${pwd}") :: Nil)
      }
    }
  }

  // read an ipython notebook and convert to arc standard job string
  def readIPYNB(uri: Option[String], notebook: String): String = {
    val objectMapper = new ObjectMapper
    val jsonTree = objectMapper.readTree(notebook)

    val kernelspecName = jsonTree.get("metadata").get("kernelspec").get("name").asText
    if (kernelspecName == "arc") {

      val cells = jsonTree.get("cells").iterator.asScala
        .filter { cell =>
          // only include 'code' cells
          cell.get("cell_type").asText == "code"
        }.map { cell =>
          // convert the raw 'source' into a string
          cell.get("source").iterator.asScala
            .map { _.asText }
            .mkString
            .trim
            .replaceAll(",$", "")
        }.toList

        val (config, lifecycle, stages) = parseIPYNBCells(cells)

        s"""
          |{
          |"plugins": {
          |"config": [$config],
          |"lifecycle": [$lifecycle]
          |},
          |"stages": [$stages]
          |}""".stripMargin
    } else {
      throw new Exception(s"""file ${uri} does not appear to be a valid arc notebook. Has kernelspec: '${kernelspecName}'.""")
    }
  }

  def parseIPYNBCells(cells: List[String]): (String, String, String) = {
    // calculate the dynamic config plugins
    val configs = cells
      .filter { cell =>
        cell.startsWith("%configplugin")
      }.map { cell =>
        cell.split("\n").drop(1).mkString("\n")
      }

    // calculate the lifecycle plugins
    val lifecycles = cells
      .filter { cell =>
        cell.startsWith("%lifecycleplugin")
      }.map { cell =>
        cell.split("\n").drop(1).mkString("\n")
      }

      // calculate the arc stages
      val stages = cells
      .zipWithIndex
      .filter { case (cell, _) =>
        val behavior = cell.trim.toLowerCase
        (
          (!behavior.startsWith("%") && cell.length > 0) // non-magic, non-empty cells assumed to be arc
          || behavior.startsWith("%arc") // explicit arc cells
          || behavior.startsWith("%sql") // explicit sql cells
          || behavior.startsWith("%sqlvalidate") // explicit sqlvalidate cells
          || behavior.startsWith("%log") // explicit log cells
          || behavior.startsWith("%metadatafilter") // explicit log cells
          || behavior.startsWith("%metadatavalidate") // explicit log cells
        )
      }
      .map { case (cell, index) =>
        val lines = cell.split("\n")
        val behavior = lines(0).trim
        val command = lines.drop(1).mkString("\n")

        behavior match {
          case b: String if (!b.startsWith("%")) => cell
          case b: String if (b.toLowerCase.startsWith("%arc")) => command
          case b: String if (b.toLowerCase.startsWith("%")) => {
            val args = parseArgs(behavior).map {
              case (k,v) => (k, v.stripPrefix(""""""").stripSuffix(""""""").trim)
            }

            val environments = args.getOrElse("environments", "").split(",").mkString(""""""", """","""", """"""")

            val sqlParams = args.get("sqlParams") match {
              case Some(sqlParams) => {
                parseArgs(sqlParams.replace(",", " ")).map{
                  case (k, v) => {
                    if (v.trim().startsWith("${")) {
                      s""""${k}": ${v}"""
                    } else {
                      s""""${k}": "${v}""""
                    }
                  }
                }.mkString(",")
              }
              case None => ""
            }
            // these represent other parameters added to the 'behaviour line' which are not jupyter control values
            val dynamicArguments = args
              .filterKeys { !List("name", "description", "sqlParams", "outputView", "environments", "numRows", "truncate", "persist", "monospace", "leftAlign", "datasetLabels", "streamingDuration").contains(_) }
              .map { case (k, v) => s""""${k}": "${v}"""" }
              .mkString(",")

            behavior.toLowerCase match {
              case b if b.startsWith("%log") =>
                s"""{
                |  "type": "LogExecute",
                |  "name": "${args.getOrElse("name", s"notebook cell ${index}")}",
                |  "description": "${args.getOrElse("description", "")}",
                |  "environments": [${environments}],
                |  "sql": \"\"\"${command}\"\"\",
                |  "sqlParams": {${sqlParams}},
                |  ${dynamicArguments}
                |}""".stripMargin
              case b if b.startsWith("%metadatafilter") =>
                s"""{
                |  "type": "MetadataFilterTransform",
                |  "name": "${args.getOrElse("name", s"notebook cell ${index}")}",
                |  "description": "${args.getOrElse("description", "")}",
                |  "environments": [${environments}],
                |  "sql": \"\"\"${command}\"\"\",
                |  "sqlParams": {${sqlParams}},
                |  "outputView": "${args.getOrElse("outputView", "")}",
                |  "persist": ${args.getOrElse("persist", "false")},
                |  ${dynamicArguments}
                |}""".stripMargin
              case b if b.startsWith("%metadatavalidate") =>
                s"""{
                |  "type": "MetadataValidate",
                |  "name": "${args.getOrElse("name", s"notebook cell ${index}")}",
                |  "description": "${args.getOrElse("description", "")}",
                |  "environments": [${environments}],
                |  "sql": \"\"\"${command}\"\"\",
                |  "sqlParams": {${sqlParams}},
                |  ${dynamicArguments}
                |}""".stripMargin
              case b if b.startsWith("%sqlvalidate") =>
                s"""{
                |  "type": "SQLValidate",
                |  "name": "${args.getOrElse("name", s"notebook cell ${index}")}",
                |  "description": "${args.getOrElse("description", "")}",
                |  "environments": [${environments}],
                |  "sql": \"\"\"${command}\"\"\",
                |  "sqlParams": {${sqlParams}},
                |  ${dynamicArguments}
                |}""".stripMargin
              case b if b.startsWith("%sql") && !b.startsWith("%sqlvalidate") =>
                s"""{
                |  "type": "SQLTransform",
                |  "name": "${args.getOrElse("name", s"notebook cell ${index}")}",
                |  "description": "${args.getOrElse("description", "")}",
                |  "environments": [${environments}],
                |  "sql": \"\"\"${command}\"\"\",
                |  "sqlParams": {${sqlParams}},
                |  "outputView": "${args.getOrElse("outputView", "")}",
                |  "persist": ${args.getOrElse("persist", "false")},
                |  ${dynamicArguments}
                |}""".stripMargin
              case _ => ""
            }
          }
        }
      }


    (configs.mkString("\n", ",\n", "\n"), lifecycles.mkString("\n", ",\n", "\n"), stages.mkString("\n",",\n","\n"))
  }

  // defines rules for parsing arguments from the jupyter notebook
  def parseArgs(input: String): collection.mutable.Map[String, String] = {
    val args = collection.mutable.Map[String, String]()
    val (vals, opts) = input.split("\\s(?=([^\"']*\"[^\"]*\")*[^\"']*$)").partition {
      _.startsWith("%")
    }
    opts.map { x =>
      // regex split on only single = signs not at start or end of line
      val pair = x.split("=(?!=)(?!$)", 2)
      if (pair.length == 2) {
        args += (pair(0) -> pair(1))
      }
    }

    args
  }

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

  def checkValidKeys(c: Config)(expectedKeys: Seq[String]): Either[Errors, String] = {

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

  def checkOneOf(c: Config)(keys: Seq[String], required: Boolean = true): Either[Errors, Boolean] = {
    val setKeys = keys.map { key => if (c.hasPath(key)) 1 else 0 }.reduce (_ + _)

    if (setKeys == 0 && required) {
      Left(List(ConfigError("", None, s"""One of ${keys.map(field => s"'${field}'").mkString("[",", ","]")} must be set.""")))
    } else if (setKeys == 0 && !required) {
      Right(true)
    } else if (setKeys == 1) {
      Right(true)
    } else {
      Left(List(ConfigError("", None, s"""Only one attribute of '${keys.map(field => s"'${field}'").mkString("[",", ","]")} is allowed.""")))
    }
  }

  def levenshteinDistance(keys: Seq[String], input: String)(limit: Int): Seq[String] = {
    val inputUTF8 = UTF8String.fromString(input)
    for {
      k <- keys
      v = inputUTF8.levenshteinDistance(UTF8String.fromString(k)) if v < limit
    } yield k
  }

  def hasPath(path: String)(implicit c: Config): Either[Errors, ConfigValue] = {
    def err(lineNumber: Option[Int], msg: String): Either[Errors, ConfigValue] = Left(ConfigError(path, lineNumber, msg) :: Nil)

    if (!c.hasPath(path)) {
      err(None, s"""Missing required attribute '$path'.""")
    } else {
      Right(c.getValue(path))
    }
  }

  def parseGlob(path: String)(glob: String)(implicit spark: SparkSession, c: Config): Either[Errors, String] = {
    def err(lineNumber: Option[Int], msg: String): Either[Errors, String] = Left(ConfigError(path, lineNumber, msg) :: Nil)

    try {
      // try to compile glob which will fail with bad characters
      GlobPattern.compile(glob)

      val isLocalMaster = spark.sparkContext.master.toLowerCase.startsWith("local")
      if (isLocalMaster && (glob.trim.startsWith("s3://") | glob.trim.startsWith("s3n://"))) {
        throw new Exception("s3:// and s3n:// are no longer supported. Please use s3a:// instead.")
      }
      Right(glob)
    } catch {
      case e: Exception => err(Some(c.getValue(path).origin.lineNumber()), e.getMessage)
    }
  }

  def readAuthentication(path: String, uri: Option[String] = None)(implicit c: Config): Either[Errors, Option[Authentication]] = {

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
              val s3aBucket = getS3Bucket(uri)
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
              Right(Some(Authentication.AmazonAccessKey(s3aBucket, accessKeyID, secretAccessKey, endpoint, sslEnabled)))
            }
            case Some("AmazonAnonymous") => {
              val s3aBucket = getS3Bucket(uri)
              Right(Some(Authentication.AmazonAnonymous(s3aBucket)))
            }
            case Some("AmazonEnvironmentVariable") => {
              val s3aBucket = getS3Bucket(uri)
              Right(Some(Authentication.AmazonEnvironmentVariable(s3aBucket)))
            }
            case Some("AmazonIAM") => {
              val s3aBucket = getS3Bucket(uri)
              val encType = authentication.get("encryptionAlgorithm").flatMap( AmazonS3EncryptionType.fromString(_) )
              val kmsId = authentication.get("kmsArn")
              val customKey = authentication.get("customKey")

              (encType, kmsId, customKey) match {
                case (None, None, None) =>
                  Right(Some(Authentication.AmazonIAM(s3aBucket, None, None, None)))
                case (Some(AmazonS3EncryptionType.SSE_S3), None, None) =>
                  Right(Some(Authentication.AmazonIAM(s3aBucket, encType, kmsId, None)))
                case (Some(AmazonS3EncryptionType.SSE_KMS), Some(arn), None) =>
                  Right(Some(Authentication.AmazonIAM(s3aBucket, encType, kmsId, None)))
                case (Some(AmazonS3EncryptionType.SSE_C), None, Some(k)) =>
                  Right(Some(Authentication.AmazonIAM(s3aBucket, encType, None, customKey)))
                case _ =>
                  throw new Exception(s"Invalid authentication options for AmazonIAM method. See docs for allowed settings.")
              }
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

  def getS3Bucket(uri: Option[String]): Option[String] = uri.map { new URI(_).getAuthority }

  def readWatermark(path: String)(implicit c: Config): Either[Errors, Option[Watermark]] = {

    def err(lineNumber: Option[Int], msg: String): Either[Errors, Option[Watermark]] = Left(ConfigError(path, lineNumber, msg) :: Nil)

    try {
      if (c.hasPath(path)) {
        val watermark = readMap(path, c)
        val eventTime = watermark.get("eventTime") match {
          case Some(v) => v
          case None => throw new Exception(s"Watermark requires 'eventTime' parameter.")
        }
        val delayThreshold = watermark.get("delayThreshold") match {
          case Some(v) => v
          case None => throw new Exception(s"Watermark requires 'delayThreshold' parameter.")
        }
        Right(Some(Watermark(eventTime, delayThreshold)))
      } else {
        Right(None)
      }
    } catch {
      case e: Exception => err(Some(c.getValue(path).origin.lineNumber()), s"Unable to read config value: ${e.getMessage}")
    }
  }

  def parseURI(path: String)(uri: String)(implicit spark: SparkSession, c: Config): Either[Errors, URI] = {
    def err(lineNumber: Option[Int], msg: String): Either[Errors, URI] = Left(ConfigError(path, lineNumber, msg) :: Nil)

    try {
      // try to parse uri
      val u = new URI(uri)
      val isLocalMaster = spark.sparkContext.master.toLowerCase.startsWith("local")
      if (isLocalMaster && (uri.trim.startsWith("s3://") | uri.trim.startsWith("s3n://"))) {
        throw new Exception("s3:// and s3n:// are no longer supported. Please use s3a:// instead.")
      }
      Right(u)
    } catch {
      case e: Exception => err(Some(c.getValue(path).origin.lineNumber()), e.getMessage)
    }
  }

  def parseOptionURI(path: String)(uri: Option[String])(implicit spark: SparkSession, c: Config): Either[Errors, Option[URI]] = {
    def err(lineNumber: Option[Int], msg: String): Either[Errors, Option[URI]] = Left(ConfigError(path, lineNumber, msg) :: Nil)

    try {
      uri match {
        case Some(uri) => {
          // try to parse uri
          val u = new URI(uri)
          val isLocalMaster = spark.sparkContext.master.toLowerCase.startsWith("local")
          if (isLocalMaster && (uri.trim.startsWith("s3://") | uri.trim.startsWith("s3n://"))) {
            throw new Exception("s3:// and s3n:// are no longer supported. Please use s3a:// instead.")
          }
          Right(Some(u))
        }
        case None => Right(None)
      }
    } catch {
      case e: Exception => err(Some(c.getValue(path).origin.lineNumber()), e.getMessage)
    }
  }

  def getExtractColumns(uriKey: String)(schema: String)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config, arcContext: ARCContext): Either[Errors, List[ExtractColumn]] = {
    val cols = ArcSchema.parseArcSchema(schema)

    cols match {
      case Left(errs) => Left(errs.map( e => ConfigError("schema error", None, Error.pipelineSimpleErrorMsg(e.errors)) ))
      case Right(extractColumns) => Right(extractColumns)
    }
  }

  def checkSimpleColumnTypes(path: String, stageType: String)(extractColumns: List[ExtractColumn])(implicit c: Config): Either[Errors, List[ExtractColumn]] = {
    def err(lineNumber: Option[Int], msg: String): Either[Errors, ExtractColumn] = Left(ConfigError(path, lineNumber, msg) :: Nil)

    val cols = extractColumns.map { extractColumn =>
      extractColumn match {
        case col: StructColumn => err(Some(c.getValue(path).origin.lineNumber()), s"""${stageType} does not support complex types like column '${col.name}' of type struct.""")
        case col: ArrayColumn => err(Some(c.getValue(path).origin.lineNumber()), s"""${stageType} does not support complex types like column '${col.name}' of type array.""")
        case col => Right(col)
      }
    }

    val (schema, errors) = cols.foldLeft[(List[ExtractColumn], List[ConfigError])]( (Nil, Nil) ) { case ( (columns, errs), metaOrError ) =>
      metaOrError match {
        case Right(c) => (c :: columns, errs)
        case Left(metaErrors) => (columns, metaErrors ::: errs)
      }
    }

    errors match {
      case Nil => Right(schema.reverse)
      case _ => Left(errors.reverse)
    }
  }

  def textContentForURI(uriKey: String, authentication: Either[Errors, Option[Authentication]])(uri: URI)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config, arcContext: ARCContext): Either[Errors, String] = {
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
      case _ => Left(ConfigError(path, None, s"Invalid state. Please raise issue.") :: Nil)
    }
  }

  def parseSaveMode(path: String)(delim: String)(implicit c: Config): Either[Errors, SaveMode] = {
    delim.toLowerCase.trim match {
      case "append" => Right(SaveMode.Append)
      case "errorifexists" => Right(SaveMode.ErrorIfExists)
      case "ignore" => Right(SaveMode.Ignore)
      case "overwrite" => Right(SaveMode.Overwrite)
      case _ => Left(ConfigError(path, None, s"Invalid state. Please raise issue.") :: Nil)
    }
  }

  def parseOutputModeType(path: String)(delim: String)(implicit c: Config): Either[Errors, OutputModeType] = {
    delim.toLowerCase.trim match {
      case "append" => Right(OutputModeType.Append)
      case "complete" => Right(OutputModeType.Complete)
      case "update" => Right(OutputModeType.Update)
      case _ => Left(ConfigError(path, None, s"Invalid state. Please raise issue.") :: Nil)
    }
  }

  def parseFailMode(path: String)(delim: String)(implicit c: Config): Either[Errors, FailMode] = {
    delim.toLowerCase.trim match {
      case "permissive" => Right(FailMode.Permissive)
      case "failfast" => Right(FailMode.FailFast)
      case _ => Left(ConfigError(path, None, s"Invalid state. Please raise issue.") :: Nil)
    }
  }

  def parseDelimiter(path: String)(delim: String)(implicit c: Config): Either[Errors, Delimiter] = {
    delim.toLowerCase.trim match {
      case "comma" => Right(Delimiter.Comma)
      case "defaulthive" => Right(Delimiter.DefaultHive)
      case "pipe" => Right(Delimiter.Pipe)
      case "custom" => Right(Delimiter.Custom)
      case _ => Left(ConfigError(path, None, s"Invalid state. Please raise issue.") :: Nil)
    }
  }

  def parseQuote(path: String)(quote: String)(implicit c: Config): Either[Errors, QuoteCharacter] = {
    quote.toLowerCase.trim match {
      case "doublequote" => Right(QuoteCharacter.DoubleQuote)
      case "singlequote" => Right(QuoteCharacter.SingleQuote)
      case "none" => Right(QuoteCharacter.Disabled)
      case _ => Left(ConfigError(path, None, s"Invalid state. Please raise issue.") :: Nil)
    }
  }

  def getJDBCDriver(path: String)(uri: String)(implicit c: Config): Either[Errors, java.sql.Driver] = {
    def err(lineNumber: Option[Int], msg: String): Either[Errors, java.sql.Driver] = Left(ConfigError(path, lineNumber, msg) :: Nil)

    // without this line tests fail as drivers have not been registered yet
    val drivers = DriverManager.getDrivers.asScala.toList.map(driver => driver.getClass.getName)

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
      // hiveconf/hivevar may not have resolved by this stage of the job so will not resolve correctly resulting in incorrect failure
      if (sql.contains("${hiveconf:") || sql.contains("${hivevar:")) {
        Right(sql)
      } else {
        val parser = spark.sessionState.sqlParser
        parser.parsePlan(sql)
        Right(sql)
      }
    } catch {
      case e: Exception => err(Some(c.getValue(path).origin.lineNumber()), e.getMessage)
    }
  }

  def verifyInlineSQLPolicy(path: String)(sql: String)(implicit c: Config, arcContext: ARCContext): Either[Errors, String] = {
    if (!arcContext.inlineSQL) {
      Left(ConfigError(path, None, s"Inline SQL (use of the 'sql' attribute) has been disabled by policy. SQL statements must be supplied via files located at 'inputURI'.") :: Nil)
    } else {
      Right(sql)
    }
  }

  def verifyInlineSchemaPolicy(path: String)(schema: String)(implicit c: Config, arcContext: ARCContext): Either[Errors, String] = {
    if (!arcContext.inlineSchema) {
      Left(ConfigError(path, None, s"Inline Schema (use of the '${path}' attribute) has been disabled by policy. The schema must be supplied via other method.") :: Nil)
    } else {
      Right(schema)
    }
  }

  def doubleMinMax(path: String, min: Option[Double], max: Option[Double])(value: java.lang.Double)(implicit c: com.typesafe.config.Config): Either[Errors, Double] = {
    (value, min, max) match {
      case (v: java.lang.Double, Some(min), None) if (v < min) => Left(ConfigError(path, None, s"'${path}' ${v} below minimal allowed value ${min}.") :: Nil)
      case (v: java.lang.Double, Some(min), Some(max)) if (v < min || v > max) => Left(ConfigError(path, None, s"'${path}' ${v} must be between ${min} and ${max}.") :: Nil)
      case (v: java.lang.Double, None, Some(max)) if (v > max) => Left(ConfigError(path, None, s"'${path}' ${v} above maximum allowed value ${max}.") :: Nil)
      case (v: java.lang.Double, _, _) => Right(v)
    }
  }

}