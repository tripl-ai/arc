package ai.tripl.arc.util

import java.net.URI

import scala.io.Source

import org.apache.http.client.methods.{HttpGet}
import org.apache.http.impl.client.HttpClients

import org.apache.spark.sql._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API.ARCContext

object CloudUtils {

  // this is the default list of providers with additional providers appended:
  // com.amazonaws.auth.ContainerCredentialsProvider to support Arc inside ECS services with taskRoleArn defined
  // org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider to support anonymous credentials
  val defaultAWSProvidersOverride = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider,com.amazonaws.auth.EnvironmentVariableCredentialsProvider,com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.ContainerCredentialsProvider,org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"

  def setHadoopConfiguration(authentication: Option[API.Authentication])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext) = {
    import spark.sparkContext.{hadoopConfiguration => hc}

    // clear s3a settings
    hc.unset("fs.s3a.access.key")
    hc.unset("fs.s3a.secret.key")
    hc.unset("fs.s3a.server-side-encryption-algorithm")
    hc.unset("fs.s3a.server-side-encryption.key")
    hc.set("fs.s3a.aws.credentials.provider", defaultAWSProvidersOverride)

    authentication match {
      case Some(API.Authentication.AmazonAccessKey(bucket, accessKeyID, secretAccessKey, endpoint, ssl)) => {
        val (accessKey, secretKey) = bucket match {
          case Some(bucket) => {
            hc.set(s"fs.s3a.$bucket.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            (s"fs.s3a.$bucket.access.key", s"fs.s3a.$bucket.secret.key")
          }
          case None => ("fs.s3a.access.key", "fs.s3a.secret.key")
        }

        hc.set(accessKey, accessKeyID)
        hc.set(secretKey, secretAccessKey)

        endpoint match {
          case Some(ep) => hc.set("fs.s3a.endpoint", ep)
          case None =>
        }

        ssl match {
          case Some(s) => hc.set("fs.s3a.connection.ssl.enabled", s.toString)
          case None =>
        }
      }
      case Some(API.Authentication.AmazonAnonymous(bucket)) => {
        bucket.foreach { bucket => hc.set(s"fs.s3a.$bucket.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")}
      }
      case Some(API.Authentication.AmazonEnvironmentVariable(bucket)) => {
        bucket.foreach { bucket => hc.set(s"fs.s3a.$bucket.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")}
      }      
      case Some(API.Authentication.AmazonIAM(bucket, encType, kmsId, customKey)) => {
        bucket.foreach { bucket => hc.set(s"fs.s3a.$bucket.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.ContainerCredentialsProvider")}
        var algorithm = "None"
        var key = "None"

        encType match {
          case Some(API.AmazonS3EncryptionType.SSE_S3) =>
            hc.set("fs.s3a.server-side-encryption-algorithm", "SSE-S3")
            algorithm = "SSE-S3"
          case Some(API.AmazonS3EncryptionType.SSE_KMS) =>
            kmsId match {
              case Some(arn) =>
                hc.set("fs.s3a.server-side-encryption-algorithm", "SSE-KMS")
                hc.set("fs.s3a.server-side-encryption.key", arn)
                algorithm = "SSE-KMS"
                key = arn
              case None => // already unset option above
            }
          case Some(API.AmazonS3EncryptionType.SSE_C) =>
            customKey match {
              case Some(ckey) =>
                hc.set("fs.s3a.server-side-encryption-algorithm", "SSE-C")
                hc.set("fs.s3a.server-side-encryption.key", ckey)
                algorithm = "SSE-C"
                key = "*****"
              case None => // already unset option above
            }
          case None => // already unset option above
        }
      }
      case Some(API.Authentication.AzureSharedKey(accountName, signature)) => {
        hc.set(s"fs.azure.account.key.${accountName}.blob.core.windows.net", signature)
        logger.debug()
          .message("hadoopConfiguration.set()")
          .field(s"fs.azure.account.key.${accountName}.blob.core.windows.net", signature)
          .log()
      }
      case Some(API.Authentication.AzureSharedAccessSignature(accountName, container, token)) => {
        hc.set(s"fs.azure.sas.${container}.${accountName}.blob.core.windows.net", token)
        logger.debug()
          .message("hadoopConfiguration.set()")
          .field(s"fs.azure.sas.${container}.${accountName}.blob.core.windows.net", token)
          .log()
      }
      case Some(API.Authentication.AzureDataLakeStorageToken(clientID, refreshToken)) => {
        hc.set("fs.adl.oauth2.access.token.provider.type", "RefreshToken")
        hc.set("fs.adl.oauth2.client.id", clientID)
        hc.set("fs.adl.oauth2.refresh.token", refreshToken)
        logger.debug()
          .message("hadoopConfiguration.set()")
          .field("fs.adl.oauth2.access.token.provider.type", "RefreshToken")
          .field("fs.adl.oauth2.client.id", clientID)
          .field("fs.adl.oauth2.refresh.token", refreshToken)
          .log()
      }
      case Some(API.Authentication.AzureDataLakeStorageGen2AccountKey(accountName, accessKey)) => {
        hc.set(s"fs.azure.account.key.${accountName}.dfs.core.windows.net", accessKey)
        logger.debug()
          .message("hadoopConfiguration.set()")
          .field(s"fs.azure.account.key.${accountName}.dfs.core.windows.net", accessKey)
          .log()
      }
      case Some(API.Authentication.AzureDataLakeStorageGen2OAuth(clientID, secret, directoryID)) => {
        hc.set("fs.azure.account.auth.type", "OAuth")
        hc.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        hc.set("fs.azure.account.oauth2.client.id", clientID)
        hc.set("fs.azure.account.oauth2.client.secret", secret)
        hc.set("fs.azure.account.oauth2.client.endpoint", s"https://login.microsoftonline.com/${directoryID}/oauth2/token")
        logger.debug()
          .message("hadoopConfiguration.set()")
          .field("fs.azure.account.auth.type", "OAuth")
          .field("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
          .field("fs.azure.account.oauth2.client.id", clientID)
          .field("fs.azure.account.oauth2.client.secret", secret)
          .field("fs.azure.account.oauth2.client.endpoint", s"https://login.microsoftonline.com/${directoryID}/oauth2/token")
          .log()
      }
      case Some(API.Authentication.GoogleCloudStorageKeyFile(projectID, keyFilePath)) => {
        hc.set("google.cloud.auth.service.account.enable", "true")
        hc.set("fs.gs.project.id", projectID)
        hc.set("google.cloud.auth.service.account.json.keyfile", keyFilePath)
        logger.debug()
          .message("hadoopConfiguration.set()")
          .field("google.cloud.auth.service.account.enable", "true")
          .field("fs.gs.project.id", projectID)
          .field("google.cloud.auth.service.account.json.keyfile", keyFilePath)
          .log()
      }
      case None =>
    }

    // update the serializableConfiguration
    arcContext.serializableConfiguration = Option(new SerializableConfiguration(spark.sparkContext.hadoopConfiguration))
  }

  // using a string filePath so that invalid paths can be used for local files
  def getTextBlob(uri: URI)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): String = {

    uri.getScheme match {
      case "http" | "https" => {
        val client = HttpClients.createDefault
        val httpGet = new HttpGet(uri);

        val response = client.execute(httpGet);

        val statusCode = response.getStatusLine.getStatusCode
        val reasonPhrase = response.getStatusLine.getReasonPhrase
        val payload = Source.fromInputStream(response.getEntity.getContent).mkString

        client.close

        if (response.getStatusLine.getStatusCode != 200) {
          throw new Exception(s"""Expected StatusCode = 200 when GET '${uri}' but server responded with ${statusCode} (${reasonPhrase}).""")
        }

        payload
      }
      case _ => {
        val oldDelimiter = spark.sparkContext.hadoopConfiguration.get("textinputformat.record.delimiter")
        val newDelimiter = s"${0x0 : Char}"

        // logging as this is a global variable and could cause strange behaviour downstream
        val newDelimiterMap = new java.util.HashMap[String, String]()
        newDelimiterMap.put("old", oldDelimiter)
        newDelimiterMap.put("new", newDelimiter)
        logger.debug()
          .field("event", "validateConfig")
          .field("type", "getTextBlob")
          .field("textinputformat.record.delimiter", newDelimiterMap)
          .log()

        // temporarily remove the delimiter so all the data is loaded as a single line
        spark.sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", newDelimiter)
        val textFile = spark.sparkContext.textFile(uri.toString).collect()(0)

        // reset delimiter back to original value
        val oldDelimiterMap = new java.util.HashMap[String, String]()
        oldDelimiterMap.put("old", newDelimiter)
        oldDelimiterMap.put("new", oldDelimiter)
        logger.debug()
          .field("event", "validateConfig")
          .field("type", "getTextBlob")
          .field("textinputformat.record.delimiter", oldDelimiterMap)
          .log()

        if (oldDelimiter == null) {
          spark.sparkContext.hadoopConfiguration.unset("textinputformat.record.delimiter")
        } else {
          spark.sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", oldDelimiter)
        }

        textFile
      }
    }
  }
}