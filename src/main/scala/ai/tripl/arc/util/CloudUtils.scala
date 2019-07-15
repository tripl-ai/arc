package ai.tripl.arc.util

import java.net.URI

import scala.io.Source

import org.apache.http.client.methods.{HttpGet}
import org.apache.http.impl.client.HttpClients

import org.apache.spark.sql._

import ai.tripl.arc.api._

object CloudUtils {

  def setHadoopConfiguration(authentication: Option[API.Authentication])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger) = {
    import spark.sparkContext.{hadoopConfiguration => hc}

    authentication match {
      case Some(API.Authentication.AmazonAccessKey(accessKeyID, secretAccessKey, endpoint, ssl)) => {
        hc.set("fs.s3a.access.key", accessKeyID)
        hc.set("fs.s3a.secret.key", secretAccessKey)

        endpoint match {
          case Some(ep) => hc.set("fs.s3a.endpoint", ep)
          case None =>
        }

        ssl match {
          case Some(s) => hc.set("fs.s3a.connection.ssl.enabled", s.toString)
          case None =>
        }

        logger.debug()
          .message("hadoopConfiguration.set()")
          .field("fs.s3a.access.key", accessKeyID)
          .field("fs.s3a.secret.key", secretAccessKey)
          .log()
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