package au.com.agl.arc.util

import java.net.URI
import java.time.Instant

import org.apache.spark.sql._

import au.com.agl.arc.api._

object CloudUtils {

  def setHadoopConfiguration(authentication: Option[API.Authentication])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger) = {
    import spark.sparkContext.{hadoopConfiguration => hc}

    authentication match {
      case Some(API.Authentication.AmazonAccessKey(accessKeyID, secretAccessKey)) => {
        hc.set("fs.s3a.access.key", accessKeyID)
        hc.set("fs.s3a.secret.key", secretAccessKey)        

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
      case None =>   
    }
  }  

  // using a string filePath so that invalid paths can be used for local files
  def getTextBlob(filePath: String)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): String = {
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
    
    val textFile = spark.sparkContext.textFile(filePath).collect()(0)

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