package au.com.agl.arc.load

import java.lang._
import java.net.URI
import scala.collection.JavaConverters._

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.client.LaxRedirectStrategy

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import scala.io.Source

import au.com.agl.arc.api.API._
import au.com.agl.arc.util._

case class Response(
    statusCode: Int,
    reasonPhrase: String,
    body: String
)

object HTTPLoad {

  def load(load: HTTPLoad)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Option[DataFrame] = {
    import spark.implicits._
    val startTime = System.currentTimeMillis() 
    val signature = "HTTPLoad requires inputView to be dataset with [value: string] signature."

    val maskedHeaders = HTTPUtils.maskHeaders(load.headers)

    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", load.getType)
    stageDetail.put("name", load.name)
    stageDetail.put("inputView", load.inputView)  
    stageDetail.put("outputURI", load.outputURI.toString)  
    stageDetail.put("headers", maskedHeaders.asJava)

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()

    val df = spark.table(load.inputView)      

    if (df.schema.length != 1 || df.schema(0).dataType != StringType) {
        throw new Exception(s"${signature} inputView '${load.inputView}' has ${df.schema.length} columns of type [${df.schema.map(f => f.dataType.simpleString).mkString(", ")}].") with DetailException {
        override val detail = stageDetail          
      }      
    }    

    val responses = try {
      df.mapPartitions(partition => {
        val poolingHttpClientConnectionManager = new PoolingHttpClientConnectionManager()
        poolingHttpClientConnectionManager.setMaxTotal(50)
        val httpClient = HttpClients.custom()
                .setConnectionManager(poolingHttpClientConnectionManager)
                .build()
        val uri = load.outputURI.toString

        partition.map(row => {
          val post = new HttpPost(uri)

          // add headers
          for ((k,v) <- load.headers) {
            post.addHeader(k,v) 
          }

          // add payload
          val stringEntity = new StringEntity(row.getString(0))
          post.setEntity(stringEntity)
          
          try {
            // send the request
            val response = httpClient.execute(post)
            
            // read and close response
            val responseEntity = response.getEntity.getContent
            val body = Source.fromInputStream(responseEntity).mkString
            response.close 

            Response(response.getStatusLine.getStatusCode, response.getStatusLine.getReasonPhrase, body)
          } finally {
            post.releaseConnection
          }
        })
      })
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stageDetail          
      }
    }

    val distinctReponses = responses
      .groupBy($"statusCode", $"reasonPhrase")
      .agg(collect_list($"body").as("body"), count("*").as("count"))

    // execute the requests and return a new dataset of distinct response codes
    distinctReponses.cache.count

    // response logging 
    // limited to 50 top response codes (by count descending) to protect against log flooding
    val responseMap = new java.util.HashMap[String, Object]()      
    distinctReponses.sort($"count".desc).limit(50).collect.foreach( response => {
      val colMap = new java.util.HashMap[String, Object]()
      colMap.put("body", response.getList(2).toArray.slice(0, 10).distinct)
      colMap.put("reasonPhrase", response.getString(1))
      colMap.put("count", Long.valueOf(response.getLong(3)))
      responseMap.put(response.getInt(0).toString, colMap)
    })
    stageDetail.put("responses", responseMap)    

    // verify status code is correct
    val validStatusCodes = load.validStatusCodes match {
      case Some(value) => value
      case None => 200 :: 201 :: 202 :: Nil
    }
    if (!(distinctReponses.map(d => d.getInt(0)).collect forall (validStatusCodes contains _))) {
      val responseMessages = distinctReponses.map(response => s"${response.getLong(3)} reponses ${response.getInt(0)} (${response.getString(1)})").collect.mkString(", ")

      throw new Exception(s"""HTTPLoad expects all response StatusCode(s) in [${validStatusCodes.mkString(", ")}] but server responded with [${responseMessages}].""") with DetailException {
        override val detail = stageDetail          
      }
    }    

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stageDetail)      
      .log()

    Option(df)
  }
}