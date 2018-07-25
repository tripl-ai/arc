package au.com.agl.arc.extract

import java.lang._
import java.net.URI
import scala.collection.JavaConverters._

import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.LaxRedirectStrategy

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import scala.io.Source

import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.util._

object HTTPExtract {

  def extract(extract: HTTPExtract)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): DataFrame = {
    import spark.implicits._
    val startTime = System.currentTimeMillis() 

    val maskedHeaders = HTTPUtils.maskHeaders(extract.headers)
    val method = extract.method.getOrElse("GET")

    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", extract.getType)
    stageDetail.put("name", extract.name)
    stageDetail.put("uri", extract.uri.toString)  
    stageDetail.put("outputView", extract.outputView)  
    stageDetail.put("persist", Boolean.valueOf(extract.persist))
    stageDetail.put("method", method)
    stageDetail.put("headers", maskedHeaders.asJava)

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()  

    val uri = extract.uri.toString

    val client = HttpClientBuilder.create.setRedirectStrategy(new LaxRedirectStrategy()).build

    val request = method match {
      case "GET" => new HttpGet(uri)
      case "POST" => { 
        val post = new HttpPost(uri)
        for (body <- extract.body) {
          post.setEntity(new StringEntity(body))
        }
        post 
      }
    }
    // add headers
    for ((k,v) <- extract.headers) {
      request.addHeader(k,v) 
    }

    // send the request
    val response = client.execute(request)

    val responseMap = new java.util.HashMap[String, Object]()
    responseMap.put("statusCode", new java.lang.Integer(response.getStatusLine.getStatusCode))
    responseMap.put("reasonPhrase", response.getStatusLine.getReasonPhrase)      
    stageDetail.put("response", responseMap)   

    // verify status code is correct
    val validStatusCodes = extract.validStatusCodes match {
      case Some(value) => value
      case None => 200 :: 201 :: 202 :: Nil
    }
    if (!validStatusCodes.contains(response.getStatusLine.getStatusCode)) {
      response.close    
      throw new Exception(s"""HTTPExtract expects a response StatusCode in [${validStatusCodes.mkString(", ")}] but server responded with ${response.getStatusLine.getStatusCode} (${response.getStatusLine.getReasonPhrase}).""") with DetailException {
        override val detail = stageDetail
      }
    }    

    // read and close response
    val responseEntity = response.getEntity.getContent
    val body = Source.fromInputStream(responseEntity).mkString
    response.close

    val df = spark.sparkContext.parallelize(Array(body)).toDF
    
    // repartition to distribute rows evenly
    val repartitionedDF = extract.partitionBy match {
      case Nil => { 
        extract.numPartitions match {
          case Some(numPartitions) => df.repartition(numPartitions)
          case None => df
        }   
      }
      case partitionBy => {
        // create a column array for repartitioning
        val partitionCols = partitionBy.map(col => df(col))
        extract.numPartitions match {
          case Some(numPartitions) => df.repartition(numPartitions, partitionCols:_*)
          case None => df.repartition(partitionCols:_*)
        }
      }
    } 
    repartitionedDF.createOrReplaceTempView(extract.outputView)

    stageDetail.put("outputColumns", Integer.valueOf(repartitionedDF.schema.length))
    stageDetail.put("numPartitions", Integer.valueOf(repartitionedDF.rdd.partitions.length))

    if (extract.persist) {
      repartitionedDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
      stageDetail.put("records", Long.valueOf(repartitionedDF.count)) 
    }    

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stageDetail)      
      .log()

    repartitionedDF
  }

}

