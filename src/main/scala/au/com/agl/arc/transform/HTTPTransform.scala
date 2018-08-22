package au.com.agl.arc.transform

import java.lang._
import scala.collection.JavaConverters._

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.client.LaxRedirectStrategy

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import scala.io.Source

import au.com.agl.arc.api.API._
import au.com.agl.arc.util._

object HTTPTransform {

  /** Phantom Type to enable compiler to find the encoder we want
    */
  type TypedRow = Row  

  def transform(transform: HTTPTransform)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Option[DataFrame] = {
    import spark.implicits._
    
    val startTime = System.currentTimeMillis() 

    val maskedHeaders = HTTPUtils.maskHeaders(transform.headers)

    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", transform.getType)
    stageDetail.put("name", transform.name)
    stageDetail.put("uri", transform.uri.toString)      
    stageDetail.put("headers", maskedHeaders.asJava)
    stageDetail.put("persist", Boolean.valueOf(transform.persist))

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()      

    val df = spark.table(transform.inputView)      

    val typedSchema = StructType(
      df.schema.fields.toList ::: List(new StructField("statusCode", IntegerType, false), new StructField("reasonPhrase", StringType, false), new StructField("body", StringType, false))
    )

    /** Create a dynamic RowEncoder from the provided schema. We use the phantom
      * TypeRow type to enable implicit resolution to find our encoder.
      */
    implicit val typedEncoder: Encoder[TypedRow] = org.apache.spark.sql.catalyst.encoders.RowEncoder(typedSchema)
    
    val responses = try {
      df.mapPartitions[TypedRow] { partition: Iterator[Row] => 
        val poolingHttpClientConnectionManager = new PoolingHttpClientConnectionManager()
        poolingHttpClientConnectionManager.setMaxTotal(50)
        val httpClient = HttpClients.custom()
                .setConnectionManager(poolingHttpClientConnectionManager)
                .build()
        val uri = transform.uri.toString

        partition.map[TypedRow] { row: Row =>
          val post = new HttpPost(uri)

          // add headers
          for ((k,v) <- transform.headers) {
            post.addHeader(k,v) 
          }

          // add payload
          val stringEntity = new StringEntity(row.getString(row.fieldIndex("value")))
          post.setEntity(stringEntity)
          
          try {
            // send the request
            val response = httpClient.execute(post)
            
            // read and close response
            val responseEntity = response.getEntity.getContent
            val body = Source.fromInputStream(responseEntity).mkString.replace("\n", "")
            response.close 

            // cast to a TypedRow to fit the Dataset map method requirements
            val result = row.toSeq ++ Seq(response.getStatusLine.getStatusCode, response.getStatusLine.getReasonPhrase, body)
            Row.fromSeq(result).asInstanceOf[TypedRow]

          } finally {
            post.releaseConnection
          }
        }
      }
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stageDetail          
      }
    } 

    val distinctReponses = responses
      .groupBy(col("statusCode"), col("reasonPhrase"))
      .agg(collect_list(col("body")).as("body"), count("*").as("count"))

    // execute the requests and return a new dataset of distinct response codes
    distinctReponses.cache.count

    // response logging 
    // limited to 50 top response codes (by count descending) to protect against log flooding
    val responseMap = new java.util.HashMap[String, Object]()      
    distinctReponses.sort(col("count").desc).limit(50).collect.foreach( response => {
      val colMap = new java.util.HashMap[String, Object]()
      colMap.put("body", response.getList(2).toArray.slice(0, 10).distinct)
      colMap.put("reasonPhrase", response.getString(1))
      colMap.put("count", Long.valueOf(response.getLong(3)))
      responseMap.put(response.getInt(0).toString, colMap)
    })
    stageDetail.put("responses", responseMap)    

    // verify status code is correct
    val validStatusCodes = transform.validStatusCodes match {
      case Some(value) => value
      case None => 200 :: 201 :: 202 :: Nil
    }
    if (!(distinctReponses.map(d => d.getInt(0)).collect forall (validStatusCodes contains _))) {
      val responseMessages = distinctReponses.map(response => s"${response.getLong(3)} reponses ${response.getInt(0)} (${response.getString(1)})").collect.mkString(", ")

      throw new Exception(s"""HTTPTransform expects all response StatusCode(s) in [${validStatusCodes.mkString(", ")}] but server responded with [${responseMessages}].""") with DetailException {
        override val detail = stageDetail          
      }
    }     

    // re-attach metadata to result
    var transformedDF = responses.drop(col("statusCode")).drop(col("reasonPhrase"))
    df.schema.fields.foreach(field => {
      transformedDF = transformedDF.withColumn(field.name, col(field.name).as(field.name, field.metadata))
    })

    transformedDF.createOrReplaceTempView(transform.outputView)

    if (transform.persist) {
      transformedDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
      stageDetail.put("records", Long.valueOf(transformedDF.count)) 
    }    

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stageDetail)      
      .log()  

    Option(transformedDF)
  }

}
