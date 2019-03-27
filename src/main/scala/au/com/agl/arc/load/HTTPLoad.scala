package au.com.agl.arc.load

import java.net.URI
import scala.collection.JavaConverters._

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{StringEntity, ByteArrayEntity}
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.client.LaxRedirectStrategy
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager

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

  def load(load: HTTPLoad)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    import spark.implicits._
    val startTime = System.currentTimeMillis() 
    val maskedHeaders = HTTPUtils.maskHeaders(load.headers)
    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", load.getType)
    stageDetail.put("name", load.name)
    for (description <- load.description) {
      stageDetail.put("description", description)    
    }    
    stageDetail.put("inputView", load.inputView)  
    stageDetail.put("outputURI", load.outputURI.toString)  
    stageDetail.put("headers", maskedHeaders.asJava)

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()

    val signature = "HTTPLoad requires inputView to be dataset with [value: string] signature."

    val df = spark.table(load.inputView)      

    if (df.schema.length != 1 || df.schema(0).dataType != StringType) {
        throw new Exception(s"${signature} inputView '${load.inputView}' has ${df.schema.length} columns of type [${df.schema.map(f => f.dataType.simpleString).mkString(", ")}].") with DetailException {
          override val detail = stageDetail          
      }      
    }    

    val responses = try {
      if (arcContext.isStreaming) {

        df.writeStream.foreach(
          new ForeachWriter[Row] {
            var poolingHttpClientConnectionManager: PoolingHttpClientConnectionManager = _
            var httpClient: CloseableHttpClient = _
            var uri: String = load.outputURI.toString

            def open(partitionId: Long, epochId: Long): Boolean = {
              // create connection pool
              poolingHttpClientConnectionManager = new PoolingHttpClientConnectionManager()
              httpClient = HttpClients.custom()
                .setConnectionManager(poolingHttpClientConnectionManager)
                .build()
              true
            }

            def process(row: Row) = {
              val post = new HttpPost(uri)

              // add headers
              for ((k,v) <- load.headers) {
                post.addHeader(k,v) 
              }

              post.setEntity(new StringEntity(row.getString(0)))       

              val response = httpClient.execute(post)

              // verify status code is correct
              if (!load.validStatusCodes.contains(response.getStatusLine.getStatusCode)) {
                throw new Exception(s"""HTTPLoad expects all response StatusCode(s) in [${load.validStatusCodes.mkString(", ")}] but server responded with ${response.getStatusLine.getStatusCode} (${response.getStatusLine.getReasonPhrase}).""")
              }      

              response.close
              post.releaseConnection      
            }

            def close(errorOrNull: Throwable): Unit = {
              // close the connection pool
              httpClient.close
              poolingHttpClientConnectionManager.close

              errorOrNull match {
                case null => 
                case _ => throw new Exception(errorOrNull)
              }
            }

          }
        ).start

        None
      } else {
        val writtenDS = df.mapPartitions(partition => {
          val poolingHttpClientConnectionManager = new PoolingHttpClientConnectionManager()
          poolingHttpClientConnectionManager.setMaxTotal(50)
          val httpClient = HttpClients.custom()
                  .setConnectionManager(poolingHttpClientConnectionManager)
                  .build()
          val uri = load.outputURI.toString

          // we are using a BufferedIterator so we can 'peek' at the first row to get column types without advancing the iterator
          // meaning we don't have to keep finding fieldIndex and dataType for each row (inefficient as they will not change)
          val bufferedPartition = partition.buffered
          val fieldIndex = bufferedPartition.hasNext match {
            case true => bufferedPartition.head.fieldIndex("value")
            case false => 0
          }
          val dataType = bufferedPartition.hasNext match {
            case true => bufferedPartition.head.schema(fieldIndex).dataType
            case false => NullType
          }        

          bufferedPartition.map(row => {
            val post = new HttpPost(uri)

            // add headers
            for ((k,v) <- load.headers) {
              post.addHeader(k,v) 
            }

            // add payload
            val entity = dataType match {
              case _: StringType => new StringEntity(row.getString(fieldIndex))
              case _: BinaryType => new ByteArrayEntity(row.get(fieldIndex).asInstanceOf[Array[scala.Byte]])
            }
            post.setEntity(entity)
            
            try {
              // send the request
              val response = httpClient.execute(post)
              
              // verify status code is correct
              if (!load.validStatusCodes.contains(response.getStatusLine.getStatusCode)) {
                throw new Exception(s"""HTTPLoad expects all response StatusCode(s) in [${load.validStatusCodes.mkString(", ")}] but server responded with ${response.getStatusLine.getStatusCode} (${response.getStatusLine.getReasonPhrase}).""")
              }

              // read and close response
              val responseEntity = response.getEntity.getContent
              val body = Source.fromInputStream(responseEntity).mkString
              response.close 

              Response(response.getStatusLine.getStatusCode, response.getStatusLine.getReasonPhrase, body)
            } finally {
              post.releaseConnection
              poolingHttpClientConnectionManager.close
            }
          })
        })
        Option(writtenDS.toDF)
      }
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        println("here")
        override val detail = stageDetail          
      }
    }

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stageDetail)      
      .log()

    responses
  }
}