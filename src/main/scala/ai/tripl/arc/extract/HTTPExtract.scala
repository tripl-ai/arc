package ai.tripl.arc.extract

import java.lang._
import java.net.URI
import scala.collection.JavaConverters._

import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.LaxRedirectStrategy

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import scala.io.Source

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.util._

case class RequestResponse(
    uri: String,
    statusCode: Int,
    reasonPhrase: String,
    contentType: String,
    body: String
)

object HTTPExtract {

  /** Phantom Type to enable compiler to find the encoder we want
    */
  type RequestResponseRow = Row

  def extract(extract: HTTPExtract)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): Option[DataFrame] = {
    import spark.implicits._
    val startTime = System.currentTimeMillis() 
    val maskedHeaders = HTTPUtils.maskHeaders(extract.headers)
    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", extract.getType)
    stageDetail.put("name", extract.name)
    for (description <- extract.description) {
      stageDetail.put("description", description)    
    }     
    stageDetail.put("outputView", extract.outputView)  
    stageDetail.put("persist", Boolean.valueOf(extract.persist))
    stageDetail.put("method", extract.method)
    stageDetail.put("headers", maskedHeaders.asJava)
    stageDetail.put("validStatusCodes", extract.validStatusCodes.asJava)

    val inputValue = extract.input match {
      case Left(view) => view
      case Right(uri) => uri.toString
    }

    stageDetail.put("input", inputValue)  

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()  

    // create a StructType schema for RequestResponse
    val typedSchema = ScalaReflection.schemaFor[RequestResponse].dataType.asInstanceOf[StructType]      

    /** Create a dynamic RowEncoder from the provided schema. We use the phantom
      * TypeRow type to enable implicit resolution to find our encoder.
      */    
    implicit val typedEncoder: Encoder[RequestResponseRow] = org.apache.spark.sql.catalyst.encoders.RowEncoder(typedSchema)

    val responses = try {
      val df = extract.input match {
        case Right(uri) => Seq(uri.toString).toDF("value")
        case Left(view) => spark.table(view)
      }

      df.mapPartitions[RequestResponseRow] { partition: Iterator[Row] => 
        val poolingHttpClientConnectionManager = new PoolingHttpClientConnectionManager()
        poolingHttpClientConnectionManager.setMaxTotal(50)
        val httpClient = HttpClients.custom()
          .setRedirectStrategy(new LaxRedirectStrategy())
          .setConnectionManager(poolingHttpClientConnectionManager)
          .build()

        // we are using a BufferedIterator so we can 'peek' at the first row to get column types without advancing the iterator
        // meaning we don't have to keep finding fieldIndex and dataType for each row (inefficient as they will not change)
        val bufferedPartition = partition.buffered
        val (uriFieldIndex, bodyFieldIndex) = if (bufferedPartition.hasNext) {
          val row = bufferedPartition.head
          val uriFieldIndex = extract.uriField match {
            case Some(uriField) => row.fieldIndex(uriField)
            case None => 0
          }
          val bodyFieldIndex = extract.bodyField match {
            case Some(bodyField) => Option(row.fieldIndex(bodyField))
            case None => None
          }
          (uriFieldIndex, bodyFieldIndex)
        } else {
          (0, None)
        }

        bufferedPartition.map[RequestResponseRow] { row: Row =>
          val uri = row.getString(uriFieldIndex)
          val body = (bodyFieldIndex, extract.body) match {
            case (Some(bodyFieldIndex), None) => row.getString(bodyFieldIndex)
            case (Some(bodyFieldIndex), Some(_)) => row.getString(bodyFieldIndex)
            case (None, Some(body)) => body
            case (None, None) => ""
          } 

          val request = extract.method match {
            case "GET" => new HttpGet(uri)
            case "POST" => { 
              val post = new HttpPost(uri)
              post.setEntity(new StringEntity(body))
              post 
            }
          }

          // add headers
          for ((k,v) <- extract.headers) {
            request.addHeader(k,v) 
          }

          try {
            // send the request
            val response = httpClient.execute(request)

            // verify status code is correct
            if (!extract.validStatusCodes.contains(response.getStatusLine.getStatusCode)) {
              throw new Exception(s"""HTTPExtract expects all response StatusCode(s) in [${extract.validStatusCodes.mkString(", ")}] but server responded with ${response.getStatusLine.getStatusCode} (${response.getStatusLine.getReasonPhrase}).""")
            }

            // read and close response
            val body = response.getEntity.getContentLength match {
              case 0 => None
              case _ => Option(Source.fromInputStream(response.getEntity.getContent).mkString)
            }
            response.close 

            // cast to a RequestResponseRow to fit the Dataset map method requirements
            val result = Seq(uri, response.getStatusLine.getStatusCode, response.getStatusLine.getReasonPhrase, Option(response.getEntity.getContentType).map(_.toString).orNull, body.orNull)
            Row.fromSeq(result).asInstanceOf[RequestResponseRow]
          } finally {
            request.releaseConnection
          }
        }
      }
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stageDetail          
      }
    }

    val df = responses.toDF
    
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

    Option(repartitionedDF)
  }

}

