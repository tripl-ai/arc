package au.com.agl.arc.transform

import java.lang._
import scala.collection.JavaConverters._

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{StringEntity, ByteArrayEntity}
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
  type TransformedRow = Row  

  def transform(transform: HTTPTransform)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    import spark.implicits._
    val startTime = System.currentTimeMillis() 
    val maskedHeaders = HTTPUtils.maskHeaders(transform.headers)
    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", transform.getType)
    stageDetail.put("name", transform.name)
    for (description <- transform.description) {
      stageDetail.put("description", description)    
    }    
    stageDetail.put("inputView", transform.inputView)  
    stageDetail.put("inputField", transform.inputField)  
    stageDetail.put("outputView", transform.outputView) 
    stageDetail.put("uri", transform.uri.toString)      
    stageDetail.put("headers", maskedHeaders.asJava)
    stageDetail.put("persist", Boolean.valueOf(transform.persist))
    stageDetail.put("validStatusCodes", transform.validStatusCodes.asJava)
    stageDetail.put("batchSize", Integer.valueOf(transform.batchSize))
    stageDetail.put("delimiter", transform.delimiter)
    stageDetail.put("failMode", transform.failMode.sparkString)

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()      

    val signature = s"HTTPTransform requires a field named '${transform.inputField}' of type 'string' or 'binary'."

    val df = spark.table(transform.inputView)      
    val schema = df.schema

    val fieldIndex = try { 
      schema.fieldIndex(transform.inputField)
    } catch {
      case e: Exception => throw new Exception(s"""${signature} inputView has: [${df.schema.map(_.name).mkString(", ")}].""") with DetailException {
        override val detail = stageDetail          
      }   
    }

    schema.fields(fieldIndex).dataType match {
      case _: StringType => 
      case _: BinaryType => 
      case _ => throw new Exception(s"""${signature} '${transform.inputField}' is of type: '${schema.fields(fieldIndex).dataType.simpleString}'.""") with DetailException {
        override val detail = stageDetail          
      }  
    }


    val typedSchema = transform.failMode match {
      case FailModeTypePermissive => StructType(df.schema.fields.toList ::: List(StructField("body", StringType, false), StructField("response", StructType(StructField("statusCode", IntegerType, false) :: StructField("reasonPhrase", StringType, false) :: StructField("contentType", StringType, false) :: StructField("responseTime", LongType, false):: Nil), false)))
      case FailModeTypeFailFast => StructType(df.schema.fields.toList ::: List(StructField("body", StringType, false)))
    }

    /** Create a dynamic RowEncoder from the provided schema. We use the phantom
      * TypeRow type to enable implicit resolution to find our encoder.
      */
    implicit val typedEncoder: Encoder[TransformedRow] = org.apache.spark.sql.catalyst.encoders.RowEncoder(typedSchema)
    
    var transformedDF = try {
      df.mapPartitions[TransformedRow] { partition: Iterator[Row] => 
        val poolingHttpClientConnectionManager = new PoolingHttpClientConnectionManager()
        poolingHttpClientConnectionManager.setMaxTotal(50)
        val httpClient = HttpClients.custom
                .setConnectionManager(poolingHttpClientConnectionManager)
                .setRedirectStrategy(new LaxRedirectStrategy)
                .build
        val uri = transform.uri.toString

        // we are using a BufferedIterator so we can 'peek' at the first row to get column types without advancing the iterator
        // meaning we don't have to keep finding fieldIndex and dataType for each row (inefficient as they will not change)
        val bufferedPartition = partition.buffered
        val fieldIndex = bufferedPartition.hasNext match {
          case true => bufferedPartition.head.fieldIndex(transform.inputField)
          case false => 0
        }
        val dataType = bufferedPartition.hasNext match {
          case true => bufferedPartition.head.schema(fieldIndex).dataType
          case false => NullType
        }

        // group so we can send multiple rows per request
        val groupedPartition = bufferedPartition.grouped(transform.batchSize)

        groupedPartition.flatMap[TransformedRow] { groupedRow => 
          val post = new HttpPost(uri)

          // add headers
          for ((k,v) <- transform.headers) {
            post.addHeader(k,v) 
          }

          // add payload
          val entity = dataType match {
            case _: StringType => {
              val delimiter = if (transform.batchSize > 1) {
                transform.delimiter
              } else {
                ""
              }        
              new StringEntity(groupedRow.map(row => row.getString(fieldIndex)).mkString(transform.delimiter))
            }
            case _: BinaryType => {
              val delimiter = if (transform.batchSize > 1) {
                transform.delimiter.getBytes
              } else {
                "".getBytes
              }
              new ByteArrayEntity(groupedRow.map(row => row.get(fieldIndex).asInstanceOf[Array[scala.Byte]]).reduce(_ ++ delimiter ++ _))
            }
          }
          post.setEntity(entity)
          
          try {
            // send the request
            val requestStartTime = System.currentTimeMillis
            val response = httpClient.execute(post)
            val responseTime = System.currentTimeMillis - requestStartTime

            // verify status code is correct
            if (transform.failMode == FailModeTypeFailFast && !transform.validStatusCodes.contains(response.getStatusLine.getStatusCode)) {
              throw new Exception(s"""HTTPTransform expects all response StatusCode(s) in [${transform.validStatusCodes.mkString(", ")}] but server responded with ${response.getStatusLine.getStatusCode} (${response.getStatusLine.getReasonPhrase}).""")
            }

            // read and close response
            val content = response.getEntity.getContent
            val body = if (transform.batchSize > 1) {
              Source.fromInputStream(content).mkString.split(transform.delimiter)
            } else {
              Array(Source.fromInputStream(content).mkString)
            }
            response.close 

            if (body.length != groupedRow.length) {
              throw new Exception(s"""HTTPTransform expects the response to contain same number of results as 'batchSize' (${transform.batchSize}) but server responded with ${body.length}.""")
            }

            // cast to a TransformedRow to fit the Dataset map method requirements
            groupedRow.zipWithIndex.map { case (row, index) => {
              transform.failMode match {
                case FailModeTypePermissive => Row.fromSeq(row.toSeq ++ Seq(body(index), Row(response.getStatusLine.getStatusCode, response.getStatusLine.getReasonPhrase, response.getEntity.getContentType.toString.replace("Content-Type: ",""), responseTime))).asInstanceOf[TransformedRow]
                case FailModeTypeFailFast => Row.fromSeq(row.toSeq ++ Seq(body(index))).asInstanceOf[TransformedRow]
              }
            }}

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

    // re-attach metadata to result
    df.schema.fields.foreach(field => {
      transformedDF = transformedDF.withColumn(field.name, col(field.name).as(field.name, field.metadata))
    })

    // repartition to distribute rows evenly
    val repartitionedDF = transform.partitionBy match {
      case Nil => { 
        transform.numPartitions match {
          case Some(numPartitions) => transformedDF.repartition(numPartitions)
          case None => transformedDF
        }   
      }
      case partitionBy => {
        // create a column array for repartitioning
        val partitionCols = partitionBy.map(col => transformedDF(col))
        transform.numPartitions match {
          case Some(numPartitions) => transformedDF.repartition(numPartitions, partitionCols:_*)
          case None => transformedDF.repartition(partitionCols:_*)
        }
      }
    } 

    repartitionedDF.createOrReplaceTempView(transform.outputView)

    if (!repartitionedDF.isStreaming) {
      stageDetail.put("outputColumns", Integer.valueOf(repartitionedDF.schema.length))
      stageDetail.put("numPartitions", Integer.valueOf(repartitionedDF.rdd.partitions.length))

      if (transform.persist) {
        repartitionedDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
        stageDetail.put("records", Long.valueOf(repartitionedDF.count)) 
      }      
    }

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stageDetail)      
      .log()  

    Option(repartitionedDF)
  }

}
