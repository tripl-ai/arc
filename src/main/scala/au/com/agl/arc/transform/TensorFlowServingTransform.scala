package au.com.agl.arc.transform

import java.lang._
import java.net.URI
import scala.collection.JavaConverters._

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{StringEntity, ByteArrayEntity}
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.client.LaxRedirectStrategy

import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.node._
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import scala.io.Source

import au.com.agl.arc.api.API._
import au.com.agl.arc.util._

object TensorFlowServingTransform {

  type TensorFlowResponseRow = Row  

  def transform(transform: TensorFlowServingTransform)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Option[DataFrame] = {
    val startTime = System.currentTimeMillis() 
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
    stageDetail.put("batchSize", Integer.valueOf(transform.batchSize))
    stageDetail.put("responseType", transform.responseType.sparkString)
    for (signatureName <- transform.signatureName) {
      stageDetail.put("signature_name", signatureName)
    }    

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()      


    val df = spark.table(transform.inputView)

    if (!df.columns.contains(transform.inputField)) {
      throw new Exception(s"""inputField '${transform.inputField}' is not present in inputView '${transform.inputView}' which has: [${df.columns.mkString(", ")}] columns.""") with DetailException {
        override val detail = stageDetail          
      }    
    }   

    val tensorFlowResponseSchema = transform.responseType match {
      case IntegerResponse => StructType(df.schema.fields.toList ::: List(new StructField("result", IntegerType, true)))
      case DoubleResponse => StructType(df.schema.fields.toList ::: List(new StructField("result", DoubleType, true)))
      case _ => StructType(df.schema.fields.toList ::: List(new StructField("result", StringType, true)))
    }

    implicit val typedEncoder: Encoder[TensorFlowResponseRow] = org.apache.spark.sql.catalyst.encoders.RowEncoder(tensorFlowResponseSchema)

    val transformedDF = try {
      df.mapPartitions[TensorFlowResponseRow] { partition: Iterator[Row] => 

        val poolingHttpClientConnectionManager = new PoolingHttpClientConnectionManager()
        poolingHttpClientConnectionManager.setMaxTotal(50)
        val httpClient = HttpClients.custom()
                .setConnectionManager(poolingHttpClientConnectionManager)
                .setRedirectStrategy(new LaxRedirectStrategy())
                .build()
        val uri = transform.uri

        val validStatusCodes = 200 :: 201 :: 202 :: Nil

        val objectMapper = new ObjectMapper()

        // get type and index so it doesnt have to be resolved for each row
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

        groupedPartition.flatMap[TensorFlowResponseRow] { groupedRow => 

          val jsonNodeFactory = new JsonNodeFactory(true)
          val node = jsonNodeFactory.objectNode

          // optionally set signature_name
          for (signatureName <- transform.signatureName) {
            node.put("signature_name", signatureName)
          }
          val instancesArray = node.putArray("instances")

          // add payload to array
          // for StringType first try to deserialise object so it can be properly serialised in the batch
          groupedRow.foreach(row => {
            dataType match {
              case _: StringType => instancesArray.add(objectMapper.readTree(row.getString(fieldIndex)))
              case _: IntegerType => instancesArray.add(row.getInt(fieldIndex))
              case _: LongType => instancesArray.add(row.getLong(fieldIndex))
              case _: FloatType => instancesArray.add(row.getFloat(fieldIndex))
              case _: DoubleType => instancesArray.add(row.getDouble(fieldIndex))
              case _: DecimalType => instancesArray.add(row.getDecimal(fieldIndex))
            }
          })

          val post = new HttpPost(uri)

          val response = try {
            // add the stringified json object to the request body
            post.setEntity(new StringEntity(objectMapper.writeValueAsString(node)))

            val response = httpClient.execute(post)

            // read and close response
            val responseEntity = response.getEntity.getContent
            val body = Source.fromInputStream(responseEntity).mkString
            response.close 

            // verify status code is correct
            if (!validStatusCodes.contains(response.getStatusLine.getStatusCode)) {
              throw new Exception(body) 
            }

            // decode the response
            val rootNode = objectMapper.readTree(body)
            rootNode.get("predictions").asScala.toList

          } finally {
            post.releaseConnection
          }

          // try to unpack result 
          groupedRow.zipWithIndex.map { case (row, index) => {
            val result = transform.responseType match {
              case IntegerResponse => Seq(response(index).asInt)
              case DoubleResponse => Seq(response(index).asDouble)
              case _ => Seq(response(index).asText)
            }

            Row.fromSeq(row.toSeq ++ result).asInstanceOf[TensorFlowResponseRow]
          }}
        }
      }
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stageDetail          
      }
    }

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
