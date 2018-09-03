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

  type TypedRow = Row  

  def transform(transform: TensorFlowServingTransform)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Option[DataFrame] = {
    val startTime = System.currentTimeMillis() 
    val stageDetail = new java.util.HashMap[String, Object]()

    val batchSize = transform.batchSize.getOrElse(10)

    stageDetail.put("type", transform.getType)
    stageDetail.put("name", transform.name)
    stageDetail.put("inputView", transform.inputView)  
    stageDetail.put("outputView", transform.outputView)  
    stageDetail.put("uri", transform.uri.toString)
    stageDetail.put("batchSize", Integer.valueOf(batchSize))

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()      


    val df = spark.table(transform.inputView)

    val typedSchema = StructType(
      df.schema.fields.toList ::: List(new StructField("resultType", StringType, false), new StructField("integerResult", IntegerType, true), new StructField("doubleResult", DoubleType, true), new StructField("objectResult", StringType, true))
    )

    implicit val typedEncoder: Encoder[TypedRow] = org.apache.spark.sql.catalyst.encoders.RowEncoder(typedSchema)

    val transformedDF = df.mapPartitions[TypedRow] { partition: Iterator[Row] => 

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
        case true => bufferedPartition.head.fieldIndex("value")
        case false => 0
      }
      val dataType = bufferedPartition.hasNext match {
        case true => bufferedPartition.head.schema(fieldIndex).dataType
        case false => NullType
      }

      // group so we can send multiple rows per request
      val groupedPartition = bufferedPartition.grouped(batchSize)

      groupedPartition.flatMap[TypedRow] { groupedRow => 

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

        // get node based on first return value and assume it applies to all results
        val nodeType = response(0).getNodeType.toString
        val nodeTypeIsDouble = response(0).isDouble
        val nodeTypeIsInt = response(0).isInt

        // try to unpack result 
        groupedRow.zipWithIndex.map { case (row, index) => {
          val result = nodeType match {
            case "NUMBER" if (nodeTypeIsInt) => Seq("INTEGER", response(index).asInt, null, null)
            case "NUMBER" if (nodeTypeIsDouble) => Seq("DOUBLE", null, response(index).asDouble, null)
            case _ => Seq("OBJECT", null, null, response(index).asText)
          }

          Row.fromSeq(row.toSeq ++ result).asInstanceOf[TypedRow]
        }}
      }
    }

    transformedDF.createOrReplaceTempView(transform.outputView)

    // execute the requests and return a new dataset of responses
    transformedDF.cache.count

    // use the first row to work out what type was returned and select just that column
    val simplifiedDF = transformedDF.first.getString(transformedDF.schema.fieldIndex("resultType")) match {
      case "INTEGER" => transformedDF.withColumn("result", col("integerResult"))
      case "DOUBLE" => transformedDF.withColumn("result", col("doubleResult"))
      case "OBJECT" => transformedDF.withColumn("result", col("objectResult"))
    }

    val outputDF = simplifiedDF.drop("integerResult").drop("doubleResult").drop("objectResult").drop("resultType")

    if (transform.persist) {
      outputDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
      stageDetail.put("records", Long.valueOf(outputDF.count)) 
    }

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stageDetail)      
      .log()  

    Option(outputDF)
  }
}
