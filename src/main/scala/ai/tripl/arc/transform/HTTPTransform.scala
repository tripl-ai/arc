package ai.tripl.arc.transform

import java.net.URI
import scala.collection.JavaConverters._

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{StringEntity, ByteArrayEntity}
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.client.LaxRedirectStrategy

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.io.Source

import ai.tripl.arc.api.API._
import ai.tripl.arc.util._

import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.HTTPUtils
import ai.tripl.arc.util.Utils

class HTTPTransform extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputView" :: "uri" :: "headers" :: "inputField" :: "persist" :: "validStatusCodes" :: "params" :: "batchSize" :: "delimiter" :: "numPartitions" :: "partitionBy" :: "failMode" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val uri = getValue[String]("uri") |> parseURI("uri") _
    val inputField = getValue[String]("inputField", default = Some("value"))
    val headers = readMap("headers", c)
    val inputView = getValue[String]("inputView")
    val outputView = getValue[String]("outputView")
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val validStatusCodes = getValue[IntList]("validStatusCodes", default = Some(200 :: 201 :: 202 :: Nil))
    val batchSize = getValue[Int]("batchSize", default = Some(1))
    val delimiter = getValue[String]("delimiter", default = Some("\n"))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val failMode = getValue[String]("failMode", default = Some("failfast"), validValues = "permissive" :: "failfast" :: Nil) |> parseFailMode("failMode") _
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, description, inputView, outputView, uri, persist, inputField, validStatusCodes, batchSize, delimiter, numPartitions, partitionBy, failMode, invalidKeys) match {
      case (Right(name), Right(description), Right(inputView), Right(outputView), Right(uri), Right(persist), Right(inputField), Right(validStatusCodes), Right(batchSize), Right(delimiter), Right(numPartitions), Right(partitionBy), Right(failMode), Right(invalidKeys)) =>

        val stage = HTTPTransformStage(
          plugin=this,
          name=name,
          description=description,
          uri=uri,
          headers=headers,
          validStatusCodes=validStatusCodes,
          inputView=inputView,
          outputView=outputView,
          inputField=inputField,
          params=params,
          persist=persist,
          batchSize=batchSize,
          delimiter=delimiter,
          numPartitions=numPartitions,
          partitionBy=partitionBy,
          failMode=failMode
        )

        stage.stageDetail.put("inputView", inputView)
        stage.stageDetail.put("inputField", inputField)
        stage.stageDetail.put("outputView", outputView)
        stage.stageDetail.put("uri", uri.toString)
        stage.stageDetail.put("headers", HTTPUtils.maskHeaders("Authorization" :: Nil)(headers).asJava)
        stage.stageDetail.put("persist", java.lang.Boolean.valueOf(persist))
        stage.stageDetail.put("validStatusCodes", validStatusCodes.asJava)
        stage.stageDetail.put("batchSize", java.lang.Integer.valueOf(batchSize))
        stage.stageDetail.put("delimiter", delimiter)
        stage.stageDetail.put("failMode", failMode.sparkString)
        stage.stageDetail.put("params", params.asJava)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, outputView, uri, persist, inputField, validStatusCodes, batchSize, delimiter, numPartitions, partitionBy, failMode, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}


case class HTTPTransformStage(
    plugin: HTTPTransform,
    name: String,
    description: Option[String],
    uri: URI,
    headers: Map[String, String],
    validStatusCodes: List[Int],
    inputView: String,
    outputView: String,
    inputField: String,
    params: Map[String, String],
    persist: Boolean,
    batchSize: Int,
    delimiter: String,
    numPartitions: Option[Int],
    partitionBy: List[String],
    failMode: FailModeType
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    HTTPTransformStage.execute(this)
  }
}

object HTTPTransformStage {

  /** Phantom Type to enable compiler to find the encoder we want
    */
  type TransformedRow = Row

  def execute(stage: HTTPTransformStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    import spark.implicits._

    val signature = s"HTTPTransform requires a field named '${stage.inputField}' of type 'string' or 'binary'."

    val df = spark.table(stage.inputView)
    val schema = df.schema
    val stageUri = stage.uri
    val stageInputField = stage.inputField
    val stageHeaders = stage.headers
    val stageBatchSize = stage.batchSize
    val stageDelimiter = stage.delimiter
    val stageFailMode = stage.failMode
    val stageValidStatusCodes = stage.validStatusCodes

    val fieldIndex = try {
      schema.fieldIndex(stage.inputField)
    } catch {
      case e: Exception => throw new Exception(s"""${signature} inputView has: [${df.schema.map(_.name).mkString(", ")}].""") with DetailException {
        override val detail = stage.stageDetail
      }
    }

    schema.fields(fieldIndex).dataType match {
      case _: StringType =>
      case _: BinaryType =>
      case _ => throw new Exception(s"""${signature} '${stage.inputField}' is of type: '${schema.fields(fieldIndex).dataType.simpleString}'.""") with DetailException {
        override val detail = stage.stageDetail
      }
    }


    val typedSchema = stage.failMode match {
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
        val uri = stageUri.toString

        // we are using a BufferedIterator so we can 'peek' at the first row to get column types without advancing the iterator
        // meaning we don't have to keep finding fieldIndex and dataType for each row (inefficient as they will not change)
        val bufferedPartition = partition.buffered
        val fieldIndex = bufferedPartition.hasNext match {
          case true => bufferedPartition.head.fieldIndex(stageInputField)
          case false => 0
        }
        val dataType = bufferedPartition.hasNext match {
          case true => bufferedPartition.head.schema(fieldIndex).dataType
          case false => NullType
        }

        // group so we can send multiple rows per request
        val groupedPartition = bufferedPartition.grouped(stageBatchSize)

        groupedPartition.flatMap[TransformedRow] { groupedRow =>
          val post = new HttpPost(uri)

          // add headers
          for ((k,v) <- stageHeaders) {
            post.addHeader(k,v)
          }

          // add payload
          val entity = dataType match {
            case _: StringType => {
              val delimiter = if (stageBatchSize > 1) {
                stageDelimiter
              } else {
                ""
              }        
              new StringEntity(groupedRow.map(row => row.getString(fieldIndex)).mkString(delimiter))
            }
            case _: BinaryType => {
              val delimiter = if (stageBatchSize > 1) {
                stageDelimiter.getBytes
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
            if (stageFailMode == FailModeTypeFailFast && !stageValidStatusCodes.contains(response.getStatusLine.getStatusCode)) {
              throw new Exception(s"""HTTPTransform expects all response StatusCode(s) in [${stageValidStatusCodes.mkString(", ")}] but server responded with ${response.getStatusLine.getStatusCode} (${response.getStatusLine.getReasonPhrase}).""")
            }

            // read and close response
            val content = response.getEntity.getContent
            val body = if (stageBatchSize > 1) {
              Source.fromInputStream(content).mkString.split(stageDelimiter)
            } else {
              Array(Source.fromInputStream(content).mkString)
            }
            response.close

            if (body.length != groupedRow.length) {
              throw new Exception(s"""HTTPTransform expects the response to contain same number of results as 'batchSize' (${stageBatchSize}) but server responded with ${body.length}.""")
            }

            // cast to a TransformedRow to fit the Dataset map method requirements
            groupedRow.zipWithIndex.map { case (row, index) => {
              stageFailMode match {
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
        override val detail = stage.stageDetail
      }
    }

    // re-attach metadata to result
    df.schema.fields.foreach(field => {
      transformedDF = transformedDF.withColumn(field.name, col(field.name).as(field.name, field.metadata))
    })

    // repartition to distribute rows evenly
    val repartitionedDF = stage.partitionBy match {
      case Nil => {
        stage.numPartitions match {
          case Some(numPartitions) => transformedDF.repartition(numPartitions)
          case None => transformedDF
        }
      }
      case partitionBy => {
        // create a column array for repartitioning
        val partitionCols = partitionBy.map(col => transformedDF(col))
        stage.numPartitions match {
          case Some(numPartitions) => transformedDF.repartition(numPartitions, partitionCols:_*)
          case None => transformedDF.repartition(partitionCols:_*)
        }
      }
    }
    if (arcContext.immutableViews) repartitionedDF.createTempView(stage.outputView) else repartitionedDF.createOrReplaceTempView(stage.outputView)

    if (!repartitionedDF.isStreaming) {
      stage.stageDetail.put("outputColumns", java.lang.Integer.valueOf(repartitionedDF.schema.length))
      stage.stageDetail.put("numPartitions", java.lang.Integer.valueOf(repartitionedDF.rdd.partitions.length))

      if (stage.persist) {
        repartitionedDF.persist(arcContext.storageLevel)
        stage.stageDetail.put("records", java.lang.Long.valueOf(repartitionedDF.count))
      }
    }

    Option(repartitionedDF)
  }

}
