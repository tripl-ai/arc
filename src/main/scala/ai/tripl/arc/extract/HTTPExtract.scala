package ai.tripl.arc.extract

import java.net.URI
import scala.collection.JavaConverters._

import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.LaxRedirectStrategy

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._

import scala.io.Source

import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.HTTPUtils
import ai.tripl.arc.util.Utils

class HTTPExtract extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "inputURI" :: "outputView" :: "body" :: "headers" :: "method" :: "numPartitions" :: "partitionBy" :: "persist" :: "validStatusCodes" :: "uriField" :: "bodyField" :: "params" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = if(c.hasPath("inputView")) getValue[String]("inputView") else Right("")
    val parsedURI = if (!c.hasPath("inputView")) getValue[String]("inputURI") |> parseURI("inputURI") _ else Right(new URI(""))
    val headers = readMap("headers", c)
    val validStatusCodes = getValue[IntList]("validStatusCodes", default = Some(200 :: 201 :: 202 :: Nil))
    val outputView = getValue[String]("outputView")
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val method = getValue[String]("method", default = Some("GET"), validValues = "GET" :: "POST" :: Nil)
    val uriField = getOptionalValue[String]("uriField")
    val bodyField = getOptionalValue[String]("bodyField")
    val body = getOptionalValue[String]("body")
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, description, inputView, parsedURI, outputView, persist, numPartitions, method, body, partitionBy, validStatusCodes, invalidKeys, uriField, bodyField) match {
      case (Right(name), Right(description), Right(inputView), Right(parsedURI), Right(outputView), Right(persist), Right(numPartitions), Right(method), Right(body), Right(partitionBy), Right(validStatusCodes), Right(invalidKeys), Right(uriField), Right(bodyField)) =>
        val input = if(c.hasPath("inputView")) Left(inputView) else Right(parsedURI)

        val stage = HTTPExtractStage(
          plugin=this,
          name=name,
          description=description,
          input=input,
          method=method,
          headers=headers,
          uriField=uriField,
          bodyField=bodyField,
          body=body,
          validStatusCodes=validStatusCodes,
          outputView=outputView,
          params=params,
          persist=persist,
          numPartitions=numPartitions,
          partitionBy=partitionBy
        )

        stage.stageDetail.put("headers", HTTPUtils.maskHeaders("Authorization" :: Nil)(stage.headers).asJava)
        input match {
          case Left(inputView) => stage.stageDetail.put("inputView", inputView)
          case Right(parsedGlob) =>stage.stageDetail.put("inputURI", parsedGlob)
        }
        stage.stageDetail.put("method", method)
        stage.stageDetail.put("outputView", outputView)
        stage.stageDetail.put("persist", java.lang.Boolean.valueOf(persist))
        stage.stageDetail.put("validStatusCodes", validStatusCodes.asJava)
        stage.stageDetail.put("params", params.asJava)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, parsedURI, outputView, persist, numPartitions, method, body, partitionBy, validStatusCodes, invalidKeys, uriField, bodyField).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

}

case class HTTPExtractStage(
    plugin: HTTPExtract,
    name: String,
    description: Option[String],
    input: Either[String, URI],
    method: String,
    headers: Map[String, String],
    uriField: Option[String],
    bodyField: Option[String],
    body: Option[String],
    validStatusCodes: List[Int],
    outputView: String,
    params: Map[String, String],
    persist: Boolean,
    numPartitions: Option[Int],
    partitionBy: List[String]
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    HTTPExtractStage.execute(this)
  }
}

object HTTPExtractStage {

  case class RequestResponse(
      uri: String,
      statusCode: Int,
      reasonPhrase: String,
      contentType: String,
      contentLength: Long,
      body: String
  )

  /** Phantom Type to enable compiler to find the encoder we want
    */
  type RequestResponseRow = Row

  def execute(stage: HTTPExtractStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    import spark.implicits._

    // create a StructType schema for RequestResponse
    val typedSchema = ScalaReflection.schemaFor[RequestResponse].dataType.asInstanceOf[StructType]

    val stageInput = stage.input
    val stageUriField = stage.uriField
    val stageBodyField = stage.bodyField
    val stageBody = stage.body
    val stageMethod = stage.method
    val stageHeaders = stage.headers
    val stageValidStatusCodes = stage.validStatusCodes

    /** Create a dynamic RowEncoder from the provided schema. We use the phantom
      * TypeRow type to enable implicit resolution to find our encoder.
      */
    implicit val typedEncoder: Encoder[RequestResponseRow] = org.apache.spark.sql.catalyst.encoders.RowEncoder(typedSchema)

    val responses = try {
      val df = stageInput match {
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
          val uriFieldIndex = stageUriField match {
            case Some(uriField) => row.fieldIndex(uriField)
            case None => 0
          }
          val bodyFieldIndex = stageBodyField match {
            case Some(bodyField) => Option(row.fieldIndex(bodyField))
            case None => None
          }
          (uriFieldIndex, bodyFieldIndex)
        } else {
          (0, None)
        }

        bufferedPartition.map[RequestResponseRow] { row: Row =>
          val uri = row.getString(uriFieldIndex)
          val body = (bodyFieldIndex, stageBody) match {
            case (Some(bodyFieldIndex), None) => row.getString(bodyFieldIndex)
            case (Some(bodyFieldIndex), Some(_)) => row.getString(bodyFieldIndex)
            case (None, Some(body)) => body
            case (None, None) => ""
          }

          val request = stageMethod match {
            case "GET" => new HttpGet(uri)
            case "POST" => {
              val post = new HttpPost(uri)
              post.setEntity(new StringEntity(body))
              post
            }
          }

          // add headers
          for ((k,v) <- stageHeaders) {
            request.addHeader(k,v)
          }

          try {
            // send the request
            val response = httpClient.execute(request)

            // verify status code is correct
            if (!stageValidStatusCodes.contains(response.getStatusLine.getStatusCode)) {
              throw new Exception(s"""HTTPExtract expects all response StatusCode(s) in [${stageValidStatusCodes.mkString(", ")}] but server responded with ${response.getStatusLine.getStatusCode} (${response.getStatusLine.getReasonPhrase}).""")
            }

            val headers = response.getAllHeaders

            // read and close response
            val (body, contentLength) = response.getEntity.getContentLength match {
              // empty
              case 0 => (None, 0L)
              // unknown
              case -1 => {
                val body = Source.fromInputStream(response.getEntity.getContent).mkString
                (Option(body), body.length.toLong)
              }
              // known
              case contentLength: Long => {
                val body = Source.fromInputStream(response.getEntity.getContent).mkString
                (Option(body), contentLength)
              }
            }
            response.close

            // cast to a RequestResponseRow to fit the Dataset map method requirements
            Row.fromSeq(
              Seq(
                uri,
                response.getStatusLine.getStatusCode,
                response.getStatusLine.getReasonPhrase,
                Option(response.getEntity.getContentType).map(_.toString).orNull,
                contentLength,
                body.orNull
              )
            ).asInstanceOf[RequestResponseRow]
          } finally {
            request.releaseConnection
          }
        }
      }
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    val df = responses.toDF

    // repartition to distribute rows evenly
    val repartitionedDF = stage.partitionBy match {
      case Nil => {
        stage.numPartitions match {
          case Some(numPartitions) => df.repartition(numPartitions)
          case None => df
        }
      }
      case partitionBy => {
        // create a column array for repartitioning
        val partitionCols = partitionBy.map(col => df(col))
        stage.numPartitions match {
          case Some(numPartitions) => df.repartition(numPartitions, partitionCols:_*)
          case None => df.repartition(partitionCols:_*)
        }
      }
    }
    if (arcContext.immutableViews) repartitionedDF.createTempView(stage.outputView) else repartitionedDF.createOrReplaceTempView(stage.outputView)

    stage.stageDetail.put("outputColumns", java.lang.Integer.valueOf(repartitionedDF.schema.length))
    stage.stageDetail.put("numPartitions", java.lang.Integer.valueOf(repartitionedDF.rdd.partitions.length))

    if (stage.persist) {
      repartitionedDF.persist(arcContext.storageLevel)

      val records = repartitionedDF.count
      stage.stageDetail.put("records", java.lang.Long.valueOf(records))
      if (records != 0) {
        stage.stageDetail.put("contentLength", java.lang.Long.valueOf(repartitionedDF.first.getLong(4)))
      }
    }

    Option(repartitionedDF)
  }

}

