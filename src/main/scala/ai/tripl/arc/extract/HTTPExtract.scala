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

import com.typesafe.config._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.CloudUtils
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.ExtractUtils
import ai.tripl.arc.util.MetadataUtils
import ai.tripl.arc.util.HTTPUtils
import ai.tripl.arc.util.Utils

class HTTPExtract extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "inputURI" :: "outputView" :: "body" :: "headers" :: "method" :: "numPartitions" :: "partitionBy" :: "persist" :: "validStatusCodes" :: "params" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = if(c.hasPath("inputView")) getValue[String]("inputView") else getValue[String]("inputURI")
    val parsedURI = if (!c.hasPath("inputView")) inputView.rightFlatMap(uri => parseURI("inputURI")(uri)) else Right(new URI(""))
    val headers = readMap("headers", c)
    val validStatusCodes = getValue[IntList]("validStatusCodes", default = Some(200 :: 201 :: 202 :: Nil))
    val outputView = getValue[String]("outputView")
    val persist = getValue[Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val contiguousIndex = getValue[Boolean]("contiguousIndex", default = Some(true))
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

        stage.stageDetail.put("headers", HTTPUtils.maskHeaders(stage.headers).asJava)
        stage.stageDetail.put("input", if(c.hasPath("inputView")) inputView else parsedURI)  
        stage.stageDetail.put("method", method)
        stage.stageDetail.put("outputView", outputView)  
        stage.stageDetail.put("persist", Boolean.valueOf(persist))
        stage.stageDetail.put("validStatusCodes", validStatusCodes.asJava)

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
      body: String
  )

  /** Phantom Type to enable compiler to find the encoder we want
    */
  type RequestResponseRow = Row

  def execute(stage: HTTPExtractStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): Option[DataFrame] = {
    import spark.implicits._

    // create a StructType schema for RequestResponse
    val typedSchema = ScalaReflection.schemaFor[RequestResponse].dataType.asInstanceOf[StructType]      

    /** Create a dynamic RowEncoder from the provided schema. We use the phantom
      * TypeRow type to enable implicit resolution to find our encoder.
      */    
    implicit val typedEncoder: Encoder[RequestResponseRow] = org.apache.spark.sql.catalyst.encoders.RowEncoder(typedSchema)

    val responses = try {
      val df = stage.input match {
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
          val uriFieldIndex = stage.uriField match {
            case Some(uriField) => row.fieldIndex(uriField)
            case None => 0
          }
          val bodyFieldIndex = stage.bodyField match {
            case Some(bodyField) => Option(row.fieldIndex(bodyField))
            case None => None
          }
          (uriFieldIndex, bodyFieldIndex)
        } else {
          (0, None)
        }

        bufferedPartition.map[RequestResponseRow] { row: Row =>
          val uri = row.getString(uriFieldIndex)
          val body = (bodyFieldIndex, stage.body) match {
            case (Some(bodyFieldIndex), None) => row.getString(bodyFieldIndex)
            case (Some(bodyFieldIndex), Some(_)) => row.getString(bodyFieldIndex)
            case (None, Some(body)) => body
            case (None, None) => ""
          } 

          val request = stage.method match {
            case "GET" => new HttpGet(uri)
            case "POST" => { 
              val post = new HttpPost(uri)
              post.setEntity(new StringEntity(body))
              post 
            }
          }

          // add headers
          for ((k,v) <- stage.headers) {
            request.addHeader(k,v) 
          }

          try {
            // send the request
            val response = httpClient.execute(request)

            // verify status code is correct
            if (!stage.validStatusCodes.contains(response.getStatusLine.getStatusCode)) {
              throw new Exception(s"""HTTPExtract expects all response StatusCode(s) in [${stage.validStatusCodes.mkString(", ")}] but server responded with ${response.getStatusLine.getStatusCode} (${response.getStatusLine.getReasonPhrase}).""")
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
    repartitionedDF.createOrReplaceTempView(stage.outputView)

    stage.stageDetail.put("outputColumns", Integer.valueOf(repartitionedDF.schema.length))
    stage.stageDetail.put("numPartitions", Integer.valueOf(repartitionedDF.rdd.partitions.length))

    if (stage.persist) {
      repartitionedDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
      stage.stageDetail.put("records", Long.valueOf(repartitionedDF.count)) 
    }    

    Option(repartitionedDF)
  }

}

