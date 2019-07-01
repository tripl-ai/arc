package ai.tripl.arc.load

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
import ai.tripl.arc.util.ListenerUtils
import ai.tripl.arc.util.HTTPUtils
import ai.tripl.arc.util.Utils

class HTTPLoad extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputURI" :: "headers" :: "validStatusCodes" :: "params" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = getValue[String]("inputView")
    val outputURI = getValue[String]("outputURI") |> parseURI("outputURI") _
    val headers = readMap("headers", c)
    val validStatusCodes = getValue[IntList]("validStatusCodes", default = Some(200 :: 201 :: 202 :: Nil))
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)     

    (name, description, outputURI, inputView, validStatusCodes, invalidKeys) match {
      case (Right(name), Right(description), Right(outputURI), Right(inputView), Right(validStatusCodes), Right(invalidKeys)) => 
        
      val stage = HTTPLoadStage(
        plugin=this,
        name=name,
        description=description,
        inputView=inputView,
        outputURI=outputURI,
        headers=headers,
        validStatusCodes=validStatusCodes,
        params=params
      )

      stage.stageDetail.put("inputView", inputView)  
      stage.stageDetail.put("outputURI", outputURI.toString)  
      stage.stageDetail.put("headers", HTTPUtils.maskHeaders("Authorization" :: Nil)(stage.headers).asJava)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, outputURI, inputView, validStatusCodes, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

  // case class HTTPLoad() extends Load { val getType = "HTTPLoad" }
case class HTTPLoadStage(
    plugin: HTTPLoad,
    name: String, 
    description: Option[String], 
    inputView: String, 
    outputURI: URI, 
    headers: Map[String, String], 
    validStatusCodes: List[Int], 
    params: Map[String, String]
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    HTTPLoadStage.execute(this)
  }
}

object HTTPLoadStage {

  case class Response(
    statusCode: Int,
    reasonPhrase: String,
    body: String
  )

  def execute(stage: HTTPLoadStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    import spark.implicits._

    val signature = "HTTPLoad requires inputView to be dataset with [value: string] signature."

    val df = spark.table(stage.inputView)      

    if (df.schema.length != 1 || df.schema(0).dataType != StringType) {
        throw new Exception(s"${signature} inputView '${stage.inputView}' has ${df.schema.length} columns of type [${df.schema.map(f => f.dataType.simpleString).mkString(", ")}].") with DetailException {
          override val detail = stage.stageDetail          
      }      
    }    

    val responses = try {
      if (arcContext.isStreaming) {

        df.writeStream.foreach(
          new ForeachWriter[Row] {
            var poolingHttpClientConnectionManager: PoolingHttpClientConnectionManager = _
            var httpClient: CloseableHttpClient = _
            var uri: String = stage.outputURI.toString

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
              for ((k,v) <- stage.headers) {
                post.addHeader(k,v) 
              }

              post.setEntity(new StringEntity(row.getString(0)))       

              val response = httpClient.execute(post)

              // verify status code is correct
              if (!stage.validStatusCodes.contains(response.getStatusLine.getStatusCode)) {
                throw new Exception(s"""HTTPLoad expects all response StatusCode(s) in [${stage.validStatusCodes.mkString(", ")}] but server responded with ${response.getStatusLine.getStatusCode} (${response.getStatusLine.getReasonPhrase}).""")
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
          val uri = stage.outputURI.toString

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
            for ((k,v) <- stage.headers) {
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
              if (!stage.validStatusCodes.contains(response.getStatusLine.getStatusCode)) {
                throw new Exception(s"""HTTPLoad expects all response StatusCode(s) in [${stage.validStatusCodes.mkString(", ")}] but server responded with ${response.getStatusLine.getStatusCode} (${response.getStatusLine.getReasonPhrase}).""")
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
        override val detail = stage.stageDetail          
      }
    }
    
    responses
  }
}