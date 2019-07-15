package ai.tripl.arc.execute

import java.net.URI
import scala.collection.JavaConverters._

import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.entity.StringEntity

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.spark.sql._

import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.HTTPUtils
import ai.tripl.arc.util.Utils

class HTTPExecute extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "uri" :: "headers" :: "payloads" :: "validStatusCodes" :: "params" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val uri = getValue[String]("uri") |> parseURI("uri") _
    val headers = readMap("headers", c)
    val payloads = readMap("payloads", c)
    val validStatusCodes = getValue[IntList]("validStatusCodes", default = Some(200 :: 201 :: 202 :: Nil))
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, description, uri, validStatusCodes, invalidKeys) match {
      case (Right(name), Right(description), Right(uri), Right(validStatusCodes), Right(invalidKeys)) =>

        val stage = HTTPExecuteStage(
          plugin=this,
          name=name,
          description=description,
          uri=uri,
          headers=headers,
          payloads=payloads,
          validStatusCodes=validStatusCodes,
          params=params
        )

        stage.stageDetail.put("uri", uri.toString)
        stage.stageDetail.put("headers", HTTPUtils.maskHeaders("Authorization" :: Nil)(stage.headers).asJava)
        stage.stageDetail.put("validStatusCodes", validStatusCodes.asJava)
        stage.stageDetail.put("params", params.asJava)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(uri, description, validStatusCodes, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class HTTPExecuteStage(
    plugin: HTTPExecute,
    name: String,
    description: Option[String],
    uri: URI,
    headers: Map[String, String],
    payloads: Map[String, String],
    validStatusCodes: List[Int],
    params: Map[String, String]
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    HTTPExecuteStage.execute(this)
  }
}

object HTTPExecuteStage {

  def execute(stage: HTTPExecuteStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    val client = HttpClientBuilder.create.build
    val post = new HttpPost(stage.uri.toString)

    // add headers
    for ((k,v) <- stage.headers) {
      post.addHeader(k,v)
    }

    // create json payload
    if (stage.payloads.size > 0) {
      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      val json = mapper.writeValueAsString(stage.payloads)

      // add payload
      val stringEntity = new StringEntity(json)
      post.setEntity(stringEntity)
    }

    // send the request
    val response = client.execute(post)
    response.close

    val responseMap = new java.util.HashMap[String, Object]()
    responseMap.put("statusCode", new java.lang.Integer(response.getStatusLine.getStatusCode))
    responseMap.put("reasonPhrase", response.getStatusLine.getReasonPhrase)
    stage.stageDetail.put("response", responseMap)

    // verify status code is correct
    if (!stage.validStatusCodes.contains(response.getStatusLine.getStatusCode)) {
      throw new Exception(s"""HTTPExecute expects a response StatusCode in [${stage.validStatusCodes.mkString(", ")}] but server responded with ${response.getStatusLine.getStatusCode} (${response.getStatusLine.getReasonPhrase}).""") with DetailException {
        override val detail = stage.stageDetail
      }
    }

    None
  }
}


