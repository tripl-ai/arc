package ai.tripl.arc.execute

import java.net.URI
import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind._

import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

import ai.tripl.arc.api.API._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.SQLUtils
import ai.tripl.arc.util.Utils

class ControlFlowExecute extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "key" :: "description" :: "environments" :: "inputURI" :: "authentication" :: "sqlParams" :: "params" :: Nil
    val name = getValue[String]("name")
    val key = getValue[String]("key", default = Some("controlFlowPluginOutcome"))
    val description = getOptionalValue[String]("description")
    val parsedURI = getValue[String]("inputURI") |> parseURI("inputURI") _
    val authentication = readAuthentication("authentication")
    val inputSQL = parsedURI |> textContentForURI("inputURI", authentication) _
    val sqlParams = readMap("sqlParams", c)
    val validSQL = inputSQL |> injectSQLParams("inputURI", sqlParams, false) _ |> validateSQL("inputURI") _
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, key, description, parsedURI, inputSQL, validSQL, invalidKeys) match {
      case (Right(name), Right(key), Right(description), Right(parsedURI), Right(inputSQL), Right(validSQL), Right(invalidKeys)) =>

        val stage = ControlFlowExecuteStage(
          plugin=this,
          name=name,
          key=key,
          description=description,
          inputURI=parsedURI,
          sql=inputSQL,
          sqlParams=sqlParams,
          params=params
        )

        stage.stageDetail.put("key", key)
        stage.stageDetail.put("inputURI", parsedURI.toString)
        stage.stageDetail.put("sql", inputSQL)
        stage.stageDetail.put("sqlParams", sqlParams.asJava)
        stage.stageDetail.put("params", params.asJava)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, key, description, parsedURI, inputSQL, validSQL, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class ControlFlowExecuteStage(
    plugin: ControlFlowExecute,
    name: String,
    key: String,
    description: Option[String],
    inputURI: URI,
    sql: String,
    sqlParams: Map[String, String],
    params: Map[String, String]
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    ControlFlowExecuteStage.execute(this)
  }
}

case class ControlFlowPayload(
  outcome: Boolean,
  message: Option[String],
  messageMap: Option[java.util.HashMap[String, Object]]
)

object ControlFlowExecuteStage {

  def execute(stage: ControlFlowExecuteStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    val signature = "ControlFlowExecute requires query to return 1 row with [outcome: boolean, message: string] signature."

    val stmt = SQLUtils.injectParameters(stage.sql, stage.sqlParams, false)
    stage.stageDetail.put("sql", stmt)

    val df = try {
      spark.sql(stmt)
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }
    val count = df.persist(arcContext.storageLevel).count

    if (df.count != 1 || df.schema.length != 2) {
      throw new Exception(s"""${signature} Query returned ${count} rows of type [${df.schema.map(f => f.dataType.simpleString).mkString(", ")}].""") with DetailException {
        override val detail = stage.stageDetail
      }
    }


    try {
      val row = df.first
      val resultIsNull = row.isNullAt(0)
      val messageIsNull = row.isNullAt(1)

      if (resultIsNull) {
        throw new Exception(s"""${signature} Query returned [null, ${if (messageIsNull) "null" else "not null"}].""") with DetailException {
          override val detail = stage.stageDetail
        }
      }

      val result = row.getBoolean(0)
      val message = row.getString(1)

      // try to parse to json
      try {
        val objectMapper = new ObjectMapper()
        var messageMap = new java.util.HashMap[String, Object]()
        messageMap = objectMapper.readValue(message, classOf[java.util.HashMap[String, Object]])
        stage.stageDetail.put("message", messageMap)
        arcContext.userData.put(stage.key, ControlFlowPayload(result, None, Option(messageMap)))
      } catch {
        case e: Exception =>
          stage.stageDetail.put("message", message)
          arcContext.userData.put(stage.key, ControlFlowPayload(result, Option(message), None))
      }

      stage.stageDetail.put("result", java.lang.Boolean.valueOf(result))

    } catch {
      case e: ClassCastException =>
        throw new Exception(s"${signature} Query returned ${count} rows of type [${df.schema.map(f => f.dataType.simpleString).mkString(", ")}].") with DetailException {
          override val detail = stage.stageDetail
        }
      case e: Exception with DetailException => throw e
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    df.unpersist

    Option(df)
  }
}


