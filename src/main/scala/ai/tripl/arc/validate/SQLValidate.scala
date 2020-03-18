package ai.tripl.arc.validate

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

class SQLValidate extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "sql" :: "authentication" :: "sqlParams" :: "params" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val authentication = readAuthentication("authentication")


    // requires 'inputURI' or 'sql'
    val isInputURI = c.hasPath("inputURI")
    val source = if (isInputURI) "inputURI" else "sql"
    val parsedURI = if (isInputURI) getValue[String]("inputURI") |> parseURI("inputURI") _ else Right(new URI(""))
    val inputSQL = if (isInputURI) parsedURI |> textContentForURI("inputURI", authentication) _ else Right("")
    val inlineSQL = if (!isInputURI) getValue[String]("sql") |> verifyInlineSQLPolicy("sql") _ else Right("")
    val sqlParams = readMap("sqlParams", c)
    val sql = if (isInputURI) inputSQL else inlineSQL
    val validSQL = sql |> injectSQLParams(source, sqlParams, false) _ |> validateSQL(source) _
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, description, parsedURI, sql, validSQL, invalidKeys) match {
      case (Right(name), Right(description), Right(parsedURI), Right(sql), Right(validSQL), Right(invalidKeys)) =>

        val uri = if (isInputURI) Option(parsedURI) else None

        val stage = SQLValidateStage(
          plugin=this,
          name=name,
          description=description,
          inputURI=parsedURI,
          sql=sql,
          sqlParams=sqlParams,
          params=params
        )

        if (uri.isDefined) stage.stageDetail.put("inputURI", parsedURI.toString)
        stage.stageDetail.put("sql", sql)
        stage.stageDetail.put("sqlParams", sqlParams.asJava)
        stage.stageDetail.put("params", params.asJava)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, parsedURI, inputSQL, validSQL, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class SQLValidateStage(
    plugin: SQLValidate,
    name: String,
    description: Option[String],
    inputURI: URI,
    sql: String,
    sqlParams: Map[String, String],
    params: Map[String, String]
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    SQLValidateStage.execute(this)
  }
}

object SQLValidateStage {

  def execute(stage: SQLValidateStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    val signature = "SQLValidate requires query to return 1 row with [outcome: boolean, message: string] signature."

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

    var messageMap = new java.util.HashMap[String, Object]()

    try {
      val row = df.first
      val resultIsNull = row.isNullAt(0)
      val messageIsNull = row.isNullAt(1)

      if (resultIsNull) {
        throw new Exception(s"""${signature} Query returned [null, ${if (messageIsNull) "null" else "not null"}].""") with DetailException {
          override val detail = stage.stageDetail
        }
      }

      val message = row.getString(1)

      // try to parse to json
      try {
        val objectMapper = new ObjectMapper()
        messageMap = objectMapper.readValue(message, classOf[java.util.HashMap[String, Object]])
        stage.stageDetail.put("message", messageMap)
      } catch {
        case e: Exception =>
          stage.stageDetail.put("message", message)
      }

      val result = row.getBoolean(0)

      // if result is false throw exception to exit job
      if (result == false) {
        throw new Exception(s"SQLValidate failed with message: '${message}'.") with DetailException {
          override val detail = stage.stageDetail
        }
      }
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


