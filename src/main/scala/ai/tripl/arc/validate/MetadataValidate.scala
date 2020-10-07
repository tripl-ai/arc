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
import ai.tripl.arc.util.MetadataUtils

class MetadataValidate extends PipelineStagePlugin with JupyterCompleter {

  val version = Utils.getFrameworkVersion

  val snippet = """{
    |  "type": "MetadataValidate",
    |  "name": "MetadataValidate",
    |  "environments": [
    |    "production",
    |    "test"
    |  ],
    |  "inputView": "inputView",
    |  "inputURI": "hdfs://*.sql"
    |}""".stripMargin

  val documentationURI = new java.net.URI(s"${baseURI}/validate/#metadatavalidate")

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "id" :: "name" :: "description" :: "environments" :: "inputURI" :: "sql" :: "inputView" :: "authentication" :: "sqlParams" :: "params" :: Nil
    val id = getOptionalValue[String]("id")
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val authentication = readAuthentication("authentication")
    val inputView = getValue[String]("inputView")
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

    (id, name, description, inputView, parsedURI, sql, validSQL, invalidKeys, authentication) match {
      case (Right(id), Right(name), Right(description), Right(inputView), Right(parsedURI), Right(sql), Right(validSQL), Right(invalidKeys), Right(authentication)) =>

        val uri = if (isInputURI) Option(parsedURI) else None

        val stage = MetadataValidateStage(
          plugin=this,
          id=id,
          name=name,
          description=description,
          inputURI=uri,
          inputView=inputView,
          sql=sql,
          sqlParams=sqlParams,
          params=params
        )

        authentication.foreach { authentication => stage.stageDetail.put("authentication", authentication.method) }
        stage.stageDetail.put("inputView", inputView)
        uri.foreach { uri => stage.stageDetail.put("inputURI", uri.toString) }
        stage.stageDetail.put("sql", sql)
        stage.stageDetail.put("sqlParams", sqlParams.asJava)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(id, name, description, inputView, parsedURI, sql, validSQL, invalidKeys, authentication).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class MetadataValidateStage(
    plugin: MetadataValidate,
    id: Option[String],
    name: String,
    description: Option[String],
    inputURI: Option[URI],
    inputView: String,
    sql: String,
    sqlParams: Map[String, String],
    params: Map[String, String]
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    MetadataValidateStage.execute(this)
  }

}


object MetadataValidateStage {

  def execute(stage: MetadataValidateStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    val signature = "MetadataValidate requires query to return 1 row with [outcome: boolean, message: string] signature."

    val stmt = SQLUtils.injectParameters(stage.sql, stage.sqlParams, false)
    stage.stageDetail.put("sql", stmt)

    val inputDF = spark.table(stage.inputView)
    val metadataSchemaDF = MetadataUtils.createMetadataDataframe(inputDF)
    metadataSchemaDF.createOrReplaceTempView("metadata")

    val df = try {
      spark.sql(stmt)
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }
    val rows = df.collect

    if (rows.length != 1 || rows.head.schema.length != 2) {
      throw new Exception(s"""${signature} Query returned ${rows.length} rows of type [${df.schema.map(f => f.dataType.simpleString).mkString(", ")}].""") with DetailException {
        override val detail = stage.stageDetail
      }
    }

    try {
      val row = rows.head
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
        stage.stageDetail.put("message", objectMapper.readValue(message, classOf[java.util.HashMap[String, Object]]))
      } catch {
        case e: Exception =>
          stage.stageDetail.put("message", message)
      }

      val result = row.getBoolean(0)

      // if result is false throw exception to exit job
      if (result == false) {
        throw new Exception(s"MetadataValidate failed with message: '${message}'.") with DetailException {
          override val detail = stage.stageDetail
        }
      }
    } catch {
      case e: ClassCastException =>
        throw new Exception(s"${signature} Query returned ${rows.length} rows of type [${df.schema.map(f => f.dataType.simpleString).mkString(", ")}].") with DetailException {
          override val detail = stage.stageDetail
        }
      case e: Exception with DetailException => throw e
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    Option(df)
  }
}


