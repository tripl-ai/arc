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

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "inputView" :: "authentication" :: "sqlParams" :: "params" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val parsedURI = getValue[String]("inputURI") |> parseURI("inputURI") _
    val authentication = readAuthentication("authentication")
    val inputView = getValue[String]("inputView")
    val inputSQL = parsedURI |> textContentForURI("inputURI", authentication) _
    val sqlParams = readMap("sqlParams", c)
    val validSQL = inputSQL |> injectSQLParams("inputURI", sqlParams, false) _ |> validateSQL("inputURI") _
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, description, parsedURI, inputView, inputSQL, validSQL, invalidKeys, authentication) match {
      case (Right(name), Right(description), Right(parsedURI), Right(inputView), Right(inputSQL), Right(validSQL), Right(invalidKeys), Right(authentication)) =>

        val stage = MetadataValidateStage(
          plugin=this,
          name=name,
          description=description,
          inputURI=parsedURI,
          inputView=inputView,
          sql=inputSQL,
          sqlParams=sqlParams,
          params=params
        )

        authentication.foreach { authentication => stage.stageDetail.put("authentication", authentication.method) }
        stage.stageDetail.put("inputURI", parsedURI.toString)
        stage.stageDetail.put("inputView", inputView)
        stage.stageDetail.put("sql", inputSQL)
        stage.stageDetail.put("sqlParams", sqlParams.asJava)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, parsedURI, inputView, inputSQL, validSQL, invalidKeys, authentication).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class MetadataValidateStage(
    plugin: MetadataValidate,
    name: String,
    description: Option[String],
    inputURI: URI,
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


