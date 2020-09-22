package ai.tripl.arc.execute

import java.net.URI
import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind._

import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

import com.typesafe.config.{ConfigFactory, ConfigParseOptions, ConfigSyntax}

import ai.tripl.arc.api.API._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.SQLUtils
import ai.tripl.arc.util.Utils

class ConfigExecute extends PipelineStagePlugin with JupyterCompleter {

  val version = Utils.getFrameworkVersion

  val snippet = """{
    |  "type": "ConfigExecute",
    |  "name": "ConfigExecute",
    |  "environments": [
    |    "production",
    |    "test"
    |  ],
    |  "inputURI": "hdfs://*.sql"
    |}""".stripMargin

  val documentationURI = new java.net.URI(s"${baseURI}/execute/#configexecute")

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "id" :: "name" :: "description" :: "environments" :: "inputURI" :: "sql" :: "authentication" :: "sqlParams" :: "params" :: Nil
    val id = getOptionalValue[String]("id")
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

    (id, name, description, parsedURI, sql, validSQL, invalidKeys, authentication) match {
      case (Right(id), Right(name), Right(description), Right(parsedURI), Right(sql), Right(validSQL), Right(invalidKeys), Right(authentication)) =>

        val uri = if (isInputURI) Option(parsedURI) else None

        val stage = ConfigExecuteStage(
          plugin=this,
          id=id,
          name=name,
          description=description,
          inputURI=parsedURI,
          sql=sql,
          sqlParams=sqlParams,
          params=params
        )

        authentication.foreach { authentication => stage.stageDetail.put("authentication", authentication.method) }
        stage.stageDetail.put("sql", sql)
        stage.stageDetail.put("sqlParams", sqlParams.asJava)
        uri.foreach { uri => stage.stageDetail.put("inputURI", uri.toString) }

        Right(stage)
      case _ =>
        val allErrors: Errors = List(id, name, description, parsedURI, sql, validSQL, invalidKeys, authentication).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class ConfigExecuteStage(
    plugin: ConfigExecute,
    id: Option[String],
    name: String,
    description: Option[String],
    inputURI: URI,
    sql: String,
    sqlParams: Map[String, String],
    params: Map[String, String]
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    ConfigExecuteStage.execute(this)
  }

}

object ConfigExecuteStage {

  def execute(stage: ConfigExecuteStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    val signature = "ConfigExecuteStage requires query to return 1 row with [message: string] signature."

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

    if (df.count != 1 || df.schema.length != 1) {
      throw new Exception(s"""${signature} Query returned ${count} rows of type [${df.schema.map(f => f.dataType.simpleString).mkString(", ")}].""") with DetailException {
        override val detail = stage.stageDetail
      }
    }

    try {
      val row = df.first
      val messageIsNull = row.isNullAt(0)

      if (messageIsNull) {
        throw new Exception(s"""${signature} Query returned [null].""") with DetailException {
          override val detail = stage.stageDetail
        }
      }

      val message = row.getString(0)

      // try to parse to json object or array[json object]
      val objectMapper = new ObjectMapper()
      var messageMap = new java.util.HashMap[String, Object]()
      messageMap = objectMapper.readValue(message, classOf[java.util.HashMap[String, Object]])
      stage.stageDetail.put("message", messageMap)

      // add the key/values to the context
      val config = ConfigFactory.parseString(message, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))
      arcContext.resolutionConfig = config.withFallback(arcContext.resolutionConfig)

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
