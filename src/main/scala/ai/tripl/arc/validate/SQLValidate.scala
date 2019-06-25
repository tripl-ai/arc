package ai.tripl.arc.validate

import java.net.URI
import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.node._

import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

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
import ai.tripl.arc.util.SQLUtils
import ai.tripl.arc.util.Utils

class SQLValidate extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def createStage(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "authentication" :: "sqlParams" :: "params" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputURI = getValue[String]("inputURI") |> parseURI("inputURI") _
    val authentication = readAuthentication("authentication")  
    val inputSQL = inputURI.rightFlatMap{ uri => textContentForURI(uri, "inputURI", authentication) }
    val sqlParams = readMap("sqlParams", c)
    val validSQL = inputSQL |> injectSQLParams("inputURI", sqlParams, false) _ |> validateSQL("inputURI") _
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    (name, description, inputURI, inputSQL, validSQL, invalidKeys) match {
      case (Right(name), Right(description), Right(inputURI), Right(inputSQL), Right(validSQL), Right(invalidKeys)) => 

        val stage = SQLValidateStage(
          plugin=this,
          name=name,
          description=description,
          inputURI=inputURI,
          sql=inputSQL,
          sqlParams=sqlParams,
          params=params
        )

        stage.stageDetail.put("inputURI", inputURI.toString)  
        stage.stageDetail.put("sql", inputSQL)   
        stage.stageDetail.put("sqlParams", sqlParams.asJava)           

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, inputURI, inputSQL, validSQL, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class SQLValidateStage(
    plugin: PipelineStagePlugin,
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
    val count = df.persist(StorageLevel.MEMORY_AND_DISK_SER).count

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


