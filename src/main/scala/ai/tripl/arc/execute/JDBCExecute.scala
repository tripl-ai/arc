package ai.tripl.arc.execute

import java.lang._
import java.net.URI
import scala.collection.JavaConverters._

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.Properties

import org.apache.spark.sql._

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
import ai.tripl.arc.util.SQLUtils
import ai.tripl.arc.util.ControlUtils._
import ai.tripl.arc.util.ListenerUtils
import ai.tripl.arc.util.Utils

class JDBCExecute extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "jdbcURL" :: "authentication" :: "params" :: "password" :: "sqlParams" :: "user" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val authentication = readAuthentication("authentication")  
    val parsedURI = getValue[String]("inputURI") |> parseURI("inputURI") _
    val inputSQL = parsedURI |> textContentForURI("inputURI", authentication) _
    val jdbcURL = getValue[String]("jdbcURL")
    val driver = jdbcURL |> getJDBCDriver("jdbcURL") _
    val user = getOptionalValue[String]("user")
    val password = getOptionalValue[String]("password")
    val sqlParams = readMap("sqlParams", c)    
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    (name, description, parsedURI, inputSQL, jdbcURL, user, password, driver, invalidKeys) match {
      case (Right(name), Right(description), Right(parsedURI), Right(inputSQL), Right(jdbcURL), Right(user), Right(password), Right(driver), Right(invalidKeys)) => 
        val stage = JDBCExecuteStage(
          plugin=this,
          name=name,
          description=description,
          inputURI=parsedURI,
          jdbcURL=jdbcURL,
          user=user,
          password=password,
          sql=inputSQL,
          sqlParams=sqlParams,
          params=params
        )
  
        stage.stageDetail.put("inputURI", parsedURI.toString)     
        stage.stageDetail.put("sql", inputSQL)
        stage.stageDetail.put("sqlParams", sqlParams.asJava)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, parsedURI, inputSQL, jdbcURL, user, password, driver, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class JDBCExecuteStage(
    plugin: JDBCExecute,
    name: String, 
    description: Option[String], 
    inputURI: URI, 
    jdbcURL: String, 
    user: Option[String], 
    password: Option[String], 
    sql: String, 
    sqlParams: Map[String, String], 
    params: Map[String, String]
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    JDBCExecuteStage.execute(this)
  }
}

object JDBCExecuteStage {

  def execute(stage: JDBCExecuteStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    // replace sql parameters
    val sql = SQLUtils.injectParameters(stage.sql, stage.sqlParams, false)
    stage.stageDetail.put("sql", sql)

    try {
      using(getConnection(stage.jdbcURL, stage.user, stage.password, stage.params)) { conn =>
        using(conn.createStatement) { stmt =>
          val res = stmt.execute(sql)
          // try to get results to throw error if one exists
          if (res) {
            stmt.getResultSet.next
          }
        }
      }

    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail         
      }
    }

    None
  }

  def getConnection(url: String, user: Option[String], password: Option[String], params: Map[String, String]): Connection = {
    val _user = user.orElse(params.get("user"))
    val _password = password.orElse(params.get("password"))


    val props = new Properties()

    (_user, _password) match {
      case (Some(u), Some(p)) =>
        props.setProperty("user", u)
        props.setProperty("password", p)
      case _ =>
    }

    for ( (k,v) <- params) {
      props.setProperty(k, v)
    }
    
    DriverManager.getConnection(url, props)
  }

}