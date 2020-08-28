package ai.tripl.arc

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.util._

class LogExecuteSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  var testName = ""
  var testURI = FileUtils.getTempDirectoryPath()
  val signature = "LogExecute requires query to return 1 row with [message: string] signature."

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark
  }

  after {
    session.stop
  }

  test("LogExecute: end-to-end") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val conf = s"""{
      "stages": [
        {
          "type": "LogExecute",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "${getClass.getResource("/conf/sql/").toString}/log_basic.sql",
          "sqlParams": {
            "placeholder": "value",
          }
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => ARC.run(pipeline)(spark, logger, arcContext)
    }
  }


  test("LogExecute: null") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val thrown = intercept[Exception with DetailException]  {
      ai.tripl.arc.execute.LogExecuteStage.execute(
        ai.tripl.arc.execute.LogExecuteStage(
          plugin=new ai.tripl.arc.execute.LogExecute,
          name=testName,
          description=None,
          inputURI=new URI(testURI),
          sql="SELECT null",
          sqlParams=Map.empty,
          params=Map.empty
        )
      )
    }
    assert(thrown.getMessage === s"${signature} Query returned [null].")
  }

  test("LogExecute: string") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    ai.tripl.arc.execute.LogExecuteStage.execute(
      ai.tripl.arc.execute.LogExecuteStage(
        plugin=new ai.tripl.arc.execute.LogExecute,
        name=testName,
        description=None,
        inputURI=new URI(testURI),
        sql="SELECT 'message'",
        sqlParams=Map.empty,
        params=Map.empty
      )
    )
  }

  test("LogExecute: json") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    ai.tripl.arc.execute.LogExecuteStage.execute(
      ai.tripl.arc.execute.LogExecuteStage(
        plugin=new ai.tripl.arc.execute.LogExecute,
        name=testName,
        description=None,
        inputURI=new URI(testURI),
        sql="""SELECT '{"stringKey": "stringValue", "numKey": 123}'""",
        sqlParams=Map.empty,
        params=Map.empty
      )
    )
  }

  test("LogExecute: No rows") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val thrown = intercept[Exception with DetailException]  {
      ai.tripl.arc.execute.LogExecuteStage.execute(
        ai.tripl.arc.execute.LogExecuteStage(
          plugin=new ai.tripl.arc.execute.LogExecute,
          name=testName,
          description=None,
          inputURI=new URI(testURI),
          sql="SELECT CAST(NULL AS STRING)",
          sqlParams=Map.empty,
          params=Map.empty
        )
      )
    }
    assert(thrown.getMessage === s"${signature} Query returned [null].")
  }


}