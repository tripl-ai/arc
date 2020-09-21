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

class ConfigExecuteSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  val signature = "ConfigExecute requires query to return 1 row with [message: string] signature."
  val inputView = "inputView"
  val outputView = "outputView"
  val targetFile = getClass.getResource("/conf/").toString

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Arc Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark
  }

  after {
    session.stop
  }

  test("ConfigExecute: end-to-end") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val conf = s"""{
      "stages": [
        {
          "type": "ConfigExecute",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "sql": "SELECT TO_JSON(NAMED_STRUCT('FILE_NAME', 'simple.conf'))",
        },
        {
          "lazy": true,
          "type": "DelimitedExtract",
          "name": "file extract 1",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "${targetFile}"$${FILE_NAME},
          "outputView": "${outputView}"
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        val df = ARC.run(pipeline)(spark, logger, arcContext).get
        assert(df.count == 29)
      }
    }
  }

  test("ConfigExecute: subquery") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val df = TestUtils.getKnownDataset
    df.createOrReplaceTempView(inputView)

    val conf = s"""{
      "stages": [
        {
          "type": "ConfigExecute",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "sql": "SELECT TO_JSON(NAMED_STRUCT('ETL_CONF_SUBQUERY', 'SELECT MAKE_DATE(2016,12,18) AS date'))"
        },
        {
          "lazy": true,
          "type": "SQLTransform",
          "name": "SQLTransform",
          "environments": [
            "production",
            "test"
          ],
          "sql": "SELECT * FROM ${inputView} INNER JOIN ($${ETL_CONF_SUBQUERY}) subquery ON ${inputView}.dateDatum = subquery.date",
          "sqlParams": {
            "ETL_CONF_SUBQUERY": $${ETL_CONF_SUBQUERY}
          },
          "outputView": "${outputView}"
        }
      ]
    }"""

    println(conf)

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        val df = ARC.run(pipeline)(spark, logger, arcContext).get
        assert(df.count == 1)
      }
    }
  }

}