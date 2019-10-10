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

class ControlFlowExecuteSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _

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
    import spark.implicits._
  }

  after {
    session.stop
  }

  // if sql returns true then delimitedextract will run and try to read inputView which does not exist
  test("ControlFlowExecute: end-to-end positive") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val conf = s"""
    {
      "plugins": {
        "lifecycle": [
          {
            "type": "ai.tripl.arc.plugins.lifecycle.ControlFlow",
            "environments": [
              "production",
              "test"
            ],
            "key": "entryCriteriaMet"
          }
        ],
      },
      "stages": [
        {
          "type": "ControlFlowExecute",
          "name": "test",
          "key": "entryCriteriaMet",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "${getClass.getResource("/conf/sql/").toString}/false.sql"
        },
        {
          "type": "DelimitedExtract",
          "name": "delimited extract",
          "environments": [
            "production",
            "test"
          ],
          "inputView": "inputView",
          "outputView": "outputView",
          "delimiter": "Comma",
          "quote": "DoubleQuote",
          "header": false
        }
      ]
    }
    """

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(error) => {
        println(error)
        assert(false)
      }
      case Right((pipeline, ctx)) => ARC.run(pipeline)(spark, logger, ctx)
    }
  }  

  // if sql returns true then delimitedextract will run and try to read inputView which does not exist
  test("ControlFlowExecute: end-to-end negative") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val conf = s"""
    {
      "plugins": {
        "lifecycle": [
          {
            "type": "ai.tripl.arc.plugins.lifecycle.ControlFlow",
            "environments": [
              "production",
              "test"
            ],
            "key": "entryCriteriaMet"
          }
        ],
      },
      "stages": [
        {
          "type": "ControlFlowExecute",
          "name": "test",
          "key": "entryCriteriaMet",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "${getClass.getResource("/conf/sql/").toString}/true.sql"
        },
        {
          "type": "DelimitedExtract",
          "name": "delimited extract",
          "environments": [
            "production",
            "test"
          ],
          "inputView": "inputView",
          "outputView": "outputView",
          "delimiter": "Comma",
          "quote": "DoubleQuote",
          "header": false
        }
      ]
    }
    """

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(error) => {
        println(error)
        assert(false)
      }
      case Right((pipeline, ctx)) => {
        val thrown = intercept[Exception with DetailException] {
          ARC.run(pipeline)(spark, logger, ctx)
        }
        assert(thrown.getMessage.contains("Table or view not found: inputView"))
      }
    }
  }  
}
