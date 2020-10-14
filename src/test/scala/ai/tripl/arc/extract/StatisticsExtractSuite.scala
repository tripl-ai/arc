package ai.tripl.arc

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API.Extract
import ai.tripl.arc.config._
import ai.tripl.arc.util._

class StatisticsExtractSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  val inputView = "inputView"
  val outputView = "outputView"

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
    import spark.implicits._
  }

  after {
    session.stop()
  }

  test("StatisticsExtractSuite: end-to-end approximate") {
    val inMemoryLoggerAppender = new InMemoryLoggerAppender()
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger(Some(inMemoryLoggerAppender))
    implicit val arcContext = TestUtils.getARCContext()

    val df = TestUtils.getKnownDataset
    df.createOrReplaceTempView(inputView)

    val conf = s"""{
      "stages": [
        {
          "type": "StatisticsExtract",
          "name": "StatisticsExtract",
          "environments": [
            "production",
            "test"
          ],
          "inputView": "${inputView}",
          "outputView": "${outputView}",
          "persist": true,
          "approximate": true
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        val df = ARC.run(pipeline)(spark, logger, arcContext).get
        val rows = df.collect
        assert(rows.length == 10)
        assert(rows.head.length == 11)
        assert(inMemoryLoggerAppender.getResult.split("\n").filter { message => message.contains("booleanDatum\":{\"count\":2,\"distinct_count\":2") }.length == 1)
      }
    }
  }

  test("StatisticsExtractSuite: end-to-end population") {
    val inMemoryLoggerAppender = new InMemoryLoggerAppender()
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger(Some(inMemoryLoggerAppender))
    implicit val arcContext = TestUtils.getARCContext()

    val df = TestUtils.getKnownDataset
    df.createOrReplaceTempView(inputView)

    val conf = s"""{
      "stages": [
        {
          "type": "StatisticsExtract",
          "name": "StatisticsExtract",
          "environments": [
            "production",
            "test"
          ],
          "inputView": "${inputView}",
          "outputView": "${outputView}",
          "persist": true,
          "approximate": false
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        val df = ARC.run(pipeline)(spark, logger, arcContext).get
        val rows = df.collect
        assert(rows.length == 10)
        assert(rows.head.length == 11)
        assert(inMemoryLoggerAppender.getResult.split("\n").filter { message => message.contains("booleanDatum\":{\"count\":2,\"distinct_count\":2") }.length == 1)
      }
    }
  }
}
