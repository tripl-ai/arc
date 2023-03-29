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

  // test("StatisticsExtractSuite: approximate/no histogram") {
  //   val inMemoryLoggerAppender = new InMemoryLoggerAppender()
  //   implicit val spark = session
  //   import spark.implicits._
  //   implicit val logger = TestUtils.getLogger(Some(inMemoryLoggerAppender))
  //   implicit val arcContext = TestUtils.getARCContext()

  //   val df = TestUtils.getKnownDataset
  //   df.createOrReplaceTempView(inputView)

  //   val conf = s"""{
  //     "stages": [
  //       {
  //         "type": "StatisticsExtract",
  //         "name": "StatisticsExtract",
  //         "environments": [
  //           "production",
  //           "test"
  //         ],
  //         "inputView": "${inputView}",
  //         "outputView": "${outputView}",
  //         "persist": true,
  //         "approximate": true,
  //         "histogram": false
  //       }
  //     ]
  //   }"""

  //   val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

  //   pipelineEither match {
  //     case Left(err) => fail(err.toString)
  //     case Right((pipeline, _)) => {
  //       val df = ARC.run(pipeline)(spark, logger, arcContext).get
  //       val rows = df.collect
  //       assert(rows.length == 10)
  //       assert(rows.head.length == 12)
  //       assert(inMemoryLoggerAppender.getResult.filter { message => message.contains("booleanDatum\":{\"data_type\":\"boolean\",\"count\":2") }.length == 1)
  //     }
  //   }
  // }

  // test("StatisticsExtractSuite: approximate/histogram") {
  //   val inMemoryLoggerAppender = new InMemoryLoggerAppender()
  //   implicit val spark = session
  //   import spark.implicits._
  //   implicit val logger = TestUtils.getLogger(Some(inMemoryLoggerAppender))
  //   implicit val arcContext = TestUtils.getARCContext()

  //   val df = TestUtils.getKnownDataset
  //   df.createOrReplaceTempView(inputView)

  //   val conf = s"""{
  //     "stages": [
  //       {
  //         "type": "StatisticsExtract",
  //         "name": "StatisticsExtract",
  //         "environments": [
  //           "production",
  //           "test"
  //         ],
  //         "inputView": "${inputView}",
  //         "outputView": "${outputView}",
  //         "persist": true,
  //         "approximate": true,
  //         "histogram": true
  //       }
  //     ]
  //   }"""

  //   val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

  //   pipelineEither match {
  //     case Left(err) => fail(err.toString)
  //     case Right((pipeline, _)) => {
  //       val df = ARC.run(pipeline)(spark, logger, arcContext).get
  //       val rows = df.collect
  //       assert(rows.length == 10)
  //       assert(rows.head.length == 15)
  //       assert(inMemoryLoggerAppender.getResult.filter { message => message.contains("booleanDatum\":{\"data_type\":\"boolean\",\"count\":2") }.length == 1)
  //     }
  //   }
  // }

  // test("StatisticsExtractSuite: no approximate/histogram") {
  //   val inMemoryLoggerAppender = new InMemoryLoggerAppender()
  //   implicit val spark = session
  //   import spark.implicits._
  //   implicit val logger = TestUtils.getLogger(Some(inMemoryLoggerAppender))
  //   implicit val arcContext = TestUtils.getARCContext()

  //   val df = TestUtils.getKnownDataset
  //   df.createOrReplaceTempView(inputView)

  //   val conf = s"""{
  //     "stages": [
  //       {
  //         "type": "StatisticsExtract",
  //         "name": "StatisticsExtract",
  //         "environments": [
  //           "production",
  //           "test"
  //         ],
  //         "inputView": "${inputView}",
  //         "outputView": "${outputView}",
  //         "persist": true,
  //         "approximate": false,
  //         "histogram": true
  //       }
  //     ]
  //   }"""

  //   val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

  //   pipelineEither match {
  //     case Left(err) => fail(err.toString)
  //     case Right((pipeline, _)) => {
  //       val df = ARC.run(pipeline)(spark, logger, arcContext).get
  //       val rows = df.collect
  //       assert(rows.length == 10)
  //       assert(rows.head.length == 15)
  //       assert(inMemoryLoggerAppender.getResult.filter { message => message.contains("booleanDatum\":{\"data_type\":\"boolean\",\"count\":2") }.length == 1)
  //     }
  //   }
  // }

  // test("StatisticsExtractSuite: no approximate/no histogram") {
  //   val inMemoryLoggerAppender = new InMemoryLoggerAppender()
  //   implicit val spark = session
  //   import spark.implicits._
  //   implicit val logger = TestUtils.getLogger(Some(inMemoryLoggerAppender))
  //   implicit val arcContext = TestUtils.getARCContext()

  //   val df = TestUtils.getKnownDataset
  //   df.createOrReplaceTempView(inputView)

  //   val conf = s"""{
  //     "stages": [
  //       {
  //         "type": "StatisticsExtract",
  //         "name": "StatisticsExtract",
  //         "environments": [
  //           "production",
  //           "test"
  //         ],
  //         "inputView": "${inputView}",
  //         "outputView": "${outputView}",
  //         "persist": true,
  //         "approximate": false,
  //         "histogram": false
  //       }
  //     ]
  //   }"""

  //   val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

  //   pipelineEither match {
  //     case Left(err) => fail(err.toString)
  //     case Right((pipeline, _)) => {
  //       val df = ARC.run(pipeline)(spark, logger, arcContext).get
  //       val rows = df.collect
  //       assert(rows.length == 10)
  //       assert(rows.head.length == 12)
  //       assert(inMemoryLoggerAppender.getResult.filter { message => message.contains("booleanDatum\":{\"data_type\":\"boolean\",\"count\":2") }.length == 1)
  //     }
  //   }
  // }
}
