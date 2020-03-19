package ai.tripl.arc.plugins

import ai.tripl.arc.config._
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.sql.{DataFrame, SparkSession}
import ai.tripl.arc.ARC
import ai.tripl.arc.api.API._
import ai.tripl.arc.util.Utils
import ai.tripl.arc.util.TestUtils

class LifecyclePluginSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _

  before {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark ETL Test")
      .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark
  }

  after {
    session.stop()
  }

  test("Read and execute config with lifecycle configuration plugin") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)
    import spark.implicits._

    // create single row dataset
    val df = Seq((s"testKey,testValue")).toDF("value")
    df.createOrReplaceTempView("inputView")

    val pipelineEither = ArcPipeline.parsePipeline(Option("classpath://conf/lifecycle_plugin.conf"), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, arcCtx)) => ARC.run(pipeline)(spark, logger, arcCtx)
    }

    val expectedBefore = Seq(("delimited extract", "before", "testValue")).toDF("stage","when","message")
    assert(TestUtils.datasetEquality(expectedBefore, spark.table("before")))

    val expectedAfter = Seq(("delimited extract", "after", "testValue", 1L)).toDF("stage","when","message","count")
    assert(TestUtils.datasetEquality(expectedAfter, spark.table("after")))
  }

  test("lifecycle return result without lifecycle") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val conf = s"""{
      "stages": [
        {
          "type": "SQLTransform",
          "name": "test",
          "environments": [
            "production",
            "test"
          ],
          "sql": "SELECT 1 AS id UNION SELECT 2 AS id UNION SELECT 3 AS id",
          "outputView": "outputView"
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, arcCtx)) =>

      val df = ARC.run(pipeline)(spark, logger, arcCtx).get

      assert(df.count == 3)
    }
  }

  test("lifecycle return results and ordering with multiple plugins") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val conf = s"""{
      "plugins": {
        "lifecycle": [
          {
            "type": "ai.tripl.arc.plugins.LimitLifecyclePlugin",
            "environments": ["test"],
            "name": "limitLifecyclePluginTest2",
            "limit": 2,
            "outputView": "limit2"
          },
          {
            "type": "ai.tripl.arc.plugins.LimitLifecyclePlugin",
            "environments": ["test"],
            "name": "limitLifecyclePluginTest1",
            "limit": 1,
            "outputView": "limit1"
          }
        ]
      },
      "stages": [
        {
          "type": "SQLTransform",
          "name": "test",
          "environments": [
            "production",
            "test"
          ],
          "sql": "SELECT 1 AS id UNION SELECT 2 AS id UNION SELECT 3 AS id",
          "outputView": "limit3"
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, arcCtx)) =>


      val df = ARC.run(pipeline)(spark, logger, arcCtx).get

      assert(spark.table("limit3").count == 3)
      assert(spark.table("limit2").count == 2)
      assert(spark.table("limit1").count == 1)
      assert(df.count == 1)
    }
  }

}
