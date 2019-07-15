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

    val df = Seq((s"testKey,testValue")).toDF("value")
    df.createOrReplaceTempView("inputView")

    val pipelineEither = ArcPipeline.parsePipeline(Option("classpath://conf/lifecycle_plugin.conf"), arcContext)

    pipelineEither match {
      case Left(_) => {
        println(pipelineEither)
        assert(false)
      }
      case Right((pipeline, arcCtx)) => ARC.run(pipeline)(spark, logger, arcCtx)
    } 
    
    val expectedBefore = Seq(("delimited extract", "before", "testValue")).toDF("stage","when","message")
    assert(TestUtils.datasetEquality(expectedBefore, spark.table("before")))

    val expectedAfter = Seq(("delimited extract", "after", "testValue", 1L, true)).toDF("stage","when","message","count","isLast")
    assert(TestUtils.datasetEquality(expectedAfter, spark.table("after")))
  }

}
