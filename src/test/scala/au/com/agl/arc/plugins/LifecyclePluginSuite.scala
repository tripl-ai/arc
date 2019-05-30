package au.com.agl.arc.plugins

import au.com.agl.arc.util.ConfigUtils
import au.com.agl.arc.util.ConfigUtils._
import au.com.agl.arc.util.log.LoggerFactory
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.sql.{DataFrame, SparkSession}
import au.com.agl.arc.ARC
import au.com.agl.arc.api.API._
import au.com.agl.arc.util.Utils
import au.com.agl.arc.util.TestDataUtils

class LifecyclePluginSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _

  before {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark ETL Test")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")   

    session = spark
  }

  after {
    session.stop()
  }

  test("Read and execute config with lifecycle configuration plugin") {
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false, lifecyclePlugins=Nil, disableDependencyValidation=false)
    import spark.implicits._

    val argsMap = collection.mutable.HashMap[String, String]()

    val df = Seq((s"testKey,testValue")).toDF("value")
    df.createOrReplaceTempView("inputView")

    val pipelineEither = ConfigUtils.parsePipeline(Option("classpath://conf/lifecycle_plugin.conf"), argsMap, ConfigUtils.Graph(Nil, Nil, false), arcContext)

    pipelineEither match {
      case Left(_) => assert(false)
      case Right((pipeline, _, arcCtx)) => ARC.run(pipeline)(spark, logger, arcCtx)
    } 
    
    val expectedBefore = Seq(("delimited extract", "before", "testValue")).toDF("stage","when","message")
    assert(TestDataUtils.datasetEquality(expectedBefore, spark.table("before")))

    val expectedAfter = Seq(("delimited extract", "after", "testValue", 1L, true)).toDF("stage","when","message","count","isLast")
    assert(TestDataUtils.datasetEquality(expectedAfter, spark.table("after")))
  }

}
