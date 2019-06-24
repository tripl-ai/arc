package ai.tripl.arc.plugins

import ai.tripl.arc.api.API._
import ai.tripl.arc.util.ConfigUtils
import ai.tripl.arc.util.log.LoggerFactory
import ai.tripl.arc.config.Error._
import ai.tripl.arc.util.TestUtils

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

class PipelineStagePluginSuite extends FunSuite with BeforeAndAfter {

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

  test("PipelineStagePlugin: getName") {
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val argsMap = collection.mutable.HashMap[String, String]()

    val pipeline = ConfigUtils.parsePipeline(Option("classpath://conf/custom_plugin.conf"), argsMap, arcContext)
    val configParms = Map[String, String](
      "foo" -> "bar"
    )

    pipeline match {
      case Right( (ETLPipeline(ArcCustomStage(plugin, name, None, params) :: Nil), _) ) =>
        assert(name === "custom plugin")
        assert(params === configParms)
        assert(plugin.getClass.getName === "ai.tripl.arc.plugins.ArcCustom")
      case _ => {
        println(pipeline)
        fail("expected CustomStage")
      }
    }
  }

  test("PipelineStagePlugin: getSimpleName") {
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val argsMap = collection.mutable.HashMap[String, String]()

    val pipeline = ConfigUtils.parsePipeline(Option("classpath://conf/custom_plugin_short.conf"), argsMap, arcContext)
    val configParms = Map[String, String](
      "foo" -> "bar"
    )

    pipeline match {
      case Right( (ETLPipeline(ArcCustomStage(plugin, name, None, params) :: Nil), _) ) =>
        assert(name === "custom plugin")
        assert(params === configParms)
        assert(plugin.getClass.getName === "ai.tripl.arc.plugins.ArcCustom")
      case _ => {
        println(pipeline)
        fail("expected CustomStage")
      }
    }
  }  


  test("PipelineStagePlugin: Missing") { 
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val argsMap = collection.mutable.HashMap[String, String]()

    val pipeline = ConfigUtils.parsePipeline(Option("classpath://conf/custom_plugin_missing.conf"), argsMap, arcContext)

    pipeline match {
      case Left(stageError) => {
        assert(stageError.toString contains "No plugins found with name ai.tripl.arc.plugins.ThisWillNotBeFound")
      }
      case Right(_) => assert(false)
    } 
  }   

  test("PipelineStagePlugin: Version Correct") { 
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val argsMap = collection.mutable.HashMap[String, String]()

    val pipeline = ConfigUtils.parsePipeline(Option("classpath://conf/custom_plugin_version_correct.conf"), argsMap, arcContext)

    pipeline match {
      case Right( (ETLPipeline(ArcCustomStage(plugin, name, None, params) :: Nil), _) ) =>
        assert(name === "custom plugin")
        assert(plugin.getClass.getName === "ai.tripl.arc.plugins.ArcCustom")
      case _ => {
        println(pipeline)
        fail()
      }
    }
  }    

  test("PipelineStagePlugin: Version Correct Long") { 
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val argsMap = collection.mutable.HashMap[String, String]()

    val pipeline = ConfigUtils.parsePipeline(Option("classpath://conf/custom_plugin_version_correct_long.conf"), argsMap, arcContext)

    pipeline match {
      case Right( (ETLPipeline(ArcCustomStage(plugin, name, None, params) :: Nil), _) ) =>
        assert(name === "custom plugin")
        assert(plugin.getClass.getName === "ai.tripl.arc.plugins.ArcCustom")
      case _ => {
        println(pipeline)
        fail()
      }
    }
  }      

  test("PipelineStagePlugin: Version Incorrect") { 
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val argsMap = collection.mutable.HashMap[String, String]()

    val pipeline = ConfigUtils.parsePipeline(Option("classpath://conf/custom_plugin_version_incorrect.conf"), argsMap, arcContext)

    pipeline match {
      case Left(stageError) => {
        assert(stageError.toString contains "No plugins found with name:version ArcCustom:1.0.2. Available plugins:")
      }
      case Right(_) => assert(false)
    }
  }     


}
