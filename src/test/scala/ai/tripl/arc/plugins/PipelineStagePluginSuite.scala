package ai.tripl.arc.plugins

import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.util.TestUtils

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

class PipelineStagePluginSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _

  val configParms = Map[String, String](
    "foo" -> "bar"
  )

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

  test("PipelineStagePlugin: getName") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val pipeline = ArcPipeline.parsePipeline(Option("classpath://conf/custom_plugin.conf"), arcContext)

    pipeline match {
      case Right((ETLPipeline(TestPipelineStageInstance(plugin, None, name, None, params) :: Nil),_)) =>
        assert(plugin.getClass.getName === "ai.tripl.arc.plugins.TestPipelineStagePlugin")
        assert(name === "custom plugin")
        assert(params === configParms)
      case _ => {
        println(pipeline)
        fail("expected CustomStage")
      }
    }
  }

  test("PipelineStagePlugin: getSimpleName") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val pipeline = ArcPipeline.parsePipeline(Option("classpath://conf/custom_plugin_short.conf"), arcContext)

    pipeline match {
      case Right((ETLPipeline(TestPipelineStageInstance(plugin, None, name, None, params) :: Nil),_)) =>
        assert(plugin.getClass.getName === "ai.tripl.arc.plugins.TestPipelineStagePlugin")
        assert(name === "custom plugin")
        assert(params === configParms)
      case _ => {
        println(pipeline)
        fail("expected CustomStage")
      }
    }
  }


  test("PipelineStagePlugin: Missing") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val pipeline = ArcPipeline.parsePipeline(Option("classpath://conf/custom_plugin_missing.conf"), arcContext)

    pipeline match {
      case Left(stageError) => {
        assert(stageError.toString contains "No plugins found with name ai.tripl.arc.plugins.ThisWillNotBeFound")
      }
      case Right(_) => assert(false)
    }
  }

  test("PipelineStagePlugin: Version Correct") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val pipeline = ArcPipeline.parsePipeline(Option("classpath://conf/custom_plugin_version_correct.conf"), arcContext)

    pipeline match {
      case Right((ETLPipeline(TestPipelineStageInstance(plugin, None, name, None, params) :: Nil),_)) =>
        assert(plugin.getClass.getName === "ai.tripl.arc.plugins.TestPipelineStagePlugin")
        assert(name === "custom plugin")
        assert(params === configParms)
      case _ => {
        println(pipeline)
        fail()
      }
    }
  }

  test("PipelineStagePlugin: Version Correct Long") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val pipeline = ArcPipeline.parsePipeline(Option("classpath://conf/custom_plugin_version_correct_long.conf"), arcContext)

    pipeline match {
      case Right((ETLPipeline(TestPipelineStageInstance(plugin, None, name, None, params) :: Nil),_)) =>
        assert(plugin.getClass.getName === "ai.tripl.arc.plugins.TestPipelineStagePlugin")
        assert(name === "custom plugin")
        assert(params === configParms)
      case _ => {
        println(pipeline)
        fail()
      }
    }
  }

  test("PipelineStagePlugin: Version Incorrect") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val pipeline = ArcPipeline.parsePipeline(Option("classpath://conf/custom_plugin_version_incorrect.conf"), arcContext)

    pipeline match {
      case Left(stageError) => {
        assert(stageError.toString contains "No plugins found with name:version TestPipelineStagePlugin:1.0.2. Available plugins:")
      }
      case Right(_) => assert(false)
    }
  }


}
