package ai.tripl.arc.plugins

import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.util.TestUtils

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

class DynamicConfigurationPluginSuite extends FunSuite with BeforeAndAfter {

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

  test("Read config with dynamic configuration plugin") {
    implicit val spark = session

    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val pipeline = ArcPipeline.parsePipeline(Option("classpath://conf/dynamic_config_plugin.conf"), arcContext)
    val configParms = Map[String, String](
      "foo" -> "baz",
      "bar" -> "testValue"
    )

    pipeline match {
      case Right((ETLPipeline(TestPipelineStageInstance(plugin, name, None, params) :: Nil),_)) =>
        assert(plugin.getClass.getName === "ai.tripl.arc.plugins.TestPipelineStagePlugin")
        assert(name === "custom plugin")
        assert(params === configParms)
      case _ => fail("expected PipelineStage")
    }
  }

  test("Test commandLineArguments precedence") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    val commandLineArguments = Map[String, String]("ARGS_MAP_VALUE" -> "before\"${arc.paramvalue}\"after")
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false, commandLineArguments=commandLineArguments)

    val pipeline = ArcPipeline.parsePipeline(Option("classpath://conf/dynamic_config_plugin_precendence.conf"), arcContext)
    val configParms = Map[String, String](
      "foo" -> "beforeparamValueafter"
    )

    pipeline match {
      case Right((ETLPipeline(TestPipelineStageInstance(plugin, name, None, params) :: Nil),_)) =>
        assert(name === "custom plugin")
        assert(params === configParms)
        assert(plugin.getClass.getName === "ai.tripl.arc.plugins.TestPipelineStagePlugin")
      case _ => {
        println(pipeline)
        fail("expected PipelineStage")
      }
    }
  }

  test("Read config with dynamic configuration plugin environments ") {
    implicit val spark = session

    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false, environment="production")

    val pipeline = ArcPipeline.parsePipeline(Option("classpath://conf/dynamic_config_plugin.conf"), arcContext)
    val configParms = Map[String, String](
      "foo" -> "baz",
      "bar" -> "productionValue"
    )

    pipeline match {
      case Right((ETLPipeline(TestPipelineStageInstance(plugin, name, None, params) :: Nil),_)) =>
        assert(name === "custom plugin")
        assert(params === configParms)
        assert(plugin.getClass.getName === "ai.tripl.arc.plugins.TestPipelineStagePlugin")
      case _ => fail("expected PipelineStage")
    }
  }

}
