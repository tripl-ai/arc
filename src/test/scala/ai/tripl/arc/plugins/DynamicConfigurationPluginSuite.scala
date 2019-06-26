package ai.tripl.arc.plugins

import ai.tripl.arc.api.API._
import ai.tripl.arc.util.ConfigUtils
import ai.tripl.arc.util.log.LoggerFactory
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
    spark.sparkContext.setLogLevel("ERROR")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")   

    session = spark
  }

  after {
    session.stop()
  }

  test("Read config with dynamic configuration plugin") {
    implicit val spark = session

    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val pipeline = ConfigUtils.parsePipeline(Option("classpath://conf/dynamic_config_plugin.conf"), arcContext)
    val configParms = Map[String, String](
      "foo" -> "baz",
      "bar" -> "testValue"
    )

    pipeline match {
      case Right( (ETLPipeline(ArcCustomStage(plugin, name, None, params) :: Nil), _) ) =>
        assert(name === "custom plugin")
        assert(params === configParms)
        assert(plugin.getClass.getName === "ai.tripl.arc.plugins.ArcCustom")
      case _ => fail("expected PipelineStage")
    }
  }

  test("Test commandLineArguments precedence") { 
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    val commandLineArguments = Map[String, String]("ARGS_MAP_VALUE" -> "before\"${arc.paramvalue}\"after")
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false, commandLineArguments=commandLineArguments)

    val pipeline = ConfigUtils.parsePipeline(Option("classpath://conf/dynamic_config_plugin_precendence.conf"), arcContext)
    val configParms = Map[String, String](
      "foo" -> "beforeparamValueafter"
    )

    pipeline match {
      case Right( (ETLPipeline(ArcCustomStage(plugin, name, None, params) :: Nil), _) ) =>
        assert(name === "custom plugin")
        assert(params === configParms)
        assert(plugin.getClass.getName === "ai.tripl.arc.plugins.ArcCustom")
      case _ => {
        println(pipeline)
        fail("expected PipelineStage")
      }
    } 
  }     

  test("Read config with dynamic configuration plugin environments ") {
    implicit val spark = session

    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false, environment="production")

    val pipeline = ConfigUtils.parsePipeline(Option("classpath://conf/dynamic_config_plugin.conf"), arcContext)
    val configParms = Map[String, String](
      "foo" -> "baz",
      "bar" -> "productionValue"
    )

    pipeline match {
      case Right( (ETLPipeline(ArcCustomStage(plugin, name, None, params) :: Nil), _) ) =>
        assert(name === "custom plugin")
        assert(params === configParms)
        assert(plugin.getClass.getName === "ai.tripl.arc.plugins.ArcCustom")
      case _ => fail("expected PipelineStage")
    }
  }    
}
