package au.com.agl.arc.plugins

import au.com.agl.arc.api.API._
import au.com.agl.arc.util.ConfigUtils
import au.com.agl.arc.util.ConfigUtils._
import au.com.agl.arc.util.log.LoggerFactory
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

    session = spark
  }

  after {
    session.stop()
  }

  test("Read config with dynamic configuration plugin") {
    implicit val spark = session

    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false)

    val argsMap = collection.mutable.HashMap[String, String]()

    val pipeline = ConfigUtils.parsePipeline(Option("classpath://conf/dynamic_config_plugin.conf"), argsMap, ConfigUtils.Graph(Nil, Nil, false), arcContext)

    pipeline match {
      case Right( (ETLPipeline(CustomStage(name, params, stage) :: Nil), graph) ) =>
        assert(name === "custom plugin")
        val configParms = Map[String, String](
          "foo" -> "baz",
          "bar" -> "paramValue"
        )
        assert(params === configParms)
        assert(stage.getClass.getName === "au.com.agl.arc.plugins.ArcCustomPipelineStage")
      case _ => fail("expected CustomStage")
    }
    
  }

  test("Test argsMap precedence") { 
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false)

    val argsMap = collection.mutable.HashMap[String, String]("ARGS_MAP_VALUE" -> "before\"${arc.paramvalue}\"after")

    val pipeline = ConfigUtils.parsePipeline(Option("classpath://conf/dynamic_config_plugin_precendence.conf"), argsMap, ConfigUtils.Graph(Nil, Nil, false), arcContext)

    pipeline match {
      case Right( (ETLPipeline(CustomStage(name, params, stage) :: Nil), graph) ) =>
        assert(name === "custom plugin")
        val configParms = Map[String, String](
          "foo" -> "beforeparamValueafter"
        )
        assert(params === configParms)
        assert(stage.getClass.getName === "au.com.agl.arc.plugins.ArcCustomPipelineStage")
      case _ => fail("expected CustomStage")
    } 
  }     

  test("Test missing plugin") { 
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false)

    val argsMap = collection.mutable.HashMap[String, String]()

    val pipeline = ConfigUtils.parsePipeline(Option("classpath://conf/custom_plugin_missing.conf"), argsMap, ConfigUtils.Graph(Nil, Nil, false), arcContext)

    pipeline match {
      case Left(stageError) => {
        assert(stageError == 
        StageError(0, "unknown",3,List(
            ConfigError("stages", Some(3), "Unknown stage type: 'au.com.agl.arc.plugins.ThisWillNotBeFound'")
          )
        ) :: Nil)
      }
      case Right(_) => assert(false)
    } 
  }     
}
