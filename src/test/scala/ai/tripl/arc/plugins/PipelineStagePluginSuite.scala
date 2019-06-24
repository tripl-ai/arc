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

  test("Read config with custom pipeline stage") {
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


  test("Test missing plugin") { 
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val argsMap = collection.mutable.HashMap[String, String]()

    val pipeline = ConfigUtils.parsePipeline(Option("classpath://conf/custom_plugin_missing.conf"), argsMap, arcContext)

    pipeline match {
      case Left(stageError) => {
        assert(stageError == 
        StageError(0,"ai.tripl.arc.plugins.ThisWillNotBeFound",3,List(
            ConfigError("stages", Some(3), "No plugins found with name 'ai.tripl.arc.plugins.ThisWillNotBeFound'")
          )
        ) :: Nil)
      }
      case Right(_) => assert(false)
    } 
  }   


}
