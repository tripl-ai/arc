package au.com.agl.arc.plugins

import au.com.agl.arc.api.API.{CustomStage, ETLPipeline}
import au.com.agl.arc.util.ConfigUtils
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

    val env = "test"

    val argsMap = collection.mutable.HashMap[String, String]()

    val pipeline = ConfigUtils.parsePipeline(Option("classpath://conf/dynamic_config_plugin.conf"), argsMap, env)

    pipeline match {
      case Right(ETLPipeline(CustomStage(name, params, stage) :: Nil)) =>
        assert(name === "custom plugin")
        val configParms = Map[String, String](
          "foo" -> "baz"
        )
        assert(params === configParms)
        assert(stage.getClass.getName === "au.com.agl.arc.plugins.ArcCustomPipelineStage")
      case _ => fail("expected CustomStage")
    }
  }

}
