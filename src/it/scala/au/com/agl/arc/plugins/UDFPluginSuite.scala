package au.com.agl.arc.plugins

import au.com.agl.arc.api.API.ARCContext
import au.com.agl.arc.udf.UDF
import au.com.agl.arc.util.log.LoggerFactory
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

case class UDFTest(value: String)
case class UDFTestResult(value: String, reverse_value: String)

class UDFPluginSuite extends FunSuite with BeforeAndAfter {

    var session: SparkSession = _
    val inputView = "inputView"
    var logger: au.com.agl.arc.util.log.logger.Logger = _

    before {
      implicit val spark = SparkSession
        .builder()
        .master("local[*]")
        .config("spark.ui.port", "9999")
        .appName("Spark ETL Test")
        .getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")

      // set for deterministic timezone
      spark.conf.set("spark.sql.session.timeZone", "UTC")

      session = spark

      logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

      // register udf
      UDF.registerUDFs(spark.sqlContext)(logger)
    }

    after {
      session.stop
    }

  test("UDFPlugin: custom udfs can be called" ) {
    implicit val spark = session
    implicit val l = logger
    import spark.implicits._
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false, lifecyclePlugins=Nil, disableDependencyValidation=false)

    val df = Seq(
      UDFTest("one"),
      UDFTest("TWO")
    ).toDF()

    df.createOrReplaceTempView(inputView)

    var output = spark.sql(s"""
    SELECT
      value, str_reverse(value) as reverse_value
    FROM ${inputView}
    """).as[UDFTestResult]

    assert(output.first.reverse_value == "eno")
  }

}
