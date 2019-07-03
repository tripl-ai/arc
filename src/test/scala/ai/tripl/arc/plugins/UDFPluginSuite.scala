package ai.tripl.arc.plugins

import ai.tripl.arc.api.API._
import ai.tripl.arc.util.ConfigUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}
import ai.tripl.arc.util.TestUtils

import ai.tripl.arc.udf.UDF

class UDFPluginSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  var logger: ai.tripl.arc.util.log.logger.Logger = _

  before {
    implicit val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark ETL Test")
      .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")   

    session = spark

    implicit val logger = TestUtils.getLogger()
    val arcContext = TestUtils.getARCContext(isStreaming=false)

    // register udf
    UDF.registerUDFs()(spark,logger,arcContext)    
  }

  after {
    session.stop()
  }

  test("UDFPluginSuite: Custom UDFs are registered") {
    implicit val spark = session

    val df = spark.sql("""
    SELECT add_ten(1) AS one_plus_ten, add_twenty(1) AS one_plus_twenty
    """)

    assert(df.first.getInt(0) == 11)
    assert(df.first.getInt(1) == 21)
  }

}
