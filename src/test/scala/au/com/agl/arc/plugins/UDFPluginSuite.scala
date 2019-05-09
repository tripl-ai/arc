package au.com.agl.arc.plugins

import au.com.agl.arc.api.API._
import au.com.agl.arc.util.ConfigUtils
import au.com.agl.arc.util.log.LoggerFactory
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

import au.com.agl.arc.udf.UDF

class UDFPluginSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  var logger: au.com.agl.arc.util.log.logger.Logger = _

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

    logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    // register udf
    UDF.registerUDFs(spark.sqlContext)(logger)    
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
