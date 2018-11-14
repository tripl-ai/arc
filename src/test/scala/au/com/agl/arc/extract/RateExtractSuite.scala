package au.com.agl.arc

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.util.log.LoggerFactory 

import au.com.agl.arc.util._
import au.com.agl.arc.util.ControlUtils._

class RateExtractSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  

  val outputView = "outputView"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("FATAL")

    session = spark
  }


  after {
    session.stop
  }

  test("RateExtract: Structured Streaming") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=true, ignoreEnvironments=false)

    val extractDataset = extract.RateExtract.extract(
      RateExtract(
        name="dataset",
        outputView=outputView, 
        rowsPerSecond=Option(10),
        rampUpTime=Option(0),
        numPartitions=Option(1),
        params=Map.empty
      )
    ).get

    val writeStream = extractDataset
      .writeStream
      .queryName("extract") 
      .format("memory")
      .start

    val df = spark.table("extract")

    try {
      Thread.sleep(2000)
      assert(df.count != 0)
    } finally {
      writeStream.stop
    }  
  }    
}
