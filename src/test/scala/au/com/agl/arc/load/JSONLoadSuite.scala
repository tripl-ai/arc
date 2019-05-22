package au.com.agl.arc

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import scala.collection.mutable.ListBuffer

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.plugins.LifecyclePlugin
import au.com.agl.arc.util.log.LoggerFactory 

import au.com.agl.arc.util.TestDataUtils

class JSONLoadSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  
  val targetFile = FileUtils.getTempDirectoryPath() + "extract.json" 
  val outputView = "dataset"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")   

    session = spark

    // ensure targets removed
    FileUtils.deleteQuietly(new java.io.File(targetFile)) 
  }

  after {
    session.stop()

    // clean up test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile))     
  }

  test("JSONLoad") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false, lifecyclePlugins=new ListBuffer[LifecyclePlugin]())

    val dataset = TestDataUtils.getKnownDataset
    dataset.createOrReplaceTempView(outputView)

    load.JSONLoad.load(
      JSONLoad(
        name=outputView, 
        description=None,
        inputView=outputView, 
        outputURI=new URI(targetFile), 
        partitionBy=Nil, 
        numPartitions=None, 
        authentication=None, 
        saveMode=SaveMode.Overwrite, 
        params=Map.empty
      )
    )

    val expected = TestDataUtils.getKnownDataset
      .withColumn("decimalDatum", $"decimalDatum".cast("double"))
      .withColumn("timestampDatum", from_unixtime(unix_timestamp($"timestampDatum"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
      .drop($"nullDatum")
    val actual = spark.read.json(targetFile)

    assert(TestDataUtils.datasetEquality(expected, actual))
  }  

  test("JSONLoad: partitionBy") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false, lifecyclePlugins=new ListBuffer[LifecyclePlugin]())

    val dataset = TestDataUtils.getKnownDataset
    dataset.createOrReplaceTempView(outputView)
    assert(dataset.select(spark_partition_id()).distinct.count === 1)      

    load.JSONLoad.load(
      JSONLoad(
        name=outputView, 
        description=None,
        inputView=outputView, 
        outputURI=new URI(targetFile), 
        partitionBy="booleanDatum" :: Nil, 
        numPartitions=None, 
        authentication=None, 
        saveMode=SaveMode.Overwrite, 
        params=Map.empty
      )
    )

    val actual = spark.read.json(targetFile)
    assert(actual.select(spark_partition_id()).distinct.count === 2)
  }  

  test("JSONLoad: Structured Streaming") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=true, ignoreEnvironments=false, lifecyclePlugins=new ListBuffer[LifecyclePlugin]())

    val readStream = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", "1")
      .load

    readStream.createOrReplaceTempView(outputView)

    load.JSONLoad.load(
      JSONLoad(
        name=outputView, 
        description=None,
        inputView=outputView, 
        outputURI=new URI(targetFile), 
        partitionBy=Nil, 
        numPartitions=None, 
        authentication=None, 
        saveMode=SaveMode.Overwrite, 
        params=Map.empty
      )
    )

    Thread.sleep(2000)
    spark.streams.active.foreach(streamingQuery => streamingQuery.stop)

    val actual = spark.read.json(targetFile)
    assert(actual.schema.map(_.name).toSet.equals(Array("timestamp", "value").toSet))
  }    

}
