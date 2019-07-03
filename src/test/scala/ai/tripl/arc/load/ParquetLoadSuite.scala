package ai.tripl.arc

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._

import ai.tripl.arc.util.TestUtils

class ParquetLoadSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  
  val targetFile = FileUtils.getTempDirectoryPath() + "extract.parquet" 
  val outputView = "dataset"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

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

  test("ParquetLoad") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val dataset = TestUtils.getKnownDataset
    dataset.createOrReplaceTempView(outputView)

    load.ParquetLoadStage.execute(
      load.ParquetLoadStage(
        plugin=new load.ParquetLoad,
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

    val expected = dataset.drop($"nullDatum")
    val actual = spark.read.parquet(targetFile)

    assert(TestUtils.datasetEquality(expected, actual))
  }  

  test("ParquetLoad: partitionBy") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val dataset = TestUtils.getKnownDataset
    dataset.createOrReplaceTempView(outputView)
    assert(dataset.select(spark_partition_id()).distinct.count === 1)      

    load.ParquetLoadStage.execute(
      load.ParquetLoadStage(
        plugin=new load.ParquetLoad,
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

    val actual = spark.read.parquet(targetFile)
    assert(actual.select(spark_partition_id()).distinct.count === 2)
  }  

  test("ParquetLoad: Structured Streaming") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=true)

    val readStream = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", "1")
      .load

    readStream.createOrReplaceTempView(outputView)

    load.ParquetLoadStage.execute(
      load.ParquetLoadStage(
        plugin=new load.ParquetLoad,
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

    val actual = spark.read.parquet(targetFile)
    assert(actual.schema.map(_.name).toSet.equals(Array("timestamp", "value").toSet))
  } 

}
