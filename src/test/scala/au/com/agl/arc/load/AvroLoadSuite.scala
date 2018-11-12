package au.com.agl.arc

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.util.log.LoggerFactory 

import au.com.agl.arc.util.TestDataUtils

class AvroLoadSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  
  val targetFile = FileUtils.getTempDirectoryPath() + "extract.avro" 
  val outputView = "dataset"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    session = spark

    // ensure targets removed
    FileUtils.deleteQuietly(new java.io.File(targetFile)) 
  }

  after {
    session.stop()

    // clean up test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile))     
  }

  test("AvroLoad") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val dataset = TestDataUtils.getKnownDataset
    dataset.createOrReplaceTempView(outputView)

    load.AvroLoad.load(
      AvroLoad(
          name=outputView, 
          inputView=outputView, 
          outputURI=new URI(targetFile), 
          partitionBy=Nil, 
          numPartitions=None, 
          authentication=None,
          saveMode=Some(SaveMode.Overwrite), 
          params=Map.empty
      )
    )

    // avro will drop nulls
    // avro will convert date and times to epoch milliseconds
    val expected = dataset
      .drop($"nullDatum")
    val actual = spark.read.format("com.databricks.spark.avro").load(targetFile)

    assert(TestDataUtils.datasetEquality(expected, actual))
  }  

  test("AvroLoad: partitionBy") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val dataset = TestDataUtils.getKnownDataset
    dataset.createOrReplaceTempView(outputView)
    assert(dataset.select(spark_partition_id()).distinct.count === 1)      

    load.AvroLoad.load(
      AvroLoad(
        name=outputView, 
        inputView=outputView, 
        outputURI=new URI(targetFile), 
        partitionBy="booleanDatum" :: Nil, 
        numPartitions=None, 
        authentication=None, 
        saveMode=Some(SaveMode.Overwrite), 
        params=Map.empty
      )
    )

    val actual = spark.read.format("com.databricks.spark.avro").load(targetFile)
    assert(actual.select(spark_partition_id()).distinct.count === 2)
  }  

}
