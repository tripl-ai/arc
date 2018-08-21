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

class DelimitedLoadSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  
  val targetFile = FileUtils.getTempDirectoryPath() + "extract.csv" 
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

    // ensure target removed
    FileUtils.deleteQuietly(new java.io.File(targetFile)) 
  }

  after {
    session.stop()

    // clean up test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile))     
  }

  test("DelimitedLoad") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val dataset = TestDataUtils.getKnownDataset
    dataset.createOrReplaceTempView(outputView)

    load.DelimitedLoad.load(
      DelimitedLoad(
          name=outputView, 
          inputView=outputView, 
          outputURI=new URI(targetFile), 
          settings=new Delimited(header=true, sep=Delimiter.Comma),
          partitionBy=Nil, 
          numPartitions=None, 
          authentication=None, 
          saveMode=Some(SaveMode.Overwrite), 
          params=Map.empty
      )
    )

    // Delimited will load everything as StringType
    // Delimited does not support writing NullType
    val expected = TestDataUtils.getKnownDataset
      .withColumn("booleanDatum", $"booleanDatum".cast("string"))
      .withColumn("dateDatum", $"dateDatum".cast("string"))
      .withColumn("decimalDatum", $"decimalDatum".cast("string"))
      .withColumn("doubleDatum", $"doubleDatum".cast("string"))
      .withColumn("integerDatum", $"integerDatum".cast("string"))
      .withColumn("longDatum", $"longDatum".cast("string"))
      .withColumn("timestampDatum", from_unixtime(unix_timestamp($"timestampDatum"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
      .drop($"nullDatum")
    val actual = spark.read.option("header", true).csv(targetFile)

    assert(TestDataUtils.datasetEquality(expected, actual))
  }  

  test("DelimitedLoad: partitionBy") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val dataset = TestDataUtils.getKnownDataset
    dataset.createOrReplaceTempView(outputView)
    assert(dataset.select(spark_partition_id()).distinct.count === 1)      

    load.DelimitedLoad.load(
      DelimitedLoad(
        name=outputView, 
        inputView=outputView, 
        outputURI=new URI(targetFile), 
        settings=new Delimited(header=true, sep=Delimiter.Comma),
        partitionBy="booleanDatum" :: Nil, 
        numPartitions=None, 
        authentication=None, 
        saveMode=Some(SaveMode.Overwrite), 
        params=Map.empty
      )
    )

    val actual = spark.read.csv(targetFile)
    assert(actual.select(spark_partition_id()).distinct.count === 2)
  }  

}
