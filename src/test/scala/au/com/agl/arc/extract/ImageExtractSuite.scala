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
import au.com.agl.arc.util._

import au.com.agl.arc.util.TestDataUtils

class ImageExtractSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  
  val targetFile = getClass.getResource("/puppy.jpg").toString
  val emptyDirectory = FileUtils.getTempDirectoryPath() + "empty.jpg" 
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
    import spark.implicits._    
  }

  after {
    session.stop()
  }

  test("ImageExtract") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false)

    val extractDataset = extract.ImageExtract.extract(
      ImageExtract(
        name=outputView,
        description=None,
        outputView=outputView,
        input=targetFile,
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        dropInvalid=true
      )
    ).get

    // test that the filename is correctly populated
    assert(extractDataset.filter($"image.origin".contains(targetFile.replace("file:", "file://"))).count == 1)
    assert(extractDataset.filter("image.width = 640").count == 1)
    assert(extractDataset.filter("image.height == 960").count == 1)
    assert(extractDataset.filter("image.nChannels == 3").count == 1)
    assert(extractDataset.filter("image.mode == 16").count == 1)
  }  

  test("ImageExtract Caching") {
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false)

    // no cache
    extract.ImageExtract.extract(
      ImageExtract(
        name=outputView,
        description=None,
        outputView=outputView,
        input=targetFile,
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        dropInvalid=true
      )
    )
    assert(spark.catalog.isCached(outputView) === false)

    // cache
    extract.ImageExtract.extract(
      ImageExtract(
        name=outputView,
        description=None,
        outputView=outputView,
        input=targetFile,
        authentication=None,
        params=Map.empty,
        persist=true,
        numPartitions=None,
        partitionBy=Nil,
        dropInvalid=true
      )
    )
    assert(spark.catalog.isCached(outputView) === true)     
  }  

  test("ImageExtract Empty Dataset") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false)

    val imageExtract = extract.ImageExtract.extract(
      ImageExtract(
        name=outputView,
        description=None,
        outputView=outputView,
        input=emptyDirectory,
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        dropInvalid=true
      )
    ).get
    
    assert(imageExtract.count == 0)
  }  

  test("ImageExtract: Structured Streaming") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=true, ignoreEnvironments=false)

    val extractDataset = extract.ImageExtract.extract(
      ImageExtract(
        name=outputView,
        description=None,
        outputView=outputView,
        input=targetFile.replace("puppy.jpg", "*.jpg"),
        authentication=None,
        params=Map.empty,
        persist=true,
        numPartitions=None,
        partitionBy=Nil,
        dropInvalid=true        
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
      // will fail if parsing does not work
      assert(df.filter($"image.origin".contains(targetFile.replace("file:", "file://"))).count != 0)
    } finally {
      writeStream.stop
    }  
  }    
}
