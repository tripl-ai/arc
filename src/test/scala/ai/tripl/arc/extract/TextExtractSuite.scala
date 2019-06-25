package ai.tripl.arc

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.util.log.LoggerFactory 

import ai.tripl.arc.util._
import ai.tripl.arc.util.ControlUtils._

class TextExtractSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  

  val outputView = "outputView"
  val targetFile = getClass.getResource("/conf/simple.conf").toString
  val targetDirectory = s"""${getClass.getResource("/conf").toString}/*.conf"""
  val emptyDirectory = FileUtils.getTempDirectoryPath() + "empty.text" 

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("FATAL")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")    

    session = spark
  }


  after {
    session.stop
  }

  test("TextExtract: multiLine false") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val dataset = extract.TextExtractStage.execute(
      extract.TextExtractStage(
        plugin=new extract.TextExtract,
        name="dataset",
        description=None,
        schema=Right(List.empty),
        outputView=outputView, 
        input=targetFile,
        authentication=None,
        persist=false,
        numPartitions=None,
        contiguousIndex=true,
        multiLine=false,
        basePath=None,
        params=Map.empty
      )
    ).get

    assert(dataset.filter($"_filename".contains(targetFile.replace("file:", "file://"))).count != 0)
    assert(dataset.count == 29)
  }    

  test("TextExtract: multiLine true") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val dataset = extract.TextExtractStage.execute(
      extract.TextExtractStage(
        plugin=new extract.TextExtract,
        name="dataset",
        description=None,
        schema=Right(List.empty),
        outputView=outputView, 
        input=targetFile,
        authentication=None,
        persist=false,
        numPartitions=None,
        contiguousIndex=true,
        multiLine=true,
        basePath=None,
        params=Map.empty
      )
    ).get

    assert(dataset.filter($"_filename".contains(targetFile.replace("file:", "file://"))).count != 0)
    assert(dataset.count == 1)
  }    

  test("TextExtract: Empty Dataset") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    // try with wildcard
    val thrown0 = intercept[Exception with DetailException] {
      extract.TextExtractStage.execute(
        extract.TextExtractStage(
          plugin=new extract.TextExtract,
          name="dataset",
          description=None,
          schema=Right(List.empty),
          outputView=outputView, 
          input=emptyDirectory,
          authentication=None,
          persist=false,
          numPartitions=None,
          contiguousIndex=true,
          multiLine=true,
          basePath=None,
          params=Map.empty
        )
      )
    }

    assert(thrown0.getMessage === "TextExtract has produced 0 columns and no schema has been provided to create an empty dataframe.")
  }  

  test("TextExtract: Structured Streaming") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=true)

    val jsonSchema = """
    [
      {
        "id": "982cbf60-7ba7-4e50-a09b-d8624a5c49e6",
        "name": "value",
        "description": "value",
        "type": "string",
        "trim": false,
        "nullable": false,
        "nullableValues": [
          "",
          "null"
        ],
        "metadata": {
          "booleanMeta": true,
          "booleanArrayMeta": [true, false],
          "stringMeta": "string",
          "stringArrayMeta": ["string0", "string1"],
          "longMeta": 10,
          "longArrayMeta": [10,20],
          "doubleMeta": 0.141,
          "doubleArrayMeta": [0.141, 0.52],
          "private": false,
          "securityLevel": 0
        }
      }
    ]
    """
    val schema = ai.tripl.arc.util.MetadataSchema.parseJsonMetadata(jsonSchema)

    val dataset = extract.TextExtractStage.execute(
      extract.TextExtractStage(
        plugin=new extract.TextExtract,
        name="dataset",
        description=None,
        schema=Right(schema.right.getOrElse(Nil)),
        outputView=outputView, 
        input=targetDirectory,
        authentication=None,
        persist=false,
        numPartitions=None,
        contiguousIndex=true,
        multiLine=true,
        basePath=None,
        params=Map.empty
      )
    ).get

    val writeStream = dataset
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
