package au.com.agl.arc

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.util.log.LoggerFactory 

import au.com.agl.arc.util._

class MetadataFilterTransformSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  
  val targetFile = FileUtils.getTempDirectoryPath() + "MetadataFilterTransformSuite.csv"   
  val inputView = "inputView"
  val outputView = "outputView"

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

    // recreate test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile)) 
    // Delimited does not support writing NullType
    TestDataUtils.getKnownDataset.drop($"nullDatum").write.csv(targetFile)    
  }

  after {
    session.stop()

    // clean up test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile))     
  }

  test("MetadataFilterTransform: private=true") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    // load csv
    val extractDataset = spark.read.csv(targetFile)
    extractDataset.createOrReplaceTempView(inputView)

    // parse json schema to List[ExtractColumn]
    val cols = au.com.agl.arc.util.MetadataSchema.parseJsonMetadata(TestDataUtils.getKnownDatasetMetadataJson)

    transform.TypingTransform.transform(
      TypingTransform(
        name="TypingTransform",
        cols=Right(cols.right.getOrElse(null)), 
        inputView=inputView,
        outputView=outputView, 
        params=Map.empty,
        persist=false,
        failMode=FailModeTypePermissive
      )
    )

    val transformed = transform.MetadataFilterTransform.transform(
      MetadataFilterTransform(
        name="MetadataFilterTransform", 
        inputView=outputView,
        inputURI=new URI(targetFile),
        sql=s"SELECT * FROM metadata WHERE metadata.private=false",
        outputView=outputView,
        persist=false,
        sqlParams=Map.empty,
        params=Map.empty
      )
    ).get

    val actual = transformed.drop($"_errors")
    val expected = TestDataUtils.getKnownDataset.select($"booleanDatum", $"longDatum", $"stringDatum")

    assert(TestDataUtils.datasetEquality(expected, actual))
  }  

  test("MetadataFilterTransform: securityLevel") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    // load csv
    val extractDataset = spark.read.csv(targetFile)
    extractDataset.createOrReplaceTempView(inputView)

    // parse json schema to List[ExtractColumn]
    val cols = au.com.agl.arc.util.MetadataSchema.parseJsonMetadata(TestDataUtils.getKnownDatasetMetadataJson)

    transform.TypingTransform.transform(
      TypingTransform(
        name="TypingTransform",
        cols=Right(cols.right.getOrElse(null)), 
        inputView=inputView,
        outputView=outputView, 
        params=Map.empty,
        persist=false,
        failMode=FailModeTypePermissive
      )
    )

    val transformed = transform.MetadataFilterTransform.transform(
      MetadataFilterTransform(
        name="MetadataFilterTransform", 
        inputView=outputView,
        inputURI=new URI(targetFile),
        sql=s"SELECT * FROM metadata WHERE metadata.securityLevel <= 4",
        outputView=outputView,
        persist=false,
        sqlParams=Map.empty,
        params=Map.empty
      )
    ).get

    val actual = transformed.drop($"_errors")
    val expected = TestDataUtils.getKnownDataset.select($"booleanDatum", $"dateDatum", $"decimalDatum", $"longDatum", $"stringDatum")

    assert(TestDataUtils.datasetEquality(expected, actual))
  }   

  test("MetadataFilterTransform: Structured Streaming") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val dataset = TestDataUtils.getKnownStringDataset.drop("nullDatum")
    dataset.createOrReplaceTempView("knownData")

    val readStream = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", "1")
      .load

    readStream.createOrReplaceTempView("readstream")

    // cross join to the rate stream purely to register this dataset as a 'streaming' dataset.
    val input = spark.sql(s"""
    SELECT knownData.*
    FROM knownData
    CROSS JOIN readstream ON true
    """)

    input.createOrReplaceTempView(inputView)

    // parse json schema to List[ExtractColumn]
    val cols = au.com.agl.arc.util.MetadataSchema.parseJsonMetadata(TestDataUtils.getKnownDatasetMetadataJson)

    val transformDataset = transform.TypingTransform.transform(
      TypingTransform(
        name="dataset",
        cols=Right(cols.right.getOrElse(Nil)), 
        inputView=inputView,
        outputView=outputView, 
        params=Map.empty,
        persist=false,
        failMode=FailModeTypePermissive
      )
    ).get

    val transformed = transform.MetadataFilterTransform.transform(
      MetadataFilterTransform(
        name="MetadataFilterTransform", 
        inputView=outputView,
        inputURI=new URI(targetFile),
        sql=s"SELECT * FROM metadata WHERE metadata.securityLevel <= 4",
        outputView=outputView,
        persist=false,
        sqlParams=Map.empty,
        params=Map.empty
      )
    ).get

    val writeStream = transformed
      .writeStream
      .queryName("transformed") 
      .format("memory")
      .start

    val df = spark.table("transformed")

    try {
      Thread.sleep(2000)
      assert(df.schema.map(_.name).toSet.equals(Array("booleanDatum", "dateDatum", "decimalDatum", "longDatum", "stringDatum").toSet))
    } finally {
      writeStream.stop
    }
  }      
}
