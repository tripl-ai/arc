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
    // spark.sparkContext.setLogLevel("ERROR")

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
        cols=cols.right.getOrElse(null), 
        inputView=inputView,
        outputView=outputView, 
        params=Map.empty,
        persist=false
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
    )

    val actual = transformed.drop($"_errors")
    val expected = TestDataUtils.getKnownDataset.select($"booleanDatum", $"longDatum", $"stringDatum")

    val actualExceptExpectedCount = actual.except(expected).count
    val expectedExceptActualCount = expected.except(actual).count
    if (actualExceptExpectedCount != 0 || expectedExceptActualCount != 0) {
      println("actual")
      actual.show(false)
      println("expected")
      expected.show(false)  
    }
    assert(actual.except(expected).count === 0)
    assert(expected.except(actual).count === 0)
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
        cols=cols.right.getOrElse(null), 
        inputView=inputView,
        outputView=outputView, 
        params=Map.empty,
        persist=false
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
    )

    val actual = transformed.drop($"_errors")
    val expected = TestDataUtils.getKnownDataset.select($"booleanDatum", $"dateDatum", $"decimalDatum", $"longDatum", $"stringDatum")

    val actualExceptExpectedCount = actual.except(expected).count
    val expectedExceptActualCount = expected.except(actual).count
    if (actualExceptExpectedCount != 0 || expectedExceptActualCount != 0) {
      println("actual")
      actual.show(false)
      println("expected")
      expected.show(false)  
    }
    assert(actual.except(expected).count === 0)
    assert(expected.except(actual).count === 0)
  }    
}
