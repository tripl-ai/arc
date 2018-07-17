package au.com.agl.arc

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.util.log.LoggerFactory 

import au.com.agl.arc.util._

class TypingTransformSuite extends FunSuite with BeforeAndAfter {

  // currently assuming local file system
  var session: SparkSession = _  
  val targetFile = FileUtils.getTempDirectoryPath() + "extract.csv" 
  val emptyDirectory = FileUtils.getTempDirectoryPath() + "empty.csv" 
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

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")    

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

  {

  }

  test("TypingTransform") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    // load csv
    // val extractDataset = spark.read.csv(targetFile).withColumn("_c7", substring($"_c7",0,23))
    val extractDataset = spark.read.csv(targetFile)
    extractDataset.createOrReplaceTempView("inputDS")

    // parse json schema to List[ExtractColumn]
    val cols = au.com.agl.arc.util.MetadataSchema.parseJsonMetadata(TestDataUtils.getKnownDatasetMetadataJson)

    val actual = transform.TypingTransform.transform(
      TypingTransform(
        name="dataset",
        cols=cols.right.getOrElse(null), 
        inputView="inputDS",
        outputView=outputView, 
        params=Map.empty,
        persist=false
      )
    )

    // add errors array to schema using udf
    val errorStructType: StructType =
      StructType(
        StructField("field", StringType, false) ::
        StructField("message", StringType, false) :: Nil
      )
    val addErrors = udf(() => new Array(0), ArrayType(errorStructType) )

    val expected = TestDataUtils.getKnownDataset
      .drop($"nullDatum")
      .withColumn("_errors", addErrors())

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