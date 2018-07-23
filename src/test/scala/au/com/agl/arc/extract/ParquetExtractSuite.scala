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

class ParquetExtractSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  
  val targetFile = FileUtils.getTempDirectoryPath() + "extract.parquet" 
  val emptyDirectory = FileUtils.getTempDirectoryPath() + "empty.parquet" 
  val emptyWildcardDirectory = FileUtils.getTempDirectoryPath() + "*.parquet.gz" 
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

    // recreate test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile)) 
    FileUtils.deleteQuietly(new java.io.File(emptyDirectory)) 
    FileUtils.forceMkdir(new java.io.File(emptyDirectory))
    // parquet does not support writing NullType
    TestDataUtils.getKnownDataset.drop($"nullDatum").write.parquet(targetFile)
  }

  after {
    session.stop()

    // clean up test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile))     
    FileUtils.deleteQuietly(new java.io.File(emptyDirectory))     
  }

  test("ParquetExtract") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val extractDataset = extract.ParquetExtract.extract(
      ParquetExtract(
        name=outputView,
        cols=Nil,
        outputView=outputView,
        input=new URI(targetFile),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil
      )
    )

    // test that the filename is correctly populated
    assert(extractDataset.filter($"_filename".contains(targetFile)).count != 0)

    val internal = extractDataset.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)
    val actual = extractDataset.drop(internal:_*)

    val expected = TestDataUtils.getKnownDataset.drop($"nullDatum")

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

  test("ParquetExtract Caching") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    // no cache
    extract.ParquetExtract.extract(
      ParquetExtract(
        name=outputView,
        cols=Nil,
        outputView=outputView,
        input=new URI(targetFile),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil
      )
    )
    assert(spark.catalog.isCached(outputView) === false)

    // cache
    extract.ParquetExtract.extract(
      ParquetExtract(
        name=outputView,
        cols=Nil,
        outputView=outputView,
        input=new URI(targetFile),
        authentication=None,
        params=Map.empty,
        persist=true,
        numPartitions=None,
        partitionBy=Nil
      )
    )
    assert(spark.catalog.isCached(outputView) === true)     
  }  

  test("ParquetExtract Empty Dataset") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val cols = 
      BooleanColumn(
        id="1",
        name="booleanDatum",
        description=None,
        primaryKey=Option(false),
        nullable=true,
        nullReplacementValue=None,
        trim=false,
        nullableValues=Nil, 
        trueValues=Nil, 
        falseValues=Nil
      ) :: Nil    

    // try with wildcard
    val thrown0 = intercept[Exception with DetailException] {
      val extractDataset = extract.ParquetExtract.extract(
        ParquetExtract(
          name=outputView,
          cols=Nil,
          outputView=outputView,
          input=new URI(emptyWildcardDirectory),
          authentication=None,
          params=Map.empty,
          persist=false,
          numPartitions=None,
          partitionBy=Nil
        )
      )
    }
    assert(thrown0.getMessage === "ParquetExtract has produced 0 columns and no schema has been provided to create an empty dataframe.")
    
    // try without providing column metadata
    val thrown1 = intercept[Exception with DetailException] {
      val extractDataset = extract.ParquetExtract.extract(
        ParquetExtract(
          name=outputView,
          cols=Nil,
          outputView=outputView,
          input=new URI(emptyDirectory),
          authentication=None,
          params=Map.empty,
          persist=false,
          numPartitions=None,
          partitionBy=Nil
        )
      )
    }
    assert(thrown1.getMessage === "ParquetExtract has produced 0 columns and no schema has been provided to create an empty dataframe.")
    
    // try with column
    val extractDataset = extract.ParquetExtract.extract(
      ParquetExtract(
        name=outputView,
        cols=cols,
        outputView=outputView,
        input=new URI(emptyDirectory),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil
      )
    )

    val internal = extractDataset.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)
    val actual = extractDataset.drop(internal:_*)

    val expected = TestDataUtils.getKnownDataset.select($"booleanDatum").limit(0)

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
