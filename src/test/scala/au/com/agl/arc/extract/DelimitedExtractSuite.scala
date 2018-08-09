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

class DelimitedExtractSuite extends FunSuite with BeforeAndAfter {

  // currently assuming local file system
  var session: SparkSession = _  
  val targetFile = FileUtils.getTempDirectoryPath() + "extract.csv" 
  val emptyDirectory = FileUtils.getTempDirectoryPath() + "empty.csv" 
  val emptyWildcardDirectory = FileUtils.getTempDirectoryPath() + "*.csv.gz" 
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
    // Delimited does not support writing NullType
    TestDataUtils.getKnownDataset.drop($"nullDatum").write.option("header", true).csv(targetFile)
  }

  after {
    session.stop()

    // clean up test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile))     
    FileUtils.deleteQuietly(new java.io.File(emptyDirectory))     
  }

  test("DelimitedExtract") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val extractDataset = extract.DelimitedExtract.extract(
      DelimitedExtract(
        name=outputView,
        cols=Nil,
        outputView=outputView,
        input=Right(new URI(targetFile)),
        settings=new Delimited(header=true, sep=Delimiter.Comma),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=None
      )
    )

    // test that the filename is correctly populated
    assert(extractDataset.filter($"_filename".contains(targetFile)).count != 0)

    val internal = extractDataset.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)
    val actual = extractDataset.drop(internal:_*)

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

  test("DelimitedExtract Caching") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
  
    // no cache
    extract.DelimitedExtract.extract(
      DelimitedExtract(
        name=outputView,
        cols=Nil,
        outputView=outputView,
        input=Right(new URI(targetFile)),
        settings=new Delimited(),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=None
      )
    )
    assert(spark.catalog.isCached(outputView) === false)

    // cache
    extract.DelimitedExtract.extract(
      DelimitedExtract(
        name=outputView,
        cols=Nil,
        outputView=outputView,
        input=Right(new URI(targetFile)),
        settings=new Delimited(header=true, sep=Delimiter.Comma),
        authentication=None,
        params=Map.empty,
        persist=true,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=None
      )
    )
    assert(spark.catalog.isCached(outputView) === true)     
  }  

  test("DelimitedExtract Empty Dataset") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val cols = 
      BooleanColumn(
        id="1",
        name="booleanDatum",
        description=None,
        nullable=true,
        nullReplacementValue=None,
        trim=false,
        nullableValues=Nil, 
        trueValues=Nil, 
        falseValues=Nil,
        metadata=None
      ) :: Nil    

    // try with wildcard
    val thrown0 = intercept[Exception with DetailException] {
      val extractDataset = extract.DelimitedExtract.extract(
        DelimitedExtract(
          name=outputView,
          cols=Nil,
          outputView=outputView,
          input=Right(new URI(emptyWildcardDirectory)),
          settings=new Delimited(),
          authentication=None,
          params=Map.empty,
          persist=false,
          numPartitions=None,
          partitionBy=Nil,
          contiguousIndex=None
        )
      )
    }
    assert(thrown0.getMessage === "DelimitedExtract has produced 0 columns and no schema has been provided to create an empty dataframe.")
    
    // try without providing column metadata
    val thrown1 = intercept[Exception with DetailException] {
      val extractDataset = extract.DelimitedExtract.extract(
        DelimitedExtract(
          name=outputView,
          cols=Nil,
          outputView=outputView,
          input=Right(new URI(emptyDirectory)),
          settings=new Delimited(),
          authentication=None,
          params=Map.empty,
          persist=false,
          numPartitions=None,
          partitionBy=Nil,
          contiguousIndex=None
        )
      )
    }
    assert(thrown1.getMessage === "DelimitedExtract has produced 0 columns and no schema has been provided to create an empty dataframe.")

    // try with column
    val extractDataset = extract.DelimitedExtract.extract(
      DelimitedExtract(
        name=outputView,
        cols=cols,
        outputView=outputView,
        input=Right(new URI(emptyDirectory)),
        settings=new Delimited(),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=None
      )
    )

    val internal = extractDataset.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)
    val actual = extractDataset.drop(internal:_*)

    val expected = TestDataUtils.getKnownDataset.select($"booleanDatum").limit(0)

    val actualExceptExpectedCount = actual.except(expected).count
    val expectedExceptActualCount = expected.except(actual).count
    if (actualExceptExpectedCount != 0 || expectedExceptActualCount != 0) {
      actual.show(false)
      expected.show(false)      
    }
    assert(actual.except(expected).count === 0)
    assert(expected.except(actual).count === 0)
  }

  test("DelimitedExtract Settings: Delimiter") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    // incorrect delimiter
    var dataset = extract.DelimitedExtract.extract(
      DelimitedExtract(
        name=outputView,
        cols=Nil,
        outputView=outputView,
        input=Right(new URI(targetFile)),
        settings=new Delimited(header=true, sep=Delimiter.Pipe, inferSchema=false),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=None
      )
    )

    val internal = dataset.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)
    val actual = dataset.drop(internal:_*)

    assert(actual.count == TestDataUtils.getKnownDataset.count)
    assert(actual.columns.length == 1)
  }  
  
  test("DelimitedExtract Settings: Header") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    // incorrect header
    var dataset = extract.DelimitedExtract.extract(
      DelimitedExtract(
        name=outputView,
        cols=Nil,
        outputView=outputView,
        input=Right(new URI(targetFile)),
        settings=new Delimited(header=false, sep=Delimiter.Comma, inferSchema=false),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=None
      )
    )
    val internal = dataset.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)
    val actual = dataset.drop(internal:_*)

    // spark.read.csv seems to have non-deterministic ordering
    assert(actual.orderBy($"_c0").first.getString(0) == "booleanDatum")  
  }   

  test("DelimitedExtract Settings: inferSchema") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    // incorrect header
    var dataset = extract.DelimitedExtract.extract(
      DelimitedExtract(
        name=outputView,
        cols=Nil,
        outputView=outputView,
        input=Right(new URI(targetFile)),
        settings=new Delimited(header=true, sep=Delimiter.Comma, inferSchema=true),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=None
      )
    )
    val internal = dataset.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)
    val actual = dataset.drop(internal:_*)

    // try to read boolean which will fail if not inferSchema
    actual.first.getBoolean(0)
  }    
}