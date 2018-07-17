package au.com.agl.arc

import java.net.URI
import java.io.PrintWriter

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

class JSONExtractSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  
  val targetFile = FileUtils.getTempDirectoryPath() + "extract.json" 
  val emptyDirectory = FileUtils.getTempDirectoryPath() + "empty.json" 
  val emptyWildcardDirectory = FileUtils.getTempDirectoryPath() + "*.json.gz" 
  val outputView = "dataset"

  val multiLineFile0 = FileUtils.getTempDirectoryPath() + "multiLine0.json" 
  val multiLineFile1 = FileUtils.getTempDirectoryPath() + "multiLine1.json" 
  val multiLineMatcher = FileUtils.getTempDirectoryPath() + "multiLine*.json"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    session = spark

    // recreate test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile)) 
    FileUtils.deleteQuietly(new java.io.File(multiLineFile0)) 
    FileUtils.deleteQuietly(new java.io.File(multiLineFile1)) 
    FileUtils.deleteQuietly(new java.io.File(emptyDirectory)) 
    FileUtils.forceMkdir(new java.io.File(emptyDirectory))
    // JSON will silently drop NullType on write
    TestDataUtils.getKnownDataset.write.json(targetFile)

    // write some multiline JSON files
    Some(new PrintWriter(multiLineFile0)).foreach{f => f.write(TestDataUtils.knownDatasetPrettyJSON(0)); f.close}
    Some(new PrintWriter(multiLineFile1)).foreach{f => f.write(TestDataUtils.knownDatasetPrettyJSON(1)); f.close}
  }

  after {
    session.stop()

    // clean up test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile))   
    FileUtils.deleteQuietly(new java.io.File(multiLineFile0)) 
    FileUtils.deleteQuietly(new java.io.File(multiLineFile1))       
    FileUtils.deleteQuietly(new java.io.File(emptyDirectory))     
  }

  test("JSONExtract") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val extractDataset = extract.JSONExtract.extract(
      JSONExtract(
        name=outputView,
        cols=Nil,
        outputView=outputView,
        input=Right(new URI(targetFile)),
        settings=new JSON(multiLine=false),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None
      )
    )

    // test that the filename is correctly populated
    assert(extractDataset.filter($"_filename".contains(targetFile)).count != 0)

    val internal = extractDataset.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)
    val actual = extractDataset.drop(internal:_*)

    // JSON does not have DecimalType or TimestampType
    // JSON will silently drop NullType on write
    val expected = TestDataUtils.getKnownDataset
      .withColumn("decimalDatum", $"decimalDatum".cast("double"))
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

  test("JSONExtract: Caching") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    // no cache
    extract.JSONExtract.extract(
      JSONExtract(
        name=outputView,
        cols=Nil,
        outputView=outputView,
        input=Right(new URI(targetFile)),
        settings=new JSON(multiLine=false),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None
      )
    )
    assert(spark.catalog.isCached(outputView) === false)

    // cache
    extract.JSONExtract.extract(
      JSONExtract(
        name=outputView,
        cols=Nil,
        outputView=outputView,
        input=Right(new URI(targetFile)),
        settings=new JSON(),
        authentication=None,
        params=Map.empty,
        persist=true,
        numPartitions=None
      )
    )
    assert(spark.catalog.isCached(outputView) === true)
  }  

  test("JSONExtract: Empty Dataset") {
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
      val extractDataset = extract.JSONExtract.extract(
        JSONExtract(
        name=outputView,
        cols=Nil,
        outputView=outputView,
        input=Right(new URI(emptyWildcardDirectory)),
        settings=new JSON(multiLine=false),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None
        )
      )
    }

    assert(thrown0.getMessage === "JSONExtract has produced 0 columns and no schema has been provided to create an empty dataframe.")

    // try without providing column metadata
    val thrown1 = intercept[Exception with DetailException] {
      val extractDataset = extract.JSONExtract.extract(
        JSONExtract(
        name=outputView,
        cols=Nil,
        outputView=outputView,
        input=Right(new URI(emptyDirectory)),
        settings=new JSON(multiLine=false),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None
        )
      )
    }

    assert(thrown1.getMessage === "JSONExtract has produced 0 columns and no schema has been provided to create an empty dataframe.")
    
    // try with column
    val extractDataset = extract.JSONExtract.extract(
      JSONExtract(
      name=outputView,
      cols=cols,
      outputView=outputView,
      input=Right(new URI(emptyDirectory)),
      settings=new JSON(multiLine=false),
      authentication=None,
      params=Map.empty,
      persist=false,
      numPartitions=None
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

  test("JSONExtract: multiLine") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    spark.sparkContext.textFile(targetFile).take(10)

    val actual0 = extract.JSONExtract.extract(
      JSONExtract(
        name=outputView,
        cols=Nil,
        outputView=outputView,
        input=Right(new URI(multiLineMatcher)),
        settings=new JSON(multiLine=false),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None
      )
    )

    val actual1 = extract.JSONExtract.extract(
      JSONExtract(
        name=outputView,
        cols=Nil,
        outputView=outputView,
        input=Right(new URI(multiLineMatcher)),
        settings=new JSON(multiLine=true),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None
      )
    )

    // check the filenames are both present
    assert(actual1.filter($"_filename".contains(multiLineFile0)).count == 1)
    assert(actual1.filter($"_filename".contains(multiLineFile1)).count == 1)

    // check all fields parsed
    assert(actual0.schema.map(_.name).contains("_corrupt_record"))
    assert(!actual1.schema.map(_.name).contains("_corrupt_record"))
    assert(actual0.count > actual1.count)
  }   

test("JSONExtract: Input Schema") {
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
      ) :: 
      IntegerColumn(
        id="2",
        name="integerDatum",
        description=None,
        primaryKey=Option(false),
        nullable=true,
        nullReplacementValue=None,
        trim=false,
        nullableValues=Nil
      ) :: Nil

    val extractDataset = extract.JSONExtract.extract(
      JSONExtract(
        name=outputView,
        cols=cols,
        outputView=outputView,
        input=Right(new URI(targetFile)),
        settings=new JSON(multiLine=false),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None
      )
    )

    val internal = extractDataset.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)
    val actual = extractDataset.drop(internal:_*)
    val expected = TestDataUtils.getKnownDataset.select($"booleanDatum", $"integerDatum")

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