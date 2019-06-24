package ai.tripl.arc

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.util.log.LoggerFactory 

import ai.tripl.arc.util._

class DelimitedExtractSuite extends FunSuite with BeforeAndAfter {

  // currently assuming local file system
  var session: SparkSession = _  
  val targetFile = FileUtils.getTempDirectoryPath() + "extract.csv" 
  val customDelimiterTargetFile = FileUtils.getTempDirectoryPath() + "extract_custom.csv" 
  val targetFileGlob = FileUtils.getTempDirectoryPath() + "ex{t,a,b,c}ract.csv" 
  val emptyDirectory = FileUtils.getTempDirectoryPath() + "empty.csv" 
  val emptyWildcardDirectory = FileUtils.getTempDirectoryPath() + "*.csv.gz" 
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

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")      

    session = spark
    import spark.implicits._

    // recreate test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile)) 
    FileUtils.deleteQuietly(new java.io.File(customDelimiterTargetFile)) 
    FileUtils.deleteQuietly(new java.io.File(emptyDirectory)) 
    FileUtils.forceMkdir(new java.io.File(emptyDirectory))
    // Delimited does not support writing NullType
    TestUtils.getKnownDataset.drop($"nullDatum").write.option("header", true).csv(targetFile)
    TestUtils.getKnownDataset.drop($"nullDatum").write.option("header", true).option("sep", "%").csv(customDelimiterTargetFile)
  }

  after {
    session.stop()

    // clean up test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile))     
    FileUtils.deleteQuietly(new java.io.File(customDelimiterTargetFile))     
    FileUtils.deleteQuietly(new java.io.File(emptyDirectory))     
  }

  test("DelimitedExtract") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    // parse json schema to List[ExtractColumn]
    val cols = ai.tripl.arc.util.MetadataSchema.parseJsonMetadata(TestUtils.getKnownDatasetMetadataJson)    

    val dataset = extract.DelimitedExtractStage.extract(
      extract.DelimitedExtractStage(
        plugin=new extract.DelimitedExtract,
        name=outputView,
        description=None,
        cols=Right(cols.right.getOrElse(Nil)),
        outputView=outputView,
        input=Right(targetFileGlob),
        settings=new Delimited(header=true, sep=Delimiter.Comma),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=true,
        basePath=None,
        inputField=None
      )
    ).get

    // test that the filename is correctly populated
    assert(dataset.filter($"_filename".contains(targetFile)).count != 0)

    val internal = dataset.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)
    val actual = dataset.drop(internal:_*)

    // Delimited will load everything as StringType
    // Delimited does not support writing NullType
    val expected = TestUtils.getKnownDataset
      .withColumn("booleanDatum", $"booleanDatum".cast("string"))
      .withColumn("dateDatum", $"dateDatum".cast("string"))
      .withColumn("decimalDatum", $"decimalDatum".cast("string"))
      .withColumn("doubleDatum", $"doubleDatum".cast("string"))
      .withColumn("integerDatum", $"integerDatum".cast("string"))
      .withColumn("longDatum", $"longDatum".cast("string"))
      .withColumn("timestampDatum", from_unixtime(unix_timestamp($"timestampDatum"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
      .drop($"nullDatum")


    assert(TestUtils.datasetEquality(expected, actual))

    // test metadata
    val timestampDatumMetadata = actual.schema.fields(actual.schema.fieldIndex("timestampDatum")).metadata    
    assert(timestampDatumMetadata.getLong("securityLevel") == 7)    
  }  

  test("DelimitedExtract inputView") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)
 
    val df = TestUtils.getKnownDataset
    df.createOrReplaceTempView("dataset")
    var payloadDataset = spark.sql(s"""
      SELECT NULL AS nullDatum, "field0|field1" AS inputField
      UNION ALL
      SELECT nullDatum, CONCAT(stringDatum, "|", stringDatum) AS inputField
      FROM dataset
    """).repartition(1)

    payloadDataset.createOrReplaceTempView(inputView)

    val dataset = extract.DelimitedExtractStage.extract(
      extract.DelimitedExtractStage(
        plugin=new extract.DelimitedExtract,
        name=outputView,
        description=None,
        cols=Right(Nil),
        outputView=outputView,
        input=Left(inputView),
        settings=new Delimited(header=true, sep=Delimiter.Pipe),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=true,
        basePath=None,
        inputField=Option("inputField")
      )
    ).get

    assert(dataset.count === 2)
    assert(dataset.columns.length === 4)
  }    

  test("DelimitedExtract Caching") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)
  
    // no cache
    extract.DelimitedExtractStage.extract(
      extract.DelimitedExtractStage(
        plugin=new extract.DelimitedExtract,
        name=outputView,
        description=None,
        cols=Right(Nil),
        outputView=outputView,
        input=Right(targetFile),
        settings=new Delimited(),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=true,
        basePath=None,
        inputField=None
      )
    )
    assert(spark.catalog.isCached(outputView) === false)

    // cache
    extract.DelimitedExtractStage.extract(
      extract.DelimitedExtractStage(
        plugin=new extract.DelimitedExtract,
        name=outputView,
        description=None,
        cols=Right(Nil),
        outputView=outputView,
        input=Right(targetFile),
        settings=new Delimited(header=true, sep=Delimiter.Comma),
        authentication=None,
        params=Map.empty,
        persist=true,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=true,
        basePath=None,
        inputField=None
      )
    )
    assert(spark.catalog.isCached(outputView) === true)     
  }  

  test("DelimitedExtract Empty Dataset") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

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
      val dataset = extract.DelimitedExtractStage.extract(
        extract.DelimitedExtractStage(
          plugin=new extract.DelimitedExtract,
          name=outputView,
          description=None,
          cols=Right(Nil),
          outputView=outputView,
          input=Right(emptyWildcardDirectory),
          settings=new Delimited(),
          authentication=None,
          params=Map.empty,
          persist=false,
          numPartitions=None,
          partitionBy=Nil,
          contiguousIndex=true,
          basePath=None,
          inputField=None
        )
      )
    }
    assert(thrown0.getMessage === "DelimitedExtract has produced 0 columns and no schema has been provided to create an empty dataframe.")
    
    // try without providing column metadata
    val thrown1 = intercept[Exception with DetailException] {
      val dataset = extract.DelimitedExtractStage.extract(
        extract.DelimitedExtractStage(
          plugin=new extract.DelimitedExtract,
          name=outputView,
          description=None,
          cols=Right(Nil),
          outputView=outputView,
          input=Right(emptyDirectory),
          settings=new Delimited(),
          authentication=None,
          params=Map.empty,
          persist=false,
          numPartitions=None,
          partitionBy=Nil,
          contiguousIndex=true,
          basePath=None,
          inputField=None
        )
      )
    }
    assert(thrown1.getMessage === "DelimitedExtract has produced 0 columns and no schema has been provided to create an empty dataframe.")

    // try with column
    val dataset = extract.DelimitedExtractStage.extract(
      extract.DelimitedExtractStage(
        plugin=new extract.DelimitedExtract,
        name=outputView,
        description=None,
        cols=Right(cols),
        outputView=outputView,
        input=Right(emptyDirectory),
        settings=new Delimited(),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=true,
        basePath=None,
        inputField=None
      )
    ).get

    val internal = dataset.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)
    val actual = dataset.drop(internal:_*)

    val expected = TestUtils.getKnownDataset.select($"booleanDatum").limit(0)

    val actualExceptExpectedCount = actual.except(expected).count
    val expectedExceptActualCount = expected.except(actual).count
    if (actualExceptExpectedCount != 0 || expectedExceptActualCount != 0) {
      actual.show(false)
      expected.show(false)      
    }
    assert(actual.except(expected).count === 0)
    assert(expected.except(actual).count === 0)
  }

  test("DelimitedExtract: Settings Delimiter") {
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    // incorrect delimiter
    val dataset = extract.DelimitedExtractStage.extract(
      extract.DelimitedExtractStage(
        plugin=new extract.DelimitedExtract,
        name=outputView,
        description=None,
        cols=Right(Nil),
        outputView=outputView,
        input=Right(targetFile),
        settings=new Delimited(header=true, sep=Delimiter.Pipe, inferSchema=false),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=true,
        basePath=None,
        inputField=None
      )
    ).get

    val internal = dataset.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)
    val actual = dataset.drop(internal:_*)

    assert(actual.count == TestUtils.getKnownDataset.count)
    assert(actual.columns.length == 1)
  }  
  
  test("DelimitedExtract: Settings Custom Delimiter") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    // incorrect delimiter
    val dataset = extract.DelimitedExtractStage.extract(
      extract.DelimitedExtractStage(
        plugin=new extract.DelimitedExtract,
        name=outputView,
        description=None,
        cols=Right(Nil),
        outputView=outputView,
        input=Right(customDelimiterTargetFile),
        settings=new Delimited(header=true, sep=Delimiter.Custom, inferSchema=false, customDelimiter="%"),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=true,
        basePath=None,
        inputField=None
      )
    ).get

    val internal = dataset.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)
    val actual = dataset.drop(internal:_*)

    // Delimited will load everything as StringType
    // Delimited does not support writing NullType
    val expected = TestUtils.getKnownDataset
      .withColumn("booleanDatum", $"booleanDatum".cast("string"))
      .withColumn("dateDatum", $"dateDatum".cast("string"))
      .withColumn("decimalDatum", $"decimalDatum".cast("string"))
      .withColumn("doubleDatum", $"doubleDatum".cast("string"))
      .withColumn("integerDatum", $"integerDatum".cast("string"))
      .withColumn("longDatum", $"longDatum".cast("string"))
      .withColumn("timestampDatum", from_unixtime(unix_timestamp($"timestampDatum"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
      .drop($"nullDatum")


    assert(TestUtils.datasetEquality(expected, actual))
  }  

  test("DelimitedExtract: Settings Header") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    // incorrect header
    val dataset = extract.DelimitedExtractStage.extract(
      extract.DelimitedExtractStage(
        plugin=new extract.DelimitedExtract,
        name=outputView,
        description=None,
        cols=Right(Nil),
        outputView=outputView,
        input=Right(targetFile),
        settings=new Delimited(header=false, sep=Delimiter.Comma, inferSchema=false),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=true,
        basePath=None,
        inputField=None
      )
    ).get

    val internal = dataset.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)
    val actual = dataset.drop(internal:_*)

    // spark.read.csv seems to have non-deterministic ordering
    assert(actual.orderBy($"_c0").first.getString(0) == "booleanDatum")  
  }   

  test("DelimitedExtract: Settings inferSchema") {
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    // incorrect header
    val dataset = extract.DelimitedExtractStage.extract(
      extract.DelimitedExtractStage(
        plugin=new extract.DelimitedExtract,
        name=outputView,
        description=None,
        cols=Right(Nil),
        outputView=outputView,
        input=Right(targetFile),
        settings=new Delimited(header=true, sep=Delimiter.Comma, inferSchema=true),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=true,
        basePath=None,
        inputField=None
      )
    ).get

    val internal = dataset.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)
    val actual = dataset.drop(internal:_*)

    // try to read boolean which will fail if not inferSchema
    actual.first.getBoolean(0)
  }    

  test("DelimitedExtract: Structured Streaming") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=true)

    // parse json schema to List[ExtractColumn]
    val cols = ai.tripl.arc.util.MetadataSchema.parseJsonMetadata(TestUtils.getKnownDatasetMetadataJson)    

    val dataset = extract.DelimitedExtractStage.extract(
      extract.DelimitedExtractStage(
        plugin=new extract.DelimitedExtract,
        name=outputView,
        description=None,
        cols=Right(cols.right.getOrElse(Nil)),
        outputView=outputView,
        input=Right(targetFileGlob),
        settings=new Delimited(header=true, sep=Delimiter.Comma),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=true,
        basePath=None,
        inputField=None
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
      df.first.getBoolean(0)
    } finally {
      writeStream.stop
    }  
  }     
}