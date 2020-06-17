package ai.tripl.arc

import java.net.URI
import java.io.PrintWriter

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._

import ai.tripl.arc.util._

class JSONExtractSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  val targetFile = FileUtils.getTempDirectoryPath() + "extract.json"
  val targetFileGlob = FileUtils.getTempDirectoryPath() + "ex{t,a,b,c}ract.json"
  val emptyDirectory = FileUtils.getTempDirectoryPath() + "empty.json"
  val emptyWildcardDirectory = FileUtils.getTempDirectoryPath() + "*.json.gz"
  val inputView = "inputView"
  val outputView = "outputView"

  val multiLineBase = FileUtils.getTempDirectoryPath() + "multiline/"
  val multiLineFile0 = multiLineBase + "multiLine0.json"
  val multiLineFile1 = multiLineBase + "multiLine1.json"
  val multiLineMatcher = multiLineBase + "multiLine*.json"

  val singleArrayBase = FileUtils.getTempDirectoryPath() + "singlearray/"
  val multiArrayBase = FileUtils.getTempDirectoryPath() + "multiarray/"
  val arrayFile0 = singleArrayBase + "array0.json"
  val arrayFile1 = multiArrayBase + "array1.json"
  val arrayFile2 = multiArrayBase + "array2.json"
  val arrayFileMatcher = multiArrayBase + "array*.json"

  val arrayFileContents = """
  [
    {
      "customerId": 1,
      "active": true
    },
    {
      "customerId": 2,
      "active": true
    },
    {
      "customerId": 3,
      "active": true
    }
  ]
  """

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark

    // recreate test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile))
    FileUtils.deleteQuietly(new java.io.File(multiLineFile0))
    FileUtils.deleteQuietly(new java.io.File(multiLineFile1))
    FileUtils.deleteQuietly(new java.io.File(emptyDirectory))
    FileUtils.forceMkdir(new java.io.File(emptyDirectory))
    // JSON will silently drop NullType on write
    TestUtils.getKnownDataset.write.json(targetFile)

    // write some multiline JSON files
    FileUtils.forceMkdir(new java.io.File(multiLineBase))
    Some(new PrintWriter(multiLineFile0)).foreach{f => f.write(TestUtils.knownDatasetPrettyJSON(0)); f.close}
    Some(new PrintWriter(multiLineFile1)).foreach{f => f.write(TestUtils.knownDatasetPrettyJSON(1)); f.close}
    FileUtils.forceMkdir(new java.io.File(singleArrayBase))
    Some(new PrintWriter(arrayFile0)).foreach{f => f.write("""[{"customerId":1,"active":true},{"customerId":2,"active":false},{"customerId":3,"active":true}]"""); f.close}
    FileUtils.forceMkdir(new java.io.File(multiArrayBase))
    Some(new PrintWriter(arrayFile1)).foreach{f => f.write(arrayFileContents); f.close}
    Some(new PrintWriter(arrayFile2)).foreach{f => f.write(arrayFileContents); f.close}
  }

  after {
    session.stop()

    // clean up test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile))
    FileUtils.deleteQuietly(new java.io.File(multiLineFile0))
    FileUtils.deleteQuietly(new java.io.File(multiLineFile1))
    FileUtils.deleteQuietly(new java.io.File(emptyDirectory))
    FileUtils.deleteQuietly(new java.io.File(multiLineBase))
    FileUtils.deleteQuietly(new java.io.File(arrayFile2))
    FileUtils.deleteQuietly(new java.io.File(arrayFile1))
    FileUtils.deleteQuietly(new java.io.File(multiArrayBase))
    FileUtils.deleteQuietly(new java.io.File(arrayFile0))
    FileUtils.deleteQuietly(new java.io.File(singleArrayBase))
  }

  test("JSONExtract") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    // parse json schema to List[ExtractColumn]
    val schema = ai.tripl.arc.util.ArcSchema.parseArcSchema(TestUtils.getKnownDatasetMetadataJson)

    val dataset = extract.JSONExtractStage.execute(
      extract.JSONExtractStage(
        plugin=new extract.JSONExtract,
        name=outputView,
        description=None,
        schema=Right(schema.right.getOrElse(Nil)),
        outputView=outputView,
        input=Right(targetFileGlob),
        settings=new JSON(multiLine=false),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=true,
        basePath=None,
        inputField=None,
        watermark=None
      )
    ).get

    // test that the filename is correctly populated
    assert(dataset.filter($"_filename".contains(targetFile)).count != 0)

    val internal = dataset.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)
    val actual = dataset
      .drop(internal:_*)
      .withColumn("decimalDatum", $"decimalDatum".cast("double"))
      .withColumn("timestampDatum", from_unixtime(unix_timestamp($"timestampDatum"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))

    // JSON does not have DecimalType or TimestampType
    // JSON will silently drop NullType on write
    val expected = TestUtils.getKnownDataset
      .withColumn("decimalDatum", $"decimalDatum".cast("double"))
      .withColumn("timestampDatum", from_unixtime(unix_timestamp($"timestampDatum"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
      .drop($"nullDatum")

    assert(TestUtils.datasetEquality(expected, actual))

    // test metadata
    val timeDatumMetadata = actual.schema.fields(actual.schema.fieldIndex("timeDatum")).metadata
    assert(timeDatumMetadata.getLong("securityLevel") == 8)
  }

  test("JSONExtract: inputView") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val df = TestUtils.getKnownDataset
    df.createOrReplaceTempView(inputView)
    var payloadDataset = spark.sql(s"""
      SELECT stringDatum, TO_JSON(NAMED_STRUCT('dateDatum', dateDatum)) AS inputField FROM ${inputView}
    """).repartition(1)
    payloadDataset.createOrReplaceTempView(inputView)

    val dataset = extract.JSONExtractStage.execute(
      extract.JSONExtractStage(
        plugin=new extract.JSONExtract,
        name=outputView,
        description=None,
        schema=Right(Nil),
        outputView=outputView,
        input=Left(inputView),
        settings=new JSON(multiLine=false),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=true,
        basePath=None,
        inputField=Option("inputField"),
        watermark=None
      )
    ).get

    assert(dataset.count === 2)
    assert(dataset.columns.length === 3)
  }

  test("JSONExtract: Caching") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    // no cache
    extract.JSONExtractStage.execute(
      extract.JSONExtractStage(
        plugin=new extract.JSONExtract,
        name=outputView,
        description=None,
        schema=Right(Nil),
        outputView=outputView,
        input=Right(targetFile),
        settings=new JSON(multiLine=false),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=true,
        basePath=None,
        inputField=None,
        watermark=None
      )
    )
    assert(spark.catalog.isCached(outputView) === false)

    // cache
    extract.JSONExtractStage.execute(
      extract.JSONExtractStage(
        plugin=new extract.JSONExtract,
        name=outputView,
        description=None,
        schema=Right(Nil),
        outputView=outputView,
        input=Right(targetFile),
        settings=new JSON(),
        authentication=None,
        params=Map.empty,
        persist=true,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=true,
        basePath=None,
        inputField=None,
        watermark=None
      )
    )
    assert(spark.catalog.isCached(outputView) === true)
  }

  test("JSONExtract: Empty Dataset") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val schema =
      BooleanColumn(
        None,
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
      extract.JSONExtractStage.execute(
        extract.JSONExtractStage(
          plugin=new extract.JSONExtract,
          name=outputView,
          description=None,
          schema=Right(Nil),
          outputView=outputView,
          input=Right(emptyWildcardDirectory),
          settings=new JSON(multiLine=false),
          authentication=None,
          params=Map.empty,
          persist=false,
          numPartitions=None,
          partitionBy=Nil,
          contiguousIndex=true,
          basePath=None,
          inputField=None,
          watermark=None
        )
      )
    }
    assert(thrown0.getMessage.contains("No files matched '"))
    assert(thrown0.getMessage.contains("*.json.gz' and no schema has been provided to create an empty dataframe."))

    // try with wildcard and basePath
    val thrown1 = intercept[Exception with DetailException] {
      extract.JSONExtractStage.execute(
        extract.JSONExtractStage(
          plugin=new extract.JSONExtract,
          name=outputView,
          description=None,
          schema=Right(Nil),
          outputView=outputView,
          input=Right(emptyDirectory),
          settings=new JSON(multiLine=false),
          authentication=None,
          params=Map.empty,
          persist=false,
          numPartitions=None,
          partitionBy=Nil,
          contiguousIndex=true,
          basePath=Some(FileUtils.getTempDirectoryPath()),
          inputField=None,
          watermark=None
        )
      )
    }
    assert(thrown1.getMessage.contains("No files matched '"))
    assert(thrown1.getMessage.contains("json' and no schema has been provided to create an empty dataframe."))


    // try without providing column metadata
    val thrown2 = intercept[Exception with DetailException] {
      extract.JSONExtractStage.execute(
        extract.JSONExtractStage(
          plugin=new extract.JSONExtract,
          name=outputView,
          description=None,
          schema=Right(Nil),
          outputView=outputView,
          input=Right(emptyDirectory),
          settings=new JSON(multiLine=false),
          authentication=None,
          params=Map.empty,
          persist=false,
          numPartitions=None,
          partitionBy=Nil,
          contiguousIndex=true,
          basePath=None,
          inputField=None,
          watermark=None
        )
      )
    }
    assert(thrown2.getMessage.contains("Input '"))
    assert(thrown2.getMessage.contains("empty.json' does not contain any fields and no schema has been provided to create an empty dataframe."))

    // try with column
    val dataset = extract.JSONExtractStage.execute(
      extract.JSONExtractStage(
        plugin=new extract.JSONExtract,
        name=outputView,
        description=None,
        schema=Right(schema),
        outputView=outputView,
        input=Right(emptyDirectory),
        settings=new JSON(multiLine=false),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=true,
        basePath=None,
        inputField=None,
        watermark=None
      )
    ).get

    val internal = dataset.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)
    val actual = dataset.drop(internal:_*)

    val expected = TestUtils.getKnownDataset.select($"booleanDatum").limit(0)

    assert(TestUtils.datasetEquality(expected, actual))
  }

  test("JSONExtract: multiLine") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val actual0 = extract.JSONExtractStage.execute(
      extract.JSONExtractStage(
        plugin=new extract.JSONExtract,
        name=outputView,
        description=None,
        schema=Right(Nil),
        outputView=outputView,
        input=Right(multiLineMatcher),
        settings=new JSON(multiLine=false),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=true,
        basePath=None,
        inputField=None,
        watermark=None
      )
    ).get

    val actual1 = extract.JSONExtractStage.execute(
      extract.JSONExtractStage(
        plugin=new extract.JSONExtract,
        name=outputView,
        description=None,
        schema=Right(Nil),
        outputView=outputView,
        input=Right(multiLineMatcher),
        settings=new JSON(multiLine=true),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=true,
        basePath=None,
        inputField=None,
        watermark=None
      )
    ).get

    // check the filenames are both present
    assert(actual1.filter($"_filename".contains(multiLineFile0)).count == 1)
    assert(actual1.filter($"_filename".contains(multiLineFile1)).count == 1)

    // check all fields parsed
    assert(actual0.schema.map(_.name).contains("_corrupt_record"))
    assert(!actual1.schema.map(_.name).contains("_corrupt_record"))
    assert(actual0.count > actual1.count)
  }

  test("JSONExtract: singleLine Array") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val actual0 = extract.JSONExtractStage.execute(
      extract.JSONExtractStage(
        plugin=new extract.JSONExtract,
        name=outputView,
        description=None,
        schema=Right(Nil),
        outputView=outputView,
        input=Right(arrayFile0),
        settings=new JSON(multiLine=false),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=true,
        basePath=None,
        inputField=None,
        watermark=None
      )
    ).get

    // check the filenames are both present
    assert(actual0.filter($"_filename".contains(arrayFile0)).count == 3)

    // check all fields parsed
    assert(!actual0.schema.map(_.name).contains("_corrupt_record"))
  }

  test("JSONExtract: multiLine Array") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val actual0 = extract.JSONExtractStage.execute(
      extract.JSONExtractStage(
        plugin=new extract.JSONExtract,
        name=outputView,
        description=None,
        schema=Right(Nil),
        outputView=outputView,
        input=Right(arrayFileMatcher),
        settings=new JSON(multiLine=false),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=true,
        basePath=None,
        inputField=None,
        watermark=None
      )
    ).get

    val actual1 = extract.JSONExtractStage.execute(
      extract.JSONExtractStage(
        plugin=new extract.JSONExtract,
        name=outputView,
        description=None,
        schema=Right(Nil),
        outputView=outputView,
        input=Right(arrayFileMatcher),
        settings=new JSON(multiLine=true),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=true,
        basePath=None,
        inputField=None,
        watermark=None
      )
    ).get

    // check all fields parsed
    assert(actual0.schema.map(_.name).contains("_corrupt_record"))
    assert(!actual1.schema.map(_.name).contains("_corrupt_record"))

    // check the filenames are both present
    assert(actual1.filter($"_filename".contains(arrayFile1)).count == 3)
    assert(actual1.filter($"_filename".contains(arrayFile2)).count == 3)
  }

  test("JSONExtract: Input Schema") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val schema =
      BooleanColumn(
        id=None,
        name="booleanDatum",
        description=None,
        nullable=true,
        nullReplacementValue=None,
        trim=false,
        nullableValues=Nil,
        trueValues=Nil,
        falseValues=Nil,
        metadata=None
      ) ::
      IntegerColumn(
        id=None,
        name="integerDatum",
        description=None,
        nullable=true,
        nullReplacementValue=None,
        trim=false,
        nullableValues=Nil,
        metadata=None,
        formatters=None
      ) :: Nil

    val dataset = extract.JSONExtractStage.execute(
      extract.JSONExtractStage(
        plugin=new extract.JSONExtract,
        name=outputView,
        description=None,
        schema=Right(schema),
        outputView=outputView,
        input=Right(targetFile),
        settings=new JSON(multiLine=false),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=true,
        basePath=None,
        inputField=None,
        watermark=None
      )
    ).get

    val internal = dataset.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)
    val actual = dataset.drop(internal:_*)
    val expected = TestUtils.getKnownDataset.select($"booleanDatum", $"integerDatum")

    assert(TestUtils.datasetEquality(expected, actual))
  }

  test("JSONExtract: Structured Streaming") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=true)

    // parse json schema to List[ExtractColumn]
    val schema = ai.tripl.arc.util.ArcSchema.parseArcSchema(TestUtils.getKnownDatasetMetadataJson)

    val dataset = extract.JSONExtractStage.execute(
      extract.JSONExtractStage(
        plugin=new extract.JSONExtract,
        name=outputView,
        description=None,
        schema=Right(schema.right.getOrElse(Nil)),
        outputView=outputView,
        input=Right(multiLineBase),
        settings=new JSON(multiLine=true),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=true,
        basePath=None,
        inputField=None,
        watermark=None
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
      // will fail if parsing does not work
      df.first.getBoolean(0)
    } finally {
      writeStream.stop
    }
  }
}