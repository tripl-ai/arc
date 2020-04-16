package ai.tripl.arc

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.avro.Schema
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.FileFilterUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.util._

class AvroExtractSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  val targetFile = FileUtils.getTempDirectoryPath() + "extract.avro"
  val targetFileGlob = FileUtils.getTempDirectoryPath() + "ex{t,a,b,c}ract.avro"
  val emptyDirectory = FileUtils.getTempDirectoryPath() + "empty.avro"
  val emptyWildcardDirectory = s"${emptyDirectory}/*.avro.gz"
  val outputView = "dataset"

  val targetBinarySchemaFile = getClass.getResource("/avro/users.avro").toString
  val targetBinaryFile = getClass.getResource("/avro/users.avrobinary").toString
  val schemaFile = getClass.getResource("/avro/user.avsc").toString

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
    import spark.implicits._

    // recreate test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile))
    FileUtils.deleteQuietly(new java.io.File(emptyDirectory))
    FileUtils.forceMkdir(new java.io.File(emptyDirectory))
    // avro does not support writing NullType
    TestUtils.getKnownDataset.drop($"nullDatum").write.format("com.databricks.spark.avro").save(targetFile)
  }

  after {
    session.stop()

    // clean up test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile))
    FileUtils.deleteQuietly(new java.io.File(emptyDirectory))
  }

  test("AvroExtract") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    // parse json schema to List[ExtractColumn]
    val schema = ai.tripl.arc.util.ArcSchema.parseArcSchema(TestUtils.getKnownDatasetMetadataJson)

    val dataset = extract.AvroExtractStage.execute(
      extract.AvroExtractStage(
        plugin=new extract.AvroExtract,
        name=outputView,
        description=None,
        schema=Right(schema.right.getOrElse(Nil)),
        outputView=outputView,
        input=Right(targetFileGlob),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        basePath=None,
        contiguousIndex=true,
        avroSchema=None,
        inputField=None
      )
    ).get

    // test that the filename is correctly populated
    assert(dataset.filter($"_filename".contains(targetFile)).count != 0)

    val internal = dataset.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)

    val expected = TestUtils.getKnownDataset
      .drop($"nullDatum")
    val actual = dataset.drop(internal:_*)

    assert(TestUtils.datasetEquality(expected, actual))

    // test metadata
    val timestampDatumMetadata = actual.schema.fields(actual.schema.fieldIndex("timestampDatum")).metadata
    assert(timestampDatumMetadata.getLong("securityLevel") == 7)
  }

  test("AvroExtract: Caching") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    // no cache
    extract.AvroExtractStage.execute(
      extract.AvroExtractStage(
        plugin=new extract.AvroExtract,
        name=outputView,
        description=None,
        schema=Right(Nil),
        outputView=outputView,
        input=Right(targetFile),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        basePath=None,
        contiguousIndex=true,
        avroSchema=None,
        inputField=None
      )
    )
    assert(spark.catalog.isCached(outputView) === false)

    // cache
    extract.AvroExtractStage.execute(
      extract.AvroExtractStage(
        plugin=new extract.AvroExtract,
        name=outputView,
        description=None,
        schema=Right(Nil),
        outputView=outputView,
        input=Right(targetFile),
        authentication=None,
        params=Map.empty,
        persist=true,
        numPartitions=None,
        partitionBy=Nil,
        basePath=None,
        contiguousIndex=true,
        avroSchema=None,
        inputField=None
      )
    )
    assert(spark.catalog.isCached(outputView) === true)
  }

  test("AvroExtract: Empty Dataset") {
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
      ) :: Nil

    // try with wildcard
    // wildcard throws path not found
    val thrown0 = intercept[Exception with DetailException] {
      extract.AvroExtractStage.execute(
        extract.AvroExtractStage(
          plugin=new extract.AvroExtract,
          name=outputView,
          description=None,
          schema=Right(Nil),
          outputView=outputView,
          input=Right(emptyWildcardDirectory),
          authentication=None,
          params=Map.empty,
          persist=false,
          numPartitions=None,
          partitionBy=Nil,
          basePath=None,
          contiguousIndex=true,
          avroSchema=None,
          inputField=None
        )
      )
    }
    assert(thrown0.getMessage.contains("Path '"))
    assert(thrown0.getMessage.contains("*.avro.gz' does not exist and no schema has been provided to create an empty dataframe."))

    // try without providing column metadata
    val thrown1 = intercept[Exception with DetailException] {
      extract.AvroExtractStage.execute(
        extract.AvroExtractStage(
          plugin=new extract.AvroExtract,
          name=outputView,
          description=None,
          schema=Right(Nil),
          outputView=outputView,
          input=Right(emptyDirectory),
          authentication=None,
          params=Map.empty,
          persist=false,
          numPartitions=None,
          partitionBy=Nil,
          basePath=None,
          contiguousIndex=true,
          avroSchema=None,
          inputField=None
        )
      )
    }
    assert(thrown1.getMessage.contains("No files matched '"))
    assert(thrown1.getMessage.contains("empty.avro' and no schema has been provided to create an empty dataframe."))

    // try with column
    val dataset = extract.AvroExtractStage.execute(
      extract.AvroExtractStage(
        plugin=new extract.AvroExtract,
        name=outputView,
        description=None,
        schema=Right(schema),
        outputView=outputView,
        input=Right(emptyDirectory),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        basePath=None,
        contiguousIndex=true,
        avroSchema=None,
        inputField=None
      )
    ).get

    val internal = dataset.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)
    val actual = dataset.drop(internal:_*)

    val expected = TestUtils.getKnownDataset.select($"booleanDatum").limit(0)

    assert(TestUtils.datasetEquality(expected, actual))
  }

  test("AvroExtract: Binary with user.avsc") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val conf = s"""{
      "stages": [
        {
          "type": "BytesExtract",
          "name": "get the binary avro file without header",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "${targetBinaryFile}",
          "outputView": "bytes_extract_output"
        },
        {
          "type": "AvroExtract",
          "name": "try to parse",
          "description": "load customer avro extract",
          "environments": [
            "production",
            "test"
          ],
          "inputView": "bytes_extract_output",
          "outputView": "avro_extract_output",
          "persist": false,
          "inputField": "value",
          "avroSchemaURI": "${schemaFile}"
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        ARC.run(pipeline)
      }
    }
  }

  test("AvroExtract: Schema Included Avro") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val dataset = extract.AvroExtractStage.execute(
      extract.AvroExtractStage(
        plugin=new extract.AvroExtract,
        name=outputView,
        description=None,
        schema=Right(Nil),
        outputView=outputView,
        input=Right(targetBinarySchemaFile),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        basePath=None,
        contiguousIndex=true,
        avroSchema=None,
        inputField=None
      )
    ).get

    assert(dataset.first.getString(0) == "Alyssa")
  }

  test("AvroExtract: Binary only Avro") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val schema = new Schema.Parser().parse(CloudUtils.getTextBlob(new URI(schemaFile)))

    extract.BytesExtractStage.execute(
      extract.BytesExtractStage(
        plugin=new extract.BytesExtract,
        name="dataset",
        description=None,
        outputView=outputView,
        input=Right(targetBinaryFile),
        authentication=None,
        persist=false,
        numPartitions=None,
        contiguousIndex=true,
        params=Map.empty,
        failMode=FailMode.FailFast
      )
    )

    val dataset = extract.AvroExtractStage.execute(
      extract.AvroExtractStage(
        plugin=new extract.AvroExtract,
        name=outputView,
        description=None,
        schema=Right(Nil),
        outputView=outputView,
        input=Left(outputView),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        basePath=None,
        contiguousIndex=true,
        avroSchema=Option(schema),
        inputField=None
      )
    ).get

    assert(dataset.select("value.*").first.getString(0) == "Alyssa")
  }
}
