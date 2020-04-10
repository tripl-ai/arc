package ai.tripl.arc

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils

import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.util._
import ai.tripl.arc.util.log.LoggerFactory

class TypingTransformSuite extends FunSuite with BeforeAndAfter {

  // currently assuming local file system
  var session: SparkSession = _
  val targetFile = FileUtils.getTempDirectoryPath() + "extract.csv"
  val emptyDirectory = FileUtils.getTempDirectoryPath() + "empty.csv"
  val inputView = "intputView"
  val outputView = "outputView"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .config("spark.sql.legacy.allowUntypedScalaUDF", true)
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("FATAL")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark
    import spark.implicits._

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    // recreate test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile))
    // Delimited does not support writing NullType
    TestUtils.getKnownDataset.repartition(1).drop(col("nullDatum")).write.csv(targetFile)
  }

  after {
    // clean up test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile))
    session.stop()
  }

  test("TypingTransform") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    // load csv
    extract.DelimitedExtractStage.execute(
      extract.DelimitedExtractStage(
        plugin=new extract.DelimitedExtract,
        name=inputView,
        description=None,
        schema=Right(Nil),
        outputView=inputView,
        input=Right(targetFile),
        settings=new Delimited(header=false, sep=Delimiter.Comma),
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

    // parse json schema to List[ExtractColumn]
    val schema = ai.tripl.arc.util.ArcSchema.parseArcSchema(TestUtils.getKnownDatasetMetadataJson)

    val actual = transform.TypingTransformStage.execute(
      transform.TypingTransformStage(
        plugin=new transform.TypingTransform,
        name="dataset",
        description=None,
        schema=Right(schema.right.getOrElse(Nil)),
        inputView=inputView,
        outputView=outputView,
        params=Map.empty,
        persist=false,
        failMode=FailMode.Permissive,
        numPartitions=None,
        partitionBy=Nil
      )
    ).get

    val expected = TestUtils.getKnownDataset
      .drop($"nullDatum")

    assert(TestUtils.datasetEquality(expected, actual.drop("_filename").drop("_index").drop("_errors")))
    assert(actual.filter(col("_filename").contains(targetFile)).count == 2)
    assert(actual.filter(col("_index").isNotNull).count == 2)
    assert(actual.filter("SIZE(_errors) = 0").count == 2)

    // test metadata
    val booleanDatumMetadata = actual.schema.fields(actual.schema.fieldIndex("booleanDatum")).metadata

    assert(booleanDatumMetadata.getBoolean("booleanMeta") == true)
    assert(booleanDatumMetadata.getBooleanArray("booleanArrayMeta").deep == Array(true, false).deep)

    assert(booleanDatumMetadata.getLong("longMeta") == 10)
    assert(booleanDatumMetadata.getLongArray("longArrayMeta").deep == Array(10, 20).deep)

    assert(booleanDatumMetadata.getDouble("doubleMeta") == 0.141)
    assert(booleanDatumMetadata.getDoubleArray("doubleArrayMeta").deep == Array(0.141, 0.52).deep)

    assert(booleanDatumMetadata.getString("stringMeta") == "string")
    assert(booleanDatumMetadata.getStringArray("stringArrayMeta").deep == Array("string0", "string1").deep)
  }

  test("TypingTransform: end-to-end") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    // load csv
    extract.DelimitedExtractStage.execute(
      extract.DelimitedExtractStage(
        plugin=new extract.DelimitedExtract,
        name=inputView,
        description=None,
        schema=Right(Nil),
        outputView=inputView,
        input=Right(targetFile),
        settings=new Delimited(header=false, sep=Delimiter.Comma),
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

    val conf = s"""{
      "stages": [
        {
          "type": "TypingTransform",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "schemaURI": "${getClass.getResource("/conf/schema/").toString}/knownDataset.json",
          "inputView": "${inputView}",
          "outputView": "${outputView}"
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        val df = ARC.run(pipeline)(spark, logger, arcContext).get
        df.createOrReplaceTempView("output")
        assert(spark.sql("""
        SELECT
            SUM(errors) = 0
        FROM (
            SELECT
                CASE WHEN SIZE(_errors) > 0 THEN 1 ELSE 0 END AS errors
            FROM output
        ) valid
        """).first.getBoolean(0) == true)
      }
    }
  }  

  test("TypingTransform: end-to-end complex type failure") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    // load csv
    extract.DelimitedExtractStage.execute(
      extract.DelimitedExtractStage(
        plugin=new extract.DelimitedExtract,
        name=inputView,
        description=None,
        schema=Right(Nil),
        outputView=inputView,
        input=Right(targetFile),
        settings=new Delimited(header=false, sep=Delimiter.Comma),
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

    val conf = s"""{
      "stages": [
        {
          "type": "TypingTransform",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "schemaURI": "${getClass.getResource("/conf/schema/").toString}/schema_complex.json",
          "inputView": "${inputView}",
          "outputView": "${outputView}"
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => assert(err.toString.contains("TypingTransform does not support complex types like column 'group' of type struct."))
      case Right((pipeline, _)) => fail("should fail")
    }
  }   

  test("TypingTransform: failMode - failfast") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    // get a corrupt dataset dataset
    // filter for a single row to have deterministic outcome
    // booleanDatum deliberately broken
    // timestamp will also fail due to format
    val extractDataset = TestUtils.getKnownStringDataset.filter("booleanDatum = true").drop("nullDatum").withColumn("booleanDatum", lit("bad"))
    extractDataset.createOrReplaceTempView(inputView)

    // parse json schema to List[ExtractColumn]
    val schema = ai.tripl.arc.util.ArcSchema.parseArcSchema(TestUtils.getKnownDatasetMetadataJson)


    // try without providing column metadata
    val thrown0 = intercept[Exception with DetailException] {
      val dataset = transform.TypingTransformStage.execute(
        transform.TypingTransformStage(
          plugin=new transform.TypingTransform,
          name="dataset",
          description=None,
          schema=Right(schema.right.getOrElse(Nil)),
          inputView=inputView,
          outputView=outputView,
          params=Map.empty,
          persist=false,
          failMode=FailMode.FailFast,
          numPartitions=None,
          partitionBy=Nil
        )
      ).get
      dataset.count
    }

    assert(thrown0.getMessage.contains("TypingTransform with failMode equal to 'failfast' cannot continue due to row with error(s): [[booleanDatum,Unable to convert 'bad' to boolean using provided true values: ['true'] or false values: ['false']], [timestampDatum,Unable to convert '2017-12-20 21:46:54' to timestamp using formatters ['uuuu-MM-dd'T'HH:mm:ss.SSSXXX'] and timezone 'UTC']]."))
  }

  test("TypingTransform: failfast due to invalid conversion") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val inputDataFrame = Seq(
      ("textDatum")
    ).toDF("textDatum") 
    inputDataFrame.createOrReplaceTempView(inputView)

    val meta = """
    [
      {
        "id": "982cbf60-7ba7-4e50-a09b-d8624a5c49e6",
        "name": "integerDatum",
        "description": "integerDatum",
        "type": "integer",
        "trim": false,
        "nullable": false,
        "nullableValues": [
            "",
            "null"
        ]
      }
    ]
    """

    val schema = ai.tripl.arc.util.ArcSchema.parseArcSchema(meta)

    val thrown0 = intercept[Exception with DetailException] {
      val dataset = transform.TypingTransformStage.execute(
        transform.TypingTransformStage(
          plugin=new transform.TypingTransform,
          name="dataset",
          description=None,
          schema=Right(schema.right.getOrElse(Nil)),
          inputView=inputView,
          outputView=outputView,
          params=Map.empty,
          persist=false,
          failMode=FailMode.Permissive,
          numPartitions=None,
          partitionBy=Nil
        )
      ).get
      dataset.count
    }

    assert(thrown0.getMessage.contains("TypingTransform with non-nullable column 'integerDatum' cannot continue due to error: Unable to convert 'textDatum' to integer using formatters ['#,##0;-#,##0']."))

  }  

  test("TypingTransform: metadata bad array") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val meta = """
    [
      {
        "id": "982cbf60-7ba7-4e50-a09b-d8624a5c49e6",
        "name": "booleanDatum",
        "description": "booleanDatum",
        "type": "boolean",
        "trim": false,
        "nullable": false,
        "nullableValues": [
            "",
            "null"
        ],
        "trueValues": [
            "true"
        ],
        "falseValues": [
            "false"
        ],
        "metadata": {
            "booleanArrayMeta": [true, false, "derp"]
        }
      }
    ]
    """

    val schema = ai.tripl.arc.util.ArcSchema.parseArcSchema(meta)
    schema match {
      case Left(stageError) => {
        assert(stageError == StageError(0, "booleanDatum",2,List(ConfigError("booleanArrayMeta", Some(20), "Metadata attribute 'booleanArrayMeta' cannot contain arrays of different types."))) :: Nil)
      }
      case Right(_) => assert(false)
    }
  }

  test("TypingTransform: metadata bad type null") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val meta = """
    [
      {
        "id": "982cbf60-7ba7-4e50-a09b-d8624a5c49e6",
        "name": "booleanDatum",
        "description": "booleanDatum",
        "type": "boolean",
        "trim": false,
        "nullable": false,
        "nullableValues": [
            "",
            "null"
        ],
        "trueValues": [
            "true"
        ],
        "falseValues": [
            "false"
        ],
        "metadata": {
            "booleanArrayMeta": null
        }
      }
    ]
    """

    val schema = ai.tripl.arc.util.ArcSchema.parseArcSchema(meta)
    schema match {
      case Left(_) => assert(false)
      case Right(_) => assert(true)
    }
  }

  test("TypingTransform: metadata bad type same name as column") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val meta = """
    [
      {
        "id": "982cbf60-7ba7-4e50-a09b-d8624a5c49e6",
        "name": "booleanDatum",
        "description": "booleanDatum",
        "type": "boolean",
        "trim": false,
        "nullable": false,
        "nullableValues": [
            "",
            "null"
        ],
        "trueValues": [
            "true"
        ],
        "falseValues": [
            "false"
        ],
        "metadata": {
          "booleanDatum": 5
        }
      }
    ]
    """

    val schema = ai.tripl.arc.util.ArcSchema.parseArcSchema(meta)
    schema match {
      case Left(stageError) => {
        assert(stageError == StageError(0, "booleanDatum",2,List(ConfigError("booleanDatum",Some(21),"Metadata attribute 'booleanDatum' cannot be the same name as column."))) :: Nil)
      }
      case Right(_) => assert(false)
    }
  }

  test("TypingTransform: metadata bad type multiple") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val meta = """
    [
      {
        "id": "982cbf60-7ba7-4e50-a09b-d8624a5c49e6",
        "name": "booleanDatum",
        "description": "booleanDatum",
        "type": "boolean",
        "trim": false,
        "nullable": false,
        "nullableValues": [
            "",
            "null"
        ],
        "trueValues": [
            "true"
        ],
        "falseValues": [
            "false"
        ],
        "metadata": {
            "badArray": [1, 1.1],
            "nullField": null
        }
      }
    ]
    """

    val schema = ai.tripl.arc.util.ArcSchema.parseArcSchema(meta)
    schema match {
      case Left(stageError) => {
        assert(stageError == StageError(0, "booleanDatum",2,List(ConfigError("badArray", Some(20),"Metadata attribute 'badArray' cannot contain `number` arrays of different types (all values must be `integers` or all values must be `doubles`)."))) :: Nil)
      }
      case Right(_) => assert(false)
    }
  }

  test("TypingTransform: metadata nested object") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val meta = """
    [
      {
        "id": "982cbf60-7ba7-4e50-a09b-d8624a5c49e6",
        "name": "booleanDatum",
        "description": "booleanDatum",
        "type": "boolean",
        "trim": false,
        "nullable": false,
        "nullableValues": [
            "",
            "null"
        ],
        "trueValues": [
            "true"
        ],
        "falseValues": [
            "false"
        ],
        "metadata": {
          "test": {
            "abc": {
              "def": true
            }
          }
        }
      }
    ]
    """

    val schema = ai.tripl.arc.util.ArcSchema.parseArcSchema(meta)

    assert(ExtractColumn.toStructField(schema.right.get(0)).metadata.json == """{"nullable":false,"test":{"abc":{"def":true}},"internal":false,"description":"booleanDatum","id":"982cbf60-7ba7-4e50-a09b-d8624a5c49e6"}""")
  }  

  test("TypingTransform: Execute with Structured Streaming" ) {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=true)

    val df = TestUtils.getKnownStringDataset.drop("nullDatum")
    df.createOrReplaceTempView("knownData")

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
    val schema = ai.tripl.arc.util.ArcSchema.parseArcSchema(TestUtils.getKnownDatasetMetadataJson)

    val dataset = transform.TypingTransformStage.execute(
      transform.TypingTransformStage(
        plugin=new transform.TypingTransform,
        name="dataset",
        description=None,
        schema=Right(schema.right.getOrElse(Nil)),
        inputView=inputView,
        outputView=outputView,
        params=Map.empty,
        persist=false,
        failMode=FailMode.Permissive,
        numPartitions=None,
        partitionBy=Nil
      )
    ).get

    val writeStream = dataset
      .writeStream
      .queryName("transformed")
      .format("memory")
      .start

    val stream = spark.table("transformed")

    try {
      Thread.sleep(2000)
      // will fail if typing does not work
      stream.first.getBoolean(0)
    } finally {
      writeStream.stop
    }
  }

  test("TypingTransform: StringType still has rules applied") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val meta = """
    [
      {
        "id": "",
        "name": "stringDatum",
        "description": "stringDatum",
        "type": "string",
        "trim": true,
        "nullable": true,
        "nullableValues": [
            "",
            "null"
        ],
        "metadata": {}
      }
    ]
    """
    val schema = ai.tripl.arc.util.ArcSchema.parseArcSchema(meta)

    val inputDataFrame = Seq((""),(" "), ("  ")).toDF("stringDatum")
    inputDataFrame.createOrReplaceTempView(inputView)

    val dataset = transform.TypingTransformStage.execute(
      transform.TypingTransformStage(
        plugin=new transform.TypingTransform,
        name="dataset",
        description=None,
        schema=Right(schema.right.getOrElse(Nil)),
        inputView=inputView,
        outputView=outputView,
        params=Map.empty,
        persist=false,
        failMode=FailMode.Permissive,
        numPartitions=None,
        partitionBy=Nil
      )
    ).get

    // ensure null has been set
    val values = dataset.collect
    assert(values(0).isNullAt(0) == true)
    assert(values(1).isNullAt(0) == true)
    assert(values(2).isNullAt(0) == true)
  }

  test("TypingTransform: binary type") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val meta = """
    [
      {
        "id": "982cbf60-7ba7-4e50-a09b-d8624a5c49e6",
        "name": "binaryDatum",
        "description": "binaryDatum",
        "type": "binary",
        "trim": false,
        "nullable": false,
        "nullableValues": [
            "",
            "null"
        ],
        "encoding": "base64",
        "metadata": {
        }
      }
    ]
    """

    val schema = ai.tripl.arc.util.ArcSchema.parseArcSchema(meta)
    schema match {
      case Left(_) => assert(false)
      case Right(stage) => assert(stage == List(BinaryColumn(Some("982cbf60-7ba7-4e50-a09b-d8624a5c49e6"),"binaryDatum",Some("binaryDatum"),false,None,false,List("", "null"),EncodingTypeBase64,Some("{}"))))
    }
  }
}