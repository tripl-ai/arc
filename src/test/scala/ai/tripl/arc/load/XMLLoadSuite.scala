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
import ai.tripl.arc.config._

import ai.tripl.arc.util.TestUtils

class XMLLoadSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  val targetFile = "/tmp/extract.xml"
  val outputView = "dataset"

  val targetSingleFile0 = FileUtils.getTempDirectoryPath() + "singlefile0.xml"
  val targetSingleFileWildcard = FileUtils.getTempDirectoryPath() + "singlefile*.xml"

  val targetSinglePartWildcard = FileUtils.getTempDirectoryPath() + "singlepart*.xml"
  val targetSinglePart0 = FileUtils.getTempDirectoryPath() + "singlepart0.xml"
  val targetSinglePart1 = FileUtils.getTempDirectoryPath() + "singlepart1.xml"

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

    // ensure targets removed
    FileUtils.deleteQuietly(new java.io.File(targetFile))
    FileUtils.deleteQuietly(new java.io.File(targetSingleFile0))
    FileUtils.deleteQuietly(new java.io.File(targetSinglePart0))
    FileUtils.deleteQuietly(new java.io.File(targetSinglePart1))

  }

  after {
    session.stop()

    // ensure targets removed
    FileUtils.deleteQuietly(new java.io.File(targetFile))
    FileUtils.deleteQuietly(new java.io.File(targetSingleFile0))
    FileUtils.deleteQuietly(new java.io.File(targetSinglePart0))
    FileUtils.deleteQuietly(new java.io.File(targetSinglePart1))
  }

  test("XMLLoad") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val dataset = TestUtils.getKnownDataset
    dataset.createOrReplaceTempView(outputView)

    load.XMLLoadStage.execute(
      load.XMLLoadStage(
        plugin=new load.XMLLoad,
        name=outputView,
        description=None,
        inputView=outputView,
        outputURI=Some(new URI(targetFile)),
        partitionBy=Nil,
        numPartitions=None,
        authentication=None,
        saveMode=SaveMode.Overwrite,
        singleFile=false,
        prefix="",
        singleFileNumPartitions=4096,
        params=Map.empty
      )
    )

    val expected = dataset.drop($"nullDatum")
      .withColumn("dateDatum", col("dateDatum").cast("string"))
      .withColumn("decimalDatum", col("decimalDatum").cast("double"))
    val actual = spark.read.format("com.databricks.spark.xml").load(targetFile)

    assert(TestUtils.datasetEquality(expected, actual))
  }

  test("XMLLoad: partitionBy") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val expected = TestUtils.getKnownDataset
    expected.createOrReplaceTempView(outputView)
    assert(expected.select(spark_partition_id()).distinct.count === 1)

    load.XMLLoadStage.execute(
      load.XMLLoadStage(
        plugin=new load.XMLLoad,
        name=outputView,
        description=None,
        inputView=outputView,
        outputURI=Some(new URI(targetFile)),
        partitionBy="booleanDatum" :: Nil,
        numPartitions=None,
        authentication=None,
        saveMode=SaveMode.Overwrite,
        singleFile=false,
        prefix="",
        singleFileNumPartitions=4096,
        params=Map.empty
      )
    )

    val actual = spark.read.format("com.databricks.spark.xml").load(targetFile)
    assert(actual.select(spark_partition_id()).distinct.count === 2)
  }

  test("XMLLoad: no outputURI, not singleFile") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val conf = s"""{
      "stages": [
        {
          "type": "XMLLoad",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputView": "${outputView}",
          "singleFile": false
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => assert(err.toString.contains("Missing required attribute 'outputURI' when not in 'singleFile' mode."))
      case Right((pipeline, _)) => fail("should fail")
    }
  }

  test("XMLLoad: singleFile no filename") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val dataset = spark.sql(s"""
    SELECT
      NAMED_STRUCT(
          '_VALUE', NAMED_STRUCT(
              'child0', 0,
              'child1', NAMED_STRUCT(
                'nested0', 0,
                'nested1', 'nestedvalue'
              )
          ),
          '_attribute', 'attribute'
      ) AS Document
    """)
    dataset.createOrReplaceTempView(outputView)

    val conf = s"""{
      "stages": [
        {
          "type": "XMLLoad",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputView": "${outputView}",
          "outputURI": "${targetSingleFile0}",
          "singleFile": true,
          "prefix": "<?xml version="1.0" encoding="UTF-8"?>\\n"
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => ARC.run(pipeline)(spark, logger, arcContext)

      val actual = spark.read.option("wholetext", true).text(targetSingleFileWildcard).withColumn("_filename", input_file_name())
      actual.persist
      assert(actual.count == 1)
      assert(actual.first.getString(0) ==
      """<?xml version=1.0 encoding=UTF-8?>
      |<Document attribute="attribute">
      |  <child0>0</child0>
      |  <child1>
      |    <nested0>0</nested0>
      |    <nested1>nestedvalue</nested1>
      |  </child1>
      |</Document>""".stripMargin)
      assert(actual.first.getString(1).contains(targetSingleFile0))
    }
  }

  test("XMLLoad: singleFile multiple filename") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val dataset = spark.sql(s"""
    SELECT
      NAMED_STRUCT(
          '_VALUE', NAMED_STRUCT(
              'child0', 0,
              'child1', NAMED_STRUCT(
                'nested0', 0,
                'nested1', 'nestedvalue'
              )
          ),
          '_attribute', 'attribute'
      ) AS Document
      ,'${targetSinglePart0}' AS filename

    UNION ALL

    SELECT
      NAMED_STRUCT(
          '_VALUE', NAMED_STRUCT(
              'child0', 1,
              'child1', NAMED_STRUCT(
                'nested0', 1,
                'nested1', 'nestedvalue'
              )
          ),
          '_attribute', 'attribute'
      ) AS Document
      ,'${targetSinglePart1}' AS filename

    """)
    dataset.createOrReplaceTempView(outputView)

    val conf = s"""{
      "stages": [
        {
          "type": "XMLLoad",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputView": "${outputView}",
          "outputURI": "${new URI(FileUtils.getTempDirectoryPath().stripSuffix("/"))}",
          "singleFile": true,
          "prefix": "<?xml version="1.0" encoding="UTF-8"?>\\n"
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => ARC.run(pipeline)(spark, logger, arcContext)

      val actual = spark.read.option("wholetext", true).text(targetSinglePartWildcard).withColumn("_filename", input_file_name())
      val rows = actual.orderBy("_filename").collect
      assert(rows.length == 2)
      assert(rows(0).getString(0) ==
      """<?xml version=1.0 encoding=UTF-8?>
      |<Document attribute="attribute">
      |  <child0>0</child0>
      |  <child1>
      |    <nested0>0</nested0>
      |    <nested1>nestedvalue</nested1>
      |  </child1>
      |</Document>""".stripMargin)
      assert(rows(0).getString(1).contains(targetSinglePart0))
      assert(rows(1).getString(0) ==
      """<?xml version=1.0 encoding=UTF-8?>
      |<Document attribute="attribute">
      |  <child0>1</child0>
      |  <child1>
      |    <nested0>1</nested0>
      |    <nested1>nestedvalue</nested1>
      |  </child1>
      |</Document>""".stripMargin)
      assert(rows(1).getString(1).contains(targetSinglePart1))
    }
  }

}
