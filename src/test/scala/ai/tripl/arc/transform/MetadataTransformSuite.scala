package ai.tripl.arc

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

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.util._

class MetadataTransformSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  val inputView = "inputView"
  val outputView = "outputView"
  val schemaView = "schemaView"

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
  }

  after {
    session.stop()
  }

  test("MetadataTransform: schemaURI") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val df = TestUtils.getKnownDataset.drop("nullDatum")
    df.createOrReplaceTempView(inputView)

    val conf = s"""{
      "stages": [
        {
          "type": "MetadataTransform",
          "name": "attach metadata",
          "environments": [
            "production",
            "test"
          ],
          "inputView": "${inputView}",
          "outputView": "${outputView}",
          "schemaURI": "${getClass.getResource("/conf/metadata").toString}/knownDataset.json",
          "failMode": "failfast"
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        ARC.run(pipeline)(spark, logger, arcContext) match {
          case Some(df) => {
            // test metadata
            val timestampDatumMetadata = df.schema.fields(df.schema.fieldIndex("timestampDatum")).metadata
            assert(timestampDatumMetadata.getLong("securityLevel") == 7)
          }
          case None => assert(false)
        }
      }
    }
  }

  test("MetadataTransform: schemaView failfast success") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)
    import spark.implicits._

    val df = TestUtils.getKnownDataset.drop("nullDatum")
    df.createOrReplaceTempView(inputView)

    // create schemaView
    val meta = spark.createDataset[String](List(TestUtils.getKnownDatasetMetadataJson))
    val schemaDF = spark.read.option("multiLine", true).json(meta)
    schemaDF.createOrReplaceTempView(schemaView)

    val conf = s"""{
      "stages": [
        {
          "type": "MetadataTransform",
          "name": "attach metadata",
          "environments": [
            "production",
            "test"
          ],
          "inputView": "${inputView}",
          "outputView": "${outputView}",
          "schemaView": "${schemaView}",
          "failMode": "failfast"
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        ARC.run(pipeline)(spark, logger, arcContext) match {
          case Some(df) => {
            // test metadata
            val timestampDatumMetadata = df.schema.fields(df.schema.fieldIndex("timestampDatum")).metadata
            assert(timestampDatumMetadata.getLong("securityLevel") == 7)
          }
          case None => assert(false)
        }
      }
    }
  }

  test("MetadataTransform: schemaView failfast failure") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)
    import spark.implicits._

    val df = TestUtils.getKnownDataset.drop("nullDatum")
    df.createOrReplaceTempView(inputView)

    // create schemaView
    val meta = spark.createDataset[String](List(TestUtils.getKnownDatasetMetadataJson))
    val schemaDF = spark.read.option("multiLine", true).json(meta)
    schemaDF.createOrReplaceTempView("schemaDF")
    val filteredDF = spark.sql(s"""SELECT * FROM schemaDF WHERE name != 'booleanDatum' ORDER BY name""")
    filteredDF.createOrReplaceTempView(schemaView)

    val thrown0 = intercept[Exception with DetailException] {
      transform.MetadataTransformStage.execute(
        transform.MetadataTransformStage(
          plugin=new transform.MetadataTransform,
          name="MetadataTransform",
          description=None,
          inputView=inputView,
          outputView=outputView,
          schema=Left(schemaView),
          schemaURI=None,
          failMode=FailModeTypeFailFast,
          persist=false,
          params=Map.empty,
          numPartitions=None,
          partitionBy=Nil
        )
      ).get
    }
    assert(thrown0.getMessage === "MetadataTransform with failMode = 'failfast' ensures that the schemaView 'schemaView' has the same columns as inputView 'inputView' but schemaView 'schemaView' has columns: ['longDatum', 'dateDatum', 'timestampDatum', 'decimalDatum', 'integerDatum', 'stringDatum', 'timeDatum', 'doubleDatum'] and 'inputView' contains columns: ['longDatum', 'dateDatum', 'timestampDatum', 'booleanDatum', 'decimalDatum', 'integerDatum', 'stringDatum', 'timeDatum', 'doubleDatum'].")
  }

  test("MetadataTransform: schemaView permissive") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)
    import spark.implicits._

    val df = TestUtils.getKnownDataset.drop("nullDatum")
    df.createOrReplaceTempView(inputView)

    // create schemaView
    val meta = spark.createDataset[String](List(TestUtils.getKnownDatasetMetadataJson))
    val schemaDF = spark.read.option("multiLine", true).json(meta)
    schemaDF.createOrReplaceTempView("schemaDF")
    val filteredDF = spark.sql(s"""SELECT * FROM schemaDF WHERE name = 'booleanDatum' ORDER BY name""")
    filteredDF.createOrReplaceTempView(schemaView)

    val conf = s"""{
      "stages": [
        {
          "type": "MetadataTransform",
          "name": "attach metadata",
          "environments": [
            "production",
            "test"
          ],
          "inputView": "${inputView}",
          "outputView": "${outputView}",
          "schemaView": "${schemaView}",
          "failMode": "permissive"
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        ARC.run(pipeline)(spark, logger, arcContext) match {
          case Some(df) => {
            // test metadata
            // all metadata fields have securityLevel
            val booleanDatumMetadata = df.schema.fields(df.schema.fieldIndex("booleanDatum")).metadata
            assert(booleanDatumMetadata.contains("securityLevel"))

            // should not have metadata set due to permissive
            val timestampDatumMetadata = df.schema.fields(df.schema.fieldIndex("timestampDatum")).metadata
            assert(!timestampDatumMetadata.contains("securityLevel"))
          }
          case None => assert(false)
        }
      }
    }
  }

}
