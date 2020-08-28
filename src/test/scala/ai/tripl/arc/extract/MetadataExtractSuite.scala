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
import ai.tripl.arc.api.API.Extract
import ai.tripl.arc.config._
import ai.tripl.arc.util._

class MetadataExtractSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  val inputView = "inputView"
  val outputView = "outputView"

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

  test("MetadataExtract: end-to-end") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()
    import spark.implicits._

    val df = TestUtils.getKnownDataset
    df.createOrReplaceTempView(inputView)

    val conf = s"""{
      "stages": [
        {
          "type": "MetadataExtract",
          "name": "extract metadata for persistence",
          "environments": [
            "production",
            "test"
          ],
          "inputView": "${inputView}",
          "outputView": "${outputView}"
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        ARC.run(pipeline)(spark, logger, arcContext) match {
          case Some(df) => {
            assert(df.columns.deep == Array("name", "nullable", "type", "metadata").deep)
            assert(df.count == 10)
          }
          case None => assert(false)
        }
      }
    }
  }

  test("MetadataExtract: end-to-end with metadata") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()
    import spark.implicits._

    val schema = ai.tripl.arc.util.ArcSchema.parseArcSchema(TestUtils.getKnownDatasetMetadataJson)
    val df = MetadataUtils.setMetadata(TestUtils.getKnownDataset, Extract.toStructType(schema.right.get))
    df.createOrReplaceTempView(inputView)

    val conf = s"""{
      "stages": [
        {
          "type": "MetadataExtract",
          "name": "extract metadata for persistence",
          "environments": [
            "production",
            "test"
          ],
          "inputView": "${inputView}",
          "outputView": "${outputView}"
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        ARC.run(pipeline)(spark, logger, arcContext) match {
          case Some(df) => {
            assert(df.where("name = 'timestampDatum'").select(col("metadata")("securityLevel")).first.getLong(0) == 7)
          }
          case None => assert(false)
        }
      }
    }
  }

}
