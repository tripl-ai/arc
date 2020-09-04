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
import ai.tripl.arc.util._

class MetadataValidateSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  val inputView = "inputView"
  val outputView = "outputView"
  val schemaView = "schemaView"
  val signature = "MetadataValidate requires query to return 1 row with [outcome: boolean, message: string] signature."

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
  }

  after {
    session.stop
  }

  test("MetadataValidate: end-to-end success") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()
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
        },
        {
          "type": "MetadataValidate",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputView": "outputView",
          "inputURI": "${spark.getClass.getResource("/conf/sql").toString}/metadata_validate.sql",
          "sqlParams": {
            "securityLevel": "10",
          }
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => ARC.run(pipeline)(spark, logger, arcContext)
    }
  }

  test("MetadataValidate: end-to-end failure") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()
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
        },
        {
          "type": "MetadataValidate",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputView": "outputView",
          "inputURI": "${spark.getClass.getResource("/conf/sql").toString}/metadata_validate.sql",
          "sqlParams": {
            "securityLevel": "1",
          }
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) =>
      val thrown0 = intercept[Exception with DetailException] {
        ARC.run(pipeline)(spark, logger, arcContext)
      }
      assert(thrown0.getMessage === """MetadataValidate failed with message: '{"count":9,"securityLevelGreaterThan1":6}'.""")
    }
  }

}