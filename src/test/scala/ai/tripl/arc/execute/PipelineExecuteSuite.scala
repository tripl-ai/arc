package ai.tripl.arc

import java.net.URI
import java.sql.DriverManager

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.util._
import ai.tripl.arc.util.log.LoggerFactory

class PipelineExecuteSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  var connection: java.sql.Connection = _

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
    session.stop
  }

  test("PipelineExecute: Nested Lifecycle Plugins") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)
    import spark.implicits._

    // create single row dataset
    val df = Seq((s"testKey,testValue")).toDF("value")
    df.createOrReplaceTempView("inputView")

    val conf = s"""
    {
      "plugins": {
        "lifecycle": [
          {
            "type": "ai.tripl.arc.plugins.TestLifecyclePlugin",
            "environments": ["test"],
            "name": "level0",
            "outputViewBefore": "level0before",
            "outputViewAfter": "level0after",
            "value": "level0"
          }
        ],
      },      
      "stages": [
        {
          "type": "PipelineExecute",
          "name": "embed the active customer pipeline",
          "environments": [
            "production",
            "test"
          ],
          "uri": "${spark.getClass.getResource("/conf/").toString}/pipeline_execute.conf",
        }
      ]
    }
    """

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, ctx)) => ARC.run(pipeline)(spark, logger, ctx)
    }

    val expectedBeforeLevel0 = Seq(("delimited extract", "before", "level0")).toDF("stage","when","message")
    assert(TestUtils.datasetEquality(expectedBeforeLevel0, spark.table("level0before")))

    val expectedAfterLevel0 = Seq(("delimited extract", "after", "level0", 1L)).toDF("stage","when","message","count")
    assert(TestUtils.datasetEquality(expectedAfterLevel0, spark.table("level0after")))    

    val expectedBeforeLevel1 = Seq(("delimited extract", "before", "level1")).toDF("stage","when","message")
    assert(TestUtils.datasetEquality(expectedBeforeLevel1, spark.table("level1before")))

    val expectedAfterLevel1 = Seq(("delimited extract", "after", "level1", 1L)).toDF("stage","when","message","count")
    assert(TestUtils.datasetEquality(expectedAfterLevel1, spark.table("level1after")))        

  }

}
