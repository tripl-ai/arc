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

import ai.tripl.arc.util.TestUtils

class ChaosMonkeySuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark

    // ensure targets removed
    spark.sparkContext.setLogLevel("INFO")
  }

  after {
    session.stop()
  }

  test("ChaosMonkey: test exception") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val conf = s"""{
      "plugins": {
        "lifecycle": [
          {
            "type": "ai.tripl.arc.plugins.lifecycle.ChaosMonkey",
            "environments": ["test"],
            "strategy": "exception",
            "probability": 0.05,
          }
        ]
      },  
      "stages": [
        {
          "type": "SQLTransform",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "sql": "SELECT 'a' AS a",
          "outputView": "outputView",
          "persist": true
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, arcCtx)) => 
   
      val thrown0 = intercept[Exception with DetailException] {
        for (_ <- 1 to 100) {
          val df = ARC.run(pipeline)(spark, logger, arcCtx).get
        }
      }

      assert(thrown0.getMessage.contains("ChaosMonkey triggered and exception thrown."))
    }
  }  

}
