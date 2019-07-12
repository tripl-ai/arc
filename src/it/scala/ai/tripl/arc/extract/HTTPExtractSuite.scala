package ai.tripl.arc

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._

import ai.tripl.arc.util._

class HTTPExtractSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  

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

  test("HTTPExtract: end-to-end") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val conf = s"""{
      "stages": [
        {
          "type": "HTTPExtract",
          "name": "load data",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "https://raw.githubusercontent.com/tripl-ai/arc/master/src/it/resources/akc_breed_info.csv",
          "outputView": "data"
        }
      ]
    }"""

    val pipelineEither = ConfigUtils.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(_) => {
        println(pipelineEither)
        assert(false)
      }
      case Right((pipeline, _)) => {
        val df = ARC.run(pipeline)(spark, logger, arcContext)
        df match {
          case Some(df) => assert(df.count != 0)
          case None => assert(false)
        }
      }
    }  
  }    
}
