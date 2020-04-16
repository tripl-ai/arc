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

class DataLineageSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  val inputFile = FileUtils.getTempDirectoryPath() + "input.parquet"
  val outputFile = FileUtils.getTempDirectoryPath() + "output.parquet"
  val outputView0 = "dataset0"
  val outputView1 = "dataset1"

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
    FileUtils.deleteQuietly(new java.io.File(inputFile))
    FileUtils.deleteQuietly(new java.io.File(outputFile))
    TestUtils.getKnownDataset.drop(col("nullDatum")).write.parquet(inputFile)
    spark.sparkContext.setLogLevel("INFO")
  }

  after {
    session.stop()
  }

  test("DataLineage: test log") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val conf = s"""{
      "plugins": {
        "lifecycle": [
          {
            "type": "ai.tripl.arc.plugins.lifecycle.DataLineage",
            "environments": ["test"],
            "output": "log"
          }
        ]
      },      
      "stages": [
        {
          "type": "ParquetExtract",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "${inputFile}",
          "outputView": "${outputView0}"
        },
        {
          "type": "SQLTransform",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "sql": "SELECT booleanDatum, decimalDatum + 1 AS decimalDatum FROM ${outputView0}",
          "outputView": "${outputView1}"
        },        
        {
          "type": "ParquetLoad",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputView": "${outputView1}",
          "outputURI": "${outputFile}",
          "saveMode": "Overwrite"
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, arcCtx)) => 
    
      val df = ARC.run(pipeline)(spark, logger, arcCtx).get
      df.show(false)
    
    }
  }  

}
