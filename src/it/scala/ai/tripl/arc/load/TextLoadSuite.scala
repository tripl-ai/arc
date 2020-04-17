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

class TextLoadSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  val outputView = "dataset"

  // minio seems to need ip address not hostname
  val bucketName = "test"
  val minioHostPort = "http://minio:9000"
  val minioAccessKey = "AKIAIOSFODNN7EXAMPLE"
  val minioSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

  val targetSingleFileWildcard = s"s3a://${bucketName}/singlepart*.txt"  
  val targetSingleFile0 = "singlepart0.txt"  
  val targetSingleFile1 = "singlepart1.txt"  
  val targetSingleFile2 = "singlepart2.txt"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark
  }

  after {
    session.stop()
  }

  test("TextLoad: singleFile") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val dataset = Seq(
      (targetSingleFile0, "a"),
      (targetSingleFile0, "b"),
      (targetSingleFile0, "c"),
      (targetSingleFile1, "d"), 
      (targetSingleFile1, "e"), 
      (targetSingleFile2, "f")
    ).toDF("filename", "value")
    dataset.createOrReplaceTempView(outputView)

    val conf = s"""{
      "stages": [
        {
          "type": "TextLoad",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputView": "${outputView}",
          "outputURI": "s3a://${bucketName}",
          "singleFile": true,
          "separator": "\\n",
          "authentication": {
            "method": "AmazonAccessKey",
            "accessKeyID": "${minioAccessKey}",
            "secretAccessKey": "${minioSecretKey}",
            "endpoint": "${minioHostPort}"
          }    
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => ARC.run(pipeline)(spark, logger, arcContext)

      val actual = spark.read.text(targetSingleFileWildcard).withColumn("_filename", input_file_name())
      actual.persist
      assert(actual.where(s"_filename LIKE '%${targetSingleFile0}'").collect.map(_.getString(0)).mkString("|") == "a|b|c")
      assert(actual.where(s"_filename LIKE '%${targetSingleFile1}'").collect.map(_.getString(0)).mkString("|") == "d|e")
      assert(actual.where(s"_filename LIKE '%${targetSingleFile2}'").collect.map(_.getString(0)).mkString("|") == "f")      
    }
  }  

}
