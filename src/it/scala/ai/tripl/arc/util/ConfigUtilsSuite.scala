package ai.tripl.arc

import java.net.URI
import java.net.InetAddress

import scala.io.Source
import scala.collection.JavaConverters._

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.api.{Delimited, Delimiter, QuoteCharacter}
import ai.tripl.arc.config.ArcPipeline
import ai.tripl.arc.util._

import com.typesafe.config._

class ConfigUtilsSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _

  val outputView = "akita"
  val bucketName = "test"

  // minio seems to need ip address not hostname
  val minioHostPort = "http://minio:9000"
  val minioAccessKey = "AKIAIOSFODNN7EXAMPLE"
  val minioSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    implicit val logger = TestUtils.getLogger()

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark
  }

  after {
    session.stop()
  }

  test("ConfigUtilsSuite: Ensure remote data and config references can be parsed") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    // note: initial files are created in the src/it/resources/minio/Dockerfile
    // then mounted in the minio command in src/it/resources/docker-compose.yml

    val conf = s"""{
      "stages": [
        {
          "type": "DelimitedExtract",
          "name": "get the file back from minio",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "s3a://${bucketName}/akc_breed_info.csv",
          "authentication": {
            "method": "AmazonAccessKey",
            "accessKeyID": "${minioAccessKey}",
            "secretAccessKey": "${minioSecretKey}",
            "endpoint": "${minioHostPort}"
          },
          "outputView": "akc_breed_info",
          "delimiter": "Comma",
          "header": true
        },
        {
          "type": "SQLTransform",
          "name": "select akita breed",
          "environments": [
            "production",
            "test"
          ],
          "sqlParams": {
            "breed": "Akita"
          },
          "inputURI": "s3a://${bucketName}/select_akita.sql",
          "outputView": "${outputView}",
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
      case Left(_) => {
        println(pipelineEither)
        assert(false)
      }
      case Right((pipeline, _)) => {
        ARC.run(pipeline)
      }
    }

    assert(spark.table(outputView).first.getString(0) == "Akita")
  }
}
