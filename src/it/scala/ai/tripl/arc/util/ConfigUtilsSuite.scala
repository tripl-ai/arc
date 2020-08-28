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
  val bucketName = "bucket0"
  val forbiddenBucketName = "bucket1"

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

    // only set default aws provider override if not provided
    if (Option(spark.sparkContext.hadoopConfiguration.get("fs.s3a.aws.credentials.provider")).isEmpty) {
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", CloudUtils.defaultAWSProvidersOverride)
    }

    session = spark
  }

  after {
    session.stop()
  }

  test("ConfigUtilsSuite: Ensure remote data and config references can be parsed") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

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
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        ARC.run(pipeline)
      }
    }

    assert(spark.table(outputView).first.getString(0) == "Akita")
  }

  test("ConfigUtilsSuite: Test that AWS permissions are scoped to bucket") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    // note: initial files are created in the src/it/resources/minio/Dockerfile
    // then mounted in the minio command in src/it/resources/docker-compose.yml
    // first stage should work as authentication provided for ${bucketName}
    // second stage should fail as authentication not provided for ${forbiddenBucketName}
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
          "outputView": "akc_breed_info0",
          "delimiter": "Comma",
          "header": true
        },
        {
          "type": "DelimitedExtract",
          "name": "get the file back from minio",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "s3a://${forbiddenBucketName}/akc_breed_info.csv",
          "outputView": "akc_breed_info1",
          "delimiter": "Comma",
          "header": true
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)
    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        val thrown0 = intercept[Exception with DetailException] {
          val df = ARC.run(pipeline).get
        }
        assert(thrown0.getMessage.contains(s"java.nio.file.AccessDeniedException: s3a://${forbiddenBucketName}/akc_breed_info.csv"))
      }
    }
  }

  test("ConfigUtilsSuite: Test https remote job config") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val expected = spark.sql("SELECT '1' AS value")
    expected.createOrReplaceTempView("inputView")

    val pipelineEither = ArcPipeline.parseConfig(Right(new URI("https://raw.githubusercontent.com/tripl-ai/arc/master/src/test/resources/conf/job/active_customers.json")), arcContext)
    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        val actual = ARC.run(pipeline).get

        assert(expected.first.getString(0) == actual.first.getString(0))
      }
    }
  }

}
