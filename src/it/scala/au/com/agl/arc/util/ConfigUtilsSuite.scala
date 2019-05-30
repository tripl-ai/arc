package au.com.agl.arc

import java.net.URI

import scala.io.Source
import scala.collection.JavaConverters._

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql._

import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.api.{Delimited, Delimiter, QuoteCharacter}
import au.com.agl.arc.util.log.LoggerFactory
import au.com.agl.arc.util.ConfigUtils
import au.com.agl.arc.util.ConfigUtils._

import com.typesafe.config._

class ConfigUtilsSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _

  val outputView = "akita"
  val bucketName = "test"

  val minioHostPort = "http://127.0.0.1:9400"
  val minioAccessKey = "AKIAIOSFODNN7EXAMPLE"
  val minioSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

  before {
    val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")         

    session = spark
  }

  after {
    session.stop()
  }

  test("ConfigUtilsSuite: Ensure remote data and config references can be parsed") {
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false, lifecyclePlugins=Nil, disableDependencyValidation=false)

    // point to local minio s3 rather than actual s3
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", minioHostPort)

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
            "secretAccessKey": "${minioSecretKey}"
          }                 
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
            "secretAccessKey": "${minioSecretKey}"
          }          
        }
      ]
    }"""

    val argsMap = collection.mutable.Map[String, String]()
    val graph = ConfigUtils.Graph(Nil, Nil, false)
    val pipelineEither = ConfigUtils.parseConfig(Left(conf), argsMap, graph, arcContext)

    pipelineEither match {
      case Left(_) => {
        println(pipelineEither)  
        assert(false)
      }
      case Right((pl, _, _)) => {
        ARC.run(pl)
      }
    }

    assert(spark.table(outputView).first.getString(0) == "Akita")
  }
}
