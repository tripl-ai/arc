package ai.tripl.arc

import java.net.URI
import org.apache.commons.lang.StringEscapeUtils

import org.apache.http.client.methods.{HttpPost}
import org.apache.http.impl.client.HttpClients
import org.apache.http.entity.StringEntity

import scala.io.Source

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.avro.Schema
import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.util.ConfigUtils._
import ai.tripl.arc.util.log.LoggerFactory 

import com.typesafe.config._

import ai.tripl.arc.util._

class AvroExtractSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  
  val targetFile = getClass.getResource("/avro/users.avro").toString 
  val targetBinaryFile = getClass.getResource("/avro/users.avrobinary").toString 
  val schemaFile = getClass.getResource("/avro/user.avsc").toString
  val outputView = "dataset"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")   

    session = spark
    import spark.implicits._    
  }

  after {
    session.stop()
  }

  test("AvroExtract: Schema Included Avro") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)  

    val extractDataset = extract.AvroExtract.extract(
      AvroExtract(
        name=outputView,
        description=None,
        cols=Right(Nil),
        outputView=outputView,
        input=Right(targetFile),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        basePath=None,
        contiguousIndex=true,
        avroSchema=None,
        inputField=None        
      )
    ).get

    assert(extractDataset.first.getString(0) == "Alyssa")
  }  

  test("AvroExtract: Binary only Avro") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)  


    val schema = new Schema.Parser().parse(CloudUtils.getTextBlob(new URI(schemaFile)))

    extract.BytesExtract.extract(
      BytesExtract(
        name="dataset",
        description=None,
        outputView=outputView, 
        input=Right(targetBinaryFile),
        authentication=None,
        persist=false,
        numPartitions=None,
        contiguousIndex=true,
        params=Map.empty,
        failMode=FailModeTypeFailFast
      )
    )

    val extractDataset = extract.AvroExtract.extract(
      AvroExtract(
        name=outputView,
        description=None,
        cols=Right(Nil),
        outputView=outputView,
        input=Left(outputView),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        basePath=None,
        contiguousIndex=true,
        avroSchema=Option(schema),
        inputField=None
      )
    ).get

    assert(extractDataset.select("value.*").first.getString(0) == "Alyssa")
  }   

  test("AvroExtract: Binary with Kafka Schema Registry") {
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false, lifecyclePlugins=Nil, disableDependencyValidation=false)


    val schema = new Schema.Parser().parse(CloudUtils.getTextBlob(new URI(schemaFile)))
    
    // POST the schema to kafka so we know it is there as ID=1
    val httpClient = HttpClients.createDefault
    val httpPost = new HttpPost("http://kafka:8081/subjects/Kafka-value/versions");
    httpPost.setEntity(new StringEntity(s"""{"schema": "${StringEscapeUtils.escapeJavaScript(schema.toString)}"}"""));
    httpPost.addHeader("Content-Type", "application/vnd.schemaregistry.v1+json") 
    val response = httpClient.execute(httpPost)
    assert(response.getStatusLine.getStatusCode == 200)
    response.close
    httpClient.close

    val conf = s"""{
      "stages": [
        {
          "type": "BytesExtract",
          "name": "get the binary avro file without header",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "${targetBinaryFile}",
          "outputView": "bytes_extract_output"
        },
        {
          "type": "AvroExtract",
          "name": "try to parse",
          "description": "load customer avro extract",
          "environments": [
            "production",
            "test"
          ],
          "inputView": "bytes_extract_output",
          "outputView": "avro_extract_output",
          "persist": false,
          "inputField": "value",
          "avroSchemaURI": "http://kafka-schema-registry:8081/schemas/ids/1"
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
  }

  test("AvroExtract: Binary with user.avsc") {
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false, lifecyclePlugins=Nil, disableDependencyValidation=false)

    val conf = s"""{
      "stages": [
        {
          "type": "BytesExtract",
          "name": "get the binary avro file without header",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "${targetBinaryFile}",
          "outputView": "bytes_extract_output"
        },
        {
          "type": "AvroExtract",
          "name": "try to parse",
          "description": "load customer avro extract",
          "environments": [
            "production",
            "test"
          ],
          "inputView": "bytes_extract_output",
          "outputView": "avro_extract_output",
          "persist": false,
          "inputField": "value",
          "avroSchemaURI": "${schemaFile}"
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
  }
}
