package au.com.agl.arc

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import collection.JavaConverters._

import org.apache.http.client.methods.HttpDelete
import org.apache.http.impl.client.HttpClientBuilder

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.util.log.LoggerFactory 

import au.com.agl.arc.util._
import au.com.agl.arc.util.ControlUtils._

import org.elasticsearch.spark.sql._ 

class ElasticsearchExtractSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  

  val testData = getClass.getResource("/akc_breed_info.csv").toString
  val outputView = "actual"
  val index = "dogs"
  val esURL = "localhost"
  val port = "9200"
  val wanOnly = "true"
  val ssl = "false"  

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("FATAL")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")   

    session = spark
  }

  after {
    session.stop
  }

  test("ElasticsearchExtract") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false, lifecyclePlugins=Nil, disableDependencyValidation=false)

    val df0 = spark.read.option("header","true").csv(testData)
    df0.createOrReplaceTempView("expected")

    val client = HttpClientBuilder.create.build
    val delete = new HttpDelete(s"http://${esURL}:9200/index")
    val response = client.execute(delete)
    response.close 

    df0.write
      .format("org.elasticsearch.spark.sql")
      .option("es.nodes.wan.only",wanOnly)
      .option("es.port",port)
      .option("es.net.ssl",ssl)
      .option("es.nodes", esURL)
      .mode("overwrite")
      .save(index)

    extract.ElasticsearchExtract.extract(
      ElasticsearchExtract(
        name="df", 
        description=None,
        input=index,
        outputView=outputView, 
        numPartitions=None,
        params=Map("es.nodes.wan.only" -> wanOnly, "es.port" -> port, "es.net.ssl" -> ssl, "es.nodes" -> esURL),
        partitionBy=Nil,
        persist=true
      )
    )   

    // reselect fields to ensure correct order
    val expected = spark.sql(s"""
    SELECT Breed, height_high_inches, height_low_inches, weight_high_lbs, weight_low_lbs FROM expected
    """)

    // reselect fields to ensure correct order
    val actual = spark.sql(s"""
    SELECT Breed, height_high_inches, height_low_inches, weight_high_lbs, weight_low_lbs FROM ${outputView}
    """)

    val actualExceptExpectedCount = actual.except(expected).count
    val expectedExceptActualCount = expected.except(actual).count
    if (actualExceptExpectedCount != 0 || expectedExceptActualCount != 0) {
      println("actual")
      actual.show(100000, false)
      println("expected")
      expected.show(100000, false)  
    }
    assert(actualExceptExpectedCount === 0)
    assert(expectedExceptActualCount === 0)
  }    

}
