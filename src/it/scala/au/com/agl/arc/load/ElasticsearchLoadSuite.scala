package au.com.agl.arc

import java.net.URI
import java.util.UUID
import java.util.Properties

import org.apache.http.client.methods.HttpDelete
import org.apache.http.impl.client.HttpClientBuilder

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.plugins.LifecyclePlugin
import au.com.agl.arc.util.log.LoggerFactory 

import org.elasticsearch.spark.sql._ 

class ElasticsearchLoadSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  
  val testData = getClass.getResource("/akc_breed_info.csv").toString
  val inputView = "expected"
  val index = "index/dogs"
  val esURL = "localhost"
  val port = "9200"
  val wanOnly = "true"
  val ssl = "false"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .config("es.index.auto.create", "true")
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

  test("ElasticsearchLoad") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false, lifecyclePlugins=new ListBuffer[LifecyclePlugin]())

    val df0 = spark.read.option("header","true").csv(testData)
    df0.createOrReplaceTempView(inputView)

    val client = HttpClientBuilder.create.build
    val delete = new HttpDelete(s"http://${esURL}:9200/index")
    val response = client.execute(delete)
    response.close 

    load.ElasticsearchLoad.load(
      ElasticsearchLoad(
        name="df", 
        description=None,
        inputView=inputView, 
        output=index,
        numPartitions=None,
        params=Map("es.nodes.wan.only" -> wanOnly, "es.port" -> port, "es.net.ssl" -> ssl, "es.nodes" -> esURL),
        saveMode=SaveMode.Overwrite,
        partitionBy=Nil
      )
    )   

    val df1 = spark.read
      .format("org.elasticsearch.spark.sql")
      .option("es.nodes.wan.only",wanOnly)
      .option("es.port",port)
      .option("es.net.ssl",ssl)
      .option("es.nodes", esURL)
      .load(index)

    df1.createOrReplaceTempView("actual")


    // reselect fields to ensure correct order
    val expected = spark.sql(s"""
    SELECT Breed, height_high_inches, height_low_inches, weight_high_lbs, weight_low_lbs FROM ${inputView}
    """)

    // reselect fields to ensure correct order
    val actual = spark.sql(s"""
    SELECT Breed, height_high_inches, height_low_inches, weight_high_lbs, weight_low_lbs FROM actual
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
