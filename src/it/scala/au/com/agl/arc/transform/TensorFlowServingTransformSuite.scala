package au.com.agl.arc

import java.net.URI
import java.sql.DriverManager

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.mortbay.jetty.handler.{AbstractHandler, ContextHandler, ContextHandlerCollection}
import org.mortbay.jetty.{Server, Request, HttpConnection}
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import scala.io.Source

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.util.log.LoggerFactory 
import au.com.agl.arc.udf.UDF

import au.com.agl.arc.util._

class TensorFlowServingTransformSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  
  val inputView = "inputView"
  val outputView = "outputView"
  val uri = s"http://localhost:9001/v1/models/simple/versions/1:predict"
  var logger: au.com.agl.arc.util.log.logger.Logger = _

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

    logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    // register udf
    UDF.registerUDFs(spark.sqlContext)(logger)
  }

  after {
    session.stop
  }    

  test("HTTPTransform: Can call TensorFlowServing via REST: integer" ) {
    implicit val spark = session
    implicit val l = logger

    val df = spark.range(1, 10).toDF
    df.createOrReplaceTempView(inputView)

    var payloadDataset = spark.sql(s"""
    SELECT 
      id
      ,id AS value 
    FROM ${inputView}
    """).repartition(1)
    payloadDataset.createOrReplaceTempView(inputView)

    val transformDataset = transform.TensorFlowServingTransform.transform(
      TensorFlowServingTransform(
        name=outputView,
        description=None,
        uri=new URI(uri),
        inputView=inputView,
        outputView=outputView,
        signatureName=None,
        responseType=IntegerResponse,
        batchSize=10,
        params=Map.empty,
        persist=false,
        inputField="value",
        numPartitions=None,
        partitionBy=Nil           
      )
    ).get

    assert(transformDataset.first.getInt(2) == 11)
  }  

  test("HTTPTransform: Can call TensorFlowServing via REST: double" ) {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val df = spark.range(1, 10).toDF
    df.createOrReplaceTempView(inputView)

    var payloadDataset = spark.sql(s"""
    SELECT 
      id
      ,id AS value 
    FROM ${inputView}
    """).repartition(1)
    payloadDataset.createOrReplaceTempView(inputView)

    val transformDataset = transform.TensorFlowServingTransform.transform(
      TensorFlowServingTransform(
        name=outputView,
        description=None,
        uri=new URI(uri),
        inputView=inputView,
        outputView=outputView,
        signatureName=None,
        responseType=DoubleResponse,
        batchSize=10,
        params=Map.empty,
        persist=false,
        inputField="value",
        numPartitions=None,
        partitionBy=Nil           
      )
    ).get

    assert(transformDataset.first.getDouble(2) == 11.0)
  }   

  test("HTTPTransform: Can call TensorFlowServing via REST: string" ) {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val df = spark.range(1, 10).toDF
    df.createOrReplaceTempView(inputView)

    var payloadDataset = spark.sql(s"""
    SELECT 
      id
      ,id AS value 
    FROM ${inputView}
    """).repartition(1)
    payloadDataset.createOrReplaceTempView(inputView)

    val transformDataset = transform.TensorFlowServingTransform.transform(
      TensorFlowServingTransform(
        name=outputView,
        description=None,
        uri=new URI(uri),
        inputView=inputView,
        outputView=outputView,
        signatureName=None,
        responseType=StringResponse,
        batchSize=10,
        params=Map.empty,
        persist=false,
        inputField="value",
        numPartitions=None,
        partitionBy=Nil           
      )
    ).get

    assert(transformDataset.first.getString(2) == "11")
  } 

  test("HTTPTransform: Can call TensorFlowServing via REST: inputField" ) {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val df = spark.range(1, 10).toDF
    df.createOrReplaceTempView(inputView)

    var payloadDataset = spark.sql(s"""
    SELECT 
      id
    FROM ${inputView}
    """).repartition(1)
    payloadDataset.createOrReplaceTempView(inputView)

    val thrown = intercept[Exception with DetailException] {
      transform.TensorFlowServingTransform.transform(
        TensorFlowServingTransform(
          name=outputView,
          description=None,
          uri=new URI(uri),
          inputView=inputView,
          outputView=outputView,
          signatureName=None,
          responseType=IntegerResponse,
          batchSize=10,
          params=Map.empty,
          persist=false,
          inputField="value",
          numPartitions=None,
          partitionBy=Nil             
        )
      )
    }
    assert(thrown.getMessage.contains("""inputField 'value' is not present in inputView 'inputView' which has: [id] columns."""))  

    val transformDataset = transform.TensorFlowServingTransform.transform(
      TensorFlowServingTransform(
        name=outputView,
        description=None,
        uri=new URI(uri),
        inputView=inputView,
        outputView=outputView,
        signatureName=None,
        responseType=IntegerResponse,
        batchSize=10,
        params=Map.empty,
        persist=false,
        inputField="id",
        numPartitions=None,
        partitionBy=Nil           
      )
    ).get

    assert(transformDataset.first.getInt(1) == 11)
  }     

  test("HTTPTransform: Can call TensorFlowServing via Structured Streaming" ) {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val readStream = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", "1")
      .load

    readStream.createOrReplaceTempView(inputView)

    val transformDataset = transform.TensorFlowServingTransform.transform(
      TensorFlowServingTransform(
        name=outputView,
        description=None,
        uri=new URI(uri),
        inputView=inputView,
        outputView=outputView,
        signatureName=None,
        responseType=IntegerResponse,
        batchSize=10,
        params=Map.empty,
        persist=false,
        inputField="value",
        numPartitions=None,
        partitionBy=Nil           
      )
    ).get

    val writeStream = transformDataset
      .writeStream
      .queryName("transformed") 
      .format("memory")
      .start

    val df = spark.table("transformed")

    try {
      Thread.sleep(2000)
      assert(df.first.getInt(2) == df.first.getLong(1).toInt+10)
    } finally {
      writeStream.stop
    }
  } 

}