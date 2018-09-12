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

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    session = spark
    import spark.implicits._

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")    

    // register udf
    UDF.registerUDFs(spark.sqlContext)    
  }

  after {
    session.stop
  }    

  test("HTTPTransform: Can call TensorFlowServing via REST: integer" ) {
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
        uri=new URI(uri),
        inputView=inputView,
        outputView=outputView,
        signatureName=None,
        responseType=Option(IntegerResponse),
        batchSize=Option(10),
        params=Map.empty,
        persist=false
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
        uri=new URI(uri),
        inputView=inputView,
        outputView=outputView,
        signatureName=None,
        responseType=Option(DoubleResponse),
        batchSize=Option(10),
        params=Map.empty,
        persist=false
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
        uri=new URI(uri),
        inputView=inputView,
        outputView=outputView,
        signatureName=None,
        responseType=Option(StringResponse),
        batchSize=Option(10),
        params=Map.empty,
        persist=false
      )
    ).get

    assert(transformDataset.first.getString(2) == "11")
  } 

}