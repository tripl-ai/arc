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

import au.com.agl.arc.util._

class HTTPTransformSuite extends FunSuite with BeforeAndAfter {

  class PostEchoHandler extends AbstractHandler {
    override def handle(target: String, request: HttpServletRequest, response: HttpServletResponse, dispatch: Int) = {
      if (HttpConnection.getCurrentConnection.getRequest.getMethod == "POST") {
        response.setContentType("text/html")
        response.setStatus(HttpServletResponse.SC_OK)
        response.getWriter().println(Source.fromInputStream(request.getInputStream).mkString)
      } else {
        response.setStatus(HttpServletResponse.SC_FORBIDDEN)
      }
      HttpConnection.getCurrentConnection.getRequest.setHandled(true) 
    }
  }    

  class EmptyHandler extends AbstractHandler {
    override def handle(target: String, request: HttpServletRequest, response: HttpServletResponse, dispatch: Int) = {
      response.setContentType("text/html")
      response.setStatus(HttpServletResponse.SC_OK)
      HttpConnection.getCurrentConnection.getRequest.setHandled(true) 
    }
  }    

  var session: SparkSession = _  
  val port = 1080
  val server = new Server(port)
  val inputView = "inputView"
  val outputView = "outputView"
  val uri = s"http://localhost:${port}"
  val badUri = s"http://localhost:${port+1}"
  val echo = "echo"
  val binary = "binary"
  val empty = "empty"
  val body = "testpayload"

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

    // register handlers 
    val postEchoContext = new ContextHandler(s"/${echo}")
    postEchoContext.setAllowNullPathInfo(false)   
    postEchoContext.setHandler(new PostEchoHandler)    
    val emptyContext = new ContextHandler(s"/${empty}")
    emptyContext.setAllowNullPathInfo(false)
    emptyContext.setHandler(new EmptyHandler)    
    val contexts = new ContextHandlerCollection()
    contexts.setHandlers(Array(postEchoContext, emptyContext));
    server.setHandler(contexts)

    // start http server
    server.start    
  }

  after {
    session.stop
    try {
      server.stop
    } catch {
      case e: Exception =>
    }
  }    

  test("HTTPTransform: Can echo post data") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val cols = au.com.agl.arc.util.MetadataSchema.parseJsonMetadata(TestDataUtils.getKnownDatasetMetadataJson)

    val dataset = TestDataUtils.getKnownDataset
    dataset.createOrReplaceTempView(inputView)
    var payloadDataset = spark.sql(s"""
      SELECT *, TO_JSON(NAMED_STRUCT('dateDatum', dateDatum)) AS value FROM ${inputView}
    """)
    val inputDataset = MetadataUtils.setMetadata(payloadDataset, Extract.toStructType(cols.right.getOrElse(Nil)))
    inputDataset.createOrReplaceTempView(inputView)

    val transformDataset = transform.HTTPTransform.transform(
      HTTPTransform(
        name=outputView,
        uri=new URI(s"${uri}/${echo}/"),
        headers=Map.empty,
        validStatusCodes=None,
        inputView=inputView,
        outputView=outputView,
        params=Map.empty,
        persist=false
      )
    ).get

    val expected = transformDataset.select(col("value"))
    val actual = transformDataset.select(col("body")).withColumnRenamed("body", "value")

    assert(TestDataUtils.datasetEquality(expected, actual))

    // test metadata
    val timestampDatumMetadata = transformDataset.schema.fields(transformDataset.schema.fieldIndex("timestampDatum")).metadata    
    assert(timestampDatumMetadata.getLong("securityLevel") == 7)      
  }  

  test("HTTPTransform: Can handle empty response") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val dataset = TestDataUtils.getKnownDataset.toJSON.toDF
    dataset.createOrReplaceTempView(inputView)

    val actual = transform.HTTPTransform.transform(
      HTTPTransform(
        name=outputView,
        uri=new URI(s"${uri}/${empty}/"),
        headers=Map.empty,
        validStatusCodes=None,
        inputView=inputView,
        outputView=outputView,
        params=Map.empty,
        persist=false
      )
    ).get

    val row = actual.first
    assert(row.getString(row.fieldIndex("body")) == "")
  }      

  test("HTTPTransform: Throws exception with 404") {
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val dataset = TestDataUtils.getKnownDataset.toJSON.toDF
    dataset.createOrReplaceTempView(inputView)

    val thrown = intercept[Exception with DetailException] {
      transform.HTTPTransform.transform(
        HTTPTransform(
          name=outputView,
          uri=new URI(s"${uri}/fail/"),
          headers=Map.empty,
          validStatusCodes=None,
          inputView=inputView,
          outputView=outputView,
          params=Map.empty,
          persist=false
        )
      ).get
    }

    assert(thrown.getMessage == "HTTPTransform expects all response StatusCode(s) in [200, 201, 202] but server responded with [2 reponses 404 (Not Found)].")    
  }

  test("HTTPTransform: validStatusCodes") {
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val dataset = TestDataUtils.getKnownDataset.toJSON.toDF
    dataset.createOrReplaceTempView(inputView)

    val thrown = intercept[Exception with DetailException] {
      transform.HTTPTransform.transform(
        HTTPTransform(
          name=outputView,
          uri=new URI(s"${uri}/${empty}/"),
          headers=Map.empty,
          validStatusCodes=Option(List(201)),
          inputView=inputView,
          outputView=outputView,
          params=Map.empty,
          persist=false
        )
      ).get
    }

    assert(thrown.getMessage == "HTTPTransform expects all response StatusCode(s) in [201] but server responded with [2 reponses 200 (OK)].")    
  }     

  test("HTTPTransform: Can echo post data - binary") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val cols = au.com.agl.arc.util.MetadataSchema.parseJsonMetadata(TestDataUtils.getKnownDatasetMetadataJson)

    val dataset = TestDataUtils.getKnownDataset
    dataset.createOrReplaceTempView(inputView)
    var payloadDataset = spark.sql(s"""
      SELECT *, BINARY(stringDatum) AS value FROM ${inputView}
    """)
    val inputDataset = MetadataUtils.setMetadata(payloadDataset, Extract.toStructType(cols.right.getOrElse(Nil)))
    inputDataset.createOrReplaceTempView(inputView)

    val transformDataset = transform.HTTPTransform.transform(
      HTTPTransform(
        name=outputView,
        uri=new URI(s"${uri}/${echo}/"),
        headers=Map.empty,
        validStatusCodes=None,
        inputView=inputView,
        outputView=outputView,
        params=Map.empty,
        persist=false
      )
    ).get

    val expected = transformDataset.select(col("stringDatum")).withColumnRenamed("stringDatum", "value")
    val actual = transformDataset.select(col("body")).withColumnRenamed("body", "value")

    assert(TestDataUtils.datasetEquality(expected, actual))
  }      
}
