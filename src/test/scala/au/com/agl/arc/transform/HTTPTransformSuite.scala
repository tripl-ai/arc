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

  class PostDataHandler extends AbstractHandler {
    val payload = """[{"booleanDatum":true,"dateDatum":"2016-12-18","decimalDatum":54.321000000000000000,"doubleDatum":42.4242,"integerDatum":17,"longDatum":1520828868,"stringDatum":"test,breakdelimiter","timeDatum":"12:34:56","timestampDatum":"2017-12-20T21:46:54.000Z"},
      |{"booleanDatum":false,"dateDatum":"2016-12-19","decimalDatum":12.345000000000000000,"doubleDatum":21.2121,"integerDatum":34,"longDatum":1520828123,"stringDatum":"breakdelimiter,test","timeDatum":"23:45:16","timestampDatum":"2017-12-29T17:21:49.000Z"}]""".stripMargin

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
  val post = "post"
  val echo = "echo"
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
    val postContext = new ContextHandler(s"/${post}")
    postContext.setAllowNullPathInfo(false) 
    postContext.setHandler(new PostDataHandler)    
    val postEchoContext = new ContextHandler(s"/${echo}")
    postEchoContext.setAllowNullPathInfo(false)   
    postEchoContext.setHandler(new PostEchoHandler)   
    val emptyContext = new ContextHandler(s"/${empty}")
    emptyContext.setAllowNullPathInfo(false)
    emptyContext.setHandler(new EmptyHandler)    
    val contexts = new ContextHandlerCollection()
    contexts.setHandlers(Array(postContext, postEchoContext, emptyContext));
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

    val dataset = TestDataUtils.getKnownDataset.toJSON.toDF
    dataset.createOrReplaceTempView(inputView)

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
}
