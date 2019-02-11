package au.com.agl.arc

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.mortbay.jetty.handler.{AbstractHandler, ContextHandler, ContextHandlerCollection}
import org.mortbay.jetty.{Server, Request, HttpConnection}
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.io.Source

import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.util.log.LoggerFactory 

import au.com.agl.arc.util.TestDataUtils

class HTTPLoadSuite extends FunSuite with BeforeAndAfter {

  class FailureHandler extends AbstractHandler {
    override def handle(target: String, request: HttpServletRequest, response: HttpServletResponse, dispatch: Int) = {
      if (HttpConnection.getCurrentConnection.getRequest.getMethod == "POST") {
        if (Source.fromInputStream(request.getInputStream).mkString.contains("1520828868")) {
          response.setStatus(HttpServletResponse.SC_UNAUTHORIZED)
        } else {
          response.setStatus(HttpServletResponse.SC_OK)
        }
      } else {
        response.setStatus(HttpServletResponse.SC_FORBIDDEN)
      }
      HttpConnection.getCurrentConnection().getRequest().setHandled(true) 
    }
  } 

  class SuccessHandler extends AbstractHandler {
    override def handle(target: String, request: HttpServletRequest, response: HttpServletResponse, dispatch: Int) = {
      if (HttpConnection.getCurrentConnection.getRequest.getMethod == "POST") {
        response.setStatus(HttpServletResponse.SC_OK)
      } else {
        response.setStatus(HttpServletResponse.SC_FORBIDDEN)
      }      
      HttpConnection.getCurrentConnection().getRequest().setHandled(true) 
    }
  } 

  class HeadersHandler extends AbstractHandler {
    override def handle(target: String, request: HttpServletRequest, response: HttpServletResponse, dispatch: Int) = {
      if (HttpConnection.getCurrentConnection.getRequest.getMethod == "POST") {
        if (request.getHeader("custom") == "success") {
            response.setStatus(HttpServletResponse.SC_OK)
        } else {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED)
        }
      } else {
        response.setStatus(HttpServletResponse.SC_FORBIDDEN)
      }        
      HttpConnection.getCurrentConnection().getRequest().setHandled(true) 
    }
  } 

  var session: SparkSession = _  
  val port = 1080
  val server = new Server(port)
  val outputView = "dataset"
  val uri = s"http://localhost:${port}"
  val key = "custom"
  val value = "success"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    session = spark

    // register handlers
    val successHandler = new ContextHandler("/success");
    successHandler.setHandler(new SuccessHandler)    
    val failureHandler = new ContextHandler("/failure");
    failureHandler.setHandler(new FailureHandler)
    val headersHandler = new ContextHandler("/headers");
    headersHandler.setHandler(new HeadersHandler)    
    val contexts = new ContextHandlerCollection()
    contexts.setHandlers(Array(successHandler, failureHandler, headersHandler));
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

  test("HTTPLoad: success") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val dataset = TestDataUtils.getKnownDataset.toJSON.toDF
    dataset.createOrReplaceTempView(outputView)

    load.HTTPLoad.load(
      HTTPLoad(
        name=outputView, 
        description=None,
        inputView=outputView, 
        outputURI=new URI(s"${uri}/success/"), // ensure trailing slash to avoid 302 redirect
        headers=Map.empty,
        validStatusCodes=200 :: 201 :: 202 :: Nil,
        params=Map.empty
      )
    )
  }

  test("HTTPLoad: failure") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val dataset = TestDataUtils.getKnownDataset.toJSON.toDF
    dataset.createOrReplaceTempView(outputView)

    val thrown = intercept[Exception] {
      load.HTTPLoad.load(
        HTTPLoad(
          name=outputView, 
          description=None,
          inputView=outputView, 
          outputURI=new URI(s"${uri}/failure/"), // ensure trailing slash to avoid 302 redirect
          headers=Map.empty,
          validStatusCodes=200 :: 201 :: 202 :: Nil,
          params=Map.empty
        )
      ).get.count
    }
    assert(thrown.getMessage.contains("HTTPLoad expects all response StatusCode(s) in [200, 201, 202] but server responded with 401 (Unauthorized)."))
  }

  test("HTTPLoad: validStatusCodes") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val dataset = TestDataUtils.getKnownDataset.toJSON.toDF
    dataset.createOrReplaceTempView(outputView)

    load.HTTPLoad.load(
      HTTPLoad(
        name=outputView, 
        description=None,
        inputView=outputView, 
        outputURI=new URI(s"${uri}/failure/"), // ensure trailing slash to avoid 302 redirect
        headers=Map(key -> value),
        validStatusCodes=200 :: 401 :: Nil,
        params=Map.empty
      )
    )
  }  

  test("HTTPLoad: headers positive") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val dataset = TestDataUtils.getKnownDataset.toJSON.toDF
    dataset.createOrReplaceTempView(outputView)

    load.HTTPLoad.load(
      HTTPLoad(
        name=outputView, 
        description=None,
        inputView=outputView, 
        outputURI=new URI(s"${uri}/headers/"), // ensure trailing slash to avoid 302 redirect
        headers=Map(key -> value),
        validStatusCodes=200 :: Nil,
        params=Map.empty
      )
    )
  } 

  test("HTTPLoad: headers negative") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val dataset = TestDataUtils.getKnownDataset.toJSON.toDF
    dataset.createOrReplaceTempView(outputView)

    val thrown = intercept[Exception] {
      load.HTTPLoad.load(
        HTTPLoad(
          name=outputView, 
          description=None,
          inputView=outputView, 
          outputURI=new URI(s"${uri}/headers/"), // ensure trailing slash to avoid 302 redirect
          headers=Map(key -> "wrong"),
          validStatusCodes=200 :: Nil,
          params=Map.empty
        )
      ).get.count
    }
    assert(thrown.getMessage.contains("HTTPLoad expects all response StatusCode(s) in [200] but server responded with 401 (Unauthorized)."))      
  } 

  test("HTTPLoad: invalid inputView") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val dataset = TestDataUtils.getKnownDataset
    dataset.createOrReplaceTempView(outputView)

    val thrown = intercept[Exception] {
      load.HTTPLoad.load(
        HTTPLoad(
          name=outputView, 
          description=None,
          inputView=outputView, 
          outputURI=new URI(s"${uri}/success/"), // ensure trailing slash to avoid 302 redirect
          headers=Map.empty,
          validStatusCodes=200 :: 201 :: 202 :: Nil,
          params=Map.empty
        )
      )
    }
    assert(thrown.getMessage == "HTTPLoad requires inputView to be dataset with [value: string] signature. inputView 'dataset' has 10 columns of type [boolean, date, decimal(38,18), double, int, bigint, string, string, timestamp, null].")            
  }   
}


