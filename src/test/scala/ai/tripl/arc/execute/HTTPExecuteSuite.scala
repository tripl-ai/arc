package ai.tripl.arc

import java.net.URI
import java.sql.DriverManager

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.mortbay.jetty.handler.{AbstractHandler, ContextHandler, ContextHandlerCollection}
import org.mortbay.jetty.{Server, Request, HttpConnection}
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.io.Source

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.util.log.LoggerFactory 

import ai.tripl.arc.util._

class HTTPExecuteSuite extends FunSuite with BeforeAndAfter {

    class HeadersHandler extends AbstractHandler {
    override def handle(target: String, request: HttpServletRequest, response: HttpServletResponse, dispatch: Int) = {
        if (request.getHeader("custom") == "success") {
            response.setStatus(HttpServletResponse.SC_OK)
        } else {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED)
        }
        HttpConnection.getCurrentConnection().getRequest().setHandled(true) 
    }
  } 

    class PayloadsHandler extends AbstractHandler {
    override def handle(target: String, request: HttpServletRequest, response: HttpServletResponse, dispatch: Int) = {
        
        val body = Source.fromInputStream(request.getInputStream).mkString

        if (body == """{"custom":"success"}""") {
            response.setStatus(HttpServletResponse.SC_OK)
        } else {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED)
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

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")      

    session = spark
    import spark.implicits._  

    // register handlers
    val headersHandler = new ContextHandler("/headers");
    headersHandler.setHandler(new HeadersHandler)
    val payloadsHandler = new ContextHandler("/payloads");
    payloadsHandler.setHandler(new PayloadsHandler)    
    val contexts = new ContextHandlerCollection()
    contexts.setHandlers(Array(headersHandler, payloadsHandler));
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

  test("HTTPExecute: Set Header") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    ai.tripl.arc.execute.HTTPExecuteStage.execute(
      ai.tripl.arc.execute.HTTPExecuteStage(
        plugin=new ai.tripl.arc.execute.HTTPExecute,
        name=outputView,
        description=None,
        uri=new URI(s"${uri}/headers/"), // ensure trailing slash to avoid 302 redirect
        headers=Map(key -> value),
        payloads=Map.empty,
        validStatusCodes=200 :: 201 :: 202 :: Nil,
        params=Map.empty
      )
    )
  }  

  test("HTTPExecute: Do Not Set Header") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val thrown = intercept[Exception] {
      ai.tripl.arc.execute.HTTPExecuteStage.execute(
        ai.tripl.arc.execute.HTTPExecuteStage(
          plugin=new ai.tripl.arc.execute.HTTPExecute,
          name=outputView,
          description=None,
          uri=new URI(s"${uri}/headers/"), // ensure trailing slash to avoid 302 redirect
          headers=Map.empty,
          payloads=Map.empty,
          validStatusCodes=200 :: 201 :: 202 :: Nil,
          params=Map.empty
        )
      )
    }
    assert(thrown.getMessage == "HTTPExecute expects a response StatusCode in [200, 201, 202] but server responded with 401 (Unauthorized).")        
  }    

  test("HTTPExecute: Set Payload") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    ai.tripl.arc.execute.HTTPExecuteStage.execute(
      ai.tripl.arc.execute.HTTPExecuteStage(
        plugin=new ai.tripl.arc.execute.HTTPExecute,
        name=outputView,
        description=None,
        uri=new URI(s"${uri}/payloads/"), // ensure trailing slash to avoid 302 redirect
        headers=Map.empty,
        payloads=Map(key -> value),
        validStatusCodes=200 :: 201 :: 202 :: Nil,
        params=Map.empty
      )
    )
  }

  test("HTTPExecute: Do Not Set Payload") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val thrown = intercept[Exception] {
      ai.tripl.arc.execute.HTTPExecuteStage.execute(
        ai.tripl.arc.execute.HTTPExecuteStage(
          plugin=new ai.tripl.arc.execute.HTTPExecute,
          name=outputView,
          description=None,
          uri=new URI(s"${uri}/payloads/"), // ensure trailing slash to avoid 302 redirect
          headers=Map.empty,
          payloads=Map.empty,
          validStatusCodes=200 :: 201 :: 202 :: Nil,
          params=Map.empty
        )
      )
    }
    assert(thrown.getMessage == "HTTPExecute expects a response StatusCode in [200, 201, 202] but server responded with 401 (Unauthorized).")            
  }

  test("HTTPExecute: no validStatusCodes") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val thrown = intercept[Exception] {
      ai.tripl.arc.execute.HTTPExecuteStage.execute(
        ai.tripl.arc.execute.HTTPExecuteStage(
          plugin=new ai.tripl.arc.execute.HTTPExecute,
          name=outputView,
          description=None,
          uri=new URI(s"${uri}/payloads/"), // ensure trailing slash to avoid 302 redirect
          headers=Map.empty,
          payloads=Map.empty,
          validStatusCodes=200 :: 201 :: 202 :: Nil,
          params=Map.empty
        )
      )
    }
    assert(thrown.getMessage == "HTTPExecute expects a response StatusCode in [200, 201, 202] but server responded with 401 (Unauthorized).")            
  }      

  test("HTTPExecute: override validStatusCodes") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    ai.tripl.arc.execute.HTTPExecuteStage.execute(
      ai.tripl.arc.execute.HTTPExecuteStage(
        plugin=new ai.tripl.arc.execute.HTTPExecute,
        name=outputView,
        description=None,
        uri=new URI(s"${uri}/payloads/"), // ensure trailing slash to avoid 302 redirect
        headers=Map.empty,
        payloads=Map.empty,
        validStatusCodes=401 :: Nil,
        params=Map.empty
      )
    )
  }    
}
