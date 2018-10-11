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

class HTTPExtractSuite extends FunSuite with BeforeAndAfter {

  class GetDataHandler extends AbstractHandler {
    val payload = """[{"booleanDatum":true,"dateDatum":"2016-12-18","decimalDatum":54.321000000000000000,"doubleDatum":42.4242,"integerDatum":17,"longDatum":1520828868,"stringDatum":"test,breakdelimiter","timeDatum":"12:34:56","timestampDatum":"2017-12-20T21:46:54.000Z"},
      |{"booleanDatum":false,"dateDatum":"2016-12-19","decimalDatum":12.345000000000000000,"doubleDatum":21.2121,"integerDatum":34,"longDatum":1520828123,"stringDatum":"breakdelimiter,test","timeDatum":"23:45:16","timestampDatum":"2017-12-29T17:21:49.000Z"}]""".stripMargin

    override def handle(target: String, request: HttpServletRequest, response: HttpServletResponse, dispatch: Int) = {
      if (HttpConnection.getCurrentConnection.getRequest.getMethod == "GET") {
        response.setContentType("text/html")
        response.setStatus(HttpServletResponse.SC_OK)
        response.getWriter().println(payload)
      } else {
        response.setStatus(HttpServletResponse.SC_FORBIDDEN)
      }
      HttpConnection.getCurrentConnection.getRequest.setHandled(true) 
    }
  } 

  class PostDataHandler extends AbstractHandler {
    val payload = """[{"booleanDatum":true,"dateDatum":"2016-12-18","decimalDatum":54.321000000000000000,"doubleDatum":42.4242,"integerDatum":17,"longDatum":1520828868,"stringDatum":"test,breakdelimiter","timeDatum":"12:34:56","timestampDatum":"2017-12-20T21:46:54.000Z"},
      |{"booleanDatum":false,"dateDatum":"2016-12-19","decimalDatum":12.345000000000000000,"doubleDatum":21.2121,"integerDatum":34,"longDatum":1520828123,"stringDatum":"breakdelimiter,test","timeDatum":"23:45:16","timestampDatum":"2017-12-29T17:21:49.000Z"}]""".stripMargin

    override def handle(target: String, request: HttpServletRequest, response: HttpServletResponse, dispatch: Int) = {
      if (HttpConnection.getCurrentConnection.getRequest.getMethod == "POST") {
        response.setContentType("text/html")
        response.setStatus(HttpServletResponse.SC_OK)
        response.getWriter().println(payload)
      } else {
        response.setStatus(HttpServletResponse.SC_FORBIDDEN)
      }
      HttpConnection.getCurrentConnection.getRequest.setHandled(true) 
    }
  }   

  class PostPayloadHandler extends AbstractHandler {
    override def handle(target: String, request: HttpServletRequest, response: HttpServletResponse, dispatch: Int) = {
      if (HttpConnection.getCurrentConnection.getRequest.getMethod == "POST" ) {
        if (Source.fromInputStream(request.getInputStream).mkString == body) {
          response.setStatus(HttpServletResponse.SC_OK)
        } else {
          response.setStatus(HttpServletResponse.SC_FORBIDDEN)
        }
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
  val get = "get"
  val post = "post"
  val payload = "payload"
  val empty = "empty"
  val body = "testpayload"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("FATAL")

    session = spark
    import spark.implicits._

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")    

    // register handlers
    val getContext = new ContextHandler(s"/${get}")
    getContext.setAllowNullPathInfo(false)
    getContext.setHandler(new GetDataHandler)
    val postContext = new ContextHandler(s"/${post}")
    postContext.setAllowNullPathInfo(false) 
    postContext.setHandler(new PostDataHandler)    
    val postPayloadContext = new ContextHandler(s"/${payload}")
    postPayloadContext.setAllowNullPathInfo(false)   
    postPayloadContext.setHandler(new PostPayloadHandler)   
    val emptyContext = new ContextHandler(s"/${empty}")
    emptyContext.setAllowNullPathInfo(false)
    emptyContext.setHandler(new EmptyHandler)
    val contexts = new ContextHandlerCollection()
    contexts.setHandlers(Array(getContext, postContext, postPayloadContext, emptyContext));
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

  test("HTTPExtract: Can read data (GET)") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val extractDataset = extract.HTTPExtract.extract(
      HTTPExtract(
        name=outputView,
        input=Right(new URI(s"${uri}/${get}/")),
        headers=Map.empty,
        validStatusCodes=None,
        outputView=outputView,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        method=Option("GET"),
        body=None
      )
    ).get

    // assert HTTP_OK
    assert(extractDataset.first.getInt(1) == 200)

    val actual = spark.read.option("multiLine", "true").json(extractDataset.select(col("body")).as[String])

    // JSON does not have DecimalType or TimestampType
    // JSON will silently drop NullType on write
    val expected = TestDataUtils.getKnownDataset
      .withColumn("decimalDatum", $"decimalDatum".cast("double"))
      .withColumn("timestampDatum", from_unixtime(unix_timestamp($"timestampDatum"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
      .drop($"nullDatum")

    assert(TestDataUtils.datasetEquality(expected, actual))
  }    

  test("HTTPExtract: Can read data (POST)") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val extractDataset = extract.HTTPExtract.extract(
      HTTPExtract(
        name=outputView,
        input=Right(new URI(s"${uri}/${post}/")),
        headers=Map.empty,
        validStatusCodes=None,
        outputView=outputView,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        method=Option("POST"),
        body=None
      )
    ).get

    // assert HTTP_OK
    assert(extractDataset.first.getInt(1) == 200)    

    val actual = spark.read.option("multiLine", "true").json(extractDataset.select(col("body")).as[String])

    // JSON does not have DecimalType or TimestampType
    // JSON will silently drop NullType on write
    val expected = TestDataUtils.getKnownDataset
      .withColumn("decimalDatum", $"decimalDatum".cast("double"))
      .withColumn("timestampDatum", from_unixtime(unix_timestamp($"timestampDatum"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
      .drop($"nullDatum")

    assert(TestDataUtils.datasetEquality(expected, actual))
  }  

  test("HTTPExtract: Can post data (POST)") {
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val extractDataset = extract.HTTPExtract.extract(
      HTTPExtract(
        name=outputView,
        input=Right(new URI(s"${uri}/${payload}/")),
        headers=Map.empty,
        validStatusCodes=Option(List(200)),
        outputView=outputView,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        method=Option("POST"),
        body=Option(body)
      )
    ).get

    // assert HTTP_OK
    assert(extractDataset.first.getInt(1) == 200)
  }  

  test("HTTPExtract: Can handle empty response") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val actual = extract.HTTPExtract.extract(
      HTTPExtract(
        name=outputView,
        input=Right(new URI(s"${uri}/${empty}/")),
        headers=Map.empty,
        validStatusCodes=None,
        outputView=outputView,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        method=None,
        body=None   
      )
    ).get

    // assert HTTP_OK
    assert(actual.first.getInt(1) == 200)    

    // ensure body is null
    assert(actual.first.isNullAt(4))
  }      

  test("HTTPExtract: Throws exception with 404") {
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val thrown = intercept[Exception with DetailException] {
      extract.HTTPExtract.extract(
        HTTPExtract(
          name=outputView,
          input=Right(new URI(s"${uri}/fail/")),
          validStatusCodes=None,
          headers=Map.empty,
          outputView=outputView,
          params=Map.empty,
          persist=false,
          numPartitions=None,
          partitionBy=Nil,
          method=None,
          body=None   
        )
      )
    }

    assert(thrown.getMessage == "HTTPExtract expects all response StatusCode(s) in [200, 201, 202] but server responded with [1 reponses 404 (Not Found)].")    
  }

  test("HTTPExtract: validStatusCodes") {
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val thrown = intercept[Exception with DetailException] {
      extract.HTTPExtract.extract(
        HTTPExtract(
          name=outputView,
          input=Right(new URI(s"${uri}/${empty}/")),
          headers=Map.empty,
          validStatusCodes=Option(List(201)),
          outputView=outputView,
          params=Map.empty,
          persist=false,
          numPartitions=None,
          partitionBy=Nil,
          method=None,
          body=None   
        )
      )
    }

    assert(thrown.getMessage == "HTTPExtract expects all response StatusCode(s) in [201] but server responded with [1 reponses 200 (OK)].")    
  }  

  test("HTTPExtract: broken url throws exception") {
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val thrown = intercept[Exception with DetailException] {
      extract.HTTPExtract.extract(
        HTTPExtract(
          name=outputView,
          input=Right(new URI(badUri)),
          headers=Map.empty,
          validStatusCodes=None,
          outputView=outputView,
          params=Map.empty,
          persist=false,
          numPartitions=None,
          partitionBy=Nil,
          method=None,
          body=None   
        )
      )
    }
    assert(thrown.getMessage.contains("Connection refused"))      
  }

  test("HTTPExtract: Can read data (GET) with DataFrame input") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val inputDF = Seq(s"${uri}/${get}/", s"${uri}/${get}/").toDF("value")
    inputDF.createOrReplaceTempView(inputView)

    val extractDataset = extract.HTTPExtract.extract(
      HTTPExtract(
        name=outputView,
        input=Left(inputView),
        headers=Map.empty,
        validStatusCodes=None,
        outputView=outputView,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        method=Option("GET"),
        body=None
      )
    ).get

    val actual = extractDataset.collect

    // assert HTTP_OK
    assert(actual(0).getInt(1) == 200)
    assert(actual(1).getInt(1) == 200)
  }    
       
}
