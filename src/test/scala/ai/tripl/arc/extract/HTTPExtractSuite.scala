package ai.tripl.arc

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

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.util._

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
        response.getWriter().print(payload)
      } else {
        response.setStatus(HttpServletResponse.SC_FORBIDDEN)
      }
      HttpConnection.getCurrentConnection.getRequest.setHandled(true)
    }
  }

  class PostPayloadHandler extends AbstractHandler {
    override def handle(target: String, request: HttpServletRequest, response: HttpServletResponse, dispatch: Int) = {
      if (HttpConnection.getCurrentConnection.getRequest.getMethod == "POST" ) {
        if (Source.fromInputStream(request.getInputStream).mkString == body0) {
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

  class PostPayloadEchoHandler extends AbstractHandler {
    override def handle(target: String, request: HttpServletRequest, response: HttpServletResponse, dispatch: Int) = {
      if (HttpConnection.getCurrentConnection.getRequest.getMethod == "POST" ) {
        response.setContentType("text/html")
        response.setStatus(HttpServletResponse.SC_OK)
        response.getWriter().print(Source.fromInputStream(request.getInputStream).mkString)
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
  val echo = "echo"
  val empty = "empty"
  val body0 = "testpayload0"
  val body1 = "testpayload1"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark
    import spark.implicits._

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
    val postPayloadEchoContext = new ContextHandler(s"/${echo}")
    postPayloadEchoContext.setAllowNullPathInfo(false)
    postPayloadEchoContext.setHandler(new PostPayloadEchoHandler)
    val emptyContext = new ContextHandler(s"/${empty}")
    emptyContext.setAllowNullPathInfo(false)
    emptyContext.setHandler(new EmptyHandler)
    val contexts = new ContextHandlerCollection()
    contexts.setHandlers(Array(getContext, postContext, postPayloadContext, postPayloadEchoContext, emptyContext));
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

  test("HTTPExtract: end-to-end") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val conf = s"""{
      "stages": [
        {
          "type": "HTTPExtract",
          "name": "load data",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "${uri}/${get}/",
          "outputView": "data"
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        val df = ARC.run(pipeline)(spark, logger, arcContext)
        df match {
          case Some(df) => assert(df.count != 0)
          case None => assert(false)
        }
      }
    }
  }

  test("HTTPExtract: Can read data (GET)") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val dataset = extract.HTTPExtractStage.execute(
      extract.HTTPExtractStage(
        plugin=new extract.HTTPExtract,
        name=outputView,
        description=None,
        input=Right(new URI(s"${uri}/${get}/")),
        uriField=None,
        bodyField=None,
        headers=Map.empty,
        validStatusCodes=200 :: 201 :: 202 :: Nil,
        outputView=outputView,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        method="GET",
        body=None
      )
    ).get

    // assert HTTP_OK
    assert(dataset.first.getInt(1) == 200)

    val actual = spark.read.option("multiLine", "true").json(dataset.select(col("body")).as[String])

    // JSON does not have DecimalType or TimestampType
    // JSON will silently drop NullType on write
    val expected = TestUtils.getKnownDataset
      .withColumn("decimalDatum", $"decimalDatum".cast("double"))
      .withColumn("timestampDatum", from_unixtime(unix_timestamp($"timestampDatum"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
      .drop($"nullDatum")

    assert(TestUtils.datasetEquality(expected, actual))
  }

  test("HTTPExtract: Can read data (POST)") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val dataset = extract.HTTPExtractStage.execute(
      extract.HTTPExtractStage(
        plugin=new extract.HTTPExtract,
        name=outputView,
        description=None,
        input=Right(new URI(s"${uri}/${post}/")),
        uriField=None,
        bodyField=None,
        headers=Map.empty,
        validStatusCodes=200 :: 201 :: 202 :: Nil,
        outputView=outputView,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        method="POST",
        body=None
      )
    ).get

    // assert HTTP_OK
    assert(dataset.first.getInt(1) == 200)

    val actual = spark.read.option("multiLine", "true").json(dataset.select(col("body")).as[String])

    // JSON does not have DecimalType or TimestampType
    // JSON will silently drop NullType on write
    val expected = TestUtils.getKnownDataset
      .withColumn("decimalDatum", $"decimalDatum".cast("double"))
      .withColumn("timestampDatum", from_unixtime(unix_timestamp($"timestampDatum"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
      .drop($"nullDatum")

    assert(TestUtils.datasetEquality(expected, actual))
  }

  test("HTTPExtract: Can post data (POST)") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val dataset = extract.HTTPExtractStage.execute(
      extract.HTTPExtractStage(
        plugin=new extract.HTTPExtract,
        name=outputView,
        description=None,
        input=Right(new URI(s"${uri}/${payload}/")),
        uriField=None,
        bodyField=None,
        headers=Map.empty,
        validStatusCodes=200 :: Nil,
        outputView=outputView,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        method="POST",
        body=Option(body0)
      )
    ).get

    // assert HTTP_OK
    assert(dataset.first.getInt(1) == 200)
  }

  test("HTTPExtract: Can handle empty response") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val dataset = extract.HTTPExtractStage.execute(
      extract.HTTPExtractStage(
        plugin=new extract.HTTPExtract,
        name=outputView,
        description=None,
        input=Right(new URI(s"${uri}/${empty}/")),
        uriField=None,
        bodyField=None,
        headers=Map.empty,
        validStatusCodes=200 :: 201 :: 202 :: Nil,
        outputView=outputView,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        method="GET",
        body=None
      )
    ).get

    // assert HTTP_OK
    assert(dataset.first.getInt(1) == 200)

    // ensure body is null
    assert(dataset.first.isNullAt(5))
  }

  test("HTTPExtract: Throws exception with 404") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val thrown = intercept[Exception with DetailException] {
      extract.HTTPExtractStage.execute(
        extract.HTTPExtractStage(
          plugin=new extract.HTTPExtract,
          name=outputView,
          description=None,
          input=Right(new URI(s"${uri}/fail/")),
          uriField=None,
          bodyField=None,
          headers=Map.empty,
          validStatusCodes=200 :: 201 :: 202 :: Nil,
          outputView=outputView,
          params=Map.empty,
          persist=false,
          numPartitions=None,
          partitionBy=Nil,
          method="GET",
          body=None
        )
      ).get.count
    }

    assert(thrown.getMessage.contains("HTTPExtract expects all response StatusCode(s) in [200, 201, 202] but server responded with 404 (Not Found)."))
  }

  test("HTTPExtract: validStatusCodes") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val thrown = intercept[Exception with DetailException] {
      extract.HTTPExtractStage.execute(
        extract.HTTPExtractStage(
          plugin=new extract.HTTPExtract,
          name=outputView,
          description=None,
          input=Right(new URI(s"${uri}/${empty}/")),
          uriField=None,
          bodyField=None,
          headers=Map.empty,
          validStatusCodes=201 :: Nil,
          outputView=outputView,
          params=Map.empty,
          persist=false,
          numPartitions=None,
          partitionBy=Nil,
          method="GET",
          body=None
        )
      ).get.count
    }

    assert(thrown.getMessage.contains("HTTPExtract expects all response StatusCode(s) in [201] but server responded with 200 (OK)."))
  }

  test("HTTPExtract: broken url throws exception") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val thrown = intercept[Exception with DetailException] {
      extract.HTTPExtractStage.execute(
        extract.HTTPExtractStage(
          plugin=new extract.HTTPExtract,
          name=outputView,
          description=None,
          input=Right(new URI(badUri)),
          uriField=None,
          bodyField=None,
          headers=Map.empty,
          validStatusCodes=200 :: 201 :: 202 :: Nil,
          outputView=outputView,
          params=Map.empty,
          persist=false,
          numPartitions=None,
          partitionBy=Nil,
          method="GET",
          body=None
        )
      ).get.count
    }
    assert(thrown.getMessage.contains("Connection refused"))
  }

  test("HTTPExtract: Can read data (GET) with DataFrame input") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val inputDF = Seq(s"${uri}/${get}/", s"${uri}/${get}/").toDF("value")
    inputDF.createOrReplaceTempView(inputView)

    val dataset = extract.HTTPExtractStage.execute(
      extract.HTTPExtractStage(
        plugin=new extract.HTTPExtract,
        name=outputView,
        description=None,
        input=Left(inputView),
        uriField=None,
        bodyField=None,
        headers=Map.empty,
        validStatusCodes=200 :: 201 :: 202 :: Nil,
        outputView=outputView,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        method="GET",
        body=None
      )
    ).get

    val actual = dataset.collect

    // assert HTTP_OK
    assert(actual(0).getInt(1) == 200)
    assert(actual(1).getInt(1) == 200)
  }

  test("HTTPExtract: Can post data (POST) with Body from DataFrame by name") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val inputDF = Seq(("blah", s"${uri}/${echo}/", body0), ("blah", s"${uri}/${echo}/", body1)).toDF("ignore", "uri", "body")
    inputDF.createOrReplaceTempView(inputView)

    val dataset = extract.HTTPExtractStage.execute(
      extract.HTTPExtractStage(
        plugin=new extract.HTTPExtract,
        name=outputView,
        description=None,
        input=Left(inputView),
        uriField=Some("uri"),
        bodyField=Some("body"),
        headers=Map.empty,
        validStatusCodes=200 :: 201 :: 202 :: Nil,
        outputView=outputView,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        method="POST",
        body=None
      )
    ).get

    val actual = dataset.collect

    // assert body has been echoed
    assert(actual(0).getString(5) == body0)
    assert(actual(1).getString(5) == body1)
  }

  test("HTTPExtract: Can post data (POST) with Body from DataFrame by name precedence") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val inputDF = Seq(("blah", s"${uri}/${echo}/", body0), ("blah", s"${uri}/${echo}/", body1)).toDF("ignore", "uri", "body")
    inputDF.createOrReplaceTempView(inputView)

    val dataset = extract.HTTPExtractStage.execute(
      extract.HTTPExtractStage(
        plugin=new extract.HTTPExtract,
        name=outputView,
        description=None,
        input=Left(inputView),
        uriField=Some("uri"),
        bodyField=Some("body"),
        headers=Map.empty,
        validStatusCodes=200 :: 201 :: 202 :: Nil,
        outputView=outputView,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        method="POST",
        body=Some("notposted")
      )
    ).get

    val actual = dataset.collect

    // assert body has been echoed
    assert(actual(0).getString(5) == body0)
    assert(actual(1).getString(5) == body1)
  }
}
