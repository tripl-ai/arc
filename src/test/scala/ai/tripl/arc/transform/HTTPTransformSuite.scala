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
import ai.tripl.arc.util._

class HTTPTransformSuite extends FunSuite with BeforeAndAfter {

  class PostEchoHandler extends AbstractHandler {
    override def handle(target: String, request: HttpServletRequest, response: HttpServletResponse, dispatch: Int) = {
      if (HttpConnection.getCurrentConnection.getRequest.getMethod == "POST") {
        requests += 1
        response.setContentType("text/html")
        response.setStatus(HttpServletResponse.SC_OK)
        val sourceBody = Source.fromInputStream(request.getInputStream).mkString
        response.getWriter().print(sourceBody)
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
  val delimiter = "\n"

  var requests = 0

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
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val cols = ai.tripl.arc.util.ArcSchema.parseArcSchema(TestUtils.getKnownDatasetMetadataJson)

    val df = TestUtils.getKnownDataset
    df.createOrReplaceTempView(inputView)
    var payloadDataset = spark.sql(s"""
      SELECT *, TO_JSON(NAMED_STRUCT('dateDatum', dateDatum)) AS value FROM ${inputView}
    """).repartition(1)
    val inputDataset = MetadataUtils.setMetadata(payloadDataset, Extract.toStructType(cols.right.getOrElse(Nil)))
    inputDataset.createOrReplaceTempView(inputView)


    val dataset = transform.HTTPTransformStage.execute(
      transform.HTTPTransformStage(
        plugin=new transform.HTTPTransform,
        name=outputView,
        description=None,
        uri=new URI(s"${uri}/${echo}/"),
        headers=Map.empty,
        validStatusCodes=200 :: 201 :: 202 :: Nil,
        inputView=inputView,
        outputView=outputView,
        params=Map.empty,
        persist=false,
        inputField="value",
        batchSize=2,
        delimiter=delimiter,
        numPartitions=None,
        partitionBy=Nil,
        failMode=FailMode.FailFast
      )
    ).get

    dataset.cache.count

    val expected = dataset.select(col("value"))
    val actual = dataset.select(col("body")).withColumnRenamed("body", "value")

    assert(TestUtils.datasetEquality(expected, actual))

    // test metadata
    val timestampDatumMetadata = dataset.schema.fields(dataset.schema.fieldIndex("timestampDatum")).metadata
    assert(timestampDatumMetadata.getLong("securityLevel") == 7)
  }

  test("HTTPTransform: Can echo post data: inputField") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val cols = ai.tripl.arc.util.ArcSchema.parseArcSchema(TestUtils.getKnownDatasetMetadataJson)

    val df = TestUtils.getKnownDataset
    df.createOrReplaceTempView(inputView)
    var payloadDataset = spark.sql(s"""
      SELECT *, TO_JSON(NAMED_STRUCT('dateDatum', dateDatum)) AS inputField FROM ${inputView}
    """).repartition(1)
    val inputDataset = MetadataUtils.setMetadata(payloadDataset, Extract.toStructType(cols.right.getOrElse(Nil)))
    inputDataset.createOrReplaceTempView(inputView)

    val dataset = transform.HTTPTransformStage.execute(
      transform.HTTPTransformStage(
        plugin=new transform.HTTPTransform,
        name=outputView,
        description=None,
        uri=new URI(s"${uri}/${echo}/"),
        headers=Map.empty,
        validStatusCodes=200 :: 201 :: 202 :: Nil,
        inputView=inputView,
        outputView=outputView,
        params=Map.empty,
        persist=false,
        inputField="inputField",
        batchSize=2,
        delimiter=delimiter,
        numPartitions=None,
        partitionBy=Nil,
        failMode=FailMode.FailFast
      )
    ).get

    val expected = dataset.select(col("inputField")).withColumnRenamed("inputField", "value")
    val actual = dataset.select(col("body")).withColumnRenamed("body", "value")

    assert(TestUtils.datasetEquality(expected, actual))
  }

  test("HTTPTransform: Can echo post data: batchSize 1") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    requests = 0
    val cols = ai.tripl.arc.util.ArcSchema.parseArcSchema(TestUtils.getKnownDatasetMetadataJson)

    val df = TestUtils.getKnownDataset
    df.createOrReplaceTempView(inputView)
    var payloadDataset = spark.sql(s"""
      SELECT *, TO_JSON(NAMED_STRUCT('dateDatum', dateDatum)) AS inputField FROM ${inputView}
    """).repartition(1)
    val inputDataset = MetadataUtils.setMetadata(payloadDataset, Extract.toStructType(cols.right.getOrElse(Nil)))
    inputDataset.createOrReplaceTempView(inputView)

    val dataset = transform.HTTPTransformStage.execute(
      transform.HTTPTransformStage(
        plugin=new transform.HTTPTransform,
        name=outputView,
        description=None,
        uri=new URI(s"${uri}/${echo}/"),
        headers=Map.empty,
        validStatusCodes=200 :: 201 :: 202 :: Nil,
        inputView=inputView,
        outputView=outputView,
        params=Map.empty,
        persist=true,
        inputField="inputField",
        batchSize=1,
        delimiter=delimiter,
        numPartitions=None,
        partitionBy=Nil,
        failMode=FailMode.FailFast
      )
    ).get

    val expected = dataset.select(col("inputField")).withColumnRenamed("inputField", "value")
    val actual = dataset.select(col("body")).withColumnRenamed("body", "value")

    assert(TestUtils.datasetEquality(expected, actual))
    assert(requests == 2)
  }

  test("HTTPTransform: Can echo post data: batchSize >1") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    requests = 0
    val cols = ai.tripl.arc.util.ArcSchema.parseArcSchema(TestUtils.getKnownDatasetMetadataJson)

    val df = TestUtils.getKnownDataset
    df.createOrReplaceTempView(inputView)
    var payloadDataset = spark.sql(s"""
      SELECT *, TO_JSON(NAMED_STRUCT('dateDatum', dateDatum)) AS inputField FROM ${inputView}
    """).repartition(1)
    val inputDataset = MetadataUtils.setMetadata(payloadDataset, Extract.toStructType(cols.right.getOrElse(Nil)))
    inputDataset.createOrReplaceTempView(inputView)

    val dataset = transform.HTTPTransformStage.execute(
      transform.HTTPTransformStage(
        plugin=new transform.HTTPTransform,
        name=outputView,
        description=None,
        uri=new URI(s"${uri}/${echo}/"),
        headers=Map.empty,
        validStatusCodes=200 :: 201 :: 202 :: Nil,
        inputView=inputView,
        outputView=outputView,
        params=Map.empty,
        persist=true,
        inputField="inputField",
        batchSize=5,
        delimiter=delimiter,
        numPartitions=None,
        partitionBy=Nil,
        failMode=FailMode.FailFast
      )
    ).get

    val expected = dataset.select(col("inputField")).withColumnRenamed("inputField", "value")
    val actual = dataset.select(col("body")).withColumnRenamed("body", "value")

    assert(TestUtils.datasetEquality(expected, actual))
    assert(requests == 1)
  }


  test("HTTPTransform: Can handle empty response") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val df = TestUtils.getKnownDataset.toJSON.toDF.repartition(1)
    df.createOrReplaceTempView(inputView)

    val dataset = transform.HTTPTransformStage.execute(
      transform.HTTPTransformStage(
        plugin=new transform.HTTPTransform,
        name=outputView,
        description=None,
        uri=new URI(s"${uri}/${empty}/"),
        headers=Map.empty,
        validStatusCodes=200 :: 201 :: 202 :: Nil,
        inputView=inputView,
        outputView=outputView,
        params=Map.empty,
        persist=false,
        inputField="value",
        batchSize=1,
        delimiter=delimiter,
        numPartitions=None,
        partitionBy=Nil,
        failMode=FailMode.FailFast
      )
    ).get

    val row = dataset.first
    assert(row.getString(row.fieldIndex("body")) == "")
  }

  test("HTTPTransform: Throws exception with 404") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val df = TestUtils.getKnownDataset.toJSON.toDF.repartition(1)
    df.createOrReplaceTempView(inputView)

    val thrown = intercept[Exception with DetailException] {
      transform.HTTPTransformStage.execute(
        transform.HTTPTransformStage(
          plugin=new transform.HTTPTransform,
          name=outputView,
          description=None,
          uri=new URI(s"${uri}/fail/"),
          headers=Map.empty,
          validStatusCodes=200 :: 201 :: 202 :: Nil,
          inputView=inputView,
          outputView=outputView,
          params=Map.empty,
          persist=false,
          inputField="value",
          batchSize=1,
          delimiter=delimiter,
          numPartitions=None,
          partitionBy=Nil,
          failMode=FailMode.FailFast
        )
      ).get.count
    }

    assert(thrown.getMessage.contains("HTTPTransform expects all response StatusCode(s) in [200, 201, 202] but server responded with 404 (Not Found)"))
  }

  test("HTTPTransform: validStatusCodes") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val dataset = TestUtils.getKnownDataset.toJSON.toDF
    dataset.createOrReplaceTempView(inputView)

    val thrown = intercept[Exception with DetailException] {
      transform.HTTPTransformStage.execute(
        transform.HTTPTransformStage(
          plugin=new transform.HTTPTransform,
          name=outputView,
          description=None,
          uri=new URI(s"${uri}/${empty}/"),
          headers=Map.empty,
          validStatusCodes=201 :: Nil,
          inputView=inputView,
          outputView=outputView,
          params=Map.empty,
          persist=false,
          inputField="value",
          batchSize=1,
          delimiter=delimiter,
          numPartitions=None,
          partitionBy=Nil,
          failMode=FailMode.FailFast
        )
      ).get.count
    }

    assert(thrown.getMessage.contains("HTTPTransform expects all response StatusCode(s) in [201] but server responded with 200 (OK)."))
  }

  test("HTTPTransform: Can echo post data - binary") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val cols = ai.tripl.arc.util.ArcSchema.parseArcSchema(TestUtils.getKnownDatasetMetadataJson)

    val df = TestUtils.getKnownDataset
    df.createOrReplaceTempView(inputView)
    var payloadDataset = spark.sql(s"""
      SELECT *, BINARY(stringDatum) AS value FROM ${inputView}
    """).repartition(1)
    val inputDataset = MetadataUtils.setMetadata(payloadDataset, Extract.toStructType(cols.right.getOrElse(Nil)))
    inputDataset.createOrReplaceTempView(inputView)

    val dataset = transform.HTTPTransformStage.execute(
      transform.HTTPTransformStage(
        plugin=new transform.HTTPTransform,
        name=outputView,
        description=None,
        uri=new URI(s"${uri}/${echo}/"),
        headers=Map.empty,
        validStatusCodes=200 :: 201 :: 202 :: Nil,
        inputView=inputView,
        outputView=outputView,
        params=Map.empty,
        persist=false,
        inputField="value",
        batchSize=2,
        delimiter=delimiter,
        numPartitions=None,
        partitionBy=Nil,
        failMode=FailMode.FailFast
      )
    ).get

    dataset.cache.count

    val expected = dataset.select(col("stringDatum")).withColumnRenamed("stringDatum", "value")
    val actual = dataset.select(col("body")).withColumnRenamed("body", "value")

    assert(TestUtils.datasetEquality(expected, actual))
  }

  test("HTTPTransform: Missing 'value' column") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val cols = ai.tripl.arc.util.ArcSchema.parseArcSchema(TestUtils.getKnownDatasetMetadataJson)

    val df = TestUtils.getKnownDataset
    df.createOrReplaceTempView(inputView)
    var payloadDataset = spark.sql(s"""
      SELECT * FROM ${inputView}
    """)
    val inputDataset = MetadataUtils.setMetadata(payloadDataset, Extract.toStructType(cols.right.getOrElse(Nil)))
    inputDataset.createOrReplaceTempView(inputView)

    val thrown0 = intercept[Exception with DetailException] {
      transform.HTTPTransformStage.execute(
        transform.HTTPTransformStage(
          plugin=new transform.HTTPTransform,
          name=outputView,
          description=None,
          uri=new URI(s"${uri}/${echo}/"),
          headers=Map.empty,
          validStatusCodes=200 :: 201 :: 202 :: Nil,
          inputView=inputView,
          outputView=outputView,
          params=Map.empty,
          persist=false,
          inputField="value",
          batchSize=1,
          delimiter=delimiter,
          numPartitions=None,
          partitionBy=Nil,
          failMode=FailMode.FailFast
        )
      ).get
    }
    assert(thrown0.getMessage === "HTTPTransform requires a field named 'value' of type 'string' or 'binary'. inputView has: [booleanDatum, dateDatum, decimalDatum, doubleDatum, integerDatum, longDatum, stringDatum, timeDatum, timestampDatum, nullDatum].")
  }

  test("HTTPTransform: Wrong type of 'value' column") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val cols = ai.tripl.arc.util.ArcSchema.parseArcSchema(TestUtils.getKnownDatasetMetadataJson)

    val df = TestUtils.getKnownDataset
    df.createOrReplaceTempView(inputView)
    var payloadDataset = spark.sql(s"""
      SELECT *, dateDatum AS value FROM ${inputView}
    """)
    val inputDataset = MetadataUtils.setMetadata(payloadDataset, Extract.toStructType(cols.right.getOrElse(Nil)))
    inputDataset.createOrReplaceTempView(inputView)

    val thrown0 = intercept[Exception with DetailException] {
      transform.HTTPTransformStage.execute(
        transform.HTTPTransformStage(
          plugin=new transform.HTTPTransform,
          name=outputView,
          description=None,
          uri=new URI(s"${uri}/${echo}/"),
          headers=Map.empty,
          validStatusCodes=200 :: 201 :: 202 :: Nil,
          inputView=inputView,
          outputView=outputView,
          params=Map.empty,
          persist=false,
          inputField="value",
          batchSize=1,
          delimiter=delimiter,
          numPartitions=None,
          partitionBy=Nil,
          failMode=FailMode.FailFast
        )
      ).get
    }
    assert(thrown0.getMessage === "HTTPTransform requires a field named 'value' of type 'string' or 'binary'. 'value' is of type: 'date'.")
  }

  test("HTTPTransform: Wrong type of 'value' column: inputField") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val cols = ai.tripl.arc.util.ArcSchema.parseArcSchema(TestUtils.getKnownDatasetMetadataJson)

    val df = TestUtils.getKnownDataset
    df.createOrReplaceTempView(inputView)
    var payloadDataset = spark.sql(s"""
      SELECT * FROM ${inputView}
    """)
    val inputDataset = MetadataUtils.setMetadata(payloadDataset, Extract.toStructType(cols.right.getOrElse(Nil)))
    inputDataset.createOrReplaceTempView(inputView)

    val inputField="stringDatumX"

    val thrown0 = intercept[Exception with DetailException] {
      transform.HTTPTransformStage.execute(
        transform.HTTPTransformStage(
          plugin=new transform.HTTPTransform,
          name=outputView,
          description=None,
          uri=new URI(s"${uri}/${echo}/"),
          headers=Map.empty,
          validStatusCodes=200 :: 201 :: 202 :: Nil,
          inputView=inputView,
          outputView=outputView,
          params=Map.empty,
          persist=false,
          inputField=inputField,
          batchSize=1,
          delimiter=delimiter,
          numPartitions=None,
          partitionBy=Nil,
          failMode=FailMode.FailFast
        )
      ).get
    }
    assert(thrown0.getMessage === s"HTTPTransform requires a field named '${inputField}' of type 'string' or 'binary'. inputView has: [booleanDatum, dateDatum, decimalDatum, doubleDatum, integerDatum, longDatum, stringDatum, timeDatum, timestampDatum, nullDatum].")
  }

  test("HTTPTransform: Structured Streaming") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=true)

    val readStream = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", "1")
      .load

    readStream.createOrReplaceTempView("readstream")

    val input = spark.sql(s"""
    SELECT TO_JSON(NAMED_STRUCT('value', value)) AS value
    FROM readstream
    """)

    input.createOrReplaceTempView(inputView)

    val dataset = transform.HTTPTransformStage.execute(
      transform.HTTPTransformStage(
        plugin=new transform.HTTPTransform,
        name=outputView,
        description=None,
        uri=new URI(s"${uri}/${echo}/"),
        headers=Map.empty,
        validStatusCodes=200 :: 201 :: 202 :: Nil,
        inputView=inputView,
        outputView=outputView,
        params=Map.empty,
        persist=false,
        inputField="value",
        batchSize=1,
        delimiter=delimiter,
        numPartitions=None,
        partitionBy=Nil,
        failMode=FailMode.FailFast
      )
    ).get

    val writeStream = dataset
      .writeStream
      .queryName("transformed")
      .format("memory")
      .start

    val df = spark.table("transformed")

    try {
      Thread.sleep(2000)
      assert(df.first.getString(1).contains("""{"value":0}"""))
    } finally {
      writeStream.stop
    }
  }

  test("HTTPTransform: FailMode.Permissive") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val df = TestUtils.getKnownDataset.toJSON.toDF
    df.createOrReplaceTempView(inputView)

    transform.HTTPTransformStage.execute(
      transform.HTTPTransformStage(
        plugin=new transform.HTTPTransform,
        name=outputView,
        description=None,
        uri=new URI(s"${uri}/${empty}/"),
        headers=Map.empty,
        validStatusCodes=201 :: Nil,
        inputView=inputView,
        outputView=outputView,
        params=Map.empty,
        persist=false,
        inputField="value",
        batchSize=1,
        delimiter=delimiter,
        numPartitions=None,
        partitionBy=Nil,
        failMode=FailMode.Permissive
      )
    ).get

    assert(spark.sql(s"""
      SELECT *
      FROM ${outputView}
      WHERE response.statusCode = 200
      AND response.reasonPhrase = 'OK'
      AND response.contentType = 'text/html'
      AND response.responseTime < 100
    """).count == 2L)
  }

}
