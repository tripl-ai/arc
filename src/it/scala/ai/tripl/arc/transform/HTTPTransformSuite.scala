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
import ai.tripl.arc.udf.UDF

import ai.tripl.arc.util._

class HTTPTransformSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  val inputView = "inputView"
  val outputView = "outputView"
  val uri = s"http://tensorflow_serving:9001/v1/models/simple/versions/1:predict"
  var logger: ai.tripl.arc.util.log.logger.Logger = _
  implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    implicit val logger = TestUtils.getLogger()

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark
    import spark.implicits._

    // register udf
    UDF.registerUDFs()(spark, logger, arcContext)
  }

  after {
    session.stop
  }

  test("HTTPTransform: Can call TensorflowServing via REST" ) {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val df = spark.range(1, 10).toDF
    df.createOrReplaceTempView(inputView)

    var payloadDataset = spark.sql(s"""
    SELECT
      id
      ,TO_JSON(NAMED_STRUCT('instances', ARRAY(id))) AS value
    FROM ${inputView}
    """)
    payloadDataset.createOrReplaceTempView(inputView)

    transform.HTTPTransformStage.execute(
      transform.HTTPTransformStage(
        plugin=new transform.HTTPTransform,
        description=None,
        name=outputView,
        uri=new URI(uri),
        headers=Map.empty,
        validStatusCodes=200 :: 201 :: 202 :: Nil,
        inputView=inputView,
        outputView=outputView,
        params=Map.empty,
        persist=false,
        inputField="value",
        batchSize=1,
        delimiter="",
        numPartitions=None,
        partitionBy=Nil,
        failMode=FailMode.FailFast
      )
    ).get


    val output = spark.sql(s"""
    SELECT get_json_integer_array(body, '$$.predictions') FROM ${outputView}
    """)

    assert(output.first.getAs[scala.collection.mutable.WrappedArray[Integer]](0)(0) == 11)
    assert(output.schema.fields(0).dataType.toString == "ArrayType(IntegerType,false)")
  }

  test("HTTPTransform: Can call TensorflowServing via REST: inputField" ) {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val df = spark.range(1, 10).toDF
    df.createOrReplaceTempView(inputView)

    var payloadDataset = spark.sql(s"""
    SELECT
      id
      ,TO_JSON(NAMED_STRUCT('instances', ARRAY(id))) AS input
    FROM ${inputView}
    """)
    payloadDataset.createOrReplaceTempView(inputView)

    transform.HTTPTransformStage.execute(
      transform.HTTPTransformStage(
        plugin=new transform.HTTPTransform,
        description=None,
        name=outputView,
        uri=new URI(uri),
        headers=Map.empty,
        validStatusCodes=200 :: 201 :: 202 :: Nil,
        inputView=inputView,
        outputView=outputView,
        params=Map.empty,
        persist=false,
        inputField="input",
        batchSize=1,
        delimiter="",
        numPartitions=None,
        partitionBy=Nil,
        failMode=FailMode.FailFast
      )
    ).get


    val output = spark.sql(s"""
    SELECT get_json_integer_array(body, '$$.predictions') FROM ${outputView}
    """)

    assert(output.first.getAs[scala.collection.mutable.WrappedArray[Integer]](0)(0) == 11)
    assert(output.schema.fields(0).dataType.toString == "ArrayType(IntegerType,false)")
  }

}