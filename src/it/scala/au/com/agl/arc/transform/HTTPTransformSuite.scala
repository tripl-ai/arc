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

class HTTPTransformSuite extends FunSuite with BeforeAndAfter {

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

  test("HTTPTransform: Can call TensorflowServing via REST" ) {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)


    val df = spark.range(1, 10).toDF
    df.createOrReplaceTempView(inputView)

    var payloadDataset = spark.sql(s"""
    SELECT 
      id
      ,TO_JSON(NAMED_STRUCT('instances', ARRAY(id))) AS value 
    FROM ${inputView}
    """)
    payloadDataset.createOrReplaceTempView(inputView)

    val transformDataset = transform.HTTPTransform.transform(
      HTTPTransform(
        name=outputView,
        uri=new URI(uri),
        headers=Map.empty,
        validStatusCodes=None,
        inputView=inputView,
        outputView=outputView,
        params=Map.empty,
        persist=false
      )
    ).get


    val output = spark.sql(s"""
    SELECT get_json_integer_array(body, '$$.predictions') FROM ${outputView}
    """)

    assert(output.first.getAs[scala.collection.mutable.WrappedArray[Integer]](0)(0) == 11)
    assert(output.schema.fields(0).dataType.toString == "ArrayType(IntegerType,false)")
  }  

}