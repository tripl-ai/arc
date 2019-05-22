package au.com.agl.arc

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import scala.collection.mutable.ListBuffer
import collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.plugins.LifecyclePlugin
import au.com.agl.arc.util.log.LoggerFactory 

import au.com.agl.arc.util._
import au.com.agl.arc.util.ControlUtils._

class BytesExtractSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  

  val outputView = "outputView"
  val dogImage = getClass.getResource("/flask_serving/dog.jpg").toString
  val uri = s"http://localhost:5000/predict"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("FATAL")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")   

    session = spark
  }


  after {
    session.stop
  }

  test("BytesExtract: Test calling flask_serving") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false, lifecyclePlugins=new ListBuffer[LifecyclePlugin]())

    extract.BytesExtract.extract(
      BytesExtract(
        name="dataset",
        description=None,
        outputView=outputView, 
        input=Right(dogImage),
        authentication=None,
        persist=false,
        numPartitions=None,
        contiguousIndex=true,
        params=Map.empty
      )
    )

    val actual = transform.HTTPTransform.transform(
      HTTPTransform(
        name="transform",
        description=None,
        uri=new URI(uri),
        headers=Map.empty,
        validStatusCodes=200 :: 201 :: 202 :: Nil,
        inputView=outputView,
        outputView=outputView,
        params=Map.empty,
        persist=false,
        inputField="value",
        batchSize=1,
        delimiter="",
        numPartitions=None,
        partitionBy=Nil,        
        failMode=FailModeTypeFailFast          
      )
    ).get    

    assert(spark.sql(s"""SELECT * FROM ${outputView} WHERE body LIKE '%predictions%'""").count != 0)
  }    

}
