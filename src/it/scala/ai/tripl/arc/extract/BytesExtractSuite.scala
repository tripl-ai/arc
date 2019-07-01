package ai.tripl.arc

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.util.log.LoggerFactory 

import ai.tripl.arc.util._
import ai.tripl.arc.util.ControlUtils._

class BytesExtractSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  

  val outputView = "outputView"
  val dogImage = getClass.getResource("/flask_serving/dog.jpg").toString
  val uri = s"http://flask_serving:5000/predict"

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
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    extract.BytesExtractStage.execute(
      extract.BytesExtractStage(
        plugin=new extract.BytesExtract,
        name="dataset",
        description=None,
        outputView=outputView, 
        input=Right(dogImage),
        authentication=None,
        persist=false,
        numPartitions=None,
        contiguousIndex=true,
        params=Map.empty,
        failMode=FailModeTypeFailFast
      )
    )

    transform.HTTPTransformStage.execute(
      transform.HTTPTransformStage(
        plugin=new transform.HTTPTransform,
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
