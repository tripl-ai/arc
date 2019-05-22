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

  val pathView = "pathView"
  val outputView = "outputView"
  val targetFile = getClass.getResource("/notes.xml.zip").toString

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

  test("BytesExtract: input") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false, lifecyclePlugins=new ListBuffer[LifecyclePlugin]())

    val extractDataset = extract.BytesExtract.extract(
      BytesExtract(
        name="dataset",
        description=None,
        outputView=outputView, 
        input=Right(targetFile),
        authentication=None,
        persist=false,
        numPartitions=None,
        contiguousIndex=true,
        params=Map.empty
      )
    ).get

    assert(extractDataset.filter($"_filename".contains(targetFile)).count != 0)
    assert(extractDataset.count == 1)
  }    

  test("BytesExtract: pathView") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false, lifecyclePlugins=new ListBuffer[LifecyclePlugin]())

    val input = Seq(targetFile, targetFile).toDF("value")
    input.createOrReplaceTempView(pathView)

    val extractDataset = extract.BytesExtract.extract(
      BytesExtract(
        name="dataset",
        description=None,
        outputView=outputView, 
        input=Left(pathView),
        authentication=None,
        persist=false,
        numPartitions=None,
        contiguousIndex=true,
        params=Map.empty
      )
    ).get

    assert(extractDataset.count == 2)
  }    

}
