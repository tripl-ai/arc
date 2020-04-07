package ai.tripl.arc

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._

import ai.tripl.arc.util.TestUtils

class TextLoadSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  val targetFile = FileUtils.getTempDirectoryPath() + "load.txt"
  val targetSingleFile = FileUtils.getTempDirectoryPath() + "single.txt"
  val targetSingleFileDelimited = FileUtils.getTempDirectoryPath() + "singledelimited.txt"
  val outputView = "dataset"

  val targetSingleFileWildcard = FileUtils.getTempDirectoryPath() + "/singlepart*.txt"  
  val targetSingleFile0 = "singlepart0.txt"  
  val targetSingleFile1 = "singlepart1.txt"  
  val targetSingleFile2 = "singlepart2.txt"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark

    // ensure targets removed
    FileUtils.deleteQuietly(new java.io.File(targetFile))
    FileUtils.deleteQuietly(new java.io.File(targetSingleFile))
    FileUtils.deleteQuietly(new java.io.File(targetSingleFile0))
    FileUtils.deleteQuietly(new java.io.File(targetSingleFile1))
    FileUtils.deleteQuietly(new java.io.File(targetSingleFile2))
  }

  after {
    session.stop()

    // clean up test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile))
    FileUtils.deleteQuietly(new java.io.File(targetSingleFile))
    FileUtils.deleteQuietly(new java.io.File(targetSingleFile0))
    FileUtils.deleteQuietly(new java.io.File(targetSingleFile1))
    FileUtils.deleteQuietly(new java.io.File(targetSingleFile2))    
  }

  test("TextLoad") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val dataset = TestUtils.getKnownDataset.select("stringDatum")
    dataset.createOrReplaceTempView(outputView)

    load.TextLoadStage.execute(
      load.TextLoadStage(
        plugin=new load.TextLoad,
        name=outputView,
        description=None,
        inputView=outputView,
        outputURI=new URI(targetFile),
        numPartitions=None,
        authentication=None,
        saveMode=SaveMode.Overwrite,
        params=Map.empty,
        singleFile=false,
        prefix="",
        separator="",
        suffix=""
      )
    )

    val expected = dataset.withColumnRenamed("stringDatum", "value")
    val actual = spark.read.text(targetFile)

    assert(TestUtils.datasetEquality(expected, actual))
  }

  test("TextLoad: singleFile") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val dataset = TestUtils.getKnownDataset.select("stringDatum")
    dataset.createOrReplaceTempView(outputView)

    load.TextLoadStage.execute(
      load.TextLoadStage(
        plugin=new load.TextLoad,
        name=outputView,
        description=None,
        inputView=outputView,
        outputURI=new URI(targetSingleFile),
        numPartitions=None,
        authentication=None,
        saveMode=SaveMode.Overwrite,
        params=Map.empty,
        singleFile=true,
        prefix="",
        separator="",
        suffix=""
      )
    )

    val actual = spark.read.text(targetSingleFile)
    val expected = Seq("test,breakdelimiterbreakdelimiter,test").toDF

    assert(TestUtils.datasetEquality(expected, actual))
  }

  test("TextLoad: singleFile with filename") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)


    val dataset = Seq(
      (targetSingleFile0, "a"),
      (targetSingleFile0, "b"),
      (targetSingleFile0, "c"),
      (targetSingleFile1, "d"), 
      (targetSingleFile1, "e"), 
      (targetSingleFile2, "f")
    ).toDF("filename", "value")
    dataset.createOrReplaceTempView(outputView)

    load.TextLoadStage.execute(
      load.TextLoadStage(
        plugin=new load.TextLoad,
        name=outputView,
        description=None,
        inputView=outputView,
        outputURI=new URI(FileUtils.getTempDirectoryPath().stripSuffix("/")),
        numPartitions=None,
        authentication=None,
        saveMode=SaveMode.Overwrite,
        params=Map.empty,
        singleFile=true,
        prefix="",
        separator="\n",
        suffix=""
      )
    )

    val actual = spark.read.text(targetSingleFileWildcard).withColumn("_filename", input_file_name())
    assert(actual.where(s"_filename LIKE '%${targetSingleFile0}'").collect.map(_.getString(0)).mkString("|") == "a|b|c")
    assert(actual.where(s"_filename LIKE '%${targetSingleFile1}'").collect.map(_.getString(0)).mkString("|") == "d|e")
    assert(actual.where(s"_filename LIKE '%${targetSingleFile2}'").collect.map(_.getString(0)).mkString("|") == "f")
  }  

  test("TextLoad: singleFile prefix/separator/suffix") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val dataset = TestUtils.getKnownDataset.toJSON
    dataset.createOrReplaceTempView(outputView)

    load.TextLoadStage.execute(
      load.TextLoadStage(
        plugin=new load.TextLoad,
        name=outputView,
        description=None,
        inputView=outputView,
        outputURI=new URI(targetSingleFileDelimited),
        numPartitions=None,
        authentication=None,
        saveMode=SaveMode.Overwrite,
        params=Map.empty,
        singleFile=true,
        prefix="[",
        separator=",",
        suffix="]"
      )
    )

    val actual = spark.read.text(targetSingleFileDelimited)
    val expected = Seq("""[{"booleanDatum":true,"dateDatum":"2016-12-18","decimalDatum":54.321000000000000000,"doubleDatum":42.4242,"integerDatum":17,"longDatum":1520828868,"stringDatum":"test,breakdelimiter","timeDatum":"12:34:56","timestampDatum":"2017-12-20T21:46:54.000Z"},{"booleanDatum":false,"dateDatum":"2016-12-19","decimalDatum":12.345000000000000000,"doubleDatum":21.2121,"integerDatum":34,"longDatum":1520828123,"stringDatum":"breakdelimiter,test","timeDatum":"23:45:16","timestampDatum":"2017-12-29T17:21:49.000Z"}]""").toDF

    assert(TestUtils.datasetEquality(expected, actual))
  }

}
