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
import ai.tripl.arc.util._

import ai.tripl.arc.util.TestUtils

class ImageExtractSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  val targetFile = getClass.getResource("/puppy.jpg").toString
  val emptyDirectory = FileUtils.getTempDirectoryPath() + "empty.jpg"
  val outputView = "dataset"

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
  }

  after {
    session.stop()
  }

  test("ImageExtract") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val dataset = extract.ImageExtractStage.execute(
      extract.ImageExtractStage(
        plugin=new extract.ImageExtract,
        name=outputView,
        description=None,
        outputView=outputView,
        input=targetFile,
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        basePath=None,
        dropInvalid=true,
        watermark=None
      )
    ).get

    // test that the filename is correctly populated
    assert(dataset.filter($"image.origin".contains(targetFile.replace("file:", "file://"))).count == 1)
    assert(dataset.filter("image.width = 640").count == 1)
    assert(dataset.filter("image.height == 960").count == 1)
    assert(dataset.filter("image.nChannels == 3").count == 1)
    assert(dataset.filter("image.mode == 16").count == 1)
  }

  test("ImageExtract: Caching") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    // no cache
    extract.ImageExtractStage.execute(
      extract.ImageExtractStage(
        plugin=new extract.ImageExtract,
        name=outputView,
        description=None,
        outputView=outputView,
        input=targetFile,
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        basePath=None,
        dropInvalid=true,
        watermark=None
      )
    )
    assert(spark.catalog.isCached(outputView) === false)

    // cache
    extract.ImageExtractStage.execute(
      extract.ImageExtractStage(
        plugin=new extract.ImageExtract,
        name=outputView,
        description=None,
        outputView=outputView,
        input=targetFile,
        authentication=None,
        params=Map.empty,
        persist=true,
        numPartitions=None,
        partitionBy=Nil,
        basePath=None,
        dropInvalid=true,
        watermark=None
      )
    )
    assert(spark.catalog.isCached(outputView) === true)
  }

  test("ImageExtract: Empty Dataset") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val dataset = extract.ImageExtractStage.execute(
      extract.ImageExtractStage(
        plugin=new extract.ImageExtract,
        name=outputView,
        description=None,
        outputView=outputView,
        input=emptyDirectory,
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        basePath=None,
        dropInvalid=true,
        watermark=None
      )
    ).get

    assert(dataset.count == 0)
  }

  test("ImageExtract:: Structured Streaming") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=true)

    val dataset = extract.ImageExtractStage.execute(
      extract.ImageExtractStage(
        plugin=new extract.ImageExtract,
        name=outputView,
        description=None,
        outputView=outputView,
        input=targetFile.replace("puppy.jpg", "*.jpg"),
        authentication=None,
        params=Map.empty,
        persist=true,
        numPartitions=None,
        partitionBy=Nil,
        basePath=None,
        dropInvalid=true,
        watermark=None
      )
    ).get

    val writeStream = dataset
      .writeStream
      .queryName("extract")
      .format("memory")
      .start

    val df = spark.table("extract")

    try {
      Thread.sleep(2000)
      // will fail if parsing does not work
      assert(df.filter($"image.origin".contains(targetFile.replace("file:", "file://"))).count != 0)
    } finally {
      writeStream.stop
    }
  }
}
