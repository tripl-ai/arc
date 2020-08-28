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
import ai.tripl.arc.util.DetailException

import ai.tripl.arc.util.TestUtils

class AvroLoadSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  val targetFile = FileUtils.getTempDirectoryPath() + "extract.avro"
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

    // ensure targets removed
    FileUtils.deleteQuietly(new java.io.File(targetFile))
  }

  after {
    session.stop()

    // clean up test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile))
  }

  test("AvroLoad") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit var arcContext = TestUtils.getARCContext()

    val dataset = TestUtils.getKnownDataset
    dataset.createOrReplaceTempView(outputView)

    val thrown0 = intercept[Exception with DetailException] {
      load.AvroLoadStage.execute(
        load.AvroLoadStage(
          plugin=new load.AvroLoad,
          name=outputView,
          description=None,
          inputView=outputView,
          outputURI=new URI(targetFile),
          partitionBy=Nil,
          numPartitions=None,
          authentication=None,
          saveMode=SaveMode.Overwrite,
          params=Map.empty
        )
      )
    }
    assert(thrown0.getMessage.contains("""inputView 'dataset' contains types {"NullType":["nullDatum"]} which are unsupported by AvroLoad and 'dropUnsupported' is set to false."""))

    arcContext = TestUtils.getARCContext(dropUnsupported=true)
    load.AvroLoadStage.execute(
      load.AvroLoadStage(
        plugin=new load.AvroLoad,
        name=outputView,
        description=None,
        inputView=outputView,
        outputURI=new URI(targetFile),
        partitionBy=Nil,
        numPartitions=None,
        authentication=None,
        saveMode=SaveMode.Overwrite,
        params=Map.empty
      )
    )

    // avro will drop nulls
    // avro will convert date and times to epoch milliseconds
    val expected = dataset
      .drop($"nullDatum")
    val actual = spark.read.format("com.databricks.spark.avro").load(targetFile)

    assert(TestUtils.datasetEquality(expected, actual))
  }

  test("AvroLoad: partitionBy") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(dropUnsupported=true)

    val dataset = TestUtils.getKnownDataset
    dataset.createOrReplaceTempView(outputView)
    assert(dataset.select(spark_partition_id()).distinct.count === 1)

    load.AvroLoadStage.execute(
      load.AvroLoadStage(
        plugin=new load.AvroLoad,
        name=outputView,
        description=None,
        inputView=outputView,
        outputURI=new URI(targetFile),
        partitionBy="booleanDatum" :: Nil,
        numPartitions=None,
        authentication=None,
        saveMode=SaveMode.Overwrite,
        params=Map.empty
      )
    )

    val actual = spark.read.format("com.databricks.spark.avro").load(targetFile)
    assert(actual.select(spark_partition_id()).distinct.count === 2)
  }

}
