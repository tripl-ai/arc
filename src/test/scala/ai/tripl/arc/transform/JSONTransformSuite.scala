package ai.tripl.arc

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql._

import ai.tripl.arc.api.API._

import ai.tripl.arc.util.TestUtils

class JSONTransformSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  val inputView = "inputView"
  val outputView = "outputView"

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

  test("JSONTransform") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    TestUtils.getKnownDataset.createOrReplaceTempView(inputView)

    val dataset = transform.JSONTransformStage.execute(
      transform.JSONTransformStage(
        plugin=new transform.JSONTransform,
        name="JSONTransform",
        description=None,
        inputView=inputView,
        outputView=outputView,
        persist=false,
        params=Map.empty,
        numPartitions=None,
        partitionBy=Nil
      )
    ).get

    // check constants in case of change in future spark version
    assert(dataset.count == 2)
    assert(dataset.schema(0).name == "value")

    // check data
    assert(dataset.first.getString(0) == """{"booleanDatum":true,"dateDatum":"2016-12-18","decimalDatum":54.321000000000000000,"doubleDatum":42.4242,"integerDatum":17,"longDatum":1520828868,"stringDatum":"test,breakdelimiter","timeDatum":"12:34:56","timestampDatum":"2017-12-20T21:46:54.000Z"}""")
  }

  test("JSONTransform: Structured Streaming") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=true)

    val readStream = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", "1")
      .load

    readStream.createOrReplaceTempView(inputView)

    val dataset = transform.JSONTransformStage.execute(
      transform.JSONTransformStage(
        plugin=new transform.JSONTransform,
        name="JSONTransform",
        description=None,
        inputView=inputView,
        outputView=outputView,
        persist=false,
        params=Map.empty,
        numPartitions=None,
        partitionBy=Nil
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
      assert(df.first.getString(0).contains(""""value":0"""))
    } finally {
      writeStream.stop
    }
  }
}
