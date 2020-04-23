package ai.tripl.arc

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql._

import ai.tripl.arc.util.log.LoggerFactory
import ai.tripl.arc.util.TestUtils

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.util._
import ai.tripl.arc.udf.UDF

class UDFSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  var logger: ai.tripl.arc.util.log.logger.Logger = _

  // minio seems to need ip address not hostname
  val bucketName = "bucket0"
  val minioHostPort = "http://minio:9000"
  val minioAccessKey = "AKIAIOSFODNN7EXAMPLE"
  val minioSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
  val targetFile = s"s3a://${bucketName}/akc_breed_info.csv"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark
    logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    val arcContext = TestUtils.getARCContext(isStreaming=false)

    // register udf
    UDF.registerUDFs()(spark, logger, arcContext)
  }

  after {
    session.stop()
  }

  test("get_uri: batch") {
    implicit val spark = session
    val df = spark.sql(s"SELECT DECODE(GET_URI('${targetFile}'), 'UTF-8') AS simpleConf")
    assert(df.first.getString(0).contains("Breed,height_low_inches"))
  }  

  test("get_uri: streaming") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=true)

    val readStream = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", "5")
      .load

    readStream.createOrReplaceTempView("readstream")

    val dataset = transform.SQLTransformStage.execute(
      transform.SQLTransformStage(
        plugin=new transform.SQLTransform,
        name="SQLTransform",
        description=None,
        inputURI=None,
        sql=s"SELECT DECODE(GET_URI('${targetFile}'), 'UTF-8') AS simpleConf FROM readstream",
        outputView="outputView",
        persist=false,
        sqlParams=Map.empty,
        authentication=Some(Authentication.AmazonAccessKey(Some(bucketName), minioAccessKey, minioSecretKey, Some(minioHostPort), None)),
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
      assert(df.count > 0)

      assert(df.first.getString(0).contains("Breed,height_low_inches"))
    } finally {
      writeStream.stop
    }
  }

}