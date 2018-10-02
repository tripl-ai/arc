package au.com.agl.arc.util

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql._

import au.com.agl.arc.api.API._
import au.com.agl.arc.api.{Delimited, Delimiter, QuoteCharacter}
import au.com.agl.arc.util.log.LoggerFactory

class ConfigUtilsSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _

  before {
    val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    session = spark
  }

  after {
    session.stop()
  }

  test("Read simple config") {
    implicit val spark = session

    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val env = "test"

    val argsMap = collection.mutable.HashMap[String, String]()

    val pipeline = ConfigUtils.parsePipeline(Option("classpath://conf/simple.conf"), argsMap, env)

    val stage = DelimitedExtract(
      name = "file extract",
      cols = Right(Nil),
      outputView = "green_tripdata0_raw",
      input = Right("/data/green_tripdata/0/*.csv"),
      settings = Delimited(Delimiter.Comma, QuoteCharacter.DoubleQuote, true, false),
      authentication = None,
      params = Map.empty,
      persist = false,
      numPartitions = None,
      partitionBy = Nil,
      contiguousIndex = None
    )

    val subDelimitedExtractStage = DelimitedExtract(
      name = "extract data from green_tripdata/1",
      cols = Right(Nil),
      outputView = "green_tripdata1_raw",
      input = Right("/data/green_tripdata/1/*.csv"),
      settings = Delimited(Delimiter.Comma, QuoteCharacter.DoubleQuote, true, false),
      authentication = None,
      params = Map.empty,
      persist = false,
      numPartitions = None,
      partitionBy = Nil,
      contiguousIndex = None
    )

    val schema =
      IntegerColumn(
        id = "f457e562-5c7a-4215-a754-ab749509f3fb",
        name = "vendor_id",
        description = Some("A code indicating the TPEP provider that provided the record."),
        nullable = true,
        nullReplacementValue = None,
        trim = true,
        nullableValues = "" :: "null" :: Nil,
        metadata = None,
        formatters = None) ::
      TimestampColumn(
        id = "d61934ed-e32e-406b-bd18-8d6b7296a8c0",
        name = "lpep_pickup_datetime",
        description = Some("The date and time when the meter was engaged."),
        nullable = true,
        nullReplacementValue = None,
        trim = true,
        nullableValues = "" :: "null" :: Nil,
        timezoneId = "America/New_York",
        formatters = "yyyy-MM-dd HH:mm:ss" :: Nil,
        time = None,
        metadata = None,
        strict = false) :: Nil


    val subTypingTransformStage = TypingTransform(
      name = "apply green_tripdata/1 data types",
      cols = Right(schema),
      inputView = "green_tripdata1_raw",
      outputView = "green_tripdata1",
      params = Map.empty,
      persist = true
    )

    val subSQLValidateStage = SQLValidate(
      name = "ensure no errors exist after data typing",
      inputURI = URI.create("classpath://conf/sql/sqlvalidate_errors.sql"),
      sql =
        """|SELECT
           |  SUM(error) = 0
           |  ,TO_JSON(NAMED_STRUCT('count', COUNT(error), 'errors', SUM(error)))
           |FROM (
           |  SELECT
           |    CASE
           |      WHEN SIZE(_errors) > 0 THEN 1
           |      ELSE 0
           |    END AS error
           |  FROM ${table_name}
           |) input_table""".stripMargin,
      sqlParams = Map("table_name" -> "green_tripdata1"),
      params = Map.empty
    )

    val expected = ETLPipeline(stage :: subDelimitedExtractStage :: subTypingTransformStage :: subSQLValidateStage :: Nil)

    assert(pipeline === Right(expected))
  }

}
