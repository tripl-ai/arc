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

  test("jsonPath") {
    implicit val spark = session

    val df = spark.sql("""
    SELECT get_json_long_array('{"nested": {"object": [2147483648, 2147483649]}}', '$.nested.object') AS test
    """)

    assert(df.first.getAs[scala.collection.mutable.WrappedArray[Long]](0)(0) == 2147483648L)
    assert(df.schema.fields(0).dataType.toString == "ArrayType(LongType,false)")
  }

  test("get_json_double_array") {
    implicit val spark = session

    val df = spark.sql("""
    SELECT get_json_double_array('[0.1, 1.1]', '$') AS test
    """)

    assert(df.first.getAs[scala.collection.mutable.WrappedArray[Double]](0)(0) == 0.1)
    assert(df.schema.fields(0).dataType.toString == "ArrayType(DoubleType,false)")
  }

  test("get_json_integer_array") {
    implicit val spark = session

    val df = spark.sql("""
    SELECT get_json_integer_array('[1, 2]', '$') AS test
    """)

    assert(df.first.getAs[scala.collection.mutable.WrappedArray[Integer]](0)(0) == 1)
    assert(df.schema.fields(0).dataType.toString == "ArrayType(IntegerType,false)")
  }

  test("get_json_long_array") {
    implicit val spark = session

    val df = spark.sql("""
    SELECT get_json_long_array('[2147483648, 2147483649]', '$') AS test
    """)

    assert(df.first.getAs[scala.collection.mutable.WrappedArray[Long]](0)(0) == 2147483648L)
    assert(df.schema.fields(0).dataType.toString == "ArrayType(LongType,false)")
  }

  test("random") {
    implicit val spark = session

    val df = spark.sql("""
    SELECT random() AS test
    """)

    assert(df.first.getDouble(0) > 0.0)
    assert(df.first.getDouble(0) < 1.0)
    assert(df.schema.fields(0).dataType.toString == "DoubleType")
  }

  test("to_xml") {
    implicit val spark = session

    val df = spark.sql("""
    SELECT
      to_xml(
        NAMED_STRUCT(
          'Document', NAMED_STRUCT(
              '_VALUE', NAMED_STRUCT(
                'child0', 0,
                'child1', NAMED_STRUCT(
                  'nested0', 0,
                  'nested1', 'nestedvalue'
                )
              ),
          '_attribute', 'attribute'
          )
        )
      ) AS xml
    """)

    assert(df.first.getString(0) ==
    """<Document attribute="attribute">
    |  <child0>0</child0>
    |  <child1>
    |    <nested0>0</nested0>
    |    <nested1>nestedvalue</nested1>
    |  </child1>
    |</Document>""".stripMargin)
  }

  test("struct_keys") {
    implicit val spark = session

    val df = spark.sql("""
    SELECT
      STRUCT_KEYS(
        NAMED_STRUCT(
          'key0', 'value0',
          'key1', 'value1'
        )
      )
    """)

    assert(df.first.getSeq[String](0) == Seq("key0", "key1"))
  }

  test("struct_contains: true") {
    implicit val spark = session

    val df = spark.sql("""
    SELECT
      ARRAY_CONTAINS(
        STRUCT_KEYS(
          NAMED_STRUCT(
            'key0', 'value0',
            'key1', 'value1'
          )
        )
        ,'key1'
      )
    """)

    assert(df.first.getBoolean(0) == true)
  }

  test("struct_contains: false") {
    implicit val spark = session

    val df = spark.sql("""
    SELECT
      ARRAY_CONTAINS(
        STRUCT_KEYS(
          NAMED_STRUCT(
            'key0', 'value0',
            'key1', 'value1'
          )
        )
        ,'key2'
      )
    """)
    assert(df.first.getBoolean(0) == false)
  }

  test("get_uri: batch") {
    implicit val spark = session

    val targetFile = getClass.getResource("/conf/simple.conf").toString
    val df = spark.sql(s"SELECT DECODE(GET_URI('${targetFile}'), 'UTF-8') AS simpleConf")

    val expected = spark.read.option("wholetext", true).text(targetFile).first.getString(0)
    val actual = df.first.getString(0)

    assert(actual == expected)
  }  

  test("get_uri: streaming") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=true)

    val targetFile = getClass.getResource("/conf/simple.conf").toString
    val expected = spark.read.option("wholetext", true).text(targetFile).first.getString(0)

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
        authentication=None,
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

      val actual = df.first.getString(0)
      assert(actual == expected)
    } finally {
      writeStream.stop
    }
  }

}