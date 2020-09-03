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

  val targetFile = getClass.getResource("/binary/akc_breed_info.csv").toString
  val targetBinaryFile = getClass.getResource("/binary/puppy.jpg").toString
  var expected: String = _

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
    val arcContext = TestUtils.getARCContext()

    expected = spark.read.option("wholetext", true).text(targetFile).first.getString(0)

    // register udf
    UDF.registerUDFs()(spark, logger, arcContext)
  }

  after {
    session.stop()
  }

  test("UDFSuite: jsonPath") {
    implicit val spark = session

    val df = spark.sql("""
    SELECT get_json_long_array('{"nested": {"object": [2147483648, 2147483649]}}', '$.nested.object') AS test
    """)

    assert(df.first.getAs[scala.collection.mutable.WrappedArray[Long]](0)(0) == 2147483648L)
    assert(df.schema.fields(0).dataType.toString == "ArrayType(LongType,false)")
  }

  test("UDFSuite: get_json_double_array") {
    implicit val spark = session

    val df = spark.sql("""
    SELECT
      get_json_double_array('[0.1, 1.1]', '$') AS test
      ,get_json_double_array(null, '$') AS null_test
    """)

    assert(df.first.getAs[scala.collection.mutable.WrappedArray[Double]](0)(0) == 0.1)
    assert(df.schema.fields(0).dataType.toString == "ArrayType(DoubleType,false)")
    assert(df.first.isNullAt(1))
  }


  test("UDFSuite: get_json_integer_array") {
    implicit val spark = session

    val df = spark.sql("""
    SELECT
      get_json_integer_array('[1, 2]', '$') AS test
      ,get_json_integer_array(null, '$') AS null_test
    """)

    assert(df.first.getAs[scala.collection.mutable.WrappedArray[Integer]](0)(0) == 1)
    assert(df.schema.fields(0).dataType.toString == "ArrayType(IntegerType,false)")
    assert(df.first.isNullAt(1))
  }

  test("UDFSuite: get_json_long_array") {
    implicit val spark = session

    val df = spark.sql("""
    SELECT
      get_json_long_array('[2147483648, 2147483649]', '$') AS test
      ,get_json_long_array(null, '$') AS null_test
    """)

    assert(df.first.getAs[scala.collection.mutable.WrappedArray[Long]](0)(0) == 2147483648L)
    assert(df.schema.fields(0).dataType.toString == "ArrayType(LongType,false)")
    assert(df.first.isNullAt(1))
  }

  test("UDFSuite: random") {
    implicit val spark = session

    val df = spark.sql("""
    SELECT
      random() AS test
    """)

    assert(df.first.getDouble(0) > 0.0)
    assert(df.first.getDouble(0) < 1.0)
    assert(df.schema.fields(0).dataType.toString == "DoubleType")
  }

  test("UDFSuite: struct_keys") {
    implicit val spark = session

    val df = spark.sql("""
    SELECT
      STRUCT_KEYS(
        NAMED_STRUCT(
          'key0', 'value0',
          'key1', 'value1'
        )
      )
      ,STRUCT_KEYS(null) AS null_test
    """)

    assert(df.first.getSeq[String](0) == Seq("key0", "key1"))
    assert(df.first.isNullAt(1))
  }

  test("UDFSuite: struct_contains: true") {
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

  test("UDFSuite: struct_contains: false") {
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

  test("UDFSuite: get_uri: null") {
    implicit val spark = session
    val df = spark.sql("""
      SELECT
        GET_URI(null)
    """)
    assert(df.first.isNullAt(0))
  }

  test("UDFSuite: get_uri: batch") {
    implicit val spark = session
    for (extension <- Seq("", ".gz", ".gzip", ".bz2", ".bzip2", ".lz4")) {
      val df = spark.sql(s"SELECT DECODE(GET_URI('${targetFile}${extension}'), 'UTF-8')")
      assert(df.first.getString(0) == expected)
    }
  }

  test("UDFSuite: get_uri: batch binary") {
    implicit val spark = session
    val df = spark.sql(s"SELECT GET_URI('${targetBinaryFile}')")
    val expected = spark.sqlContext.sparkContext.binaryFiles(targetBinaryFile).map { case (_, portableDataStream) => portableDataStream.toArray }.collect.head
    assert(df.first.getAs[Array[Byte]](0).deep == expected.deep)
  }

  test("UDFSuite: get_uri: streaming") {
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
        id=None,
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

  test("UDFSuite: get_uri_array: batch") {
    implicit val spark = session
    val df = spark.sql(s"""
    SELECT
      DECODE(col, 'UTF-8') AS value
    FROM (
      SELECT EXPLODE(GET_URI_ARRAY('${targetFile}*'))
    ) files
    """)
    assert(df.count == 6)
    val rows = df.collect()
    assert(rows.forall( row => row.getString(0) == expected))
  }

  test("UDFSuite: get_uri_array: batch transform") {
    implicit val spark = session
    val df = spark.sql(s"""
    SELECT TRANSFORM(GET_URI_ARRAY('${targetFile}*'), x -> DECODE(x, 'UTF-8'))
    """)
    assert(df.count == 1)
    assert(df.first.getAs[Seq[String]](0).mkString == List(expected, expected, expected, expected, expected, expected).mkString)
  }

  test("UDFSuite: get_uri_filename_array: batch") {
    implicit val spark = session
    val df = spark.sql(s"""
    SELECT
      DECODE(col._1, 'UTF-8') AS value
      ,col._2 AS filename
    FROM (
      SELECT EXPLODE(GET_URI_FILENAME_ARRAY('${targetFile}*'))
    ) files
    """)

    // df.show(false)
    assert(df.count == 6)
    val rows = df.collect()
    assert(rows.forall( row => row.getString(0) == expected && row.getString(1).contains("akc_breed_info.csv")))
  }

  test("UDFSuite: probit") {
    implicit val spark = session

    val df = spark.sql("""
    SELECT
      probit(null) AS probit_null
      ,probit(0.025) AS probit_0
    """)
    assert(df.first.isNullAt(0))
    assert(df.first.getDouble(1) == -1.959963984540054)
  }

  test("UDFSuite: probnorm") {
    implicit val spark = session

    val df = spark.sql("""
    SELECT
      probnorm(null) AS probnorm_null
      ,probnorm(-1.959963984540054) AS probit_0
    """)
    assert(df.first.isNullAt(0))
    assert(df.first.getDouble(1) == 0.025000000000000022)
  }

}

