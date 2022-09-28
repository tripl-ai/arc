package ai.tripl.arc

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils

import org.apache.log4j.{Level, Logger}

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.util._
import ai.tripl.arc.util.log.LoggerFactory

class SQLTransformSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  val targetFile = FileUtils.getTempDirectoryPath() + "transform.parquet"
  val inputView = "inputView"
  val outputView = "outputView"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Arc Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    Logger.getLogger("org").setLevel(Level.ERROR)

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark
    import spark.implicits._

    // recreate test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile))
    // parquet does not support writing NullType
    // include partition by to test pushdown
    TestUtils.getKnownDataset.drop($"nullDatum").withColumn("_monotonically_increasing_id", monotonically_increasing_id()).write.partitionBy("dateDatum").parquet(targetFile)
  }

  after {
    session.stop()

    // clean up test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile))
  }

  test("SQLTransform") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val df = TestUtils.getKnownDataset
    df.createOrReplaceTempView(inputView)

    val dataset = transform.SQLTransformStage.execute(
      transform.SQLTransformStage(
        plugin=new transform.SQLTransform,
        id=None,
        name="SQLTransform",
        description=None,
        inputURI=Option(new URI(targetFile)),
        sql=s"SELECT * FROM ${inputView} WHERE booleanDatum = FALSE",
        outputView=outputView,
        persist=false,
        sqlParams=Map.empty,
        authentication=None,
        params=Map.empty,
        numPartitions=None,
        partitionBy=Nil
      )
    ).get

    val actual = dataset.drop($"nullDatum")
    val expected = df.filter(dataset("booleanDatum")===false).drop($"nullDatum")

    assert(TestUtils.datasetEquality(expected, actual))
  }

  test("SQLTransform: end-to-end") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val conf = s"""{
      "stages": [
        {
          "type": "SQLTransform",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "${getClass.getResource("/conf/sql/").toString}/basic.sql",
          "outputView": "customer",
          "persist": false,
          "sqlParams": {
            "placeholder": "value",
          }
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => ARC.run(pipeline)(spark, logger, arcContext)
    }
  }

  test("SQLTransform: end-to-end inline sql") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val conf = s"""{
      "stages": [
        {
          "type": "SQLTransform",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "sql": "SELECT TRUE, '$${placeholder}'",
          "outputView": "customer",
          "persist": false,
          "sqlParams": {
            "placeholder": "value",
          }
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => ARC.run(pipeline)(spark, logger, arcContext)
    }
  }

  test("SQLTransform: persist") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val df = TestUtils.getKnownDataset
    df.createOrReplaceTempView(inputView)

    transform.SQLTransformStage.execute(
      transform.SQLTransformStage(
        plugin=new transform.SQLTransform,
        id=None,
        name="SQLTransform",
        description=None,
        inputURI=Option(new URI(targetFile)),
        sql=s"SELECT * FROM ${inputView}",
        outputView=outputView,
        persist=false,
        sqlParams=Map.empty,
        authentication=None,
        params=Map.empty,
        numPartitions=None,
        partitionBy=Nil
      )
    )
    assert(spark.catalog.isCached(outputView) === false)

    transform.SQLTransformStage.execute(
      transform.SQLTransformStage(
        plugin=new transform.SQLTransform,
        id=None,
        name="SQLTransform",
        description=None,
        inputURI=Option(new URI(targetFile)),
        sql=s"SELECT * FROM ${inputView}",
        outputView=outputView,
        persist=true,
        sqlParams=Map.empty,
        authentication=None,
        params=Map.empty,
        numPartitions=None,
        partitionBy=Nil
      )
    )
    assert(spark.catalog.isCached(outputView) === true)
  }

  test("SQLTransform: sqlParams") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val df = TestUtils.getKnownDataset
    df.createOrReplaceTempView(inputView)

    val dataset = transform.SQLTransformStage.execute(
      transform.SQLTransformStage(
        plugin=new transform.SQLTransform,
        id=None,
        name="SQLTransform",
        description=None,
        inputURI=Option(new URI(targetFile)),
        sql=s"SELECT * FROM ${inputView} WHERE booleanDatum = $${sql_boolean_param}",
        outputView=outputView,
        persist=false,
        sqlParams=Map("sql_boolean_param" -> "FALSE"),
        authentication=None,
        params=Map.empty,
        numPartitions=None,
        partitionBy=Nil
      )
    ).get

    val actual = dataset.drop($"nullDatum")
    val expected = df.filter(df("booleanDatum")===false).drop($"nullDatum")

    assert(TestUtils.datasetEquality(expected, actual))
  }

  test("SQLTransform: sqlParams missing") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    // try with wildcard
    val thrown0 = intercept[Exception with DetailException] {
      transform.SQLTransformStage.execute(
        transform.SQLTransformStage(
          plugin=new transform.SQLTransform,
          id=None,
          name="SQLTransform",
          description=None,
          inputURI=Option(new URI(targetFile)),
          sql=s"SELECT * FROM ${inputView} WHERE stringDatum = '$${sql_string_param}' AND booleanDatum = $${sql_boolean}",
          outputView=outputView,
          persist=false,
          sqlParams=Map("sql_string_param" -> "test"),
          authentication=None,
          params=Map.empty,
          numPartitions=None,
          partitionBy=Nil
        )
      ).get
    }
    assert(thrown0.getMessage === "No replacement value found in parameters: [sql_string_param] for placeholders: [${sql_boolean}].")
  }

  // test("SQLTransform: sqlParams with environment variable") {
  //   implicit val spark = session
  //   import spark.implicits._
  //   implicit val logger = TestUtils.getLogger()
  //   implicit val arcContext = TestUtils.getARCContext()

  //   val df = TestUtils.getKnownDataset
  //   df.createOrReplaceTempView(inputView)

  //   val dataset = transform.SQLTransformStage.execute(
  //     transform.SQLTransformStage(
  //       plugin=new transform.SQLTransform,
  //       name="SQLTransform",
  //       description=None,
  //       inputURI=Option(new URI(targetFile)),
  //       sql=s"SELECT * FROM ${inputView} WHERE stringDatum = '$${sql_string_param}' AND booleanDatum = $${ETL_CONF_TEST_SQL_PARAM}",
  //       outputView=outputView,
  //       persist=false,
  //       sqlParams=Map("sql_string_param" -> "test,breakdelimiter"),
  //       authentication=None,
  //       params=Map.empty,
  //       numPartitions=None,
  //       partitionBy=Nil
  //     )
  //   ).get

  //   val actual = dataset.drop($"nullDatum")
  //   val expected = df.filter(df("booleanDatum")===true).drop($"nullDatum")

  //   assert(TestUtils.datasetEquality(expected, actual))
  // }

  test("SQLTransform: predicatePushdown") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val dataset = transform.SQLTransformStage.execute(
      transform.SQLTransformStage(
        plugin=new transform.SQLTransform,
        id=None,
        name="SQLTransform",
        description=None,
        inputURI=Option(new URI(targetFile)),
        sql=s"SELECT * FROM parquet.`${targetFile}` WHERE booleanDatum = $${sql_boolean_param}",
        outputView=outputView,
        persist=false,
        sqlParams=Map("sql_boolean_param" -> "FALSE"),
        authentication=None,
        params=Map.empty,
        numPartitions=None,
        partitionBy=Nil
      )
    ).get

    val dataFilters = QueryExecutionUtils.getDataFilters(dataset.queryExecution.executedPlan).toArray.mkString(",")
    assert(dataFilters.contains("isnotnull(booleanDatum"))
    assert(dataFilters.contains("),NOT booleanDatum"))
  }

  test("SQLTransform: Execute with Structured Streaming" ) {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=true)

    val readStream = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", "1")
      .load

    readStream.createOrReplaceTempView("readstream")

    val dataset = transform.SQLTransformStage.execute(
      transform.SQLTransformStage(
        plugin=new transform.SQLTransform,
        id=None,
        name="SQLTransform",
        description=None,
        inputURI=Option(new URI(targetFile)),
        sql=s"SELECT * FROM readstream",
        outputView=outputView,
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
    } finally {
      writeStream.stop
    }
  }
}
