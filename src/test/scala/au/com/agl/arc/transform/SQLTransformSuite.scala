package au.com.agl.arc

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.util.log.LoggerFactory 

import au.com.agl.arc.util._

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
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    session = spark
    import spark.implicits._

    // recreate test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile)) 
    // parquet does not support writing NullType
    // include partition by to test pushdown
    TestDataUtils.getKnownDataset.drop($"nullDatum").write.partitionBy("dateDatum").parquet(targetFile)  
  }

  after {
    session.stop()

    // clean up test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile))         
  }

  test("SQLTransform") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val dataset = TestDataUtils.getKnownDataset
    dataset.createOrReplaceTempView(inputView)

    val transformed = transform.SQLTransform.transform(
      SQLTransform(
        name="SQLTransform", 
        inputURI=new URI(targetFile),
        sql=s"SELECT * FROM ${inputView} WHERE booleanDatum = FALSE",
        outputView=outputView,
        persist=false,
        sqlParams=Map.empty,
        params=Map.empty
      )
    ).get

    val actual = transformed.drop($"nullDatum")
    val expected = dataset.filter(dataset("booleanDatum")===false).drop($"nullDatum")

    val actualExceptExpectedCount = actual.except(expected).count
    val expectedExceptActualCount = expected.except(actual).count
    if (actualExceptExpectedCount != 0 || expectedExceptActualCount != 0) {
      println("actual")
      actual.show(false)
      println("expected")
      expected.show(false)  
    }
    assert(actual.except(expected).count === 0)
    assert(expected.except(actual).count === 0)
  }  

  test("SQLTransform: persist") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val dataset = TestDataUtils.getKnownDataset
    dataset.createOrReplaceTempView(inputView)

    transform.SQLTransform.transform(
      SQLTransform(
        name="SQLTransform", 
        inputURI=new URI(targetFile),
        sql=s"SELECT * FROM ${inputView}",
        outputView=outputView,
        persist=false,
        sqlParams=Map.empty,
        params=Map.empty
      )
    )
    assert(spark.catalog.isCached(outputView) === false)

    transform.SQLTransform.transform(
      SQLTransform(
        name="SQLTransform", 
        inputURI=new URI(targetFile),
        sql=s"SELECT * FROM ${inputView}",
        outputView=outputView,
        persist=true,
        sqlParams=Map.empty,
        params=Map.empty
      )
    )
    assert(spark.catalog.isCached(outputView) === true)    
  }    

  test("SQLTransform: sqlParams") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val dataset = TestDataUtils.getKnownDataset
    dataset.createOrReplaceTempView(inputView)

    val transformed = transform.SQLTransform.transform(
      SQLTransform(
        name="SQLTransform", 
        inputURI=new URI(targetFile),
        sql=s"SELECT * FROM ${inputView} WHERE booleanDatum = $${sql_boolean_param}",
        outputView=outputView,
        persist=false,
        sqlParams=Map("sql_boolean_param" -> "FALSE"),
        params=Map.empty
      )
    ).get

    val actual = transformed.drop($"nullDatum")
    val expected = dataset.filter(dataset("booleanDatum")===false).drop($"nullDatum")

    val actualExceptExpectedCount = actual.except(expected).count
    val expectedExceptActualCount = expected.except(actual).count
    if (actualExceptExpectedCount != 0 || expectedExceptActualCount != 0) {
      println("actual")
      actual.show(false)
      println("expected")
      expected.show(false)  
    }
    assert(actual.except(expected).count === 0)
    assert(expected.except(actual).count === 0)
  }     

  test("SQLTransform: partitionPushdown") {
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val transformed = transform.SQLTransform.transform(
      SQLTransform(
        name="SQLTransform", 
        inputURI=new URI(targetFile),
        sql=s"SELECT * FROM parquet.`${targetFile}` WHERE DAY(dateDatum) = 19",
        outputView=outputView,
        persist=false,
        sqlParams=Map("sql_boolean_param" -> "FALSE"),
        params=Map.empty
      )
    ).get

    val partitionFilters = QueryExecutionUtils.getPartitionFilters(transformed.queryExecution.executedPlan).toArray.mkString(",")
    assert(partitionFilters.contains("(dayofmonth(dateDatum"))
    assert(partitionFilters.contains(") = 19)"))
  }  

  test("SQLTransform: predicatePushdown") {
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val transformed = transform.SQLTransform.transform(
      SQLTransform(
        name="SQLTransform", 
        inputURI=new URI(targetFile),
        sql=s"SELECT * FROM parquet.`${targetFile}` WHERE booleanDatum = FALSE",
        outputView=outputView,
        persist=false,
        sqlParams=Map("sql_boolean_param" -> "FALSE"),
        params=Map.empty
      )
    ).get

    val dataFilters = QueryExecutionUtils.getDataFilters(transformed.queryExecution.executedPlan).toArray.mkString(",")
    assert(dataFilters.contains("isnotnull(booleanDatum"))
    assert(dataFilters.contains("),(booleanDatum"))
    assert(dataFilters.contains(" = false)"))
  }    

  test("SQLTransform: partitionPushdown and predicatePushdown") {
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val transformed = transform.SQLTransform.transform(
      SQLTransform(
        name="SQLTransform", 
        inputURI=new URI(targetFile),
        sql=s"SELECT * FROM parquet.`${targetFile}` WHERE DAY(dateDatum) = 19 AND booleanDatum = FALSE",
        outputView=outputView,
        persist=false,
        sqlParams=Map("sql_boolean_param" -> "FALSE"),
        params=Map.empty
      )
    ).get

    val partitionFilters = QueryExecutionUtils.getPartitionFilters(transformed.queryExecution.executedPlan).toArray.mkString(",")
    assert(partitionFilters.contains("(dayofmonth(dateDatum"))
    assert(partitionFilters.contains(") = 19)"))
    val dataFilters = QueryExecutionUtils.getDataFilters(transformed.queryExecution.executedPlan).toArray.mkString(",")
    assert(dataFilters.contains("isnotnull(booleanDatum"))
    assert(dataFilters.contains("),(booleanDatum"))
    assert(dataFilters.contains(" = false)"))
  }    
}
