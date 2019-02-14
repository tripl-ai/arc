package au.com.agl.arc

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.util.log.LoggerFactory 

import au.com.agl.arc.util.TestDataUtils

class DiffTransformSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  
  val inputLeftView = "inputLeftView"
  val inputRightView = "inputRightView"
  val outputIntersectionView = "outputIntersectionView"
  val outputLeftView = "outputLeftView"
  val outputRightView = "outputRightView"

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

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")    
  }

  after {
    session.stop()
  }

  def setNullableStateForAllColumns(df: DataFrame, nullable: Boolean) : DataFrame = {
    df.sqlContext.createDataFrame(df.rdd, StructType(df.schema.map(_.copy(nullable = nullable))))
  }

  test("DiffTransform") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    TestDataUtils.getKnownDataset.createOrReplaceTempView(inputLeftView)
    TestDataUtils.getKnownAlteredDataset.createOrReplaceTempView(inputRightView)

    val transformed = transform.DiffTransform.transform(
      DiffTransform(
        name="DiffTransform", 
        description=None,
        inputLeftView=inputLeftView,
        inputRightView=inputRightView,
        outputIntersectionView=Option(outputIntersectionView),
        outputLeftView=Option(outputLeftView),
        outputRightView=Option(outputRightView),
        persist=true,
        params=Map.empty
      )
    )

    assert(spark.table(outputIntersectionView).filter($"integerDatum" === 17).count == 1)
    assert(spark.table(outputLeftView).filter($"integerDatum" === 34).count == 1)
    assert(spark.table(outputRightView).filter($"integerDatum" === 35).count == 1)
  }  
}
