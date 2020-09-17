package ai.tripl.arc

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.util._
import ai.tripl.arc.util.log.LoggerFactory

import ai.tripl.arc.util.TestUtils

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
    spark.sparkContext.setLogLevel("INFO")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark
    import spark.implicits._
  }

  after {
    session.stop()
  }

    test("DiffTransform: end-to-end") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    TestUtils.getKnownDataset.createOrReplaceTempView(inputLeftView)
    TestUtils.getKnownAlteredDataset.createOrReplaceTempView(inputRightView)

    val conf = s"""{
      "stages": [
        {
          "type": "DiffTransform",
          "name": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputLeftView": "${inputLeftView}",
          "inputLeftKeys": ["longDatum"],
          "inputRightView": "${inputRightView}",
          "inputRightKeys": ["longDatum"],
          "outputLeftView": "${outputLeftView}",
          "outputIntersectionView": "${outputIntersectionView}",
          "outputRightView": "${outputRightView}",
          "persist": true
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) =>
        ARC.run(pipeline)(spark, logger, arcContext)
      assert(spark.table(outputIntersectionView).filter(col("left.integerDatum") === 17).count == 1)
      assert(spark.table(outputIntersectionView).filter(col("right.integerDatum") === 35).count == 1)
      assert(spark.table(outputIntersectionView).count == 2)
      assert(spark.table(outputLeftView).count == 0)
      assert(spark.table(outputRightView).count == 0)
    }
  }

  test("DiffTransform: no inputKeys") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    TestUtils.getKnownDataset.createOrReplaceTempView(inputLeftView)
    TestUtils.getKnownAlteredDataset.createOrReplaceTempView(inputRightView)

    transform.DiffTransformStage.execute(
      transform.DiffTransformStage(
        plugin=new transform.DiffTransform,
        id=None,
        name="DiffTransform",
        description=None,
        inputLeftView=inputLeftView,
        inputLeftKeys=Nil,
        inputRightView=inputRightView,
        inputRightKeys=Nil,
        outputIntersectionView=Option(outputIntersectionView),
        outputLeftView=Option(outputLeftView),
        outputRightView=Option(outputRightView),
        persist=true,
        params=Map.empty
      )
    )

    assert(spark.table(outputIntersectionView).filter(col("integerDatum") === 17).count == 1)
    assert(spark.table(outputLeftView).filter(col("integerDatum") === 34).count == 1)
    assert(spark.table(outputRightView).filter(col("integerDatum") === 35).count == 1)
  }

  test("DiffTransform: inputKeys") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    TestUtils.getKnownDataset.createOrReplaceTempView(inputLeftView)
    TestUtils.getKnownAlteredDataset.createOrReplaceTempView(inputRightView)

    transform.DiffTransformStage.execute(
      transform.DiffTransformStage(
        plugin=new transform.DiffTransform,
        id=None,
        name="DiffTransform",
        description=None,
        inputLeftView=inputLeftView,
        inputLeftKeys=List("longDatum"),
        inputRightView=inputRightView,
        inputRightKeys=List("longDatum"),
        outputIntersectionView=Option(outputIntersectionView),
        outputLeftView=Option(outputLeftView),
        outputRightView=Option(outputRightView),
        persist=true,
        params=Map.empty
      )
    )

    assert(spark.table(outputIntersectionView).filter(col("left.integerDatum") === 17).count == 1)
    assert(spark.table(outputIntersectionView).filter(col("right.integerDatum") === 35).count == 1)
    assert(spark.table(outputIntersectionView).count == 2)
    assert(spark.table(outputLeftView).count == 0)
    assert(spark.table(outputRightView).count == 0)

    transform.DiffTransformStage.execute(
      transform.DiffTransformStage(
        plugin=new transform.DiffTransform,
        id=None,
        name="DiffTransform",
        description=None,
        inputLeftView=inputLeftView,
        inputLeftKeys=List("longDatum", "booleanDatum"),
        inputRightView=inputRightView,
        inputRightKeys=List("longDatum", "booleanDatum"),
        outputIntersectionView=Option(outputIntersectionView),
        outputLeftView=Option(outputLeftView),
        outputRightView=Option(outputRightView),
        persist=true,
        params=Map.empty
      )
    )

    assert(spark.table(outputIntersectionView).count == 1)
    assert(spark.table(outputLeftView).count == 1)
    assert(spark.table(outputRightView).count == 1)
  }
}
