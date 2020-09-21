package ai.tripl.arc.plugins

import ai.tripl.arc.ARC
import ai.tripl.arc.api.API._
import ai.tripl.arc.config.ArcPipeline
import ai.tripl.arc.config.Error._
import ai.tripl.arc.util.TestUtils

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}
import ai.tripl.arc.extract.ParquetExtract

class SimilarityJoinTransformSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  val leftView = "leftView"
  val rightView = "rightView"
  val outputView = "outputView"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Arc Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    implicit val logger = TestUtils.getLogger()

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark
    import spark.implicits._
  }

  after {
    session.stop()
  }

  test("SimilarityJoinTransformSuite") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val leftDF = Seq(
      ("GANSW705647478",Option("UNIT 3"),59,"INVERNESS","AVENUE","PENSHURST",2222,"NSW"),
      ("GANSW704384670",Option("UNIT 10"),30,"ARCHER","STREET","CHATSWOOD",2067,"NSW"),
      ("GANSW716607633",Option("UNIT 1"),95,"GARDINER","ROAD","ORANGE",2800,"NSW"),
      ("GANSW704527834",None,26,"LINKS","AVENUE","CRONULLA",2230,"NSW"),
      ("GANSW704579026",None,13,"VALLEY","ROAD","DENHAMS BEACH",2536,"NSW"),
      ("GANSW712760955",Option("UNIT 17"),39,"MACARTHUR","STREET","GRIFFITH",2680,"NSW"),
      ("GANSW704356027",None,66,"MILLERS","ROAD","CATTAI",2756,"NSW"),
      ("GANSW705978672",None,74,"CANYON","DRIVE","STANHOPE GARDENS",2768,"NSW"),
      ("GANSW717662718",Option("UNIT 744"),9,"ROTHSCHILD","AVENUE","ROSEBERY",2018,"NSW"),
      ("GANSW710590397",Option("UNIT 303"),2,"DIND","STREET","MILSONS POINT",2061,"NSW")
    ).toDF("gnaf_pid", "flat_number", "number_first", "street_name", "street_type", "locality_name", "postcode", "state")
    leftDF.createOrReplaceTempView(leftView)

    val rightDF = Seq(
      (0L,"U3 59 INVERNESS AVENUE","NSW 2222 PENSHURST"),
      (1L,"74 CANYON DR", "NSW 2768 STANEHOPE GDNS.")
    ).toDF("id", "street", "state_postcode_suburb")
    rightDF.createOrReplaceTempView(rightView)

    val conf = s"""{
      "stages": [
        {
          "type": "SimilarityJoinTransform",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "leftView": "${leftView}",
          "leftFields": ["flat_number", "number_first", "street_name", "street_type", "locality_name", "postcode", "state"],
          "rightView": "${rightView}",
          "rightFields": ["street", "state_postcode_suburb"],
          "outputView": "${outputView}",
          "threshold": 0.75,
          "shingleLength": 3,
          "numHashTables": 10
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        val df = ARC.run(pipeline)(spark, logger, arcContext)
        assert(df.get.count == 1)
      }
    }

  }

  test("SimilarityJoinTransformSuite: Empty Vector") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val leftDF = Seq(
      ("GANSW705647478",Option("UNIT 3"),59,"INVERNESS","AVENUE","PENSHURST",2222,"NSW"),
      ("GANSW704384670",Option("UNIT 10"),30,"ARCHER","STREET","CHATSWOOD",2067,"NSW"),
      ("GANSW716607633",Option("UNIT 1"),95,"GARDINER","ROAD","ORANGE",2800,"NSW"),
      ("GANSW704527834",None,26,"LINKS","AVENUE","CRONULLA",2230,"NSW"),
      ("GANSW704579026",None,13,"VALLEY","ROAD","DENHAMS BEACH",2536,"NSW"),
      ("GANSW712760955",Option("UNIT 17"),39,"MACARTHUR","STREET","GRIFFITH",2680,"NSW"),
      ("GANSW704356027",None,66,"MILLERS","ROAD","CATTAI",2756,"NSW"),
      ("GANSW705978672",None,74,"CANYON","DRIVE","STANHOPE GARDENS",2768,"NSW"),
      ("GANSW717662718",Option("UNIT 744"),9,"ROTHSCHILD","AVENUE","ROSEBERY",2018,"NSW"),
      ("GANSW710590397",Option("UNIT 303"),2,"DIND","STREET","MILSONS POINT",2061,"NSW")
    ).toDF("gnaf_pid", "flat_number", "number_first", "street_name", "street_type", "locality_name", "postcode", "state")
    leftDF.createOrReplaceTempView(leftView)

    val rightDF = Seq(
      (0L,"a","a")
    ).toDF("id", "street", "state_postcode_suburb")
    rightDF.createOrReplaceTempView(rightView)

    val conf = s"""{
      "stages": [
        {
          "type": "SimilarityJoinTransform",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "leftView": "${leftView}",
          "leftFields": ["flat_number", "number_first", "street_name", "street_type", "locality_name", "postcode", "state"],
          "rightView": "${rightView}",
          "rightFields": ["street", "state_postcode_suburb"],
          "outputView": "${outputView}",
          "threshold": 0.75,
          "shingleLength": 3,
          "numHashTables": 10
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        val df = ARC.run(pipeline)(spark, logger, arcContext)
        assert(df.get.count == 0)
      }
    }

  }

  test("SimilarityJoinTransformSuite: Threshold Limit") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val leftDF = Seq(
      ("GANSW705647478",Option("UNIT 3"),59,"INVERNESS","AVENUE","PENSHURST",2222,"NSW")
    ).toDF("gnaf_pid", "flat_number", "number_first", "street_name", "street_type", "locality_name", "postcode", "state")
    leftDF.createOrReplaceTempView(leftView)

    val rightDF = Seq(
      (0L,"a","a")
    ).toDF("id", "street", "state_postcode_suburb")
    rightDF.createOrReplaceTempView(rightView)

    val conf = s"""{
      "stages": [
        {
          "type": "SimilarityJoinTransform",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "leftView": "${leftView}",
          "leftFields": ["flat_number", "number_first", "street_name", "street_type", "locality_name", "postcode", "state"],
          "rightView": "${rightView}",
          "rightFields": ["street", "state_postcode_suburb"],
          "outputView": "${outputView}",
          "threshold": 1.1,
          "shingleLength": 3,
          "numHashTables": 10
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => assert(err.toString.contains("""'threshold' 1.1 must be between 0.0 and 1.0."""))
      case Right((pipeline, _)) => fail("should fail")
    }

  }
}
