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

  // minio seems to need ip address not hostname
  val bucketName = "bucket0"
  val minioHostPort = "http://minio:9000"
  val minioAccessKey = "AKIAIOSFODNN7EXAMPLE"
  val minioSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
  val targetFile = s"s3a://${bucketName}/akc_breed_info.csv"

  var expected: String = _

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Arc Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    // only set default aws provider override if not provided
    if (Option(spark.sparkContext.hadoopConfiguration.get("fs.s3a.aws.credentials.provider")).isEmpty) {
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", CloudUtils.defaultAWSProvidersOverride)
    }

    expected = spark.read.option("wholetext", true).text(getClass.getResource("/minio/it/bucket0/akc_breed_info.csv").toString).first.getString(0)

    UDF.registerUDFs()(spark, logger, arcContext)
  }

  after {
    session.stop()
  }

  test("UDFSuite: get_uri - batch") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    // test multiple extension types
    for (extension <- Seq("", ".gz", ".gzip", ".bz2", ".bzip2", ".lz4")) {
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
            "authentication": {
              "method": "AmazonAccessKey",
              "accessKeyID": "${minioAccessKey}",
              "secretAccessKey": "${minioSecretKey}",
              "endpoint": "${minioHostPort}"
            },
            "sql": "SELECT DECODE(GET_URI('${targetFile}${extension}'), 'UTF-8')",
            "outputView": "outputView"
          }
        ]
      }"""

      val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)
      pipelineEither match {
        case Left(err) => fail(err.toString)
        case Right((pipeline, _)) => {
          val df = ARC.run(pipeline).get
          assert(df.first.getString(0) == expected)
        }
      }
    }
  }

  test("UDFSuite: get_uri - batch glob") {
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
          "authentication": {
            "method": "AmazonAccessKey",
            "accessKeyID": "${minioAccessKey}",
            "secretAccessKey": "${minioSecretKey}",
            "endpoint": "${minioHostPort}"
          },
          "sql": "SELECT DECODE(GET_URI('s3a://${bucketName}/{a,b,c}kc_breed_info.csv'), 'UTF-8')",
          "outputView": "outputView",
          "persist": true
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)
    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        val df = ARC.run(pipeline).get
        assert(df.first.getString(0) == expected)
      }
    }
  }

  test("UDFSuite: get_uri - batch binary") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val expected = spark.sqlContext.sparkContext.binaryFiles(getClass.getResource("/minio/it/bucket0/puppy.jpg").toString).map { case (_, portableDataStream) => portableDataStream.toArray }.collect.head

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
          "authentication": {
            "method": "AmazonAccessKey",
            "accessKeyID": "${minioAccessKey}",
            "secretAccessKey": "${minioSecretKey}",
            "endpoint": "${minioHostPort}"
          },
          "sql": "SELECT GET_URI('s3a://${bucketName}/puppy.jpg')",
          "outputView": "outputView",
          "persist": true
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)
    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        val df = ARC.run(pipeline).get
        assert(df.first.getAs[Array[Byte]](0).deep == expected.deep)
      }
    }
  }

  // nyc taxi data no longer available
  // test("UDFSuite: get_uri - batch remote s3a://") {
  //   implicit val spark = session
  //   implicit val logger = TestUtils.getLogger()
  //   implicit val arcContext = TestUtils.getARCContext()

  //   val conf = s"""{
  //     "stages": [
  //       {
  //         "type": "SQLTransform",
  //         "name": "test",
  //         "description": "test",
  //         "environments": [
  //           "production",
  //           "test"
  //         ],
  //         "authentication": {
  //           "method": "AmazonAnonymous"
  //         },
  //         "sql": "SELECT DECODE(GET_URI('s3a://nyc-tlc/trip*data/green_tripdata_2013-08.csv'), 'UTF-8')",
  //         "outputView": "outputView",
  //         "persist": true
  //       }
  //     ]
  //   }"""

  //   val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)
  //   pipelineEither match {
  //     case Left(err) => fail(err.toString)
  //     case Right((pipeline, _)) => {
  //       val df = ARC.run(pipeline).get
  //       assert(df.first.getString(0).startsWith("VendorID,lpep_pickup_datetime"))
  //     }
  //   }
  // }

  test("UDFSuite: get_uri - batch remote https://") {
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
          "sql": "SELECT DECODE(GET_URI('https://raw.githubusercontent.com/tripl-ai/arc/master/src/it/resources/minio/it/bucket0/akc_breed_info.csv'), 'UTF-8')",
          "outputView": "outputView",
          "persist": true
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)
    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        val df = ARC.run(pipeline).get
        assert(df.first.getString(0) == expected)
      }
    }
  }

  test("UDFSuite: get_uri - streaming") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=true)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", CloudUtils.defaultAWSProvidersOverride)
    CloudUtils.setHadoopConfiguration(Some(Authentication.AmazonAccessKey(None, minioAccessKey, minioSecretKey, Some(minioHostPort), None)))
    arcContext.serializableConfiguration = new SerializableConfiguration(spark.sparkContext.hadoopConfiguration)

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
      Thread.sleep(5000)
      assert(df.count > 0)

      assert(df.first.getString(0).contains("Breed,height_low_inches"))
    } finally {
      writeStream.stop
    }
  }

}