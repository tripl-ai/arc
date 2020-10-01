package ai.tripl.arc

import java.net.URI

import scala.io.Source
import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper

import com.typesafe.config._

import org.apache.spark.sql._

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.config.Error
import ai.tripl.arc.config.Error._
import ai.tripl.arc.util.log.LoggerFactory
import ai.tripl.arc.transform.SQLTransformStage
import ai.tripl.arc.plugins.pipeline.LazyEvaluatorStage
import ai.tripl.arc.util._

class ConfigUtilsSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _

  val outputView = "outputView"
  val targetFile = getClass.getResource("/conf/").toString

  before {
    val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .appName("Arc Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark
  }

  after {
    session.stop()
  }

  // This test loops through the /src/test/resources/docs_resources directory and tries to parse each file as a config
  // the same config files are used (embedded) on the documentation site so this ensures the examples will work.
  test("Read documentation config files") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    var commandLineArguments = Map[String, String]("JOB_RUN_DATE" -> "0", "ETL_CONF_BASE_URL" -> "")
    implicit val arcContext = TestUtils.getARCContext(commandLineArguments=commandLineArguments)

    val resourcesDir = getClass.getResource("/docs_resources/").getPath

    // this is used to test that vertexExists works with predfefined tables like from a hive metastore
    val emptyDF = spark.emptyDataFrame
    emptyDF.createOrReplaceTempView("customer")

    for (filename <- TestUtils.getListOfFiles(resourcesDir)) {
      val fileContents = Source.fromFile(filename).mkString

      // inject a stage to register the 'customer' view to stop downstream stages breaking
      val conf = s"""{"stages": [${fileContents.trim}]}"""

      // replace sql directory with config so that the examples read correctly but have resource to validate
      val sqlConf = conf.replaceAll("hdfs://datalake/sql/", getClass.getResource("/conf/sql/").toString)

      // replace ml directory with config so that the examples read correctly but have resource to validate
      val mlConf = sqlConf.replaceAll("hdfs://datalake/ml/", getClass.getResource("/conf/ml/").toString)

      // replace meta directory with config so that the examples read correctly but have resource to validate
      val metaConf = mlConf.replaceAll("hdfs://datalake/schema/", getClass.getResource("/conf/schema/").toString)

      // replace job directory with config so that the examples read correctly but have resource to validate
      val jobConf = metaConf.replaceAll("hdfs://datalake/job/", getClass.getResource("/conf/job/").toString)

      try {
        val pipelineEither = ArcPipeline.parseConfig(Left(jobConf), arcContext)

        pipelineEither match {
          case Left(errors) => {
            assert(false, s"Error in config ${filename}: ${Error.pipelineErrorMsg(errors)}")
          }
          case Right(pipeline) => {
            assert(true)
          }
        }
      } catch {
        case e: Exception => {
          println(s"error in: ${filename}\nerror: ${e}\ncontents: ${jobConf}")
          assert(false)
        }
      }
    }
  }

  test("Test missing keys exception") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val conf = """{
      "stages": [
        {
          "type": "DelimitedExtract",
          "name": "file extract",
          "environments": [
            "production",
            "test"
          ]
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(stageError) => {
        assert(stageError ==
        StageError(0, "file extract",3,List(
            ConfigError("inputURI", None, "Missing required attribute 'inputURI'.")
            ,ConfigError("outputView", None, "Missing required attribute 'outputView'.")
          )
        ) :: Nil)
      }
      case Right(_) => assert(false)
    }
  }

  test("Test extraneous attributes") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val conf = """{
      "stages": [
        {
          "type": "DelimitedExtract",
          "name": "file extract 0",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "/tmp/test.csv",
          "outputView": "output",
        },
        {
          "type": "DelimitedExtract",
          "name": "file extract 1",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "/tmp/test.csv",
          "outputVew": "output",
          "nothinglikeanything": false
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(stageError) => {
        assert(stageError ==
        StageError(1, "file extract 1",13,List(
            ConfigError("outputView", None, "Missing required attribute 'outputView'.")
            ,ConfigError("nothinglikeanything", Some(22), "Invalid attribute 'nothinglikeanything'.")
            ,ConfigError("outputVew", Some(21), "Invalid attribute 'outputVew'. Perhaps you meant one of: ['outputView'].")
          )
        ) :: Nil)
      }
      case Right(_) => assert(false)
    }
  }

  test("Test invalid validValues") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val conf = """{
      "stages": [
        {
          "type": "DelimitedExtract",
          "name": "file extract",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "/tmp/test.csv",
          "outputView": "output",
          "delimiter": "abc"
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(stageError) => {
        assert(stageError == StageError(0, "file extract",3,List(ConfigError("delimiter", Some(12), "Invalid value. Valid values are ['Comma','Pipe','DefaultHive','Custom']."))) :: Nil)
      }
      case Right(_) => assert(false)
    }
  }

  test("Test read custom delimiter") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val conf = """{
      "stages": [
        {
          "type": "DelimitedExtract",
          "name": "file extract",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "/tmp/test.csv",
          "outputView": "output",
          "delimiter": "Custom"
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(stageError) => {
        assert(stageError == StageError(0, "file extract",3,List(ConfigError("customDelimiter", None, "Missing required attribute 'customDelimiter'."))) :: Nil)
      }
      case Right(_) => assert(false)
    }
  }

  test("Test read custom delimiter success") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val conf = """{
      "stages": [
        {
          "type": "DelimitedExtract",
          "name": "file extract",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "/tmp/test.csv",
          "outputView": "output",
          "delimiter": "Custom",
          "customDelimiter": "%"
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        pipeline.stages(0) match {
          case s: extract.DelimitedExtractStage => assert(s.settings.customDelimiter == "%")
          case _ => assert(false)
        }
      }
    }
  }

  test("Test config substitutions") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val conf = """{
      "common": {
        "environments": [
            "production",
            "test"
        ],
        "name": "foo"
      },
      "stages": [
        {
          "environments": ${common.environments}
          "type": "RateExtract",
          "name": ${common.name},
          "outputView": "stream",
          "rowsPerSecond": 1,
          "rampUpTime": 1,
          "numPartitions": 1
        }
      ]
    }"""


    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        assert(pipeline.stages(0).name == "foo")
      }
    }
  }

  test("Test not List[Object]") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val conf = """{
      "stages":
      {
        "type": "RateExtract",
        "name": "RateExtract",
        "environments": [
          "production",
          "test"
        ],
        "outputView": "stream",
        "rowsPerSecond": 1,
        "rampUpTime": 1,
        "numPartitions": 1
      }
    }"""


    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(errors) => {
        assert(errors.toString contains "Expected stages to be a List of Objects")
      }
      case Right((pipeline, _)) => {
        assert(false)
      }
    }
  }

  // this test reads a pipeline of sqltransforms which depend on the previous stage being run (including subpiplines)
  // this is to ensure that the stages are executed in the correct order
  test("Test read correct order") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val df = TestUtils.getKnownDataset
    df.createOrReplaceTempView("start")

    val pipelineEither = ArcPipeline.parseConfig(Right(new URI("classpath://conf/pipeline.conf")), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        ARC.run(pipeline)
        assert(spark.sql("SELECT * FROM stage4").count == 2)
      }
    }

  }

  test("Test throw error with corrupt") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val df = TestUtils.getKnownDataset
    df.createOrReplaceTempView("start")

    val thrown0 = intercept[Exception] {
      val pipelineEither = ArcPipeline.parseConfig(Right(new URI("classpath://conf/broken.conf")), arcContext)
    }
    assert(thrown0.getMessage === "Key 'stages' missing from job configuration. Have keys: [a.stages].")
  }

  test("Test config watermark negative") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val conf = """{
      "stages": [
        {
          "type": "DelimitedExtract",
          "name": "file extract 1",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "/tmp/test.csv",
          "outputView": "output",
          "watermark": {
          }
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(errors) => {
        assert(errors.toString contains "Watermark requires 'eventTime' parameter")
      }
      case Right((_, _)) => {
        assert(false)
      }
    }
  }

  test("Test config watermark positive") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val conf = """{
      "stages": [
        {
          "type": "DelimitedExtract",
          "name": "file extract 1",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "/tmp/test.csv",
          "outputView": "output",
          "watermark": {
            "eventTime": "timecolumn",
            "delayThreshold": "1 hour"
          }
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        val stage = pipeline.stages(0).asInstanceOf[extract.DelimitedExtractStage]
        assert(stage.watermark.get.eventTime === "timecolumn")
        assert(stage.watermark.get.delayThreshold === "1 hour")
      }
    }
  }

  // this test verifies ipynb policy works
  test("Test read .ipynb policy") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(ipynb=false)

    val thrown0 = intercept[Exception] {
      val pipelineEither = ArcPipeline.parseConfig(Right(new URI("classpath://conf/python3.ipynb")), arcContext)
    }
    assert(thrown0.getMessage.contains("Support for IPython Notebook Configuration Files (.ipynb) for configuration 'classpath://conf/python3.ipynb' has been disabled by policy."))
  }

  // this test verifies ipynb to job conversion works
  test("Test read .ipynb positive") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val pipelineEither = ArcPipeline.parseConfig(Right(new URI("classpath://conf/job.ipynb")), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, arcCtx)) => {
        assert(arcCtx.activeLifecyclePlugins.length == 1)
        assert(arcCtx.dynamicConfigurationPlugins.length == 1)
        assert(pipeline.stages.length == 2)
        assert(pipeline.stages(0).asInstanceOf[extract.RateExtractStage].outputView == "stream")
        assert(pipeline.stages(1).asInstanceOf[extract.RateExtractStage].outputView == "stream2")
      }
    }
  }

  // this test verifies ipynb to job conversion works
  test("Test read .ipynb negative") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val thrown0 = intercept[Exception] {
      val pipelineEither = ArcPipeline.parseConfig(Right(new URI("classpath://conf/python3.ipynb")), arcContext)
    }
    assert(thrown0.getMessage.contains("does not appear to be a valid arc notebook. Has kernelspec: 'python3'."))
  }

  // this test verifies inline sql policy
  test("Test inlinesql policy") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(inlineSQL=false,commandLineArguments=Map[String,String]("INPUTVIEW_ARGUMENT" -> "stream0"))

    val pipelineEither = ArcPipeline.parseConfig(Right(new URI("classpath://conf/inlinesql.ipynb")), arcContext)

    pipelineEither match {
      case Left(err) => assert(err.toString.contains("Inline SQL (use of the 'sql' attribute) has been disabled by policy. SQL statements must be supplied via files located at 'inputURI'."))
      case Right((pipeline, arcCtx)) => fail()
    }
  }

  // this test verifies ipynb for inline sql
  test("Test read .ipynb inlinesql") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(commandLineArguments=Map[String,String]("INPUTVIEW_ARGUMENT" -> "stream0"))

    val pipelineEither = ArcPipeline.parseConfig(Right(new URI("classpath://conf/inlinesql.ipynb")), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, arcCtx)) => {
        assert(arcCtx.activeLifecyclePlugins.length == 1)
        assert(arcCtx.dynamicConfigurationPlugins.length == 1)
        assert(pipeline.stages.length == 5)
        assert(pipeline.stages(0).asInstanceOf[extract.RateExtractStage].outputView == "stream0")
        val sqlTransformStage0 = pipeline.stages(1).asInstanceOf[transform.SQLTransformStage]
        assert(sqlTransformStage0.outputView == "stream1")
        assert(sqlTransformStage0.sql == "SELECT *\nFROM ${inputView}")
        assert(sqlTransformStage0.sqlParams == Map[String, String]("inputView" -> "stream0"))
        assert(pipeline.stages(2).asInstanceOf[extract.RateExtractStage].outputView == "stream2")
        val sqlValidateStage0 = pipeline.stages(3).asInstanceOf[validate.SQLValidateStage]
        assert(sqlValidateStage0.sql == "SELECT\n  TRUE AS valid\n  ,\"${message}\" AS message")
        assert(sqlValidateStage0.sqlParams == Map[String, String]("message" -> "stream0"))
        val logExecuteStage0 = pipeline.stages(4).asInstanceOf[ai.tripl.arc.execute.LogExecuteStage]
        assert(logExecuteStage0.sql == "SELECT\n  \"${message}\" AS message")
        assert(logExecuteStage0.sqlParams == Map[String, String]("message" -> "stream0"))
      }
    }
  }

  test("Test read authentication AmazonIAM with KMS") {

    val authConf = """{
      "authentication": {
        "method": "AmazonIAM",
        "encryptionAlgorithm": "SSE-KMS",
        "kmsArn": "586E7EA1-845F-41D5-A2F6-9B72A4A76243"
      }
    }"""

    implicit val etlConf = ConfigFactory.parseString(authConf, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))

    val auth = ConfigUtils.readAuthentication("authentication")

    auth match {
      case Right(Some(Authentication.AmazonIAM(None, encType, arn, customKey))) => {
        assert(encType == Some(AmazonS3EncryptionType.SSE_KMS))
        assert(arn == Some("586E7EA1-845F-41D5-A2F6-9B72A4A76243"))
        assert(customKey == None)
      }
      case _ => fail("unable to read AmazonIAM auth config")
    }
  }

  test("Test config s3 deprecation") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val conf = """{
      "stages": [
        {
          "type": "DelimitedExtract",
          "name": "file extract 1",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "s3://tmp/test.csv",
          "outputView": "output"
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(errors) => {
        assert(errors.toString contains "s3:// and s3n:// are no longer supported. Please use s3a:// instead.")
      }
      case Right((_, _)) => {
        assert(false)
      }
    }
  }

  // this test verifies ipynb with hiveconf
  test("Test read .ipynb with ${hiveconf:}") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val targetFile = getClass.getResource("/conf/hive_variable.ipynb").toString
    val file = spark.read.option("wholetext", true).text(targetFile)
    val conf = ConfigUtils.readIPYNB(None, file.first.getString(0))
    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        assert(pipeline.stages(0).asInstanceOf[SQLTransformStage].sql == "SELECT ${hiveconf:test_variable}")
      }
    }
  }

  test("ConfigUtils: LazyEvaluator") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val conf = s"""{
      "stages": [
        {
          "resolution": "lazy",
          "type": "DelimitedExtract",
          "name": "file extract 1",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "${targetFile}"$${FILE_NAME},
          "outputView": "${outputView}"
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString())
      case Right((pipeline, arcCtx)) => {

        val thrown0 = intercept[Exception with DetailException] {
          val df = ARC.run(pipeline)(spark, logger, arcCtx)
        }
        assert(thrown0.getMessage.contains("Could not resolve substitution to a value: ${FILE_NAME}"))

        // replace the config
        val commandLineArgumentsJson = new ObjectMapper().writeValueAsString(Map[String, String]("FILE_NAME" -> "simple.conf").asJava).replace("\\", "")
        val commandLineArgumentsConf = ConfigFactory.parseString(commandLineArgumentsJson, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))
        arcCtx.resolutionConfig = commandLineArgumentsConf.resolveWith(arcCtx.resolutionConfig)
        val df = ARC.run(pipeline)(spark, logger, arcCtx).get
        assert(df.count == 29)
      }
    }
  }

  test("ConfigUtils: LazyEvaluator - invalid value") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val conf = s"""{
      "stages": [
        {
          "resolution": "lazee",
          "type": "DelimitedExtract",
          "name": "file extract 1",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "${targetFile}"$${FILE_NAME},
          "outputView": "${outputView}"
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => assert(ai.tripl.arc.config.Error.pipelineErrorMsg(err).contains("- resolution (Line 4): Invalid value. Valid values are ['strict','lazy']"))
      case Right((pipeline, arcCtx)) => fail("expected error")
    }
  }

  test("ConfigUtils: LazyEvaluator - ipynb") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    {
      val targetFile0 = getClass.getResource("/conf/lazy_job_fail.ipynb").toString
      val file = spark.read.option("wholetext", true).text(targetFile0)
      val conf = ConfigUtils.readIPYNB(None, file.first.getString(0))
      val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)
      pipelineEither match {
        case Left(err) => {
          println(err)
          assert(err.toString.contains("Could not resolve substitution to a value: ${LAZY_PARAMETER}"))
        }
        case Right((_, _)) => fail("should fail")
      }
    }

    {
      val targetFile = getClass.getResource("/conf/lazy_job_pass.ipynb").toString
      val file = spark.read.option("wholetext", true).text(targetFile)
      val conf = ConfigUtils.readIPYNB(None, file.first.getString(0))
      val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)
      pipelineEither match {
        case Left(err) => fail(err.toString)
        case Right((pipeline, _)) => {
          assert(pipeline.stages(1).isInstanceOf[LazyEvaluatorStage])
        }
      }
    }
  }


}
