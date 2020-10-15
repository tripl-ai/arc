package ai.tripl.arc

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.commons.lang3.exception.ExceptionUtils
import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ai.tripl.arc.util._
import ai.tripl.arc.api._
import ai.tripl.arc.api.API._

import ai.tripl.arc.util.TestUtils

class ARCSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  val jobFileGood = getClass.getResource("/conf/no_dependency_pipeline_good.conf").toString
  val jobFileBad = getClass.getResource("/conf/no_dependency_pipeline_bad.conf").toString

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

    // replace security manager with one that catches sys.exit and returns exception
    System.setSecurityManager(new NoExitSecurityManager())
    session = spark
  }

  after {
    System.setSecurityManager(null)
    session.stop()
  }

  test("ARCSuite: missing etl.config.uri") {
    val inMemoryLoggerAppender = new InMemoryLoggerAppender()
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger(Some(inMemoryLoggerAppender))

    try {
      ARC.main(Array())
      fail("expected exception")
    } catch {
      case e: ExitOKException => fail("expected exception")
      case e: ExitErrorException => {
        assert(inMemoryLoggerAppender.getResult.contains("No config defined as a command line argument --etl.config.uri or ETL_CONF_URI environment variable."))
      }
    }
  }

  test("ARCSuite: missing etl.config.environment") {
    val inMemoryLoggerAppender = new InMemoryLoggerAppender()
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger(Some(inMemoryLoggerAppender))

    try {
      ARC.main(Array(s"--etl.config.uri=${jobFileGood}"))
      fail("expected exception")
    } catch {
      case e: ExitOKException => fail("expected exception")
      case e: ExitErrorException => {
        assert(inMemoryLoggerAppender.getResult.contains("No environment defined as a command line argument --etl.config.environment or ETL_CONF_ENV environment variable."))
      }
    }
  }

  test("ARCSuite: end-to-end success") {
    val inMemoryLoggerAppender = new InMemoryLoggerAppender()
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger(Some(inMemoryLoggerAppender))

    try {
      ARC.main(Array(s"--etl.config.uri=${jobFileGood}", "--etl.config.environment=production"))
      fail("expected exception")
    } catch {
      case e: ExitOKException => assert(true)
      case e: ExitErrorException => fail("expected exception")
    }
    assert(inMemoryLoggerAppender.getResult.split("\n").filter { message => message.contains("\"event\":\"exit\"") && message.contains("\"type\":\"SQLTransform\"") }.length == 1)
  }

  test("ARCSuite: end-to-end lintOnly good") {
    val inMemoryLoggerAppender = new InMemoryLoggerAppender()
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger(Some(inMemoryLoggerAppender))

    try {
      ARC.main(Array(s"--etl.config.uri=${jobFileGood}", "--etl.config.environment=production", "--etl.config.lintOnly=true"))
      fail("expected exception")
    } catch {
      case e: ExitOKException => {
        assert(inMemoryLoggerAppender.getResult.split("\n").filter { message => message.contains("\"event\":\"exit\"") && message.contains("\"type\":\"SQLTransform\"") }.length == 0)
        assert(inMemoryLoggerAppender.getResult.split("\n").filter { message => message.contains("\"event\":\"exit\"") && message.contains("\"status\":\"success\"") && message.contains("\"lintOnly\":true") }.length == 1)
      }
      case e: ExitErrorException => fail("expected exception")
    }
  }

  test("ARCSuite: end-to-end lintOnly bad") {
    val inMemoryLoggerAppender = new InMemoryLoggerAppender()
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger(Some(inMemoryLoggerAppender))

    try {
      ARC.main(Array(s"--etl.config.uri=${jobFileBad}", "--etl.config.environment=production", "--etl.config.lintOnly=true"))
      fail("expected exception")
    } catch {
      case e: ExitOKException => fail("expected exception")
      case e: ExitErrorException => {
        assert(inMemoryLoggerAppender.getResult.split("\n").filter { message => message.contains("\"event\":\"exit\"") && message.contains("\"status\":\"failure\"") && message.contains("\"lintOnly\":true") && message.contains("FROM 'key' AS key") }.length == 1)
      }
    }
  }

}

