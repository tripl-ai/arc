package ai.tripl.arc

import java.net.URI
import java.sql.DriverManager

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._

import ai.tripl.arc.util._

class JDBCExecuteSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  var connection: java.sql.Connection = _

  val url = "jdbc:derby:memory:JDBCExecuteSuite"
  val dbtable = "APP.KNOWN"
  val newTable = "NEWTABLE"
  val newColumn = "${column_name}"
  val outputView = "dataset"
  var testURI = FileUtils.getTempDirectoryPath()

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

    // need to disable SecurityManager to allow a derby instance
    System.setSecurityManager(null)
    DriverManager.registerDriver(new org.apache.derby.jdbc.EmbeddedDriver())
    connection = DriverManager.getConnection(s"${url};create=true")

    // create known table
    // JDBC does not support creating a table with NullType column (understandably)
    TestUtils.getKnownDataset.drop($"nullDatum")
      .write
      .format("jdbc")
      .option("url", url)
      .option("dbtable", dbtable)
      .save()
  }

  after {
    session.stop()
    connection.close()
    // dropping table will throw a good exception by design
    // see Removing an in-memory database derby docs
    try {
      DriverManager.getConnection(s"${url};drop=true")
    } catch {
      case e: Exception =>
    }
  }

  test("JDBCExecute") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    ai.tripl.arc.execute.JDBCExecuteStage.execute(
      ai.tripl.arc.execute.JDBCExecuteStage(
        plugin=new ai.tripl.arc.execute.JDBCExecute,
        name=outputView,
        description=None,
        inputURI=new URI(testURI),
        jdbcURL = url,
        sql=s"CREATE TABLE ${newTable} (COLUMN0 VARCHAR(100) NOT NULL, PRIMARY KEY (COLUMN0))",
        params= Map.empty,
        sqlParams=Map.empty
      )
    )

    // read back to ensure execute has happened
    val actual = { spark.read
      .format("jdbc")
      .option("url", url)
      .option("dbtable", s"${newTable}")
      .load()
    }

    assert(actual.count == 0)
  }

  test("JDBCExecute: Bad serverName") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val thrown = intercept[Exception with DetailException] {
      ai.tripl.arc.execute.JDBCExecuteStage.execute(
        ai.tripl.arc.execute.JDBCExecuteStage(
          plugin=new ai.tripl.arc.execute.JDBCExecute,
          name=outputView,
          description=None,
          inputURI=new URI(testURI),
          jdbcURL = "jdbc:derby:invalid",
          sql=s"CREATE TABLE ${newTable} (COLUMN0 VARCHAR(100) NOT NULL, PRIMARY KEY (COLUMN0))",
          params= Map.empty,
          sqlParams=Map.empty
        )
      )
    }
    assert(thrown.getMessage == "java.sql.SQLException: Database 'invalid' not found.")
  }

  test("JDBCExecute: sqlParams") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    ai.tripl.arc.execute.JDBCExecuteStage.execute(
      ai.tripl.arc.execute.JDBCExecuteStage(
        plugin=new ai.tripl.arc.execute.JDBCExecute,
        name=outputView,
        description=None,
        inputURI=new URI(testURI),
        jdbcURL = url,
        sql=s"CREATE TABLE ${newTable} (${newColumn} VARCHAR(100) NOT NULL, PRIMARY KEY (COLUMN0))",
        params= Map.empty,
        sqlParams=Map("column_name" -> "COLUMN0")
      )
    )

    // read back to ensure execute has happened
    val actual = { spark.read
      .format("jdbc")
      .option("url", url)
      .option("dbtable", s"${newTable}")
      .load()
    }

    assert(actual.count == 0)
  }

  test("JDBCExecute: Bad sqlserver connection parameters") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val thrown = intercept[Exception with DetailException] {
      ai.tripl.arc.execute.JDBCExecuteStage.execute(
        ai.tripl.arc.execute.JDBCExecuteStage(
          plugin=new ai.tripl.arc.execute.JDBCExecute,
          name=outputView,
          description=None,
          inputURI=new URI(testURI),
          jdbcURL = "0.0.0.0",
          sql=s"CREATE TABLE ${newTable} (COLUMN0 VARCHAR(100) NOT NULL, PRIMARY KEY (COLUMN0))",
          params= Map.empty,
          sqlParams=Map.empty
        )
      )
    }
    assert(thrown.getMessage.contains("No suitable driver found"))
  }

  test("JDBCExecute: Bad jdbcType") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val thrown = intercept[Exception with DetailException] {
      ai.tripl.arc.execute.JDBCExecuteStage.execute(
        ai.tripl.arc.execute.JDBCExecuteStage(
          plugin=new ai.tripl.arc.execute.JDBCExecute,
          name=outputView,
          description=None,
          inputURI=new URI(testURI),
          jdbcURL = "",
          sql=s"CREATE TABLE ${newTable} (COLUMN0 VARCHAR(100) NOT NULL, PRIMARY KEY (COLUMN0))",
          params= Map.empty,
          sqlParams=Map.empty
        )
      )
    }
    assert(thrown.getMessage == "java.sql.SQLException: No suitable driver found for ")
  }

  test("JDBCExecute: Connection Params") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    val thrown = intercept[Exception with DetailException] {
      ai.tripl.arc.execute.JDBCExecuteStage.execute(
        ai.tripl.arc.execute.JDBCExecuteStage(
          plugin=new ai.tripl.arc.execute.JDBCExecute,
          name=outputView,
          description=None,
          inputURI=new URI(testURI),
          jdbcURL = "jdbc:derby:memory:JDBCExecuteSuite/connectionParamsTestDB",
          sql=s"CREATE TABLE ${newTable} (COLUMN0 VARCHAR(100) NOT NULL, PRIMARY KEY (COLUMN0))",
          params=Map.empty,
          sqlParams=Map.empty
        )
      )
    }
    assert(thrown.getMessage == "java.sql.SQLException: Database 'memory:JDBCExecuteSuite/connectionParamsTestDB' not found.")

    ai.tripl.arc.execute.JDBCExecuteStage.execute(
      ai.tripl.arc.execute.JDBCExecuteStage(
        plugin=new ai.tripl.arc.execute.JDBCExecute,
        name=outputView,
        description=None,
        inputURI=new URI(testURI),
        jdbcURL = "jdbc:derby:memory:JDBCExecuteSuite/connectionParamsTestDB",
        sql=s"CREATE TABLE ${newTable} (COLUMN0 VARCHAR(100) NOT NULL, PRIMARY KEY (COLUMN0))",
        params=Map("create" -> "true"),
        sqlParams=Map.empty
      )
    )
  }
}
