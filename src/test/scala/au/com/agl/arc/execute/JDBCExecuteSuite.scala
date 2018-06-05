package au.com.agl.arc

import java.net.URI
import java.sql.DriverManager

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.util.log.LoggerFactory 

import au.com.agl.arc.util._

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
    spark.sparkContext.setLogLevel("WARN")

    session = spark
    import spark.implicits._

    // need to disable SecurityManager to allow a derby instance
    System.setSecurityManager(null)
    DriverManager.registerDriver(new org.apache.derby.jdbc.EmbeddedDriver())
    connection = DriverManager.getConnection(s"${url};create=true")

    // create known table
    // JDBC does not support creating a table with NullType column (understandably)
    TestDataUtils.getKnownDataset.drop($"nullDatum")
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
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    au.com.agl.arc.execute.JDBCExecute.execute(
      JDBCExecute(
        name=outputView, 
        inputURI=new URI(testURI), 
        sql=s"CREATE TABLE ${newTable} (COLUMN0 VARCHAR(100) NOT NULL, PRIMARY KEY (COLUMN0))", 
        params= Map("jdbcType" -> "Derby", "serverName" -> url), 
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

  test("JDBCExecute: sqlParams") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    au.com.agl.arc.execute.JDBCExecute.execute(
      JDBCExecute(
        name=outputView, 
        inputURI=new URI(testURI), 
        sql=s"CREATE TABLE ${newTable} (${newColumn} VARCHAR(100) NOT NULL, PRIMARY KEY (COLUMN0))", 
        params= Map("jdbcType" -> "Derby", "serverName" -> url), 
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

  test("JDBCExecute: Bad serverName") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val thrown = intercept[Exception with DetailException] {
      au.com.agl.arc.execute.JDBCExecute.execute(
        JDBCExecute(
          name=outputView, 
          inputURI=new URI(testURI), 
          sql=s"CREATE TABLE ${newTable} (COLUMN0 VARCHAR(100) NOT NULL, PRIMARY KEY (COLUMN0))", 
          params= Map("jdbcType" -> "Derby", "serverName" -> "jdbc:derby:invalid"), 
          sqlParams=Map.empty
        )
      )
    }
    assert(thrown.getMessage == "java.sql.SQLException: Database 'invalid' not found.")
  }  

  test("JDBCExecute: Bad jdbcType") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val thrown = intercept[Exception with DetailException] {
      au.com.agl.arc.execute.JDBCExecute.execute(
        JDBCExecute(
          name=outputView, 
          inputURI=new URI(testURI), 
          sql=s"CREATE TABLE ${newTable} (COLUMN0 VARCHAR(100) NOT NULL, PRIMARY KEY (COLUMN0))", 
          params= Map("jdbcType" -> "unknown"), 
          sqlParams=Map.empty
        )
      )
    }
    assert(thrown.getMessage == "java.lang.Exception: unknown jdbcType: 'unknown'")
  }    
}
