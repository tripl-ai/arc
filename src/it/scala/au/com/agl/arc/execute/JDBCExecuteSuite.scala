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

  val sqlserverurl = "jdbc:sqlserver://localhost:1433"
  val dbtable = "sys_views"
  val outputView = "dataset"
  var testURI = FileUtils.getTempDirectoryPath()
  val user = "sa"
  val password = "SecretPass!2018" // see docker-compose.yml for password  

  before {
    implicit val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.ui.port", "9999")
      .appName("Spark ETL Test")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")       

    session = spark
    import spark.implicits._

    // early resolution of jdbc drivers or else cannot find message
    DriverManager.getDrivers    
  }

  after {
    session.stop
  }

  test("JDBCExecute: sqlserver succeed") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val transaction = s"""
    |SET XACT_ABORT ON;
    |
    |BEGIN TRANSACTION;
    |
    |DROP TABLE IF EXISTS master.dbo.sys_views;
    |
    |SELECT * 
    |INTO master.dbo.sys_views
    |FROM INFORMATION_SCHEMA.VIEWS;
    |
    |COMMIT;
    """.stripMargin

    au.com.agl.arc.execute.JDBCExecute.execute(
      JDBCExecute(
        name=outputView, 
        description=None,
        inputURI=new URI(testURI), 
        jdbcURL=sqlserverurl,
        user=Option(user),
        password=Option(password),
        sql=transaction, 
        params=Map.empty, 
        sqlParams=Map.empty
      )
    )

    // read back to ensure execute has happened
    val actual = { spark.read
      .format("jdbc")
      .option("url", sqlserverurl)
      .option("user", user)
      .option("password", password)
      .option("dbtable", s"${dbtable}")
      .load()
    }

    assert(actual.count == 1)
  }

  test("JDBCExecute: sqlserver failure statement") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val transaction = s"""
    |SET XACT_ABORT ON;
    |
    |SELECT * 
    |INTO master.dbo.sys_failure
    |FROM INFORMATION_SCHEMA.DOES_NOT_EXIST;
    """.stripMargin

    val thrown = intercept[Exception with DetailException] {
      au.com.agl.arc.execute.JDBCExecute.execute(
        JDBCExecute(
          name=outputView, 
          description=None,
          inputURI=new URI(testURI), 
          jdbcURL=sqlserverurl,
          user=Option(user),
          password=Option(password),
          sql=transaction, 
          params=Map.empty, 
          sqlParams=Map.empty
        )
      )
    }
    assert(thrown.getMessage.contains("""Invalid object name 'INFORMATION_SCHEMA.DOES_NOT_EXIST'"""))  
  }
}
