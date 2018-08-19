package au.com.agl.arc

import java.net.URI
import java.sql.Connection
import java.sql.DriverManager
import java.util.Properties
import java.util.UUID

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.util.log.LoggerFactory 

import au.com.agl.arc.util._
import au.com.agl.arc.util.ControlUtils._

class JDBCExtractSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  

  val sqlserverurl = "jdbc:sqlserver://localhost:1433"
  val postgresurl = "jdbc:postgresql://localhost:5432/"
  val sqlserver_db = "hyphen-database"
  val sqlserver_schema = "dbo"
  val sqlserver_table = "hyphen-table"
  val sqlserver_fullname = s"[${sqlserver_db}].${sqlserver_schema}.[${sqlserver_table}]"
  val dbtable = "output"
  val postgrestable = "target"

  val user = "sa"
  val password = "SecretPass!2018" // see docker-compose.yml for password

  val connectionProperties = new Properties()
  var connection: Connection = null


  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("FATAL")

    session = spark
    
    // early resolution of jdbc drivers or else cannot find message
    DriverManager.getDrivers

    connectionProperties.put("user", user)
    connectionProperties.put("password", password)  

    using(DriverManager.getConnection(sqlserverurl, connectionProperties)) { connection =>
      connection.createStatement.execute(s"IF NOT EXISTS(select * from sys.databases where name='${sqlserver_db}') CREATE DATABASE [${sqlserver_db}]")
    }
  }


  after {
    session.stop
  }

  test("JDBCLoad: sqlserver normal") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val dataset = TestDataUtils.getKnownDataset
    dataset.createOrReplaceTempView(dbtable)

    val expected = load.JDBCLoad.load(
      JDBCLoad(
        name="dataset",
        inputView=dbtable, 
        jdbcURL=sqlserverurl, 
        driver=DriverManager.getDriver(sqlserverurl),
        tableName=s"[${sqlserver_db}].${sqlserver_schema}.[${sqlserver_table}]", 
        partitionBy=Nil, 
        numPartitions=None, 
        isolationLevel=None,
        batchsize=None, 
        truncate=None,
        createTableOptions=None,
        createTableColumnTypes=None,        
        saveMode=Some(SaveMode.Overwrite), 
        bulkload=Option(false),
        tablock=None,
        params=Map("user" -> user, "password" -> password)
      )
    ).get

    val actual = extract.JDBCExtract.extract(
      JDBCExtract(
        name="dataset",
        cols=Nil,
        outputView=dbtable, 
        jdbcURL=sqlserverurl, 
        driver=DriverManager.getDriver(sqlserverurl),
        tableName=s"[${sqlserver_db}].${sqlserver_schema}.[${sqlserver_table}]", 
        numPartitions=None, 
        fetchsize=None,
        partitionBy=Nil,
        customSchema=None, 
        persist=false,
        params=Map("user" -> user, "password" -> password)
      )
    ).get

    assert(actual.except(expected).count === 0)
    assert(expected.except(actual).count === 0)
  }    

}
