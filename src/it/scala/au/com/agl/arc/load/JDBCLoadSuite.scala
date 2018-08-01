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

class JDBCLoadSuite extends FunSuite with BeforeAndAfter {

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
  val password = "SecretPass2018" // see docker-compose.yml for password
  var connection: Connection = null

  val connectionProperties = new Properties()


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

    try {
      connection = DriverManager.getConnection(sqlserverurl, connectionProperties)    
      connection.createStatement.execute(s"IF NOT EXISTS(select * from sys.databases where name='${sqlserver_db}') CREATE DATABASE [${sqlserver_db}]")
    } finally {
      connection.close
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

    load.JDBCLoad.load(
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
    )

    val actual = { spark.read
      .format("jdbc")
      .option("url", sqlserverurl)
      .option("user", user)
      .option("password", password)
      .option("dbtable", s"(SELECT COUNT(*) AS count FROM [${sqlserver_db}].${sqlserver_schema}.[${sqlserver_table}]) result")
      .load()
    }

    assert(actual.first.getInt(0) == 2)
  }    

  test("JDBCLoad: sqlserver bulk SaveMode.Overwrite") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val dataset = TestDataUtils.getKnownDataset
    dataset.createOrReplaceTempView(dbtable)

    try {
      connection = DriverManager.getConnection(sqlserverurl, connectionProperties)    
      connection.createStatement.execute(s"""
      DROP TABLE IF EXISTS [${sqlserver_db}].${sqlserver_schema}.[${sqlserver_table}]
      """)
      connection.createStatement.execute(s"""
      CREATE TABLE [${sqlserver_db}].${sqlserver_schema}.[${sqlserver_table}] (
        [booleanDatum] [bit] NOT NULL,
        [dateDatum] [date] NULL,
        [decimalDatum] [decimal](38, 18) NULL,
        [doubleDatum] [float] NOT NULL,
        [integerDatum] [int] NULL,
        [longDatum] [bigint] NOT NULL,
        [stringDatum] [nvarchar](max) NULL,
        [timeDatum] [nvarchar](max) NULL,
        [timestampDatum] [datetime] NULL
      )
      """)
    } catch {
      case e: Exception =>
    } finally {
      connection.close
    }    

    load.JDBCLoad.load(
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
        saveMode=None, 
        bulkload=Option(true),
        tablock=None,
        params=Map("user" -> user, "password" -> password)
      )
    )

    val actual = { spark.read
      .format("jdbc")
      .option("url", sqlserverurl)
      .option("user", user)
      .option("password", password)      
      .option("dbtable", s"(SELECT COUNT(*) AS count FROM [${sqlserver_db}].${sqlserver_schema}.[${sqlserver_table}]) result")
      .load()
    }

    assert(actual.first.getInt(0) == 2)
  }     

  test("JDBCLoad: sqlserver bulk SaveMode.Overwrite truncate") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val dataset = TestDataUtils.getKnownDataset
    dataset.createOrReplaceTempView(dbtable)

    try {
      connection = DriverManager.getConnection(sqlserverurl, connectionProperties)    
      connection.createStatement.execute(s"""
      DROP TABLE IF EXISTS [${sqlserver_db}].${sqlserver_schema}.[${sqlserver_table}]
      """)
      connection.createStatement.execute(s"""
      CREATE TABLE [${sqlserver_db}].${sqlserver_schema}.[${sqlserver_table}] (
        [booleanDatum] [bit] NOT NULL,
        [dateDatum] [date] NULL,
        [decimalDatum] [decimal](38, 18) NULL,
        [doubleDatum] [float] NOT NULL,
        [integerDatum] [int] NULL,
        [longDatum] [bigint] NOT NULL,
        [stringDatum] [nvarchar](max) NULL,
        [timeDatum] [nvarchar](max) NULL,
        [timestampDatum] [datetime] NULL
      )
      """)
    } catch {
      case e: Exception =>
    } finally {
      connection.close
    }     

    load.JDBCLoad.load(
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
        truncate=Option(true),
        createTableOptions=None,
        createTableColumnTypes=None,        
        saveMode=Option(SaveMode.Append), 
        bulkload=Option(true),
        tablock=None,
        params=Map("user" -> user, "password" -> password)
      )
    )

    val actual = { spark.read
      .format("jdbc")
      .option("url", sqlserverurl)
      .option("user", user)
      .option("password", password)      
      .option("dbtable", s"(SELECT COUNT(*) AS count FROM [${sqlserver_db}].${sqlserver_schema}.[${sqlserver_table}]) result")
      .load()
    }

    assert(actual.first.getInt(0) == 2)
  }     

  test("JDBCLoad: sqlserver bulk no table") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val dataset = TestDataUtils.getKnownDataset
    dataset.createOrReplaceTempView(dbtable)

    try {
      connection = DriverManager.getConnection(sqlserverurl, connectionProperties)    
      connection.createStatement.execute(s"""
      DROP TABLE IF EXISTS [${sqlserver_db}].${sqlserver_schema}.[${sqlserver_table}]
      """)
    } catch {
      case e: Exception =>
    } finally {
      connection.close
    }     

    val thrown = intercept[Exception with DetailException] {
      load.JDBCLoad.load(
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
          truncate=Option(true),
          createTableOptions=None,
          createTableColumnTypes=None,        
          saveMode=None, 
          bulkload=Option(true),
          tablock=None,
          params=Map("user" -> user, "password" -> password)
        )
      )
    }
    assert(thrown.getMessage.contains(s"""java.lang.Exception: Table '[${sqlserver_db}].${sqlserver_schema}.[${sqlserver_table}]' does not exist and 'bulkLoad' equals 'true' so cannot continue."""))      
  }     


  test("JDBCLoad: sqlserver bulk SaveMode.Append") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val dataset = TestDataUtils.getKnownDataset
    dataset.createOrReplaceTempView(dbtable)

    try {
      connection = DriverManager.getConnection(sqlserverurl, connectionProperties)    
      connection.createStatement.execute(s"""
      DROP TABLE IF EXISTS [${sqlserver_db}].${sqlserver_schema}.[${sqlserver_table}]
      """)
      connection.createStatement.execute(s"""
      CREATE TABLE [${sqlserver_db}].${sqlserver_schema}.[${sqlserver_table}] (
        [booleanDatum] [bit] NOT NULL,
        [dateDatum] [date] NULL,
        [decimalDatum] [decimal](38, 18) NULL,
        [doubleDatum] [float] NOT NULL,
        [integerDatum] [int] NULL,
        [longDatum] [bigint] NOT NULL,
        [stringDatum] [nvarchar](max) NULL,
        [timeDatum] [nvarchar](max) NULL,
        [timestampDatum] [datetime] NULL
      )
      """)
    } catch {
      case e: Exception =>
    } finally {
      connection.close
    }  

    load.JDBCLoad.load(
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
        saveMode=Option(SaveMode.Overwrite), 
        bulkload=Option(true),
        tablock=None,
        params=Map("user" -> user, "password" -> password)
      )
    )

    load.JDBCLoad.load(
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
        saveMode=Option(SaveMode.Append), 
        bulkload=Option(true),
        tablock=None,
        params=Map("user" -> user, "password" -> password)
      )
    )

    val actual = { spark.read
      .format("jdbc")
      .option("url", sqlserverurl)
      .option("user", user)
      .option("password", password)      
      .option("dbtable", s"(SELECT COUNT(*) AS count FROM [${sqlserver_db}].${sqlserver_schema}.[${sqlserver_table}]) result")
      .load()
    }

    assert(actual.first.getInt(0) == 4)
  }          

  test("JDBCLoad: postgres normal") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val dataset = TestDataUtils.getKnownDataset
    dataset.createOrReplaceTempView(dbtable)

    load.JDBCLoad.load(
      JDBCLoad(
        name="dataset",
        inputView=dbtable, 
        jdbcURL=postgresurl, 
        driver=DriverManager.getDriver(postgresurl),
        tableName=s"sa.public.${postgrestable}", 
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
    )

    val actual = { spark.read
      .format("jdbc")
      .option("url", postgresurl)
      .option("user", user)
      .option("password", password)
      .option("dbtable", s"(SELECT COUNT(*) AS count FROM ${postgrestable}) result")
      .load()
    }

    assert(actual.first.getLong(0) == 2)
  } 

}
