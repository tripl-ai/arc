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
    implicit var arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false)

    val dataset = TestDataUtils.getKnownDataset
    dataset.createOrReplaceTempView(dbtable)

    load.JDBCLoad.load(
      JDBCLoad(
        name="dataset",
        description=None,
        inputView=dbtable, 
        jdbcURL=sqlserverurl, 
        driver=DriverManager.getDriver(sqlserverurl),
        tableName=s"[${sqlserver_db}].${sqlserver_schema}.[${sqlserver_table}]", 
        partitionBy=Nil, 
        numPartitions=None, 
        isolationLevel=IsolationLevelReadCommitted,
        batchsize=1000, 
        truncate=false,
        createTableOptions=None,
        createTableColumnTypes=None,        
        saveMode=SaveMode.Overwrite, 
        bulkload=false,
        tablock=true,
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
    implicit var arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false)

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
        description=None,
        inputView=dbtable, 
        jdbcURL=sqlserverurl, 
        driver=DriverManager.getDriver(sqlserverurl),
        tableName=s"[${sqlserver_db}].${sqlserver_schema}.[${sqlserver_table}]", 
        partitionBy=Nil, 
        numPartitions=None, 
        isolationLevel=IsolationLevelReadCommitted,
        batchsize=1000, 
        truncate=false,
        createTableOptions=None,
        createTableColumnTypes=None,        
        saveMode=SaveMode.Overwrite, 
        bulkload=true,
        tablock=true,
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
    implicit var arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false)

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
        description=None,
        inputView=dbtable, 
        jdbcURL=sqlserverurl, 
        driver=DriverManager.getDriver(sqlserverurl),
        tableName=s"[${sqlserver_db}].${sqlserver_schema}.[${sqlserver_table}]", 
        partitionBy=Nil, 
        numPartitions=None, 
        isolationLevel=IsolationLevelReadCommitted,
        batchsize=1000, 
        truncate=true,
        createTableOptions=None,
        createTableColumnTypes=None,        
        saveMode=SaveMode.Append, 
        bulkload=true,
        tablock=true,
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
    implicit var arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false)

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
          description=None,
          inputView=dbtable, 
          jdbcURL=sqlserverurl, 
          driver=DriverManager.getDriver(sqlserverurl),
          tableName=s"[${sqlserver_db}].${sqlserver_schema}.[${sqlserver_table}]", 
          partitionBy=Nil, 
          numPartitions=None, 
          isolationLevel=IsolationLevelReadCommitted,
          batchsize=1000, 
          truncate=true,
          createTableOptions=None,
          createTableColumnTypes=None,        
          saveMode=SaveMode.Overwrite, 
          bulkload=true,
          tablock=true,
          params=Map("user" -> user, "password" -> password)
        )
      )
    }

    assert(thrown.getMessage.contains(s"""java.lang.Exception: Table 'dbo.[hyphen-table]' does not exist in database 'hyphen-database' and 'bulkLoad' equals 'true' so cannot continue."""))      
  }     

  test("JDBCLoad: sqlserver bulk wrong schema column names") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit var arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false)

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

    val dataset = TestDataUtils.getKnownDataset.withColumnRenamed("booleanDatum", "renamedBooleanDatum")
    dataset.createOrReplaceTempView(dbtable)

    val thrown = intercept[Exception with DetailException] {
      load.JDBCLoad.load(
        JDBCLoad(
          name="dataset",
          description=None,
          inputView=dbtable, 
          jdbcURL=sqlserverurl, 
          driver=DriverManager.getDriver(sqlserverurl),
          tableName=s"[${sqlserver_db}].${sqlserver_schema}.[${sqlserver_table}]", 
          partitionBy=Nil, 
          numPartitions=None, 
          isolationLevel=IsolationLevelReadCommitted,
          batchsize=1000, 
          truncate=false,
          createTableOptions=None,
          createTableColumnTypes=None,        
          saveMode=SaveMode.Overwrite, 
          bulkload=true,
          tablock=true,
          params=Map("user" -> user, "password" -> password)
        )
      )
    }

    assert(thrown.getMessage.contains(s"""java.lang.Exception: Input dataset 'output' has schema [renamedBooleanDatum: boolean, dateDatum: date, decimalDatum: decimal(38,18), doubleDatum: double, integerDatum: int, longDatum: bigint, stringDatum: string, timeDatum: string, timestampDatum: timestamp] which does not match target table 'dbo.[hyphen-table]' which has schema [booleanDatum: boolean, dateDatum: date, decimalDatum: decimal(38,18), doubleDatum: double, integerDatum: int, longDatum: bigint, stringDatum: string, timeDatum: string, timestampDatum: timestamp]. Ensure names, types and field orders align."""))          
  }     

  test("JDBCLoad: sqlserver bulk wrong schema column types") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit var arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false)

    try {
      connection = DriverManager.getConnection(sqlserverurl, connectionProperties)    
      connection.createStatement.execute(s"""
      DROP TABLE IF EXISTS [${sqlserver_db}].${sqlserver_schema}.[${sqlserver_table}]
      """)
      connection.createStatement.execute(s"""
      CREATE TABLE [${sqlserver_db}].${sqlserver_schema}.[${sqlserver_table}] (
        [booleanDatum] [int] NOT NULL,
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

    val dataset = TestDataUtils.getKnownDataset
    dataset.createOrReplaceTempView(dbtable)

    val thrown = intercept[Exception with DetailException] {
      load.JDBCLoad.load(
        JDBCLoad(
          name="dataset",
          description=None,
          inputView=dbtable, 
          jdbcURL=sqlserverurl, 
          driver=DriverManager.getDriver(sqlserverurl),
          tableName=s"[${sqlserver_db}].${sqlserver_schema}.[${sqlserver_table}]", 
          partitionBy=Nil, 
          numPartitions=None, 
          isolationLevel=IsolationLevelReadCommitted,
          batchsize=1000, 
          truncate=false,
          createTableOptions=None,
          createTableColumnTypes=None,        
          saveMode=SaveMode.Overwrite, 
          bulkload=true,
          tablock=true,
          params=Map("user" -> user, "password" -> password)
        )
      )
    }

    assert(thrown.getMessage.contains(s"""java.lang.Exception: Input dataset 'output' has schema [booleanDatum: boolean, dateDatum: date, decimalDatum: decimal(38,18), doubleDatum: double, integerDatum: int, longDatum: bigint, stringDatum: string, timeDatum: string, timestampDatum: timestamp] which does not match target table 'dbo.[hyphen-table]' which has schema [booleanDatum: int, dateDatum: date, decimalDatum: decimal(38,18), doubleDatum: double, integerDatum: int, longDatum: bigint, stringDatum: string, timeDatum: string, timestampDatum: timestamp]. Ensure names, types and field orders align."""))          
  }  

  test("JDBCLoad: sqlserver bulk SaveMode.Append") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit var arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false)

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
        description=None,
        inputView=dbtable, 
        jdbcURL=sqlserverurl, 
        driver=DriverManager.getDriver(sqlserverurl),
        tableName=s"[${sqlserver_db}].${sqlserver_schema}.[${sqlserver_table}]", 
        partitionBy=Nil, 
        numPartitions=None, 
        isolationLevel=IsolationLevelReadCommitted,
        batchsize=1000, 
        truncate=false,
        createTableOptions=None,
        createTableColumnTypes=None,        
        saveMode=SaveMode.Overwrite, 
        bulkload=true,
        tablock=true,
        params=Map("user" -> user, "password" -> password)
      )
    )

    load.JDBCLoad.load(
      JDBCLoad(
        name="dataset",
        description=None,
        inputView=dbtable, 
        jdbcURL=sqlserverurl, 
        driver=DriverManager.getDriver(sqlserverurl),
        tableName=s"[${sqlserver_db}].${sqlserver_schema}.[${sqlserver_table}]", 
        partitionBy=Nil, 
        numPartitions=None, 
        isolationLevel=IsolationLevelReadCommitted,
        batchsize=1000, 
        truncate=false,
        createTableOptions=None,
        createTableColumnTypes=None,        
        saveMode=SaveMode.Append, 
        bulkload=true,
        tablock=true,
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
    implicit var arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false)

    val dataset = TestDataUtils.getKnownDataset
    dataset.createOrReplaceTempView(dbtable)

    load.JDBCLoad.load(
      JDBCLoad(
        name="dataset",
        description=None,
        inputView=dbtable, 
        jdbcURL=postgresurl, 
        driver=DriverManager.getDriver(postgresurl),
        tableName=s"sa.public.${postgrestable}", 
        partitionBy=Nil, 
        numPartitions=None, 
        isolationLevel=IsolationLevelReadCommitted,
        batchsize=1000, 
        truncate=false,
        createTableOptions=None,
        createTableColumnTypes=None,        
        saveMode=SaveMode.Overwrite, 
        bulkload=false,
        tablock=true,
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

  test("JDBCLoad: Structured Streaming") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit var arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=true, ignoreEnvironments=false)

    val uuid = s"""a${UUID.randomUUID.toString.replace("-","")}"""

    try {
      connection = DriverManager.getConnection(postgresurl, connectionProperties)    
      connection.createStatement.execute(s"""
      DROP TABLE IF EXISTS sa.public.${uuid}
      """)
      connection.createStatement.execute(s"""
      CREATE TABLE sa.public.${uuid} (
        timestamp timestamp NULL,
        value bigint NOT NULL
      )
      """)
    } finally {
      connection.close
    }  

    val readStream = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", "2")
      .option("numPartitions", "3")
      .load

    readStream.createOrReplaceTempView(dbtable)    

    load.JDBCLoad.load(
      JDBCLoad(
        name="dataset",
        description=None,
        inputView=dbtable, 
        jdbcURL=postgresurl, 
        driver=DriverManager.getDriver(postgresurl),
        tableName=s"sa.public.${uuid}", 
        partitionBy=Nil, 
        numPartitions=None, 
        isolationLevel=IsolationLevelReadCommitted,
        batchsize=1000, 
        truncate=false,
        createTableOptions=None,
        createTableColumnTypes=None,        
        saveMode=SaveMode.Overwrite, 
        bulkload=false,
        tablock=true,
        params=Map("user" -> user, "password" -> password)
      )
    )

    Thread.sleep(2000)
    spark.streams.active.foreach(streamingQuery => streamingQuery.stop)

    val actual = { spark.read
      .format("jdbc")
      .option("url", postgresurl)
      .option("user", user)
      .option("password", password)      
      .option("dbtable", s"(SELECT * FROM sa.public.${uuid}) result")
      .load()
    }

    assert(actual.count > 0)
  }  

}
