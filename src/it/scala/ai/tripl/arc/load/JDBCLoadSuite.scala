package ai.tripl.arc

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

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.util._
import ai.tripl.arc.util.ControlUtils._

class JDBCLoadSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _

  val sqlserverurl = "jdbc:sqlserver://sqlserver:1433"
  val postgresurl = "jdbc:postgresql://postgres:5432/"
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
    spark.sparkContext.setLogLevel("INFO")
    implicit val logger = TestUtils.getLogger()

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

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
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(dropUnsupported=true)

    val dataset = TestUtils.getKnownDataset
    dataset.createOrReplaceTempView(dbtable)

    load.JDBCLoadStage.execute(
      load.JDBCLoadStage(
        plugin=new load.JDBCLoad,
        id=None,
        name="dataset",
        description=None,
        inputView=dbtable,
        jdbcURL=sqlserverurl,
        driver=DriverManager.getDriver(sqlserverurl),
        tableName=s"[${sqlserver_db}].${sqlserver_schema}.[${sqlserver_table}]",
        partitionBy=Nil,
        numPartitions=None,
        isolationLevel=IsolationLevel.ReadCommitted,
        batchsize=1000,
        truncate=false,
        createTableOptions=None,
        createTableColumnTypes=None,
        saveMode=SaveMode.Overwrite,
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

  test("JDBCLoad: postgres normal") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(dropUnsupported=true)

    val dataset = TestUtils.getKnownDataset
    dataset.createOrReplaceTempView(dbtable)

    load.JDBCLoadStage.execute(
      load.JDBCLoadStage(
        plugin=new load.JDBCLoad,
        id=None,
        name="dataset",
        description=None,
        inputView=dbtable,
        jdbcURL=postgresurl,
        driver=DriverManager.getDriver(postgresurl),
        tableName=s"sa.public.${postgrestable}",
        partitionBy=Nil,
        numPartitions=None,
        isolationLevel=IsolationLevel.ReadCommitted,
        batchsize=1000,
        truncate=false,
        createTableOptions=None,
        createTableColumnTypes=None,
        saveMode=SaveMode.Overwrite,
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
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

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

    load.JDBCLoadStage.execute(
      load.JDBCLoadStage(
        plugin=new load.JDBCLoad,
        id=None,
        name="dataset",
        description=None,
        inputView=dbtable,
        jdbcURL=postgresurl,
        driver=DriverManager.getDriver(postgresurl),
        tableName=s"sa.public.${uuid}",
        partitionBy=Nil,
        numPartitions=None,
        isolationLevel=IsolationLevel.ReadCommitted,
        batchsize=1000,
        truncate=false,
        createTableOptions=None,
        createTableColumnTypes=None,
        saveMode=SaveMode.Overwrite,
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
