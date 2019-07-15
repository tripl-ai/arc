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

class JDBCExtractSuite extends FunSuite with BeforeAndAfter {

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

  test("JDBCExtract: sqlserver normal") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val dataset = TestUtils.getKnownDataset
    dataset.createOrReplaceTempView(dbtable)

    val expected = load.JDBCLoadStage.execute(
      load.JDBCLoadStage(
        plugin=new load.JDBCLoad,
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
        tablock=true,
        params=Map("user" -> user, "password" -> password)
      )
    ).get

    val actual = extract.JDBCExtractStage.execute(
      extract.JDBCExtractStage(
        plugin=new extract.JDBCExtract,
        name="dataset",
        description=None,
        schema=Right(Nil),
        outputView=dbtable,
        jdbcURL=sqlserverurl,
        driver=DriverManager.getDriver(sqlserverurl),
        tableName=s"[${sqlserver_db}].${sqlserver_schema}.[${sqlserver_table}]",
        numPartitions=None,
        fetchsize=None,
        partitionBy=Nil,
        customSchema=None,
        persist=false,
        partitionColumn=None,
        predicates=Nil,
        params=Map("user" -> user, "password" -> password)
      )
    ).get

    assert(actual.except(expected).count === 0)
    assert(expected.except(actual).count === 0)
  }


  test("JDBCExtract: sqlserver partitionColumn") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val dataset = TestUtils.getKnownDataset
    dataset.createOrReplaceTempView(dbtable)

    val expected = load.JDBCLoadStage.execute(
      load.JDBCLoadStage(
        plugin=new load.JDBCLoad,
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
        tablock=true,
        params=Map("user" -> user, "password" -> password)
      )
    ).get

    val actual = extract.JDBCExtractStage.execute(
      extract.JDBCExtractStage(
        plugin=new extract.JDBCExtract,
        name="dataset",
        description=None,
        schema=Right(Nil),
        outputView=dbtable,
        jdbcURL=sqlserverurl,
        driver=DriverManager.getDriver(sqlserverurl),
        tableName=s"[${sqlserver_db}].${sqlserver_schema}.[${sqlserver_table}]",
        numPartitions=Option(2),
        fetchsize=None,
        partitionBy=Nil,
        customSchema=None,
        persist=false,
        partitionColumn=Option("integerDatum"),
        predicates=Nil,
        params=Map("user" -> user, "password" -> password)
      )
    ).get

    assert(actual.except(expected).count === 0)
    assert(expected.except(actual).count === 0)
  }

  test("JDBCExtract: sqlserver partitionColumn with Subquery") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val dataset = TestUtils.getKnownDataset
    dataset.createOrReplaceTempView(dbtable)

    val expected = load.JDBCLoadStage.execute(
      load.JDBCLoadStage(
        plugin=new load.JDBCLoad,
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
        tablock=true,
        params=Map("user" -> user, "password" -> password)
      )
    ).get

    val actual = extract.JDBCExtractStage.execute(
      extract.JDBCExtractStage(
        plugin=new extract.JDBCExtract,
        name="dataset",
        description=None,
        schema=Right(Nil),
        outputView=dbtable,
        jdbcURL=sqlserverurl,
        driver=DriverManager.getDriver(sqlserverurl),
        tableName=s"(SELECT * FROM [${sqlserver_db}].${sqlserver_schema}.[${sqlserver_table}]) ds",
        numPartitions=Option(2),
        fetchsize=None,
        partitionBy=Nil,
        customSchema=None,
        persist=false,
        partitionColumn=Option("integerDatum"),
        predicates=Nil,
        params=Map("user" -> user, "password" -> password)
      )
    ).get

    assert(actual.except(expected).count === 0)
    assert(expected.except(actual).count === 0)
  }

  // see docker-entrypoint-initdb.d/init.sql to define data
  test("JDBCExtract: get metadata from postgres") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val actual = extract.JDBCExtractStage.execute(
      extract.JDBCExtractStage(
        plugin=new extract.JDBCExtract,
        name="meta",
        description=None,
        schema=Right(Nil),
        outputView="meta",
        jdbcURL=postgresurl,
        driver=DriverManager.getDriver(postgresurl),
        tableName=s"(SELECT * FROM meta WHERE dataset = 'known_dataset' AND version = 0 ORDER BY index) meta",
        numPartitions=None,
        fetchsize=None,
        partitionBy=Nil,
        customSchema=None,
        persist=false,
        partitionColumn=None,
        predicates=Nil,
        params=Map("user" -> user, "password" -> password)
      )
    ).get

    // test metadata
    val meta = ai.tripl.arc.util.MetadataSchema.parseDataFrameMetadata(actual).right.getOrElse(Nil)
    val metaSchema = Extract.toStructType(meta)
    val timestampDatumMetadata = metaSchema.fields(metaSchema.fieldIndex("timestampDatum")).metadata
    assert(timestampDatumMetadata.getLong("securityLevel") == 7)

  }
}
