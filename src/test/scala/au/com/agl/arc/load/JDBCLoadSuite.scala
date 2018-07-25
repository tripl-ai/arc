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

class JDBCLoadSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  
  var connection: java.sql.Connection = _

  val url = "jdbc:derby:memory:JDBCLoadSuite"
  val dbtable = "known"
  val user = ""
  val password = ""

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("FATAL")

    session = spark

    // need to disable SecurityManager to allow a derby instance
    System.setSecurityManager(null)
    DriverManager.registerDriver(new org.apache.derby.jdbc.EmbeddedDriver())
    connection = DriverManager.getConnection(s"${url};create=true")
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

  test("JDBCLoad") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val dataset = TestDataUtils.getKnownDataset
    dataset.createOrReplaceTempView(dbtable)

    load.JDBCLoad.load(
      JDBCLoad(
        name="dataset",
        inputView=dbtable, 
        jdbcURL=url, 
        driver=DriverManager.getDriver(url),
        tableName=dbtable, 
        partitionBy=Nil, 
        numPartitions=None, 
        isolationLevel=None,
        batchsize=None, 
        truncate=None,
        createTableOptions=None,
        createTableColumnTypes=None,        
        saveMode=Some(SaveMode.Overwrite), 
        bulkload=Option(false),
        params=Map.empty 
      )
    )

    val actual = { spark.read
      .format("jdbc")
      .option("url", url)
      .option("dbtable", s"(SELECT COUNT(*) AS count FROM ${dbtable}) ${dbtable}")
      .load()
    }

    assert(actual.first.getInt(0) == 2)
  }    

  test("JDBCLoad: SQL Injection") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val known = TestDataUtils.getKnownDataset
    known.createOrReplaceTempView(dbtable)

    val thrown0 = intercept[Exception with DetailException] {
      load.JDBCLoad.load(
        JDBCLoad(
          name="dataset",
          inputView=dbtable, 
          jdbcURL=url, 
          driver=DriverManager.getDriver(url),
          tableName="dbtable; SELECT * FROM sys.sysroles", 
          partitionBy=Nil, 
          numPartitions=None, 
          isolationLevel=None,
          batchsize=None, 
          truncate=None,
          createTableOptions=None,
          createTableColumnTypes=None,          
          saveMode=Some(SaveMode.Overwrite), 
          bulkload=Option(false),
          params=Map.empty 
        )
      )
    }
    assert(thrown0.getMessage === """java.sql.SQLSyntaxErrorException: Syntax error: Encountered ";" at line 1, column 21.""")

    val thrown1 = intercept[Exception with DetailException] {
      load.JDBCLoad.load(
        JDBCLoad(
          name="dataset",
          inputView=dbtable, 
          jdbcURL=url, 
          driver=DriverManager.getDriver(url),
          tableName="dbtable WHERE 1=1", 
          partitionBy=Nil, 
          numPartitions=None, 
          isolationLevel=None,
          batchsize=None, 
          truncate=None,
          createTableOptions=None,
          createTableColumnTypes=None,
          saveMode=Some(SaveMode.Overwrite), 
          bulkload=Option(false),
          params=Map.empty 
        )
      )
    }
    assert(thrown1.getMessage === """java.sql.SQLSyntaxErrorException: Syntax error: Encountered "WHERE" at line 1, column 22.""")      
  }

  test("JDBCLoad: sqlserver bulkload") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val known = TestDataUtils.getKnownDataset
    known.createOrReplaceTempView(dbtable)

    val thrown0 = intercept[Exception with DetailException] {
      load.JDBCLoad.load(
        JDBCLoad(
          name="dataset",
          inputView=dbtable, 
          jdbcURL="jdbc:sqlserver://localhost:1433", 
          driver=DriverManager.getDriver("jdbc:sqlserver://localhost:1433"),
          tableName=dbtable, 
          partitionBy=Nil, 
          numPartitions=None, 
          isolationLevel=None,
          batchsize=None, 
          truncate=None,
          createTableOptions=None,
          createTableColumnTypes=None,          
          saveMode=Some(SaveMode.Overwrite), 
          bulkload=Option(true),
          params=Map.empty 
        )
      )
    }
    assert(thrown0.getMessage === """java.lang.Exception: tableName should contain 3 components database.schema.table currently has 1 component(s).""")

    val thrown1 = intercept[Exception with DetailException] {
      load.JDBCLoad.load(
        JDBCLoad(
          name="dataset",
          inputView=dbtable, 
          jdbcURL="jdbc:sqlserver://localhost:1433", 
          driver=DriverManager.getDriver("jdbc:sqlserver://localhost:1433"),
          tableName="mydatabase.myschema.mytable", 
          partitionBy=Nil, 
          numPartitions=None, 
          isolationLevel=None,
          batchsize=None, 
          truncate=None,
          createTableOptions=None,
          createTableColumnTypes=None,          
          saveMode=Some(SaveMode.Overwrite), 
          bulkload=Option(true),
          params=Map.empty 
        )
      )
    }
    assert(thrown1.getMessage === """com.microsoft.sqlserver.jdbc.SQLServerException: The TCP/IP connection to the host localhost, port 1433 has failed. Error: "Connection refused. Verify the connection properties. Make sure that an instance of SQL Server is running on the host and accepting TCP/IP connections at the port. Make sure that TCP connections to the port are not blocked by a firewall.".""")  
  }  
}
