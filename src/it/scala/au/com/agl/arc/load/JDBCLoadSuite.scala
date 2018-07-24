package au.com.agl.arc

import java.net.URI
import java.sql.DriverManager
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
  val dbtable = s"a${UUID.randomUUID.toString.take(8)}"
  val user = "sa"
  val password = "SecretPass2018" // see docker-compose.yml for password

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
        params=Map("user" -> user, "password" -> password)
      )
    )

    val actual = { spark.read
      .format("jdbc")
      .option("url", sqlserverurl)
      .option("user", user)
      .option("password", password)
      .option("dbtable", s"(SELECT COUNT(*) AS count FROM ${dbtable}) ${dbtable}")
      .load()
    }

    assert(actual.first.getInt(0) == 2)
  }    

  test("JDBCLoad: sqlserver bulk") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val dataset = TestDataUtils.getKnownDataset
    dataset.createOrReplaceTempView(dbtable)

    val thrown = intercept[Exception with DetailException] {
      load.JDBCLoad.load(
        JDBCLoad(
          name="dataset",
          inputView=dbtable, 
          jdbcURL=sqlserverurl, 
          driver=DriverManager.getDriver(sqlserverurl),
          tableName=s"master.dbo.${dbtable}", 
          partitionBy=Nil, 
          numPartitions=None, 
          isolationLevel=None,
          batchsize=None, 
          truncate=None,
          createTableOptions=None,
          createTableColumnTypes=None,        
          saveMode=None, 
          bulkload=Option(true),
          params=Map("user" -> user, "password" -> password)
        )
      )
    }
    assert(thrown.getMessage.contains("""but source has 2 records and target has 4 records"""))  

    val actual = { spark.read
      .format("jdbc")
      .option("url", sqlserverurl)
      .option("user", user)
      .option("password", password)      
      .option("dbtable", s"(SELECT COUNT(*) AS count FROM master.dbo.[${dbtable}]) result")
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
        params=Map("user" -> user, "password" -> password)
      )
    )

    val actual = { spark.read
      .format("jdbc")
      .option("url", postgresurl)
      .option("user", user)
      .option("password", password)
      .option("dbtable", s"(SELECT COUNT(*) AS count FROM ${dbtable}) ${dbtable}")
      .load()
    }

    assert(actual.first.getLong(0) == 2)
  } 

}
