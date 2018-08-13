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

import au.com.agl.arc.util.TestDataUtils

class JDBCExtractSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  
  var connection: java.sql.Connection = _

  val url = "jdbc:derby:memory:JDBCExtractSuite"
  val dbtable = "known"
  val outputView = "dataset"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

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

  test("JDBCExtract: Table") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    // parse json schema to List[ExtractColumn]
    val cols = au.com.agl.arc.util.MetadataSchema.parseJsonMetadata(TestDataUtils.getKnownDatasetMetadataJson)    

    val actual = extract.JDBCExtract.extract(
      JDBCExtract(
        name=outputView, 
        cols=cols.right.getOrElse(Nil),
        outputView=dbtable, 
        jdbcURL=url,
        driver=DriverManager.getDriver(url),
        tableName=dbtable, 
        numPartitions=None, 
        partitionBy=Nil,
        fetchsize=None, 
        customSchema=None,
        params=Map.empty, 
        persist=false
      )
    )

    val expected = TestDataUtils.getKnownDataset.drop($"nullDatum")

    val actualExceptExpectedCount = actual.except(expected).count
    val expectedExceptActualCount = expected.except(actual).count
    if (actualExceptExpectedCount != 0 || expectedExceptActualCount != 0) {
      println("actual")
      actual.show(false)
      println("expected")
      expected.show(false)
    }
    assert(actual.except(expected).count === 0)
    assert(expected.except(actual).count === 0)

    // test metadata
    val timestampDatumMetadata = actual.schema.fields(actual.schema.fieldIndex("timestampDatum")).metadata    
    assert(timestampDatumMetadata.getLong("securityLevel") == 7)        
  }     

  test("JDBCExtract: Query") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val actual = extract.JDBCExtract.extract(
      JDBCExtract(
        name=outputView, 
        cols=Nil,
        outputView=dbtable, 
        jdbcURL=url, 
        driver=DriverManager.getDriver(url),
        tableName=s"(SELECT * FROM ${dbtable}) dbtable", 
        numPartitions=None,
        partitionBy=Nil,
        fetchsize=None, 
        customSchema=None,
        params=Map.empty, 
        persist=false
      )
    )

    val expected = TestDataUtils.getKnownDataset.drop($"nullDatum")

    val actualExceptExpectedCount = actual.except(expected).count
    val expectedExceptActualCount = expected.except(actual).count
    if (actualExceptExpectedCount != 0 || expectedExceptActualCount != 0) {
      println("actual")
      actual.show(false)
      println("expected")
      expected.show(false)
    }
    assert(actual.except(expected).count === 0)
    assert(expected.except(actual).count === 0)
  }   

  test("JDBCExtract: Query returning Empty Dataset") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val actual = extract.JDBCExtract.extract(
      JDBCExtract(
        name=outputView, 
        cols=Nil,
        outputView=dbtable, 
        jdbcURL=url, 
        driver=DriverManager.getDriver(url),
        tableName=s"(SELECT * FROM ${dbtable} WHERE false) dbtable", 
        numPartitions=None,
        partitionBy=Nil,
        fetchsize=None, 
        customSchema=None,
        params=Map.empty, 
        persist=false
      )
    )

    val expected = TestDataUtils.getKnownDataset.drop($"nullDatum")

    // data types will mismatch due to derby but test that we have at least same column names
    assert(actual.schema.map(_.name) === expected.schema.map(_.name))
  }   
}
