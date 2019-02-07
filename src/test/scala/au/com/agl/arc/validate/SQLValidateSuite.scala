package au.com.agl.arc

import java.net.URI

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

import au.com.agl.arc.util.TestDataUtils

class SQLValidateSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  
  var testName = ""
  var testURI = FileUtils.getTempDirectoryPath()
  val signature = "SQLValidate requires query to return 1 row with [outcome: boolean, message: string] signature."

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    session = spark
  }

  after {
    session.stop()
  }

  test("SQLValidate: true, null") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    validate.SQLValidate.validate(
      SQLValidate(
        name=testName, 
        description=None,
        inputURI=new URI(testURI),
        sql="SELECT true, null",
        sqlParams=Map.empty,
        params=Map.empty
      )
    )
  }  

  test("SQLValidate: true, string") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    validate.SQLValidate.validate(
      SQLValidate(
        name=testName, 
        description=None,
        inputURI=new URI(testURI),
        sql="SELECT true, 'message'",
        sqlParams=Map.empty,
        params=Map.empty
      )
    )
  }   

  test("SQLValidate: true, json") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    validate.SQLValidate.validate(
      SQLValidate(
        name=testName, 
        description=None,
        inputURI=new URI(testURI),
        sql="""SELECT true, '{"stringKey": "stringValue", "numKey": 123}'""",
        sqlParams=Map.empty,
        params=Map.empty
      )
    )
  }    

  test("SQLValidate: false, null") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val thrown = intercept[Exception with DetailException]  {
      validate.SQLValidate.validate(
        SQLValidate(
          name=testName, 
          description=None,
          inputURI=new URI(testURI),
          sql="SELECT false, null",
          sqlParams=Map.empty,
          params=Map.empty
        )
      )
    }
    assert(thrown.getMessage === "SQLValidate failed with message: 'null'.")
  }  

  test("SQLValidate: false, string") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val thrown = intercept[Exception with DetailException]  {
      validate.SQLValidate.validate(
        SQLValidate(
          name=testName, 
          description=None,
          inputURI=new URI(testURI),
          sql="SELECT false, 'this is my message'",
          sqlParams=Map.empty,
          params=Map.empty
        )
      )
    }
    assert(thrown.getMessage === "SQLValidate failed with message: 'this is my message'.")
  }

  test("SQLValidate: false, json") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val thrown = intercept[Exception with DetailException] {
      validate.SQLValidate.validate(
        SQLValidate(
          name=testName, 
          description=None,
          inputURI=new URI(testURI),
          sql="""SELECT false, TO_JSON(NAMED_STRUCT('stringKey', 'stringValue', 'numKey', 123))""",
          sqlParams=Map.empty,
          params=Map.empty
        )
      )
    }

    assert(thrown.getMessage === """SQLValidate failed with message: '{"stringKey":"stringValue","numKey":123}'.""")
  }  

  test("SQLValidate: string, boolean") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val thrown = intercept[Exception with DetailException]  {
      validate.SQLValidate.validate(
        SQLValidate(
          name=testName, 
          description=None,
          inputURI=new URI(testURI),
          sql="SELECT 'string', true",
          sqlParams=Map.empty,
          params=Map.empty
        )
      )
    }
    assert(thrown.getMessage === s"${signature} Query returned 1 rows of type [string, boolean].")
  } 

  test("SQLValidate: rows != 1") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val thrown0 = intercept[Exception with DetailException]  {
      validate.SQLValidate.validate(
        SQLValidate(
          name=testName, 
          description=None,
          inputURI=new URI(testURI),
          sql="SELECT true, 'message' WHERE false",
          sqlParams=Map.empty,
          params=Map.empty
        )
      )
    }
    assert(thrown0.getMessage === s"${signature} Query returned 0 rows of type [boolean, string].")

    val thrown1 = intercept[Exception with DetailException]  {
      validate.SQLValidate.validate(
        SQLValidate(
          name=testName, 
          description=None,
          inputURI=new URI(testURI),
          sql="SELECT true, 'message' UNION ALL SELECT true, 'message'",
          sqlParams=Map.empty,
          params=Map.empty
        )
      )
    }
    assert(thrown1.getMessage === s"${signature} Query returned 2 rows of type [boolean, string].")
  }  

  test("SQLValidate: columns != 2") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val thrown0 = intercept[Exception with DetailException]  {
      validate.SQLValidate.validate(
        SQLValidate(
          name=testName, 
          description=None,
          inputURI=new URI(testURI),
          sql="SELECT true",
          sqlParams=Map.empty,
          params=Map.empty
        )
      )
    }
    assert(thrown0.getMessage === s"${signature} Query returned 1 rows of type [boolean].")

    val thrown1 = intercept[Exception with DetailException]  {
      validate.SQLValidate.validate(
        SQLValidate(
          name=testName, 
          description=None,
          inputURI=new URI(testURI),
          sql="SELECT true, 'message', true",
          sqlParams=Map.empty,
          params=Map.empty
        )
      )
    }
    assert(thrown1.getMessage === s"${signature} Query returned 1 rows of type [boolean, string, boolean].")
  }    

  test("SQLValidate: sqlParams") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    validate.SQLValidate.validate(
      SQLValidate(
        name=testName, 
        description=None,
        inputURI=new URI(testURI),
        sql="""SELECT 0.1 > ${threshold}, 'message'""",
        sqlParams=Map("threshold" -> "0.05"),
        params=Map.empty
      )
    )

    val thrown = intercept[Exception with DetailException]  {
      validate.SQLValidate.validate(
        SQLValidate(
          name=testName, 
          description=None,
          inputURI=new URI(testURI),
          sql="""SELECT 0.01 > ${threshold}, 'message'""",
          sqlParams=Map("threshold" -> "0.05"),
          params=Map.empty
        )
      )
    }
    assert(thrown.getMessage === s"SQLValidate failed with message: 'message'.")
  } 

  test("SQLValidate: No rows") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val thrown = intercept[Exception with DetailException]  {
      validate.SQLValidate.validate(
        SQLValidate(
          name=testName, 
          description=None,
          inputURI=new URI(testURI),
          sql="SELECT CAST(NULL AS BOOLEAN), CAST(NULL AS STRING)",
          sqlParams=Map.empty,
          params=Map.empty
        )
      )
    }
    assert(thrown.getMessage === s"SQLValidate requires query to return 1 row with [outcome: boolean, message: string] signature. Query returned [null, null].")    
  }


}