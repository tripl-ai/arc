package ai.tripl.arc

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.node._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._

import ai.tripl.arc.util.TestUtils
import ai.tripl.arc.util.ExtractUtils
import ai.tripl.arc.util.ArcSchema
import ai.tripl.arc.util.MetadataUtils

class ArcSchemaSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  val targetFile = FileUtils.getTempDirectoryPath() + "extract.json"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark

    // ensure targets removed
    FileUtils.deleteQuietly(new java.io.File(targetFile))
  }

  after {
    session.stop()

    // clean up test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile))
  }

  test("Schema good") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val arcSchema = """[
    {
      "id" : "",
      "name" : "first_name",
      "description" : "Customer First Name",
      "type" : "string",
      "trim" : true,
      "nullable" : true,
      "nullableValues" : [ "", "null" ],
      "metadata": {
        "primaryKey" : true,
        "position": 1
      }
    }
    ]"""

    ArcSchema.parseArcSchema(arcSchema) match {
      case Left(err) => fail(err.toString)
      case Right(extractColumns) =>
        val structType = ExtractUtils.getSchema(Right(extractColumns))
        assert(structType.isDefined)
    }
  }

  test("Schema nested error - fields missing") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val arcSchema = """[
    {
      "id" : "",
      "name" : "first_name",
      "description" : "Customer First Name",
      "type" : "string",
      "trim" : true,
      "nullable" : true,
      "nullableValues" : [ "", "null" ],
      "metadata": {
        "primaryKey" : true,
        "position": 1
      }
    },
    {
      "id" : "",
      "name" : "group",
      "description" : "Customer First Name",
      "type" : "struct",
      "nullable" : true,
      "metadata": {
        "primaryKey" : true,
        "position": 1
      }
    }
    ]"""

    ArcSchema.parseArcSchema(arcSchema) match {
      case Left(err) => assert(err.toString.contains("Missing required attribute 'fields'."))
      case Right(extractColumns) => fail("should fail")
    }
  }

  test("Schema nested error - fields empty") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val arcSchema = """[
    {
      "id" : "",
      "name" : "first_name",
      "description" : "Customer First Name",
      "type" : "string",
      "trim" : true,
      "nullable" : true,
      "nullableValues" : [ "", "null" ],
      "metadata": {
        "primaryKey" : true,
        "position": 1
      }
    },
    {
      "id" : "",
      "name" : "group",
      "description" : "Customer First Name",
      "type" : "struct",
      "nullable" : true,
      "metadata": {
        "primaryKey" : true,
        "position": 1
      },
      "fields": []
    }
    ]"""

    ArcSchema.parseArcSchema(arcSchema) match {
      case Left(err) => assert(err.toString.contains("'fields' must have at least 1 element."))
      case Right(extractColumns) => fail("should fail")
    }
  }

  test("Schema nested error") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val arcSchema = """[
    {
      "id" : "",
      "name" : "first_name",
      "description" : "Customer First Name",
      "type" : "string",
      "trim" : true,
      "nullable" : true,
      "nullableValues" : [ "", "null" ],
      "metadata": {
        "primaryKey" : true,
        "position": 1
      }
    },
    {
      "id" : "",
      "name" : "group",
      "description" : "Customer First Name",
      "type" : "struct",
      "nullable" : true,
      "metadata": {
        "primaryKey" : true,
        "position": 1
      },
      "fields": [
        {
          "id" : "",
          "name" : "nested0",
          "type" : "string",
          "trim" : true,
          "nullable" : true,
          "nullableValues" : [ "", "null" ],
          "metadata": {
            "primaryKey" : true,
            "position": 1
          }
        },
        {
          "id" : "",
          "type" : "string",
          "trim" : true,
          "nullable" : true,
          "nullableValues" : [ "", "null" ],
          "metadata": {
            "primaryKey" : true,
            "position": 1
          }
        }
      ]
    }
    ]"""

    ArcSchema.parseArcSchema(arcSchema) match {
      case Left(err) => assert(err.toString.contains("Missing required attribute 'name'"))
      case Right(extractColumns) => fail("should fail")
    }
  }

  test("Schema nested valid") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val arcSchema = """[
    {
      "id" : "",
      "name" : "first_name",
      "description" : "Customer First Name",
      "type" : "string",
      "trim" : true,
      "nullable" : true,
      "nullableValues" : [ "", "null" ],
      "metadata": {
        "primaryKey" : true,
        "position": 1
      }
    },
    {
      "id" : "",
      "name" : "group",
      "description" : "Customer First Name",
      "type" : "struct",
      "nullable" : true,
      "metadata": {
        "primaryKey" : true,
        "position": 1
      },
      "fields": [
        {
          "id" : "",
          "name" : "nested0",
          "type" : "string",
          "trim" : true,
          "nullable" : true,
          "nullableValues" : [ "", "null" ],
          "metadata": {
            "primaryKey" : true,
            "position": 1
          }
        },
        {
          "id" : "",
          "name" : "nested0",
          "type" : "string",
          "trim" : true,
          "nullable" : true,
          "nullableValues" : [ "", "null" ],
          "metadata": {
            "primaryKey" : true,
            "position": 1
          }
        }
      ]
    }
    ]"""

    ArcSchema.parseArcSchema(arcSchema) match {
      case Left(err) => fail(err.toString)
      case Right(extractColumns) =>
        val structType = ExtractUtils.getSchema(Right(extractColumns))
        assert(structType.isDefined)
    }
  }

  test("Schema double nested") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val arcSchema = """[
    {
      "id" : "",
      "name" : "first_name",
      "description" : "Customer First Name",
      "type" : "string",
      "trim" : true,
      "nullable" : true,
      "nullableValues" : [ "", "null" ],
      "metadata": {
        "primaryKey" : true,
        "position": 1
      }
    },
    {
      "id" : "",
      "name" : "group",
      "description" : "Customer First Name",
      "type" : "struct",
      "nullable" : true,
      "metadata": {
        "primaryKey" : true,
        "position": 1
      },
      "fields": [
        {
          "id" : "",
          "name" : "nested0",
          "type" : "string",
          "trim" : true,
          "nullable" : true,
          "nullableValues" : [ "", "null" ],
          "metadata": {
            "primaryKey" : true,
            "position": 1
          }
        },
        {
          "id" : "",
          "name" : "group2",
          "description" : "Customer First Name",
          "type" : "struct",
          "nullable" : true,
          "metadata": {
            "primaryKey" : true,
            "position": 1
          },
          "fields": [
            {
              "id" : "",
              "name" : "nested1",
              "type" : "string",
              "trim" : true,
              "nullable" : true,
              "nullableValues" : [ "", "null" ],
              "metadata": {
                "primaryKey" : true,
                "position": 1
              }
            }
          ]
        }
      ]
    }
    ]"""

    ArcSchema.parseArcSchema(arcSchema) match {
      case Left(err) => fail(err.toString)
      case Right(extractColumns) =>
        val structType = ExtractUtils.getSchema(Right(extractColumns))
        assert(structType.isDefined)
    }
  }

  test("Schema array simple success") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val arcSchema = """[
    {
      "id" : "",
      "name" : "group",
      "description" : "Customer First Name",
      "type" : "array",
      "nullable" : true,
      "metadata": {
        "primaryKey" : true,
        "position": 1
      },
      "elementType": {
        "id" : "",
        "name" : "nested0",
        "type" : "string",
        "trim" : true,
        "nullable" : true,
        "nullableValues" : [ "", "null" ],
        "metadata": {
          "primaryKey" : true,
          "position": 1
        }
      }
    }
    ]"""

    ArcSchema.parseArcSchema(arcSchema) match {
      case Left(err) => fail(err.toString)
      case Right(extractColumns) =>
        val structType = ExtractUtils.getSchema(Right(extractColumns))
        assert(structType.isDefined)
    }
  }

 test("Schema array simple failure - elementType missing") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val arcSchema = """[
    {
      "id" : "",
      "name" : "group",
      "description" : "Customer First Name",
      "type" : "array",
      "nullable" : true,
      "metadata": {
        "primaryKey" : true,
        "position": 1
      }
    }
    ]"""

    ArcSchema.parseArcSchema(arcSchema) match {
      case Left(err) => assert(err.toString.contains("Missing required attribute 'elementType'."))
      case Right(extractColumns) => fail("should fail")
    }
  }

 test("Schema array simple failure - elementType empty") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val arcSchema = """[
    {
      "id" : "",
      "name" : "group",
      "description" : "Customer First Name",
      "type" : "array",
      "nullable" : true,
      "metadata": {
        "primaryKey" : true,
        "position": 1
      },
      "elementType": {}
    },
    ]"""

    ArcSchema.parseArcSchema(arcSchema) match {
      case Left(err) => assert(err.toString.contains("Missing required attribute 'type'."))
      case Right(extractColumns) => fail("should fail")
    }
  }

  test("Schema array complex success") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val arcSchema = """[
    {
      "id": "",
      "name": "group",
      "description": "Customer First Name",
      "type": "array",
      "nullable": true,
      "metadata": {
        "primaryKey": true,
        "position": 1
      },
      "elementType": {
        "id": "",
        "name": "group0",
        "type": "struct",
        "nullable": true,
        "metadata": {
          "primaryKey": true,
          "position": 1
        },
        "fields": [
          {
            "id": "",
            "name": "nested0",
            "type": "string",
            "trim": true,
            "nullable": true,
            "nullableValues": [
              "",
              "null"
            ],
            "metadata": {
              "primaryKey": true,
              "position": 1
            }
          }
        ]
      }
    }
    ]"""

    ArcSchema.parseArcSchema(arcSchema) match {
      case Left(err) => fail(err.toString)
      case Right(extractColumns) =>
        val structType = ExtractUtils.getSchema(Right(extractColumns))
        assert(structType.isDefined)
    }
  }

  test("Schema array complex failure - different types") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val arcSchema = """[
    {
      "id": "",
      "name": "array",
      "description": "Customer First Name",
      "type": "array",
      "nullable": true,
      "metadata": {
        "primaryKey": true,
        "position": 1
      },
      "elementType": [
        {
          "id": "",
          "name": "nested0",
          "type": "string",
          "trim": true,
          "nullable": true,
          "nullableValues": [
            "",
            "null"
          ],
          "metadata": {
            "primaryKey": true,
            "position": 1
          }
        }
      ]
    }
    ]"""

    ArcSchema.parseArcSchema(arcSchema) match {
      case Left(err) => assert(err.toString.contains("'elementType' must be of type object"))
      case Right(extractColumns) => fail("should fail")
    }
  }

  test("Schema with schema key") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val arcSchema = """{
      "schema": [
        {
          "id": "",
          "name": "first_name",
          "description": "Customer First Name",
          "type": "string",
          "trim": true,
          "nullable": true,
          "nullableValues": [
            "",
            "null"
          ],
          "metadata": {
            "primaryKey": true,
            "position": 1
          }
        }
      ]
    }"""

    ArcSchema.parseArcSchema(arcSchema) match {
      case Left(err) => fail(err.toString)
      case Right(extractColumns) =>
        val structType = ExtractUtils.getSchema(Right(extractColumns))
        assert(structType.isDefined)
    }
  }

  test("Schema without schema key") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val arcSchema = """{
      "notschema": [
        {
          "id": "",
          "name": "first_name",
          "description": "Customer First Name",
          "type": "string",
          "trim": true,
          "nullable": true,
          "nullableValues": [
            "",
            "null"
          ],
          "metadata": {
            "primaryKey": true,
            "position": 1
          }
        }
      ]
    }"""

    val thrown0 = intercept[Exception] {
      ArcSchema.parseArcSchema(arcSchema)
    }
    assert(thrown0.getMessage === "does not appear to be an Arc schema. Must be either of type LIST or contain schema within 'schema' attribute.")
  }

  test("Schema with schema key and substitution") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val arcSchema = """{
      "common": {
        "nested0": {
          "name": "nested0",
          "type": "string",
          "trim": true,
          "nullable": true,
          "nullableValues": [
            "",
            "null"
          ]
        }
      },
      "schema": [
        {
          "name": "group",
          "type": "struct",
          "nullable": true,
          "fields": [
            ${common.nested0},
            {
              "name": "nested1",
              "type": "string",
              "trim": true,
              "nullable": true,
              "nullableValues": [
                "",
                "null"
              ]
            }
          ]
        }
      ]
    }"""

    ArcSchema.parseArcSchema(arcSchema) match {
      case Left(err) => fail(err.toString)
      case Right(extractColumns) =>
        val structType = ExtractUtils.getSchema(Right(extractColumns)).get
        assert(structType.prettyJson ==
        """{
        |  "type" : "struct",
        |  "fields" : [ {
        |    "name" : "group",
        |    "type" : {
        |      "type" : "struct",
        |      "fields" : [ {
        |        "name" : "nested0",
        |        "type" : "string",
        |        "nullable" : true,
        |        "metadata" : {
        |          "internal" : false,
        |          "nullable" : true
        |        }
        |      }, {
        |        "name" : "nested1",
        |        "type" : "string",
        |        "nullable" : true,
        |        "metadata" : {
        |          "internal" : false,
        |          "nullable" : true
        |        }
        |      } ]
        |    },
        |    "nullable" : true,
        |    "metadata" : {
        |      "internal" : false,
        |      "nullable" : true
        |    }
        |  } ]
        |}""".stripMargin)
    }
  }

  test("Schema roundtrip") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val arcSchema = """[
    {
      "name" : "first_name",
      "description" : "Customer First Name",
      "type" : "string",
      "trim" : true,
      "nullable" : true,
      "nullableValues" : [ "", "null" ],
      "metadata": {
        "primaryKey" : true,
        "position": 1
      }
    },
    {
      "name" : "group",
      "description" : "Customer First Name",
      "type" : "struct",
      "nullable" : true,
      "metadata": {
        "primaryKey" : true,
        "position": 2
      },
      "fields": [
        {
          "name" : "nested0",
          "type" : "string",
          "trim" : true,
          "nullable" : true,
          "nullableValues" : [ "", "null" ],
          "metadata": {
            "primaryKey" : true,
            "position": 3
          }
        },
        {
          "name" : "nested1",
          "type" : "string",
          "trim" : true,
          "nullable" : true,
          "nullableValues" : [ "", "null" ],
          "metadata": {
            "primaryKey" : false,
            "position": 4
          }
        }
      ]
    },
    {
      "name" : "group",
      "description" : "Customer First Name",
      "type" : "array",
      "nullable" : true,
      "metadata": {
        "primaryKey" : true,
        "position": 5
      },
      "elementType": {
        "id" : "",
        "name" : "nested0",
        "type" : "string",
        "trim" : true,
        "nullable" : true,
        "nullableValues" : [ "", "null" ],
        "metadata": {
          "primaryKey" : true,
          "position": 6
        }
      }
    }
    ]"""

    ArcSchema.parseArcSchema(arcSchema) match {
      case Left(err) => fail(err.toString)
      case Right(extractColumns) =>
        val structType = ExtractUtils.getSchema(Right(extractColumns)).get
        val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], structType)
        val jsonSchema = MetadataUtils.makeMetadataFromDataframe(df)
        ArcSchema.parseArcSchema(jsonSchema) match {
          case Left(err) => fail(err.toString)
          case _ =>
        }
    }
  }

}
