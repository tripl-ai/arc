package ai.tripl.arc

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.util._
import ai.tripl.arc.util.log.LoggerFactory

import ai.tripl.arc.util.TestUtils

class XMLExtractSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  val targetFile = FileUtils.getTempDirectoryPath() + "extract.xml"
  val targetFileGlob = FileUtils.getTempDirectoryPath() + "ex{t,a,b,c}ract.xml"
  val emptyDirectory = FileUtils.getTempDirectoryPath() + "empty.xml"
  val emptyWildcardDirectory = FileUtils.getTempDirectoryPath() + "*.xml.gz"
  val inputView = "dataset"
  val outputView = "dataset"

  val xsdSchemaValid = getClass.getResource("/conf/xml/shiporder_good.xsd").toString
  val xsdSchemaInvalid = getClass.getResource("/conf/xml/shiporder_bad.xsd").toString
  val xmlRecordValid = getClass.getResource("/conf/xml/shiporder_good.xml").toString
  val xmlRecordInvalid = getClass.getResource("/conf/xml/shiporder_bad.xml").toString

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark

    // recreate test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile))
    FileUtils.deleteQuietly(new java.io.File(emptyDirectory))
    FileUtils.forceMkdir(new java.io.File(emptyDirectory))

    // force com.sun.xml.* implementation for writing xml to be compatible with spark-xml library
    System.setProperty("javax.xml.stream.XMLOutputFactory", "com.sun.xml.internal.stream.XMLOutputFactoryImpl")
    // XML will silently drop NullType on write
    TestUtils.getKnownDataset.write.option("rowTag", "testRow").format("com.databricks.spark.xml").save(targetFile)
  }

  after {
    session.stop()

    // clean up test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile))
    FileUtils.deleteQuietly(new java.io.File(emptyDirectory))
  }

  test("XMLExtract: end-to-end") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val conf = s"""{
      "stages": [
        {
          "type": "XMLExtract",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "${targetFileGlob}",
          "outputView": "${outputView}"
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        val df = ARC.run(pipeline)(spark, logger, arcContext).get
        assert(df.count == 2)
      }
    }
  }

  test("XMLExtract: end-to-end with schema") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val conf = s"""{
      "stages": [
        {
          "type": "XMLExtract",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "${targetFileGlob}",
          "outputView": "${outputView}",
          "schemaURI": "${getClass.getResource("/conf/schema/").toString}/knownDatasetXML.json"
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        val df = ARC.run(pipeline)(spark, logger, arcContext).get

        val expectedSchema = """|{
        |  "type" : "struct",
        |  "fields" : [ {
        |    "name" : "testRow",
        |    "type" : {
        |      "type" : "struct",
        |      "fields" : [ {
        |        "name" : "booleanDatum",
        |        "type" : "boolean",
        |        "nullable" : false,
        |        "metadata" : {
        |          "booleanMeta" : true,
        |          "nullable" : false,
        |          "internal" : false,
        |          "private" : false,
        |          "stringArrayMeta" : [ "string0", "string1" ],
        |          "doubleArrayMeta" : [ 0.141, 0.52 ],
        |          "description" : "booleanDatum",
        |          "longMeta" : 10,
        |          "securityLevel" : 0,
        |          "doubleMeta" : 0.141,
        |          "id" : "982cbf60-7ba7-4e50-a09b-d8624a5c49e6",
        |          "stringMeta" : "string",
        |          "booleanArrayMeta" : [ true, false ],
        |          "longArrayMeta" : [ 10, 20 ]
        |        }
        |      }, {
        |        "name" : "dateDatum",
        |        "type" : "date",
        |        "nullable" : true,
        |        "metadata" : {
        |          "nullable" : true,
        |          "internal" : false,
        |          "private" : true,
        |          "description" : "dateDatum",
        |          "securityLevel" : 3,
        |          "id" : "0e8109ba-1000-4b7d-8a4c-b01bae07027f"
        |        }
        |      }, {
        |        "name" : "decimalDatum",
        |        "type" : "decimal(38,18)",
        |        "nullable" : true,
        |        "metadata" : {
        |          "nullable" : true,
        |          "internal" : false,
        |          "private" : true,
        |          "description" : "decimalDatum",
        |          "securityLevel" : 2,
        |          "id" : "9712c383-22d1-44a6-9ca2-0087af4857f1"
        |        }
        |      }, {
        |        "name" : "doubleDatum",
        |        "type" : "double",
        |        "nullable" : true,
        |        "metadata" : {
        |          "nullable" : true,
        |          "internal" : false,
        |          "private" : true,
        |          "description" : "doubleDatum",
        |          "securityLevel" : 8,
        |          "id" : "31541ea3-5b74-4753-857c-770bd601c35b"
        |        }
        |      }, {
        |        "name" : "integerDatum",
        |        "type" : "integer",
        |        "nullable" : true,
        |        "metadata" : {
        |          "nullable" : true,
        |          "internal" : false,
        |          "private" : true,
        |          "description" : "integerDatum",
        |          "securityLevel" : 10,
        |          "id" : "a66f3bbe-d1c6-44c7-b096-a4be59fdcd78"
        |        }
        |      }, {
        |        "name" : "longDatum",
        |        "type" : "long",
        |        "nullable" : true,
        |        "metadata" : {
        |          "nullable" : true,
        |          "internal" : false,
        |          "private" : false,
        |          "description" : "longDatum",
        |          "securityLevel" : 0,
        |          "id" : "1c0eec1d-17cd-45da-8744-7a9ef5b8b086"
        |        }
        |      }, {
        |        "name" : "stringDatum",
        |        "type" : "string",
        |        "nullable" : true,
        |        "metadata" : {
        |          "nullable" : true,
        |          "internal" : false,
        |          "private" : false,
        |          "description" : "stringDatum",
        |          "securityLevel" : 0,
        |          "id" : "9712c383-22d1-44a6-9ca2-0087af4857f1"
        |        }
        |      }, {
        |        "name" : "timeDatum",
        |        "type" : "string",
        |        "nullable" : true,
        |        "metadata" : {
        |          "nullable" : true,
        |          "internal" : false,
        |          "private" : true,
        |          "description" : "timeDatum",
        |          "securityLevel" : 8,
        |          "id" : "eb17a18e-4664-4016-8beb-cd2a492d4f20"
        |        }
        |      }, {
        |        "name" : "timestampDatum",
        |        "type" : "timestamp",
        |        "nullable" : true,
        |        "metadata" : {
        |          "nullable" : true,
        |          "internal" : false,
        |          "private" : true,
        |          "description" : "timestampDatum",
        |          "securityLevel" : 7,
        |          "id" : "8e42c8f0-22a8-40db-9798-6dd533c1de36"
        |        }
        |      } ]
        |    },
        |    "nullable" : true,
        |    "metadata" : {
        |      "nullable" : true,
        |      "internal" : false,
        |      "description" : "testrow",
        |      "primaryKey" : true,
        |      "id" : "",
        |      "position" : 1
        |    }
        |  }, {
        |    "name" : "_filename",
        |    "type" : "string",
        |    "nullable" : false,
        |    "metadata" : {
        |      "internal" : true
        |    }
        |  }, {
        |    "name" : "_index",
        |    "type" : "integer",
        |    "nullable" : true,
        |    "metadata" : {
        |      "internal" : true
        |    }
        |  } ]
        |}""".stripMargin

        assert(df.schema.prettyJson == expectedSchema)

        val expected = TestUtils.getKnownDataset
          .drop(col("nullDatum"))

        val internal = df.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)
        val actual = df.drop(internal:_*).select("testRow.*")

        assert(TestUtils.datasetEquality(expected, actual))
      }
    }
  }

  test("XMLExtract: end-to-end with dynamic text input") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    spark.sql(s"""
    SELECT 'b' AS notValue, '${targetFileGlob}' AS value
    """).createOrReplaceTempView(inputView)


    val conf = s"""{
      "stages": [
        {
          "type": "TextExtract",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputView": "${inputView}",
          "outputView": "${outputView}",
          "multiLine": true
        },
        {
          "type": "XMLExtract",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputView": "${outputView}",
          "inputField": "value",
          "outputView": "${outputView}",
          "schemaURI": "${getClass.getResource("/conf/schema/").toString}/knownDatasetXML.json"
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        val df = ARC.run(pipeline)(spark, logger, arcContext).get

        val expected = TestUtils.getKnownDataset
          .drop(col("nullDatum"))

        val internal = df.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)
        val actual = df.drop(internal:_*).select("testRow.*")

        assert(TestUtils.datasetEquality(expected, actual))
      }
    }
  }


  test("XMLExtract: Caching") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    // no cache
    extract.XMLExtractStage.execute(
      extract.XMLExtractStage(
        plugin=new extract.XMLExtract,
        name=outputView,
        description=None,
        schema=Right(Nil),
        outputView=outputView,
        input=Right(targetFile),
        inputField=None,
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=true,
        xsd=None
      )
    )
    assert(spark.catalog.isCached(outputView) === false)

    // cache
    extract.XMLExtractStage.execute(
      extract.XMLExtractStage(
        plugin=new extract.XMLExtract,
        name=outputView,
        description=None,
        schema=Right(Nil),
        outputView=outputView,
        input=Right(targetFile),
        inputField=None,
        authentication=None,
        params=Map.empty,
        persist=true,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=true,
        xsd=None
      )
    )
    assert(spark.catalog.isCached(outputView) === true)
  }

  test("XMLExtract: Empty Dataset") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val schema =
      BooleanColumn(
        None,
        name="booleanDatum",
        description=None,
        nullable=true,
        nullReplacementValue=None,
        trim=false,
        nullableValues=Nil,
        trueValues=Nil,
        falseValues=Nil,
        metadata=None
      ) :: Nil

    // try with wildcard
    val thrown0 = intercept[Exception with DetailException] {
      extract.XMLExtractStage.execute(
        extract.XMLExtractStage(
          plugin=new extract.XMLExtract,
          name=outputView,
          description=None,
          schema=Right(Nil),
          outputView=outputView,
          input=Right(emptyWildcardDirectory),
          inputField=None,
          authentication=None,
          params=Map.empty,
          persist=false,
          numPartitions=None,
          partitionBy=Nil,
          contiguousIndex=true,
          xsd=None
        )
      )
    }
    assert(thrown0.getMessage.contains("*.xml.gz' does not exist and no schema has been provided to create an empty dataframe."))

    // try without providing column metadata
    val thrown1 = intercept[Exception with DetailException] {
      extract.XMLExtractStage.execute(
        extract.XMLExtractStage(
          plugin=new extract.XMLExtract,
          name=outputView,
          description=None,
          schema=Right(Nil),
          outputView=outputView,
          input=Right(emptyDirectory),
          inputField=None,
          authentication=None,
          params=Map.empty,
          persist=false,
          numPartitions=None,
          partitionBy=Nil,
          contiguousIndex=true,
          xsd=None
        )
      )
    }
    assert(thrown1.getMessage.contains("empty.xml' does not contain any fields and no schema has been provided to create an empty dataframe."))

    // try with column
    val dataset = extract.XMLExtractStage.execute(
      extract.XMLExtractStage(
        plugin=new extract.XMLExtract,
        name=outputView,
        description=None,
        schema=Right(schema),
        outputView=outputView,
        input=Right(emptyDirectory),
        inputField=None,
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=true,
        xsd=None
      )
    ).get

    val expected = TestUtils.getKnownDataset.select($"booleanDatum").limit(0)

    val internal = dataset.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)
    val actual = dataset.drop(internal:_*)

    assert(TestUtils.datasetEquality(expected, actual))
  }

  test("XMLExtract: xsd validation positive") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val conf = s"""{
      "stages": [
        {
          "type": "XMLExtract",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "${xmlRecordValid}",
          "xsdURI": "${xsdSchemaValid}",
          "outputView": "shiporder",
          "persist": false
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        val df = ARC.run(pipeline)(spark, logger, arcContext).get
      }
    }
  }

  test("XMLExtract: xsd validation negative") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val conf = s"""{
      "stages": [
        {
          "type": "XMLExtract",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "${xmlRecordInvalid}",
          "xsdURI": "${xsdSchemaValid}",
          "outputView": "shiporder",
          "persist": false
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        val thrown0 = intercept[Exception with DetailException] {
          ARC.run(pipeline)(spark, logger, arcContext)
        }
        assert(thrown0.getMessage.contains("'one' is not a valid value for 'integer'"))
      }
    }
  }

  test("XMLExtract: invalid xsd") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val conf = s"""{
      "stages": [
        {
          "type": "XMLExtract",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "${xmlRecordValid}",
          "xsdURI": "${xsdSchemaInvalid}",
          "outputView": "shiporder",
          "persist": false
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => assert(err.toString.contains("""The prefix "xs" for element "xs:element" is not bound."""))
      case Right((pipeline, _)) => fail("should throw error")
    }
  }
}