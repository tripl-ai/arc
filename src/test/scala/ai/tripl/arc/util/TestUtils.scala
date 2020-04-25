package ai.tripl.arc.util

import java.io.File
import java.sql.Date
import java.sql.Timestamp
import java.time.{ZoneId, ZonedDateTime}
import java.util.ServiceLoader
import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.core._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import ai.tripl.arc.api.API.ARCContext
import ai.tripl.arc.plugins._
import ai.tripl.arc.util.log.LoggerFactory
import org.apache.log4j.{Level, Logger}

case class KnownData(
    booleanDatum: Boolean,
    dateDatum: Date,
    decimalDatum: Decimal,
    doubleDatum: Double,
    integerDatum: Integer,
    longDatum: Long,
    stringDatum: String,
    timeDatum: String,
    timestampDatum: Timestamp,
    nullDatum: Null
)

object TestUtils {
    def getLogger()(implicit spark: SparkSession): ai.tripl.arc.util.log.logger.Logger = {
        val loader = ai.tripl.arc.util.Utils.getContextOrSparkClassLoader
        val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("breeze").setLevel(Level.ERROR)
        logger
    }

    def getARCContext(isStreaming: Boolean, environment: String = "test", commandLineArguments: Map[String,String] = Map[String,String](), ipynb: Boolean = true, inlineSQL: Boolean = true)(implicit spark: SparkSession): ARCContext = {
      val loader = ai.tripl.arc.util.Utils.getContextOrSparkClassLoader

      ARCContext(
        jobId=None,
        jobName=None,
        environment=Option(environment),
        environmentId=None,
        configUri=None,
        isStreaming=isStreaming,
        ignoreEnvironments=false,
        commandLineArguments=commandLineArguments,
        storageLevel=StorageLevel.MEMORY_AND_DISK_SER,
        immutableViews=false,
        ipynb=ipynb,
        inlineSQL=inlineSQL,
        dynamicConfigurationPlugins=ServiceLoader.load(classOf[DynamicConfigurationPlugin], loader).iterator().asScala.toList,
        lifecyclePlugins=ServiceLoader.load(classOf[LifecyclePlugin], loader).iterator().asScala.toList,
        activeLifecyclePlugins=Nil,
        pipelineStagePlugins=ServiceLoader.load(classOf[PipelineStagePlugin], loader).iterator().asScala.toList,
        udfPlugins=ServiceLoader.load(classOf[UDFPlugin], loader).iterator().asScala.toList,
        serializableConfiguration=new SerializableConfiguration(spark.sparkContext.hadoopConfiguration),
        userData=collection.mutable.Map.empty
      )
    }

    def datasetEquality(expected: DataFrame, actual: DataFrame)(implicit spark: SparkSession): Boolean = {
        import spark.implicits._

        // if both are empty ignore
        if (expected.count != 0 || actual.count != 0) {

            val expectedHashDF = expected.withColumn("_hashLeft", sha2(to_json(struct(expected.columns.sorted.map(col):_*)),512))
            val actualHashDF = actual.withColumn("_hashRight", sha2(to_json(struct(actual.columns.sorted.map(col):_*)),512))

            val transformedDF = expectedHashDF
                .joinWith(actualHashDF, expectedHashDF("_hashLeft") === actualHashDF("_hashRight"), "full")
                .withColumnRenamed("_1", "expected")
                .withColumnRenamed("_2", "actual")

            transformedDF.persist

            val expectedExceptActual = transformedDF.filter(col("actual").isNull)
            val actualExceptExpected = transformedDF.filter(col("expected").isNull)
            val expectedExceptActualCount = expectedExceptActual.count
            val actualExceptExpectedCount = actualExceptExpected.count

            if (expectedExceptActualCount != 0 || actualExceptExpectedCount != 0) {
                println("EXPECTED")
                println(expected.schema)
                expected.show(false)
                println("ACTUAL")
                println(actual.schema)
                actual.show(false)

                transformedDF.unpersist
                false
            } else {
                true
            }
        } else {
            true
        }
    }

    def getKnownDataset()(implicit spark: SparkSession): DataFrame = {
        import spark.implicits._

        val dataset = Seq(
            KnownData(booleanDatum=true, dateDatum=Date.valueOf("2016-12-18"), decimalDatum=Decimal(54.321, 10, 3), doubleDatum=42.4242, integerDatum=17, longDatum=1520828868, stringDatum="test,breakdelimiter", timestampDatum=Timestamp.from(ZonedDateTime.of(2017, 12, 20, 21, 46, 54, 0, ZoneId.of("UTC")).toInstant), timeDatum="12:34:56", nullDatum=null),
            KnownData(booleanDatum=false, dateDatum=Date.valueOf("2016-12-19"), decimalDatum=Decimal(12.345, 10, 3), doubleDatum=21.2121, integerDatum=34, longDatum=1520828123, stringDatum="breakdelimiter,test", timestampDatum=Timestamp.from(ZonedDateTime.of(2017, 12, 29, 17, 21, 49, 0, ZoneId.of("UTC")).toInstant), timeDatum="23:45:16", nullDatum=null)
        )

        dataset.toDF
    }

    // modified dataset for DiffTransform test
    def getKnownAlteredDataset()(implicit spark: SparkSession): DataFrame = {
        import spark.implicits._

        val dataset = Seq(
            // same first row
            KnownData(booleanDatum=true, dateDatum=Date.valueOf("2016-12-18"), decimalDatum=Decimal(54.321, 10, 3), doubleDatum=42.4242, integerDatum=17, longDatum=1520828868, stringDatum="test,breakdelimiter", timestampDatum=Timestamp.from(ZonedDateTime.of(2017, 12, 20, 21, 46, 54, 0, ZoneId.of("UTC")).toInstant), timeDatum="12:34:56", nullDatum=null),
            // altered second row (only booleanDatum and integerValue value has been changed)
            KnownData(booleanDatum=true, dateDatum=Date.valueOf("2016-12-19"), decimalDatum=Decimal(12.345, 10, 3), doubleDatum=21.2121, integerDatum=35, longDatum=1520828123, stringDatum="breakdelimiter,test", timestampDatum=Timestamp.from(ZonedDateTime.of(2017, 12, 29, 17, 21, 49, 0, ZoneId.of("UTC")).toInstant), timeDatum="23:45:16", nullDatum=null)
        )

        dataset.toDF
    }

    def getKnownStringDataset()(implicit spark: SparkSession): DataFrame = {
        val df = getKnownDataset()
        df.select(df.columns.map(c => col(c).cast(StringType)) : _*)
    }

    def knownDatasetPrettyJSON(row: Int)(implicit spark: SparkSession): String = {
        val json = getKnownDataset().toJSON.collect.toList(row)
        val objectMapper = new ObjectMapper()
        val jsonTree = objectMapper.readTree(json)

        objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonTree)
    }

    def getListOfFiles(dir: String):List[File] = {
        val d = new File(dir)
        if (d.exists && d.isDirectory) {
            d.listFiles.filter(_.isFile).filter(f => !f.getPath.contains("README.md")).toList
        } else {
            List[File]()
        }
    }

    def getKnownDatasetMetadataJson(): String = {
    """
    [
        {
            "id": "982cbf60-7ba7-4e50-a09b-d8624a5c49e6",
            "name": "booleanDatum",
            "description": "booleanDatum",
            "type": "boolean",
            "trim": false,
            "nullable": false,
            "nullableValues": [
                "",
                "null"
            ],
            "trueValues": [
                "true"
            ],
            "falseValues": [
                "false"
            ],
            "metadata": {
                "booleanMeta": true,
                "booleanArrayMeta": [true, false],
                "stringMeta": "string",
                "stringArrayMeta": ["string0", "string1"],
                "longMeta": 10,
                "longArrayMeta": [10,20],
                "doubleMeta": 0.141,
                "doubleArrayMeta": [0.141, 0.52],
                "private": false,
                "securityLevel": 0
            }
        },
        {
            "id": "0e8109ba-1000-4b7d-8a4c-b01bae07027f",
            "name": "dateDatum",
            "description": "dateDatum",
            "type": "date",
            "trim": true,
            "nullable": true,
            "nullableValues": [
                "",
                "null"
            ],
            "formatters": [
                "yyyy-MM-dd"
            ],
            "metadata": {
                "private": true,
                "securityLevel": 3
            }
        },
        {
            "id": "9712c383-22d1-44a6-9ca2-0087af4857f1",
            "name": "decimalDatum",
            "description": "decimalDatum",
            "type": "decimal",
            "trim": true,
            "nullable": true,
            "nullableValues": [
                "",
                "null"
            ],
            "precision": 38,
            "scale": 18,
            "metadata": {
                "private": true,
                "securityLevel": 2
            }
        },
        {
            "id": "31541ea3-5b74-4753-857c-770bd601c35b",
            "name": "doubleDatum",
            "description": "doubleDatum",
            "type": "double",
            "trim": true,
            "nullable": true,
            "nullableValues": [
                "",
                "null"
            ],
            "metadata": {
                "private": true,
                "securityLevel": 8
            }
        },
        {
            "id": "a66f3bbe-d1c6-44c7-b096-a4be59fdcd78",
            "name": "integerDatum",
            "description": "integerDatum",
            "type": "integer",
            "trim": true,
            "nullable": true,
            "nullableValues": [
                "",
                "null"
            ],
            "metadata": {
                "private": true,
                "securityLevel": 10
            }
        },
        {
            "id": "1c0eec1d-17cd-45da-8744-7a9ef5b8b086",
            "name": "longDatum",
            "description": "longDatum",
            "type": "long",
            "trim": true,
            "nullable": true,
            "nullableValues": [
                "",
                "null"
            ],
            "metadata": {
                "private": false,
                "securityLevel": 0
            }
        },
        {
            "id": "9712c383-22d1-44a6-9ca2-0087af4857f1",
            "name": "stringDatum",
            "description": "stringDatum",
            "type": "string",
            "trim": true,
            "nullable": true,
            "nullableValues": [
                "",
                "null"
            ],
            "metadata": {
                "private": false,
                "securityLevel": 0
            }
        },
        {
            "id": "eb17a18e-4664-4016-8beb-cd2a492d4f20",
            "name": "timeDatum",
            "description": "timeDatum",
            "type": "time",
            "trim": true,
            "nullable": true,
            "nullableValues": [
                "",
                "null"
            ],
            "formatters": [
                "HH:mm:ss"
            ],
            "metadata": {
                "private": true,
                "securityLevel": 8
            }
        },
        {
            "id": "8e42c8f0-22a8-40db-9798-6dd533c1de36",
            "name": "timestampDatum",
            "description": "timestampDatum",
            "type": "timestamp",
            "trim": true,
            "nullable": true,
            "nullableValues": [
                "",
                "null"
            ],
            "formatters": [
                "uuuu-MM-dd'T'HH:mm:ss.SSSXXX"
            ],
            "timezoneId": "UTC",
            "metadata": {
                "private": true,
                "securityLevel": 7
            }
        }
    ]
    """
    }
}

