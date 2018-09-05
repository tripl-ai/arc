package au.com.agl.arc.util

import java.net.URI
import java.time.Instant
import java.util.UUID

import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.node._
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import au.com.agl.arc.api.API._

object MetadataUtils {

  // turns a schema into a dataframe
  def createMetadataDataframe(input: DataFrame)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): DataFrame = {
    import spark.implicits._

    // this is a hack but having to deal with StructTypes and StructFields
    val schemaDataframe = spark.sparkContext.parallelize(Seq(input.schema.json)).toDF.as[String]
    val parsedSchema = spark.read.json(schemaDataframe)
    parsedSchema.createOrReplaceTempView("parsedSchema")

    val schema = spark.sql("""
    SELECT 
      field.name AS name
      ,field.type AS type
      ,field.nullable AS nullable
      ,field.metadata AS metadata
    FROM (
      SELECT 
        EXPLODE(fields) AS field 
      FROM parsedSchema
    ) subquery
    """)

    schema.cache.count
    schema
  }

  // attach metadata by column name name to input dataframe
  // only attach metadata if the column with same name exists
  def setMetadata(input: DataFrame, schema: StructType): DataFrame = {
    // needs to be var not val as we are mutating by overriding columns with metadata attached
    var output = input

    schema.foreach(field => {
      if (output.columns.contains(field.name)) {
        output = output.withColumn(field.name, col(field.name).as(field.name, field.metadata))
      }
    })

    output
  }

  // a helper function to speed up the creation of a metadata formatted file
  def makeMetataFromDataframe(input: DataFrame): String = {
    val fields = input.schema.map(field => {
      val objectMapper = new ObjectMapper()
      val jsonNodeFactory = new JsonNodeFactory(true)
      val node = jsonNodeFactory.objectNode

      node.set("id", jsonNodeFactory.textNode(UUID.randomUUID().toString))
      node.set("name", jsonNodeFactory.textNode(field.name))
      node.set("description", jsonNodeFactory.textNode(""))
      node.set("nullable", jsonNodeFactory.booleanNode(field.nullable))
      node.set("trim", jsonNodeFactory.booleanNode(true))

      val nullableValuesArray = node.putArray("nullableValues")
      nullableValuesArray.add("")
      nullableValuesArray.add("null")

      node.set("metadata", jsonNodeFactory.objectNode())

      field.dataType match {
        case _: BooleanType => {
          node.set("type", jsonNodeFactory.textNode("boolean"))

          val trueValuesArray = node.putArray("trueValues")
          trueValuesArray.add("true")

          val falseValuesArray = node.putArray("falseValues")
          falseValuesArray.add("false")            
        }
        case _: DateType => {
          node.set("type", jsonNodeFactory.textNode("date"))
          
          val formattersArray = node.putArray("formatters")
          formattersArray.add("yyyy-MM-dd")
        }
        case _: DecimalType => {
          val decimalField = field.dataType.asInstanceOf[DecimalType]

          node.set("type", jsonNodeFactory.textNode("decimal"))
          node.set("precision", jsonNodeFactory.numberNode(decimalField.precision))
          node.set("scale", jsonNodeFactory.numberNode(decimalField.scale))
        }
        case _: DoubleType => node.set("type", jsonNodeFactory.textNode("double"))
        case _: IntegerType => node.set("type", jsonNodeFactory.textNode("integer"))
        case _: LongType => node.set("type", jsonNodeFactory.textNode("long"))
        case _: StringType => node.set("type", jsonNodeFactory.textNode("string"))
        case _: TimestampType => {
          node.set("type", jsonNodeFactory.textNode("timestamp"))

          val formattersArray = node.putArray("formatters")
          formattersArray.add("yyyy-MM-dd'T'HH:mm:ssZ")

          node.set("timezoneId", jsonNodeFactory.textNode("UTC"))
        }
        case _: NullType => 
      }

      objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(node)
    })

    s"""[${fields.mkString(",")}]"""
  }
}

