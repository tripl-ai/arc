package ai.tripl.arc.util

import scala.collection.mutable.Map
import scala.util.{Try, Success, Failure}

import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.node._

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object MetadataUtils {

  // convertes the schema of an input dataframe into a dataframe [name, nullable, type, metadata]
  def createMetadataDataframe(input: DataFrame)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): DataFrame = {
    import spark.implicits._

    // this is a hack but having to create a StructType using union of all metadata maps is not trivial
    val schemaDataframe = spark.sparkContext.parallelize(Seq(input.schema.json)).toDF.as[String]
    val parsedSchema = spark.read.json(schemaDataframe)

    // create schema dataframe
    val schema = parsedSchema.select(explode(col("fields"))).select("col.*")

    // add metadata column if missing
    val output = if (schema.columns.contains("metadata")) {
      schema
    } else {
      schema.withColumn("metadata", typedLit(Map[String, String]()))
    }

    output.cache.count
    output
  }

  // a map of full field name strings/metadata
  // a.b.c.d -> metadata
  def getFieldMetadataMap(schema: StructType, parentElement: Option[String] = None, fieldMap: Map[String, Metadata] = scala.collection.mutable.Map[String, Metadata]()): Map[String, Metadata] = {
    schema.foreach { field =>
      val key = s"""${parentElement.getOrElse("")}${if (parentElement.isDefined) "." else ""}${field.name}"""
      field.dataType match {
        case structType: StructType => getFieldMetadataMap(structType, Option(key), fieldMap)
        case _ => fieldMap += (key -> field.metadata)
      }
    }
    fieldMap
  }

  // if the field exists in the incoming fieldMetadataMap override the field's metadata
  def upsertMetadata(structType: StructType, fieldMetadataMap: Map[String, Metadata], parentElement: Option[String] = None): StructType = {
    StructType(structType.map { field =>
      val key = s"""${parentElement.getOrElse("")}${if (parentElement.isDefined) "." else ""}${field.name}"""
      field.dataType match {
        case structType: StructType => StructField(field.name, upsertMetadata(structType, fieldMetadataMap, Option(key)), field.nullable, field.metadata)
        case _ => StructField(field.name, field.dataType, field.nullable, fieldMetadataMap.getOrElse(key, field.metadata) )
      }
    })
  }

  // attach metadata by column name name to input dataframe
  // only attach metadata if the column with same name exists
  def setMetadata(input: DataFrame, incomingSchema: StructType)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): DataFrame = {

    // todo: make a consistent method for replacing metadata which supports streaming
    if (input.isStreaming) {
      // needs to be var not val as we are mutating by overriding columns with metadata attached
      var output = input

      incomingSchema.foreach(field => {
        if (output.columns.contains(field.name)) {
          output = output.withColumn(field.name, col(field.name).as(field.name, field.metadata))
        }
      })

      output      
    } else {
      // create a new schema merging the input dataframe's schema with the incomingSchema 
      // if the incomingSchema has a metadata field set then override the field metadata in the input dataframe 
      val outputSchema = upsertMetadata(input.schema, getFieldMetadataMap(incomingSchema))

      // replace the schema
      spark.createDataFrame(input.rdd, outputSchema)
    }
  }

  // a helper function to speed up the creation of a metadata formatted file
  def makeMetadataFromDataframe(input: DataFrame): String = {
    val fields = input.schema.map(field => {
      val objectMapper = new ObjectMapper()
      val jsonNodeFactory = new JsonNodeFactory(true)
      val node = jsonNodeFactory.objectNode

      field.dataType match {
        case _: BooleanType => {
          node.set[ObjectNode]("id", jsonNodeFactory.textNode(""))
          node.set[ObjectNode]("name", jsonNodeFactory.textNode(field.name))
          node.set[ObjectNode]("description", jsonNodeFactory.textNode(""))

          node.set[ObjectNode]("type", jsonNodeFactory.textNode("boolean"))

          val trueValuesArray = node.putArray("trueValues")
          trueValuesArray.add("true")

          val falseValuesArray = node.putArray("falseValues")
          falseValuesArray.add("false")

          node.set[ObjectNode]("nullable", jsonNodeFactory.booleanNode(field.nullable))
          node.set[ObjectNode]("trim", jsonNodeFactory.booleanNode(true))

          val nullableValuesArray = node.putArray("nullableValues")
          nullableValuesArray.add("")
          nullableValuesArray.add("null")

          node.set[ObjectNode]("metadata", jsonNodeFactory.objectNode())

          Option(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(node))
        }
        case _: DateType => {
          node.set[ObjectNode]("id", jsonNodeFactory.textNode(""))
          node.set[ObjectNode]("name", jsonNodeFactory.textNode(field.name))
          node.set[ObjectNode]("description", jsonNodeFactory.textNode(""))

          node.set[ObjectNode]("type", jsonNodeFactory.textNode("date"))

          val formattersArray = node.putArray("formatters")
          formattersArray.add("uuuu-MM-dd")

          node.set[ObjectNode]("nullable", jsonNodeFactory.booleanNode(field.nullable))
          node.set[ObjectNode]("trim", jsonNodeFactory.booleanNode(true))

          val nullableValuesArray = node.putArray("nullableValues")
          nullableValuesArray.add("")
          nullableValuesArray.add("null")

          node.set[ObjectNode]("metadata", jsonNodeFactory.objectNode())

          Option(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(node))
        }
        case _: DecimalType => {
          node.set[ObjectNode]("id", jsonNodeFactory.textNode(""))
          node.set[ObjectNode]("name", jsonNodeFactory.textNode(field.name))
          node.set[ObjectNode]("description", jsonNodeFactory.textNode(""))

          val decimalField = field.dataType.asInstanceOf[DecimalType]

          node.set[ObjectNode]("type", jsonNodeFactory.textNode("decimal"))
          node.set[ObjectNode]("precision", jsonNodeFactory.numberNode(decimalField.precision))
          node.set[ObjectNode]("scale", jsonNodeFactory.numberNode(decimalField.scale))

          node.set[ObjectNode]("nullable", jsonNodeFactory.booleanNode(field.nullable))
          node.set[ObjectNode]("trim", jsonNodeFactory.booleanNode(true))

          val nullableValuesArray = node.putArray("nullableValues")
          nullableValuesArray.add("")
          nullableValuesArray.add("null")

          node.set[ObjectNode]("metadata", jsonNodeFactory.objectNode())

          Option(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(node))
        }
        case _: DoubleType => {
          node.set[ObjectNode]("id", jsonNodeFactory.textNode(""))
          node.set[ObjectNode]("name", jsonNodeFactory.textNode(field.name))
          node.set[ObjectNode]("description", jsonNodeFactory.textNode(""))

          node.set[ObjectNode]("type", jsonNodeFactory.textNode("double"))

          node.set[ObjectNode]("nullable", jsonNodeFactory.booleanNode(field.nullable))
          node.set[ObjectNode]("trim", jsonNodeFactory.booleanNode(true))

          val nullableValuesArray = node.putArray("nullableValues")
          nullableValuesArray.add("")
          nullableValuesArray.add("null")

          node.set[ObjectNode]("metadata", jsonNodeFactory.objectNode())

          Option(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(node))
        }
        case _: IntegerType => {
          node.set[ObjectNode]("id", jsonNodeFactory.textNode(""))
          node.set[ObjectNode]("name", jsonNodeFactory.textNode(field.name))
          node.set[ObjectNode]("description", jsonNodeFactory.textNode(""))

          node.set[ObjectNode]("type", jsonNodeFactory.textNode("integer"))

          node.set[ObjectNode]("nullable", jsonNodeFactory.booleanNode(field.nullable))
          node.set[ObjectNode]("trim", jsonNodeFactory.booleanNode(true))

          val nullableValuesArray = node.putArray("nullableValues")
          nullableValuesArray.add("")
          nullableValuesArray.add("null")

          node.set[ObjectNode]("metadata", jsonNodeFactory.objectNode())

          Option(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(node))
        }
        case _: LongType => {
          node.set[ObjectNode]("id", jsonNodeFactory.textNode(""))
          node.set[ObjectNode]("name", jsonNodeFactory.textNode(field.name))
          node.set[ObjectNode]("description", jsonNodeFactory.textNode(""))

          node.set[ObjectNode]("type", jsonNodeFactory.textNode("long"))

          node.set[ObjectNode]("nullable", jsonNodeFactory.booleanNode(field.nullable))
          node.set[ObjectNode]("trim", jsonNodeFactory.booleanNode(true))

          val nullableValuesArray = node.putArray("nullableValues")
          nullableValuesArray.add("")
          nullableValuesArray.add("null")

          node.set[ObjectNode]("metadata", jsonNodeFactory.objectNode())

          Option(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(node))
        }
        case _: StringType => {
          node.set[ObjectNode]("id", jsonNodeFactory.textNode(""))
          node.set[ObjectNode]("name", jsonNodeFactory.textNode(field.name))
          node.set[ObjectNode]("description", jsonNodeFactory.textNode(""))

          node.set[ObjectNode]("type", jsonNodeFactory.textNode("string"))

          node.set[ObjectNode]("nullable", jsonNodeFactory.booleanNode(field.nullable))
          node.set[ObjectNode]("trim", jsonNodeFactory.booleanNode(true))

          val nullableValuesArray = node.putArray("nullableValues")
          nullableValuesArray.add("")
          nullableValuesArray.add("null")

          node.set[ObjectNode]("metadata", jsonNodeFactory.objectNode())

          Option(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(node))
        }
        case _: TimestampType => {
          node.set[ObjectNode]("id", jsonNodeFactory.textNode(""))
          node.set[ObjectNode]("name", jsonNodeFactory.textNode(field.name))
          node.set[ObjectNode]("description", jsonNodeFactory.textNode(""))

          node.set[ObjectNode]("type", jsonNodeFactory.textNode("timestamp"))

          val formattersArray = node.putArray("formatters")
          formattersArray.add("uuuu-MM-dd HH:mm:ss")

          node.set[ObjectNode]("timezoneId", jsonNodeFactory.textNode("UTC"))

          node.set[ObjectNode]("nullable", jsonNodeFactory.booleanNode(field.nullable))
          node.set[ObjectNode]("trim", jsonNodeFactory.booleanNode(true))

          val nullableValuesArray = node.putArray("nullableValues")
          nullableValuesArray.add("")
          nullableValuesArray.add("null")

          node.set[ObjectNode]("metadata", jsonNodeFactory.objectNode())

          Option(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(node))
        }
        case _: ArrayType => None
        case _: NullType => None
      }
    })

    s"""[${fields.flatten.mkString(",")}]"""
  }
}

