package au.com.agl.arc.util

import java.time.LocalTime

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.node._

import au.com.agl.arc.api.API._

object MetadataSchema {

  type ParseResult = Either[List[String], List[ExtractColumn]]

  def parseJsonMetadata(sourceJson: String): ParseResult = {

    val objectMapper = new ObjectMapper()
    val rootNode = objectMapper.readTree(sourceJson)

    val res: ParseResult = if (!rootNode.isArray) {
      Left("Expected Json Array" :: Nil)
    } else {
      val elements = rootNode.elements().asScala.toList 
      val cols: List[Either[String, ExtractColumn]] = for (n <- elements) yield {
        if (n.isObject) {
          // attributes
          val id = n.get("id").textValue
          val name = n.get("name").textValue
          val description = if (n.has("description")) Option(n.get("description").textValue) else None
          val primaryKey = if (n.has("primaryKey")) Option(n.get("primaryKey").asBoolean) else None
          
          // behaviours
          val trim = if(n.has("trim")) n.get("trim").asBoolean else false
          val nullable = n.get("nullable").asBoolean
          val nullReplacementValue = if(n.has("nullReplacementValue")) Option(n.get("nullReplacementValue").textValue) else None
          val nullableValues = {
            if (n.has("nullableValues")) {
              val nullableValueElements = n.get("nullableValues")
              assert(nullableValueElements.isArray, s"Expected Json String Array for nullableValues for column $name")

              nullableValueElements.elements.asScala.filter( _.isTextual ).map(_.textValue).toList
            } else {
              Nil
            }
          }

          n.get("type").textValue match {
            case "string" => {
              val length = if (n.has("length")) Option(n.get("length").asInt) else None
              Right(StringColumn(id, name, description, primaryKey, nullable, nullReplacementValue, trim, nullableValues, length))
            }
            case "integer" => Right(IntegerColumn(id, name, description, primaryKey, nullable, nullReplacementValue, trim, nullableValues))
            case "long" => Right(LongColumn(id, name, description, primaryKey, nullable, nullReplacementValue, trim, nullableValues))
            case "double" => Right(DoubleColumn(id, name, description, primaryKey, nullable, nullReplacementValue, trim, nullableValues))
            case "decimal" => {
              val precision = n.get("precision").asInt
              val scale = n.get("scale").asInt
              val formatter = if (n.has("formatter")) Option(n.get("formatter").textValue) else None
              Right(DecimalColumn(id, name, description, primaryKey, nullable, nullReplacementValue, trim, nullableValues, precision, scale, formatter))
            }
            case "boolean" => {
              val trueValues = asStringArray(n.get("trueValues"))
              val falseValues = asStringArray(n.get("falseValues"))
              Right(BooleanColumn(id, name, description, primaryKey, nullable, nullReplacementValue, trim, nullableValues, trueValues, falseValues))
            }
            case "timestamp" => {
              val formatters = asStringArray(n.get("formatters"))
              val tz = n.get("timezoneId").textValue
              val time = n.has("time") match {
                case true => {
                  val timeConfig = n.get("time")
                  Option(LocalTime.of(timeConfig.get("hour").asInt, timeConfig.get("minute").asInt, timeConfig.get("second").asInt, timeConfig.get("nano").asInt))
                }
                case false => None
              }
              
              Right(TimestampColumn(id, name, description, primaryKey, nullable, nullReplacementValue, trim, nullableValues, tz, formatters, time))
            }
            case "date" => {
              val formatters = asStringArray(n.get("formatters"))
              Right(DateColumn(id, name, description, primaryKey, nullable, nullReplacementValue, trim, nullableValues, formatters))
            }
            case "time" => {
              val formatters = asStringArray(n.get("formatters"))
              Right(TimeColumn(id, name, description, primaryKey, nullable, nullReplacementValue, trim, nullableValues, formatters))
            }            
            case unknown => Left(s"Unknown type '${unknown}' for column '${name}''")
          }
        } else {
          Left("expected Json object at: " + elements.indexOf(n))
        }
      }

      val (errors, validCols) = cols.foldLeft[(List[String], List[ExtractColumn])](Nil, Nil){ case ((errors, validCols), c) =>
        c match {
          case Left(err) => (err :: errors, validCols)
          case Right(vc) => (errors, vc :: validCols)
        }
      }

      if (!errors.isEmpty) {
        throw new IllegalArgumentException(s"schema error: ${errors.toList}")
      }

      if (!errors.isEmpty) Left(errors.toList) else Right(validCols.toList.reverse)
    }

    res
  }

  def asStringArray(node: JsonNode): List[String] = {
    node.elements.asScala.filter( _.isTextual ).map( _.textValue ).toList
  }

}
