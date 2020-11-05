package ai.tripl.arc.util

import java.time.LocalTime
import java.time.format.DateTimeFormatter

import scala.collection.JavaConverters._
import scala.util.matching.Regex

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.config.ConfigUtils._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.util.EitherUtils._

import com.typesafe.config._
import org.apache.commons.lang.StringEscapeUtils

object ArcSchema {

  type ParseResult = Either[List[StageError], List[ExtractColumn]]

  def parseArcSchemaDataFrame(source: DataFrame)(implicit logger: ai.tripl.arc.util.log.logger.Logger): ParseResult = {
    parseArcSchema(s"""[${source.toJSON.collect.mkString(",")}]""")
  }

  def parseArcSchema(source: String)(implicit logger: ai.tripl.arc.util.log.logger.Logger): ParseResult = {
    val base = ConfigFactory.load()

    // typesafe config requires an object at the root level (not array)
    val wrappedSource = try {
      val etlConf = ConfigFactory.parseString(source, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))
      // must have a schema at root
      if (!etlConf.hasPath("schema")) {
        throw new Exception("does not appear to be an Arc schema. Must be either of type LIST or contain schema within 'schema' attribute.")
      }
      source
    } catch {
      case e: com.typesafe.config.ConfigException if (e.getMessage.contains("has type LIST rather than object at file root")) =>
        s"""{"schema": ${source}}"""
    }

    // try to parse the config file
    val etlConf = ConfigFactory.parseString(wrappedSource, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))
    val config = etlConf.withFallback(base)
    val fields = config.resolve().getObjectList("schema").asScala.map(_.toConfig).toList

    val cols = fields.zipWithIndex.map { case (field, idx) =>
      readField(field, idx, false)
    }

    val (schema, errors) = cols.foldLeft[(List[ExtractColumn], List[StageError])]( (Nil, Nil) ) { case ( (columns, errs), metaOrError ) =>
      metaOrError match {
        case Right(c) => (c :: columns, errs)
        case Left(metaErrors) => (columns, metaErrors ::: errs)
      }
    }

    errors match {
      case Nil => Right(schema.reverse)
      case _ => Left(errors.reverse)
    }
  }

  def readField(field: Config, idx: Integer, nested: Boolean)(implicit logger: ai.tripl.arc.util.log.logger.Logger): Either[List[Error.StageError], ExtractColumn] = {
    import ConfigReader._
    implicit var c = field

    // test keys
    val baseKeys = "id" :: "name" :: "description" :: "type" :: "metadata" :: Nil

    // common attributes
    val id = ConfigReader.getOptionalValue[String]("id")
    val name = ConfigReader.getValue[String]("name")
    val description = ConfigReader.getOptionalValue[String]("description")
    val _type = ConfigReader.getValue[String]("type", validValues = "boolean" :: "date" :: "decimal" :: "double" :: "integer" :: "long" :: "string" :: "time" :: "timestamp" :: "binary" :: "struct" :: "array" :: Nil)

    (name, _type) match {
      case (Right(n), Right(t)) => {

        val metadata: Either[Errors, Option[String]] = if( c.hasPath("metadata") ) {

          // if the metadata has been extracted from a database (e.g. a postgres jsonb field) it may be
          // mapped to a string by the spark jdbc dialect. in this case unescape the string and parse it as a config
          // so the values can be verified
          val meta = c.getValue("metadata").valueType match {
            case ConfigValueType.STRING => {
              ConfigFactory.parseString(StringEscapeUtils.unescapeJava(c.getString("metadata")), ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))
            }
            case _ => c.getObject("metadata").toConfig
          }

          val valid = validateMetadata(n, meta)
          if (valid.forall(_.isRight)) {
            Right(Option(meta.root.render(ConfigRenderOptions.concise())))
          } else {
            Left(valid.collect{ case Left(errs) => errs })
          }
        } else {
          Right(None)
        }

        t match {

          case "binary" => {
            // test keys
            val expectedKeys = "trim" :: "nullable" :: "nullReplacementValue" :: "nullableValues" :: "encoding" :: baseKeys
            val trim = ConfigReader.getValue[java.lang.Boolean]("trim", default = Some(false))
            val nullable = ConfigReader.getValue[java.lang.Boolean]("nullable")
            val nullReplacementValue = ConfigReader.getOptionalValue[String]("nullReplacementValue")
            val nullableValues = ConfigReader.getValue[StringList]("nullableValues", default = Some(Nil))
            val invalidKeys = checkValidKeys(c)(expectedKeys)

            val encoding = getValue[String]("encoding", validValues = "base64" :: "hexadecimal" :: Nil) |> parseEncoding("encoding") _

            (id, name, description, _type, nullable, nullReplacementValue, trim, nullableValues, metadata, encoding) match {
              case (Right(id), Right(name), Right(description), Right(_type), Right(nullable), Right(nullReplacementValue), Right(trim), Right(nullableValues), Right(metadata), Right(encoding)) => {
                Right(BinaryColumn(id, name, description, nullable, nullReplacementValue, trim, nullableValues, encoding, metadata))
              }
              case _ => {
                val allErrors: Errors = List(id, name, description, _type, nullable, nullReplacementValue, trim, nullableValues, metadata, encoding, invalidKeys).collect{ case Left(errs) => errs }.flatten
                val metaName = stringOrDefault(name, "unnamed meta")
                val err = StageError(idx, metaName, c.origin.lineNumber, allErrors)
                Left(err :: Nil)
              }
            }
          }

          case "boolean" => {
            // test keys
            val expectedKeys = "trim" :: "nullable" :: "nullReplacementValue" :: "nullableValues" :: "trueValues" :: "falseValues" :: baseKeys
            val invalidKeys = checkValidKeys(c)(expectedKeys)
            val trim = ConfigReader.getValue[java.lang.Boolean]("trim", default = Some(false))
            val nullable = ConfigReader.getValue[java.lang.Boolean]("nullable")
            val nullReplacementValue = ConfigReader.getOptionalValue[String]("nullReplacementValue")
            val nullableValues = ConfigReader.getValue[StringList]("nullableValues", default = Some(Nil))

            val trueValues = ConfigReader.getValue[StringList]("trueValues")
            val falseValues = ConfigReader.getValue[StringList]("falseValues")

            (id, name, description, _type, nullable, nullReplacementValue, trim, nullableValues, metadata, trueValues, falseValues) match {
              case (Right(id), Right(name), Right(description), Right(_type), Right(nullable), Right(nullReplacementValue), Right(trim), Right(nullableValues), Right(metadata), Right(trueValues), Right(falseValues)) => {
                Right(BooleanColumn(id, name, description, nullable, nullReplacementValue, trim, nullableValues, trueValues, falseValues, metadata))
              }
              case _ => {
                val allErrors: Errors = List(id, name, description, _type, nullable, nullReplacementValue, trim, nullableValues, metadata, trueValues, falseValues, invalidKeys).collect{ case Left(errs) => errs }.flatten
                val metaName = stringOrDefault(name, "unnamed meta")
                val err = StageError(idx, metaName, c.origin.lineNumber, allErrors)
                Left(err :: Nil)
              }
            }
          }

          case "date" => {
            // test keys
            val expectedKeys = "trim" :: "nullable" :: "nullReplacementValue" :: "nullableValues" :: "formatters" :: "caseSensitive" :: baseKeys
            val trim = ConfigReader.getValue[java.lang.Boolean]("trim", default = Some(false))
            val nullable = ConfigReader.getValue[java.lang.Boolean]("nullable")
            val nullReplacementValue = ConfigReader.getOptionalValue[String]("nullReplacementValue")
            val nullableValues = ConfigReader.getValue[StringList]("nullableValues", default = Some(Nil))
            val caseSensitive = ConfigReader.getValue[java.lang.Boolean]("caseSensitive", default = Some(false))
            val invalidKeys = checkValidKeys(c)(expectedKeys)

            val formatters = ConfigReader.getValue[StringList]("formatters") |> validateDateTimeFormatter("formatters") _

            (id, name, description, _type, nullable, nullReplacementValue, trim, nullableValues, metadata, formatters, caseSensitive) match {
              case (Right(id), Right(name), Right(description), Right(_type), Right(nullable), Right(nullReplacementValue), Right(trim), Right(nullableValues), Right(metadata), Right(formatters), Right(caseSensitive)) => {

                // test if strict mode possible and throw warning
                val strict = formatters.forall(formatter => strictDateTimeFormatter(name, formatter))
                Right(DateColumn(id, name, description, nullable, nullReplacementValue, trim, nullableValues, formatters, metadata, strict, caseSensitive))
              }
              case _ => {
                val allErrors: Errors = List(id, name, description, _type, nullable, nullReplacementValue, trim, nullableValues, metadata, formatters, caseSensitive, invalidKeys).collect{ case Left(errs) => errs }.flatten
                val metaName = stringOrDefault(name, "unnamed meta")
                val err = StageError(idx, metaName, c.origin.lineNumber, allErrors)
                Left(err :: Nil)
              }
            }
          }

          case "decimal" => {
            // test keys
            val expectedKeys = "trim" :: "nullable" :: "nullReplacementValue" :: "nullableValues" :: "precision" :: "scale" :: "formatters" :: baseKeys
            val trim = ConfigReader.getValue[java.lang.Boolean]("trim", default = Some(false))
            val nullable = ConfigReader.getValue[java.lang.Boolean]("nullable")
            val nullReplacementValue = ConfigReader.getOptionalValue[String]("nullReplacementValue")
            val nullableValues = ConfigReader.getValue[StringList]("nullableValues", default = Some(Nil))
            val invalidKeys = checkValidKeys(c)(expectedKeys)

            val precision = ConfigReader.getValue[Int]("precision")
            val scale = ConfigReader.getValue[Int]("scale")
            val formatters = ConfigReader.getOptionalValue[StringList]("formatters")

            (id, name, description, _type, nullable, nullReplacementValue, trim, nullableValues, precision, scale, metadata, formatters) match {
              case (Right(id), Right(name), Right(description), Right(_type), Right(nullable), Right(nullReplacementValue), Right(trim), Right(nullableValues), Right(precision), Right(scale), Right(metadata), Right(formatters)) => {
                Right(DecimalColumn(id, name, description, nullable, nullReplacementValue, trim, nullableValues, precision, scale, metadata, formatters))
              }
              case _ => {
                val allErrors: Errors = List(id, name, description, _type, nullable, nullReplacementValue, trim, nullableValues, precision, scale, metadata, formatters, invalidKeys).collect{ case Left(errs) => errs }.flatten
                val metaName = stringOrDefault(name, "unnamed meta")
                val err = StageError(idx, metaName, c.origin.lineNumber, allErrors)
                Left(err :: Nil)
              }
            }
          }

          case "double" => {
            // test keys
            val expectedKeys = "trim" :: "nullable" :: "nullReplacementValue" :: "nullableValues"  :: "formatters" :: baseKeys
            val trim = ConfigReader.getValue[java.lang.Boolean]("trim", default = Some(false))
            val nullable = ConfigReader.getValue[java.lang.Boolean]("nullable")
            val nullReplacementValue = ConfigReader.getOptionalValue[String]("nullReplacementValue")
            val nullableValues = ConfigReader.getValue[StringList]("nullableValues", default = Some(Nil))
            val invalidKeys = checkValidKeys(c)(expectedKeys)

            val formatters = ConfigReader.getOptionalValue[StringList]("formatters")

            (id, name, description, _type, nullable, nullReplacementValue, trim, nullableValues, metadata, formatters) match {
              case (Right(id), Right(name), Right(description), Right(_type), Right(nullable), Right(nullReplacementValue), Right(trim), Right(nullableValues), Right(metadata), Right(formatters)) => {
                Right(DoubleColumn(id, name, description, nullable, nullReplacementValue, trim, nullableValues,  metadata, formatters))
              }
              case _ => {
                val allErrors: Errors = List(id, name, description, _type, nullable, nullReplacementValue, trim, nullableValues, metadata, formatters, invalidKeys).collect{ case Left(errs) => errs }.flatten
                val metaName = stringOrDefault(name, "unnamed meta")
                val err = StageError(idx, metaName, c.origin.lineNumber, allErrors)
                Left(err :: Nil)
              }
            }
          }

          case "integer" => {
            // test keys
            val expectedKeys = "trim" :: "nullable" :: "nullReplacementValue" :: "nullableValues"  :: "formatters" :: baseKeys
            val trim = ConfigReader.getValue[java.lang.Boolean]("trim", default = Some(false))
            val nullable = ConfigReader.getValue[java.lang.Boolean]("nullable")
            val nullReplacementValue = ConfigReader.getOptionalValue[String]("nullReplacementValue")
            val nullableValues = ConfigReader.getValue[StringList]("nullableValues", default = Some(Nil))
            val invalidKeys = checkValidKeys(c)(expectedKeys)

            val formatters = ConfigReader.getOptionalValue[StringList]("formatters")

            (id, name, description, _type, nullable, nullReplacementValue, trim, nullableValues, metadata, formatters) match {
              case (Right(id), Right(name), Right(description), Right(_type), Right(nullable), Right(nullReplacementValue), Right(trim), Right(nullableValues), Right(metadata), Right(formatters)) => {
                Right(IntegerColumn(id, name, description, nullable, nullReplacementValue, trim, nullableValues, metadata, formatters))
              }
              case _ => {
                val allErrors: Errors = List(id, name, description, _type, nullable, nullReplacementValue, trim, nullableValues, metadata, formatters, invalidKeys).collect{ case Left(errs) => errs }.flatten
                val metaName = stringOrDefault(name, "unnamed meta")
                val err = StageError(idx, metaName, c.origin.lineNumber, allErrors)
                Left(err :: Nil)
              }
            }
          }

          case "long" => {
            // test keys
            val expectedKeys = "trim" :: "nullable" :: "nullReplacementValue" :: "nullableValues"  :: "formatters" :: baseKeys
            val trim = ConfigReader.getValue[java.lang.Boolean]("trim", default = Some(false))
            val nullable = ConfigReader.getValue[java.lang.Boolean]("nullable")
            val nullReplacementValue = ConfigReader.getOptionalValue[String]("nullReplacementValue")
            val nullableValues = ConfigReader.getValue[StringList]("nullableValues", default = Some(Nil))
            val invalidKeys = checkValidKeys(c)(expectedKeys)

            val formatters = ConfigReader.getOptionalValue[StringList]("formatters")

            (id, name, description, _type, nullable, nullReplacementValue, trim, nullableValues, metadata, formatters) match {
              case (Right(id), Right(name), Right(description), Right(_type), Right(nullable), Right(nullReplacementValue), Right(trim), Right(nullableValues), Right(metadata), Right(formatters)) => {
                Right(LongColumn(id, name, description, nullable, nullReplacementValue, trim, nullableValues, metadata, formatters))
              }
              case _ => {
                val allErrors: Errors = List(id, name, description, _type, nullable, nullReplacementValue, trim, nullableValues, metadata, formatters, invalidKeys).collect{ case Left(errs) => errs }.flatten
                val metaName = stringOrDefault(name, "unnamed meta")
                val err = StageError(idx, metaName, c.origin.lineNumber, allErrors)
                Left(err :: Nil)
              }
            }
          }

          case "string" => {
            // test keys
            val expectedKeys = "trim" :: "nullable" :: "nullReplacementValue" :: "nullableValues"  :: "minLength" :: "maxLength" :: "regex" :: baseKeys
            val trim = ConfigReader.getValue[java.lang.Boolean]("trim", default = Some(false))
            val nullable = ConfigReader.getValue[java.lang.Boolean]("nullable")
            val nullReplacementValue = ConfigReader.getOptionalValue[String]("nullReplacementValue")
            val nullableValues = ConfigReader.getValue[StringList]("nullableValues", default = Some(Nil))
            val invalidKeys = checkValidKeys(c)(expectedKeys)

            val minLength = ConfigReader.getOptionalValue[Int]("minLength")
            val maxLength = ConfigReader.getOptionalValue[Int]("maxLength")
            val regex = ConfigReader.getOptionalValue[String]("regex") |> validateRegex("regex") _

            (id, name, description, _type, nullable, nullReplacementValue, trim, nullableValues, metadata, minLength, maxLength, regex) match {
              case (Right(id), Right(name), Right(description), Right(_type), Right(nullable), Right(nullReplacementValue), Right(trim), Right(nullableValues), Right(metadata), Right(minLength), Right(maxLength), Right(regex)) => {
                Right(StringColumn(id, name, description, nullable, nullReplacementValue, trim, nullableValues, metadata, minLength, maxLength, regex))
              }
              case _ => {
                val allErrors: Errors = List(id, name, description, _type, nullable, nullReplacementValue, trim, nullableValues, metadata, minLength, maxLength, regex, invalidKeys).collect{ case Left(errs) => errs }.flatten
                val metaName = stringOrDefault(name, "unnamed meta")
                val err = StageError(idx, metaName, c.origin.lineNumber, allErrors)
                Left(err :: Nil)
              }
            }
          }

          case "time" => {
            // test keys
            val expectedKeys = "trim" :: "nullable" :: "nullReplacementValue" :: "nullableValues"  :: "formatters" :: baseKeys
            val trim = ConfigReader.getValue[java.lang.Boolean]("trim", default = Some(false))
            val nullable = ConfigReader.getValue[java.lang.Boolean]("nullable")
            val nullReplacementValue = ConfigReader.getOptionalValue[String]("nullReplacementValue")
            val nullableValues = ConfigReader.getValue[StringList]("nullableValues", default = Some(Nil))
            val invalidKeys = checkValidKeys(c)(expectedKeys)

            val formatters = ConfigReader.getValue[StringList]("formatters")

            (id, name, description, _type, nullable, nullReplacementValue, trim, nullableValues, metadata, formatters) match {
              case (Right(id), Right(name), Right(description), Right(_type), Right(nullable), Right(nullReplacementValue), Right(trim), Right(nullableValues), Right(metadata), Right(formatters)) => {
                Right(TimeColumn(id, name, description, nullable, nullReplacementValue, trim, nullableValues, formatters, metadata))
              }
              case _ => {
                val allErrors: Errors = List(id, name, description, _type, nullable, nullReplacementValue, trim, nullableValues, metadata, formatters, invalidKeys).collect{ case Left(errs) => errs }.flatten
                val metaName = stringOrDefault(name, "unnamed meta")
                val err = StageError(idx, metaName, c.origin.lineNumber, allErrors)
                Left(err :: Nil)
              }
            }
          }

          case "timestamp" => {
            // test keys
            val expectedKeys = "trim" :: "nullable" :: "nullReplacementValue" :: "nullableValues" :: "formatters" :: "timezoneId" :: "time" :: "caseSensitive" :: baseKeys
            val trim = ConfigReader.getValue[java.lang.Boolean]("trim", default = Some(false))
            val nullable = ConfigReader.getValue[java.lang.Boolean]("nullable")
            val nullReplacementValue = ConfigReader.getOptionalValue[String]("nullReplacementValue")
            val nullableValues = ConfigReader.getValue[StringList]("nullableValues", default = Some(Nil))
            val caseSensitive = ConfigReader.getValue[java.lang.Boolean]("caseSensitive", default = Some(false))
            val invalidKeys = checkValidKeys(c)(expectedKeys)

            val formatters = ConfigReader.getValue[StringList]("formatters") |> validateDateTimeFormatter("formatters") _
            val timezoneId = ConfigReader.getValue[String]("timezoneId")

            // try to parse time if exists
            val time: Either[Errors, Option[LocalTime]]= if (c.hasPath("time")) {
              c = c.getObject("time").toConfig

              val hour = ConfigReader.getValue[Int]("hour")
              val minute = ConfigReader.getValue[Int]("minute")
              val second = ConfigReader.getValue[Int]("second")
              val nano = ConfigReader.getValue[Int]("nano")

              (hour, minute, second, nano) match {
                case (Right(hour), Right(minute), Right(second), Right(nano)) => Right(Option(LocalTime.of(hour, minute, second, nano)))
                case _ => {
                  val errors = List(hour, minute, second, nano).collect{ case Left(errs) => errs }.flatten

                  Left(List(ConfigError("time", Some(c.origin.lineNumber), s"""Invalid value. ${errors.map(configError => configError.message).mkString(", ")}""")))
                }
              }
            } else {
              Right(None)
            }

            (id, name, description, _type, nullable, nullReplacementValue, trim, nullableValues, metadata, formatters, timezoneId, time, caseSensitive) match {
              case (Right(id), Right(name), Right(description), Right(_type), Right(nullable), Right(nullReplacementValue), Right(trim), Right(nullableValues), Right(metadata), Right(formatters), Right(timezoneId), Right(time), Right(caseSensitive)) => {

                // test if strict mode possible and throw warning
                val strict = formatters.forall(formatter => strictDateTimeFormatter(name, formatter))

                Right(TimestampColumn(id, name, description, nullable, nullReplacementValue, trim, nullableValues, timezoneId, formatters, time, metadata, strict, caseSensitive))
              }
              case _ => {
                val allErrors: Errors = List(id, name, description, _type, nullable, nullReplacementValue, trim, nullableValues, metadata, formatters, timezoneId, time, caseSensitive, invalidKeys).collect{ case Left(errs) => errs }.flatten
                val metaName = stringOrDefault(name, "unnamed meta")
                val err = StageError(idx, metaName, c.origin.lineNumber, allErrors)
                Left(err :: Nil)
              }
            }
          }

          case "struct" => {
            // test keys
            val expectedKeys = "trim" :: "nullable" :: "nullReplacementValue" :: "nullableValues" :: "fields" :: baseKeys
            val trim = Right(false)
            val nullable = ConfigReader.getValue[java.lang.Boolean]("nullable")
            val nullReplacementValue = Right(None)
            val nullableValues = Right(Nil)
            val invalidKeys = checkValidKeys(c)(expectedKeys)

            val hasFields = hasPath("fields") |> valueTypeArray("fields", 1) _
            val fieldsConfig = if (hasFields.isRight) c.getConfigList("fields").asScala.toList else Nil
            val fields = fieldsConfig.zipWithIndex.map { case (field, idx) =>
              readField(field, idx, false)
            }

            (id, name, description, _type, nullable, nullReplacementValue, trim, nullableValues, metadata, hasFields, fields.forall { _.isRight}) match {
              case (Right(id), Right(name), Right(description), Right(_type), Right(nullable), Right(nullReplacementValue), Right(trim), Right(nullableValues), Right(metadata), Right(hasFields), true) => {
                Right(StructColumn(id, name, description, nullable, nullReplacementValue, trim, nullableValues, fields.map { _.right.get }, metadata))
              }
              case _ => {
                val childErrors = fields.collect { case Left(errors) => errors }.flatten.map { errors => Left(errors.errors) }
                val allErrors: Errors = List(List(id), List(name), List(description), List(_type), List(nullable), List(nullReplacementValue), List(trim), List(nullableValues), List(metadata), List(invalidKeys), List(hasFields), childErrors).flatten.collect{ case Left(errs) => errs }.flatten
                val metaName = stringOrDefault(name, "unnamed meta")
                val err = StageError(idx, metaName, c.origin.lineNumber, allErrors)
                Left(err :: Nil)
              }
            }
          }

          case "array" => {
            // test keys
            val expectedKeys = "trim" :: "nullable" :: "nullReplacementValue" :: "nullableValues" :: "elementType" :: baseKeys
            val trim = Right(false)
            val nullable = ConfigReader.getValue[java.lang.Boolean]("nullable")
            val nullReplacementValue = Right(None)
            val nullableValues = Right(Nil)
            val invalidKeys = checkValidKeys(c)(expectedKeys)

            val hasElementType = hasPath("elementType") |> valueTypeObject("elementType") _
            val elementType = if (hasElementType.isRight) Option(c.getConfig("elementType")) else None
            val elementField = elementType.map { child => readField(child, 0, true) }.getOrElse(Right(StringColumn(None,"",None,true,None,true,Nil,None,None,None,None)))

            (id, name, description, _type, nullable, nullReplacementValue, trim, nullableValues, metadata, hasElementType, elementField) match {
              case (Right(id), Right(name), Right(description), Right(_type), Right(nullable), Right(nullReplacementValue), Right(trim), Right(nullableValues), Right(metadata), Right(hasElementType), Right(elementField)) => {
                Right(ArrayColumn(id, name, description, nullable, nullReplacementValue, trim, nullableValues, elementField, metadata))
              }
              case _ => {
                val childErrors = if (elementField.isLeft) {
                  elementField.left.get.map { error => Left(error.errors) }
                } else {
                  Nil
                }
                val allErrors: Errors = List(List(id), List(name), List(description), List(_type), List(nullable), List(nullReplacementValue), List(trim), List(nullableValues), List(metadata), List(invalidKeys), List(hasElementType), childErrors).flatten.collect{ case Left(errs) => errs }.flatten
                val metaName = stringOrDefault(name, "unnamed meta")
                val err = StageError(idx, metaName, c.origin.lineNumber, allErrors)
                Left(err :: Nil)
              }
            }
          }

        }
      }
      case _ => {
        val allErrors: Errors = List(id, name, description, _type).collect{ case Left(errs) => errs }.flatten
        val metaName = stringOrDefault(name, "unnamed meta")
        val err = StageError(idx, metaName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
      }
    }
  }

  def validateMetadata(name: String, config: Config): List[Either[ConfigError, scala.Boolean]]  = {
    config.entrySet.asScala.toList.map {
      node => {
        node.getValue.valueType match {
          case ConfigValueType.OBJECT => {
            validateMetadata(node.getKey, node.getValue.atKey(""))
          }
          case ConfigValueType.LIST => {
            val listNode = config.getList(node.getKey)
            if (listNode.size == 0) {
              List(Left(ConfigError(node.getKey, Some(listNode.origin.lineNumber), s"Metadata attribute '${node.getKey}' cannot contain empty `arrays`.")))
            } else {
              val nodeList = listNode.iterator.asScala.toList
              val nodeType = nodeList(0).valueType
              nodeType match {
                case ConfigValueType.NULL => List(Left(ConfigError(node.getKey, Some(listNode.origin.lineNumber), s"Metadata attribute '${node.getKey}' cannot contain `null` values inside `array`.")))
                case ConfigValueType.OBJECT => validateMetadata(node.getKey, node.getValue.atKey(""))
                case ConfigValueType.LIST => List(Left(ConfigError(node.getKey, Some(listNode.origin.lineNumber), s"Metadata attribute '${node.getKey}}' cannot contain nested `arrays` inside `array`.")))
                case _ => {
                  if (nodeList.forall(_.valueType == nodeType)) {
                    nodeType match {
                      case ConfigValueType.NUMBER => {
                        // test all values are of same class as first value (as .valueType does not differentiate between double and integer)
                        val numberClass = nodeList(0).getClass
                        if (nodeList.forall(_.getClass == numberClass)) {
                          List(Right(true))
                        } else {
                          List(Left(ConfigError(node.getKey, Some(listNode.origin.lineNumber), s"Metadata attribute '${node.getKey}' cannot contain `number` arrays of different types (all values must be `integers` or all values must be `doubles`).")))
                        }
                      }
                      case _ => List(Right(true))
                    }
                  } else {
                    List(Left(ConfigError(node.getKey, Some(listNode.origin.lineNumber), s"Metadata attribute '${node.getKey}' cannot contain arrays of different types.")))
                  }
                }
              }
            }
          }
          case _ => List(Right(true))
        }
      }
    }.flatten
  }

  def validateDateTimeFormatter(path: String)(formatters: StringList)(implicit c: Config): Either[Errors, StringList] = {
    val (validFormatters, errors) = formatters.foldLeft[(StringList, Errors)]( (Nil, Nil) ) { case ( (patterns, errs), pattern ) =>
      try {
        DateTimeFormatter.ofPattern(pattern)
        (pattern :: patterns, errs)
      } catch {
        case e: Exception => (patterns, List(ConfigError(path, Some(c.getValue(path).origin.lineNumber()), e.getMessage)) ::: errs)
      }
    }

    errors match {
      case Nil => Right(validFormatters.reverse)
      case _ => Left(errors.reverse)
    }
  }

  def strictDateTimeFormatter(name: String, pattern: String)(implicit logger: ai.tripl.arc.util.log.logger.Logger): Boolean = {
    val formatter = DateTimeFormatter.ofPattern(pattern).toString
    if (formatter.contains("(YearOfEra,") && !formatter.contains("(Era,")) {
      logger.warn()
        .field("event", "deprecation")
        .field("message", s"'YearOfEra' ('yyyy') set without 'Era' ('GG') in field '${name}' with pattern '${pattern}'. Either add 'Era' ('GG') or change to 'Year' ('uuuu'). This formatter will not work in future versions.")
        .log()
      false
    } else {
      true
    }
  }

  def valueTypeObject(path: String)(config: ConfigValue)(implicit c: Config): Either[Errors, Config] = {
    def err(lineNumber: Option[Int], msg: String): Either[Errors, Config] = Left(ConfigError(path, lineNumber, msg) :: Nil)
      config.valueType match {
        case ConfigValueType.OBJECT => Right(c.getConfig(path))
        case _ => err(Some(c.getValue(path).origin.lineNumber()), s"""'${path}' must be of type object.""")
      }
  }

  def valueTypeArray(path: String, minLength: Int)(config: ConfigValue)(implicit c: Config): Either[Errors, com.typesafe.config.ConfigList] = {
    def err(lineNumber: Option[Int], msg: String): Either[Errors, com.typesafe.config.ConfigList] = Left(ConfigError(path, lineNumber, msg) :: Nil)
      config.valueType match {
        case ConfigValueType.LIST => {
          val configList = c.getList(path)
          if (configList.size >= minLength) {
            Right(configList)
          } else {
            err(Some(c.getValue(path).origin.lineNumber()), s"""'${path}' must have at least ${minLength} ${if (minLength > 1) "elements" else "element"}.""")
          }
        }
        case _ => err(Some(c.getValue(path).origin.lineNumber()), s"""'${path}' must be of type list.""")
      }
  }

  def validateRegex(path: String)(regex: Option[String])(implicit c: Config): Either[Errors, Option[Regex]] = {
    def err(lineNumber: Option[Int], msg: String): Either[Errors, Option[Regex]] = Left(ConfigError(path, lineNumber, msg) :: Nil)

    try {
      // try to compile regex which will fail with bad characters
      regex match {
        case Some(regex) => Right(Option(new Regex(regex)))
        case None => Right(None)
      }
    } catch {
      case e: Exception => err(Some(c.getValue(path).origin.lineNumber()), e.getMessage)
    }
  }

}
