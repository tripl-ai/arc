package au.com.agl.arc.util

import java.sql.Date
import java.sql.Timestamp
import java.time.LocalDate
import java.time.LocalTime
import java.time.{ZoneId, ZonedDateTime}
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.format.SignStyle
import java.time.temporal.ChronoField

import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import au.com.agl.arc.api._

object Typing {

  import API._

  /** Phantom Type to enable compiler to find the encoder we want
    */
  type TypedRow = Row

  /** Uses the provided schema to perform a DataFrame map from Row to a TypedRow.
    * We must use the DataFrame map and not RDD as RDD operations break the 
    * logical plan which is required for lineage.
    */
  private def performTyping(df: DataFrame, extract: ColumnarExtract, typedSchema: StructType)( implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Dataset[TypedRow] = {
    val incomingSchema = df.schema.zipWithIndex

    /** Create a dynamic RowEncoder from the provided schema. We use the phantom
      * TypeRow type to enable implicit resolution to find our encoder.
      */
    implicit val typedEncoder: Encoder[TypedRow] = org.apache.spark.sql.catalyst.encoders.RowEncoder(typedSchema)

    df.map[TypedRow] { row: Row =>
        // type each column in the row accordind to the schema, we accumulate errors to append to a separate column
        val (values, errors) = incomingSchema.foldLeft[(List[Any], List[TypingError])]((Nil, Nil)){ case ((valuesAccum, errorsAccum), (field, fieldIdx)) =>
          val fieldMetadata = field.metadata
          
          // If field is flagged as internal pass through directly as it will not be in the incoming metadata          
          if (fieldMetadata.contains("internal") && fieldMetadata.getBoolean("internal") == true) {
              (row.get(fieldIdx) :: valuesAccum, errorsAccum)
          } else {
            val col = extract.cols(fieldIdx)
            // Pass through when the incoming type matches the outgoing type
            if (col.sparkDataType == field.dataType) {
                (row.get(fieldIdx) :: valuesAccum, errorsAccum)
            } else {              
              // TODO: add support for converting between types etc decimal to timestamp, date to timestamp etc
              Typing.typeValue(row.getString(fieldIdx), col) match {
                case (Some(v), Some(err)) => (v :: valuesAccum, err :: errorsAccum)
                case (Some(v), None) => (v :: valuesAccum, errorsAccum)
                case (None, Some(err)) => (null :: valuesAccum, err :: errorsAccum)
                case (None, None) => (null :: valuesAccum, errorsAccum)
              }
            }
          }
        }

        val allErrors: List[Row] = errors match {
          case Nil => Nil
          case _ =>
            errors.map { err =>
              Row(err.field, err.message)
            }
        }

        // TODO: added idx column back (if not in the extract)
        //val rowValues = allErrors :: idx :: values
        val rowValues = allErrors :: values

        // cast to a TypedRow to fit the Dataset map method requirements
        Row(rowValues.reverse:_*).asInstanceOf[TypedRow]
    }
  }

  def typeDataFrame(untypedDataframe: DataFrame, extract: ColumnarExtract)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): DataFrame = {
    val schema = Extract.toStructType(extract.cols)
    val internalFields = untypedDataframe.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).toList
    
    val typedSchema = StructType(
      schema.fields.toList ::: internalFields ::: Typing.typedFields
    )
    
    val typedDS = performTyping(untypedDataframe, extract, typedSchema)

    typedDS.toDF
  }

  // +--------------+--------------------------+--------------------------+--------------+-----------------------------+
  // | value.isNull | value.isAllowedNullValue | col.nullReplacementValue | col.nullable | Result                      |
  // +--------------+--------------------------+--------------------------+--------------+-----------------------------+
  // | true         | false                    | false                    | true         | return null                 |
  // +--------------+--------------------------+--------------------------+--------------+-----------------------------+
  // | true         | false                    | false                    | false        | exception                   |
  // +--------------+--------------------------+--------------------------+--------------+-----------------------------+
  // | true         | false                    | true                     | true         | return nullReplacementValue |
  // +--------------+--------------------------+--------------------------+--------------+-----------------------------+
  // | true         | false                    | true                     | false        | return nullReplacementValue |
  // +--------------+--------------------------+--------------------------+--------------+-----------------------------+
  // | false        | true                     | false                    | true         | return null                 |
  // +--------------+--------------------------+--------------------------+--------------+-----------------------------+
  // | false        | true                     | false                    | false        | exception                   |
  // +--------------+--------------------------+--------------------------+--------------+-----------------------------+
  // | false        | true                     | true                     | true         | return nullReplacementValue |
  // +--------------+--------------------------+--------------------------+--------------+-----------------------------+
  // | false        | true                     | true                     | false        | return nullReplacementValue |
  // +--------------+--------------------------+--------------------------+--------------+-----------------------------+
  def typeValue(value: String, col: ExtractColumn): (Option[Any], Option[TypingError]) = {
    import Typeable._

    val valueToType = if (col.trim && value != null) value.trim else value
    val isNull = valueToType == null
    val isAllowedNullValue = col.nullableValues.contains(valueToType)

    if (isNull || isAllowedNullValue) {
        col.nullReplacementValue match {
            case Some(nullReplacementValue) => {
              col match {
                case sc:StringColumn => StringTypeable.typeValue(sc, nullReplacementValue)
                case ic:IntegerColumn => IntegerTypeable.typeValue(ic, nullReplacementValue)
                case lc:LongColumn => LongTypeable.typeValue(lc, nullReplacementValue)
                case dc:DoubleColumn => DoubleTypeable.typeValue(dc, nullReplacementValue)
                case dc:DecimalColumn => DecimalTypeable.typeValue(dc, nullReplacementValue)
                case tc:TimestampColumn => TimestampTypeable.typeValue(tc, nullReplacementValue)
                case bc:BooleanColumn => BooleanTypeable.typeValue(bc, nullReplacementValue)
                case dc:DateColumn => DateTypeable.typeValue(dc, nullReplacementValue)
                case dc:TimeColumn => TimeTypeable.typeValue(dc, nullReplacementValue)
              }                
            }
            case None => {
              if (col.nullable) {
                (None, None)
              } else {
                (None, Some(TypingError.nullReplacementValueNullErrorForCol(col)))       
              }
            }
        }
    } else {
      // else take string value and try to convert to column type
      col match {
        case sc:StringColumn => StringTypeable.typeValue(sc, valueToType)
        case ic:IntegerColumn => IntegerTypeable.typeValue(ic, valueToType)
        case lc:LongColumn => LongTypeable.typeValue(lc, valueToType)
        case dc:DoubleColumn => DoubleTypeable.typeValue(dc, valueToType)
        case dc:DecimalColumn => DecimalTypeable.typeValue(dc, valueToType)
        case tc:TimestampColumn => TimestampTypeable.typeValue(tc, valueToType)
        case bc:BooleanColumn => BooleanTypeable.typeValue(bc, valueToType)
        case dc:DateColumn => DateTypeable.typeValue(dc, valueToType)
        case tc:TimeColumn => TimeTypeable.typeValue(tc, valueToType)
      }  
    }
  }

  val errorStructType: StructType =
    StructType(
      StructField("field", StringType, false) ::
      StructField("message", StringType, false) :: Nil
    )

  val typedFields: List[StructField] =
    // TODO: add _index column back
    //StructField("_index", LongType, false, new MetadataBuilder().putBoolean("internal", true).build()) ::
    StructField("_errors", ArrayType(errorStructType), true, new MetadataBuilder().putBoolean("internal", true).build()) :: Nil

  type TypingResult[S] = (Option[S], Option[TypingError])

  sealed trait Typeable[T <: ExtractColumn, S] {
    def typeValue(col: T, value: String): TypingResult[S]
  }

  object Typeable {

    object StringTypeable extends Typeable[StringColumn, String] {
      
      def typeValue(col: StringColumn, value: String): (Option[String], Option[TypingError]) = {
          Option(value) -> None
      }

    }

    object IntegerTypeable extends Typeable[IntegerColumn, Int] {

      def typeValue(col: IntegerColumn, value: String): (Option[Int], Option[TypingError]) = {
          try {
            Option(value.toInt) -> None
          } catch {
            case e: Exception =>
              None -> Some(TypingError.forCol(col, s"Unable to convert '${value}' to integer"))
          }
      }

    }

    object DoubleTypeable extends Typeable[DoubleColumn, Double] {
      def typeValue(col: DoubleColumn, value: String): (Option[Double], Option[TypingError]) = {
          try {
              if (value.toDouble.isInfinite)
                throw new Exception()
              Option(value.toDouble) -> None
          } catch {
            case e: Exception =>
              None -> Some(TypingError.forCol(col, s"Unable to convert '${value}' to double"))
          }
      }
      
    }

    object LongTypeable extends Typeable[LongColumn, Long] {
      def typeValue(col: LongColumn, value: String): (Option[Long], Option[TypingError]) = {
          try {
            Option(value.toLong) -> None
          } catch {
            case e: Exception =>
              None -> Some(TypingError.forCol(col, s"Unable to convert '${value}' to long"))
          }
      }
    }

    object DecimalTypeable extends Typeable[DecimalColumn, Decimal] {
      def typeValue(col: DecimalColumn, value: String): (Option[Decimal], Option[TypingError]) = {
          try {
            decimalOrError(col, value)
          } catch {
            case e: Exception =>
              None -> Some(TypingError.forCol(col, s"Unable to convert '${value}' to decimal(${col.precision}, ${col.scale})"))
          }
      }

      def decimalOrError(col: DecimalColumn, value: String): TypingResult[Decimal] = {
        val v = Decimal(value)
        if (v.changePrecision(col.precision, col.scale)) {
          Option(v) -> None
        } else {
          None -> Some(TypingError.forCol(col, s"Unable to convert '${value}' to decimal(${col.precision}, ${col.scale})"))
        }
      }
      
    }

    object BooleanTypeable extends Typeable[BooleanColumn, Boolean] {
      def typeValue(col: BooleanColumn, value: String): (Option[Boolean], Option[TypingError]) = {
          booleanOrError(col, value)
      }

      def booleanOrError(col: BooleanColumn, value: String): TypingResult[Boolean] = {
        try {
          if (col.trueValues.contains(value)) {
            Option(true) -> None
          } else if (col.falseValues.contains(value)) {
            Option(false) -> None
          } else {
            None -> Some(TypingError.forCol(col, s"""Unable to convert '${value}' to boolean using provided true values: [${col.trueValues.map(c => s"'${c}'").mkString(", ")}] or false values: [${col.falseValues.map(c => s"'${c}'").mkString(", ")}]"""))
          }
        } catch {
          case e: Exception =>
            None -> Some(TypingError.forCol(col, s"Unable to convert '${value}' to boolean"))
        }
      }

    }

    object DateTimeUtils {
      private val memoizedFormatters: collection.mutable.Map[String, DateTimeFormatter] = {
        val dtf = new collection.mutable.HashMap[String, DateTimeFormatter]()

        val epochFormatter = new DateTimeFormatterBuilder()
          .appendValue(ChronoField.INSTANT_SECONDS, 10, 10, SignStyle.NEVER)
          .toFormatter()
          .withZone(ZoneId.of("UTC"))

        val epochMillisFormatter = new DateTimeFormatterBuilder()
          .appendValue(ChronoField.INSTANT_SECONDS, 10, 10, SignStyle.NEVER)
          .appendValue(ChronoField.MILLI_OF_SECOND, 3)
          .toFormatter()
          .withZone(ZoneId.of("UTC"))          

        dtf.put("ssssssssss:UTC", epochFormatter)
        dtf.put("sssssssssssss:UTC", epochMillisFormatter)
        dtf
      }

      private def zonedDateTimeFormatter(pattern: String, tz: ZoneId): DateTimeFormatter = {
        val key = s"${pattern}:${tz.getId}"
        // get the existing formatter or add it to memory
        memoizedFormatters.get(key).getOrElse {
          val formatter = DateTimeFormatter.ofPattern(pattern).withZone(tz)
          memoizedFormatters.put(key, formatter)
          formatter
        }
      }

      private def dateTimeFormatter(pattern: String): DateTimeFormatter = {
        // get the existing formatter or add it to memory
        memoizedFormatters.get(pattern).getOrElse {
          val formatter = DateTimeFormatter.ofPattern(pattern)
          memoizedFormatters.put(pattern, formatter)
          formatter
        }
      }          

      @scala.annotation.tailrec
      def parseDateTime(formatters: List[String], tz: ZoneId, value: String): Option[ZonedDateTime] = {
        formatters match {
          case Nil => None
          case head :: tail =>
            try {
              // get the formatter from memory if available
              val fmt = zonedDateTimeFormatter(head, tz) 
              Option(ZonedDateTime.parse(value, fmt))
            } catch {
              case e: Exception =>
                // Log Error and occurances?
                parseDateTime(tail, tz, value)
            }
        }
      }

      @scala.annotation.tailrec
      def parseDate(formatters: List[String], value: String): Option[LocalDate] = {
        formatters match {
          case Nil => None
          case head :: tail =>
            try {
              // get the formatter from memory if available
              val fmt = dateTimeFormatter(head)               
              Option(LocalDate.parse(value, fmt))
            } catch {
              case e: Exception =>
                // Log Error and occurances?
                parseDate(tail, value)
            }
        }
      }

      @scala.annotation.tailrec
      def parseTime(formatters: List[String], value: String): Option[LocalTime] = {
        formatters match {
          case Nil => None
          case head :: tail =>
            try {
              // get the formatter from memory if available
              val fmt = dateTimeFormatter(head)               
              Option(LocalTime.parse(value, fmt))
            } catch {
              case e: Exception =>
                // Log Error and occurances?
                parseTime(tail, value)
            }
        }
      }      
    }

    object TimestampTypeable extends Typeable[TimestampColumn, Timestamp] {
      import DateTimeUtils._

      def timestampOrError(col: TimestampColumn, value: String): TypingResult[Timestamp] = {
        val tz = ZoneId.of(col.timezoneId)
        val dt = col.time match {
          case Some(time) => { 
            val date = parseDate(col.formatters, value)
            date.map( _dt => _dt.atStartOfDay(tz).withHour(time.getHour).withMinute(time.getMinute).withSecond(time.getSecond).withNano(time.getNano))
          }
          case None => parseDateTime(col.formatters, tz, value)
        }
       
        val v = dt.map( _dt => Timestamp.from(_dt.toInstant()))
        if(v == None)        
          throw new Exception()  
        v -> None
      }

      def typeValue(col: TimestampColumn, value: String): (Option[Timestamp], Option[TypingError]) = {
          try {
            timestampOrError(col, value)
          } catch {
            case e: Exception =>
              None -> Some(TypingError.forCol(col, s"""Unable to convert '$value' to timestamp using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}] and timezone '${col.timezoneId}'"""))
          }
      }
    }

    object DateTypeable extends Typeable[DateColumn, Date] {
      import DateTimeUtils._

      def dateOrError(col: DateColumn, value: String): TypingResult[Date] = {
        val dt = parseDate(col.formatters, value)        
        val v = dt.map( _dt => java.sql.Date.valueOf(_dt))
        if(v == None)        
          throw new Exception()        
        v -> None
      }

      def typeValue(col: DateColumn, value: String): (Option[Date], Option[TypingError]) = {
          try {
            dateOrError(col, value)
          } catch {
            case e: Exception =>
              None -> Some(TypingError.forCol(col, s"""Unable to convert '$value' to date using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}]"""))
          }
      }
    }

    object TimeTypeable extends Typeable[TimeColumn, String] {
      import DateTimeUtils._

      def timeOrError(col: TimeColumn, value: String): TypingResult[String] = {
        val tm = parseTime(col.formatters, value)        
        val v = tm.map( _tm => _tm.format(DateTimeFormatter.ISO_LOCAL_TIME))
        if(v == None)        
          throw new Exception()        
        v -> None
      }

      def typeValue(col: TimeColumn, value: String): (Option[String], Option[TypingError]) = {
          try {
            timeOrError(col, value)
          } catch {
            case e: Exception =>
              None -> Some(TypingError.forCol(col, s"""Unable to convert '$value' to time using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}]"""))
          }
      }
    } 

  }
}

