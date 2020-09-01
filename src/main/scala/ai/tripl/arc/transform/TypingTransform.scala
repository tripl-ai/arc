package ai.tripl.arc.transform

import java.sql.Date
import java.sql.Timestamp
import java.text.DecimalFormat
import java.text.ParsePosition
import java.time.LocalDate
import java.time.LocalTime
import java.time.{ZoneId, ZonedDateTime}
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.format.ResolverStyle
import java.time.format.SignStyle
import java.time.temporal.ChronoField
import org.apache.commons.codec.binary.Base64
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.util.LongAccumulator

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.Utils

import com.typesafe.config._

class TypingTransform extends PipelineStagePlugin with JupyterCompleter {

  val version = Utils.getFrameworkVersion

  val snippet = """{
    |  "type": "TypingTransform",
    |  "name": "TypingTransform",
    |  "environments": [
    |    "production",
    |    "test"
    |  ],
    |  "inputView": "inputView",
    |  "schemaURI": "hdfs://*.json",
    |  "outputView": "outputView"
    |}""".stripMargin

  val documentationURI = new java.net.URI(s"${baseURI}/transform/#typingtransform")

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "id" :: "name" :: "description" :: "environments" :: "schema" :: "schemaURI" :: "schemaView" :: "inputView" :: "outputView" :: "authentication" :: "failMode" :: "persist" :: "params" :: "numPartitions" :: "partitionBy" :: Nil
    val id = getOptionalValue[String]("id")
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val authentication = readAuthentication("authentication")

    val source = checkOneOf(c)(Seq("schema", "schemaURI", "schemaView"))
    val (schema, schemaURI, schemaView) = if (source.isRight) {
      (
        if (c.hasPath("schema")) Right(c.getConfigList("schema").asScala.map { o => o.root().render(ConfigRenderOptions.concise()) }.mkString("[", ",", "]") ) |> verifyInlineSchemaPolicy("schema") _ |> getExtractColumns("schema") _ else Right(List.empty),
        if (c.hasPath("schemaURI")) getValue[String]("schemaURI") |> parseURI("schemaURI") _ |> textContentForURI("schemaURI", authentication) |> getExtractColumns("schemaURI") _ else Right(List.empty),
        if (c.hasPath("schemaView")) getValue[String]("schemaView") else Right("")
      )
    } else {
      (Right(List.empty), Right(List.empty), Right(""))
    }

    val inputView = getValue[String]("inputView")
    val outputView = getValue[String]("outputView")
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val failMode = getValue[String]("failMode", default = Some("permissive"), validValues = "permissive" :: "failfast" :: Nil) |> parseFailMode("failMode") _
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (id, name, description, source, schema, schemaURI, schemaView, inputView, outputView, persist, failMode, numPartitions, partitionBy, invalidKeys, authentication) match {
      case (Right(id), Right(name), Right(description), Right(source), Right(schema), Right(schemaURI), Right(schemaView), Right(inputView), Right(outputView), Right(persist), Right(failMode), Right(numPartitions), Right(partitionBy), Right(invalidKeys), Right(authentication)) =>
        val _schema = if (c.hasPath("schemaView")) {
          Left(schemaView)
        } else if (c.hasPath("schemaURI")) {
          Right(schemaURI)
        } else {
          Right(schema)
        }

        val stage = TypingTransformStage(
          plugin=this,
          id=id,
          name=name,
          description=description,
          schema=_schema,
          inputView=inputView,
          outputView=outputView,
          params=params,
          persist=persist,
          failMode=failMode,
          numPartitions=numPartitions,
          partitionBy=partitionBy
        )

        if (c.hasPath("schemaView")) {
          stage.stageDetail.put("schemaView", c.getString("schemaView"))
        } else if (c.hasPath("schemaURI")) {
          stage.stageDetail.put("schemaURI", c.getString("schemaURI"))
        }
        authentication.foreach { authentication => stage.stageDetail.put("authentication", authentication.method) }
        numPartitions.foreach { numPartitions => stage.stageDetail.put("numPartitions", Integer.valueOf(numPartitions)) }
        stage.stageDetail.put("failMode", failMode.sparkString)
        stage.stageDetail.put("inputView", inputView)
        stage.stageDetail.put("outputView", outputView)
        stage.stageDetail.put("partitionBy", partitionBy.asJava)
        stage.stageDetail.put("persist", java.lang.Boolean.valueOf(persist))

        Right(stage)
      case _ =>
        val allErrors: Errors = List(id, name, description, source, schema, schemaURI, schemaView, inputView, outputView, persist, authentication, failMode, invalidKeys, numPartitions, partitionBy).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}
case class TypingTransformStage(
    plugin: TypingTransform,
    id: Option[String],
    name: String,
    description: Option[String],
    schema: Either[String, List[ExtractColumn]],
    inputView: String,
    outputView: String,
    params: Map[String, String],
    persist: Boolean,
    failMode: FailMode,
    numPartitions: Option[Int],
    partitionBy: List[String]
  ) extends TransformPipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    TypingTransformStage.execute(this)
  }

}

object TypingTransformStage {

  def execute(stage: TypingTransformStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    val cols = stage.schema match {
      case Right(cols) => {
        cols match {
          case Nil => throw new Exception(s"""TypingTransform requires an input schema to define how to transform data but the provided schema has 0 columns.""") with DetailException {
            override val detail = stage.stageDetail
          }
          case c => c
        }
      }
      case Left(view) => {
        val parseResult: ai.tripl.arc.util.ArcSchema.ParseResult = ai.tripl.arc.util.ArcSchema.parseArcSchemaDataFrame(spark.table(view))
        parseResult match {
          case Right(cols) => cols
          case Left(errors) => throw new Exception(s"""Schema view '${view}' to cannot be parsed as it has errors: ${errors.mkString(", ")}.""") with DetailException {
            override val detail = stage.stageDetail
          }
        }
      }
    }
    stage.stageDetail.put("columns", cols.map(_.name).asJava)

    val df = spark.table(stage.inputView)

    // get schema length filtering out any internal fields
    val inputColumnCount = df.schema.filter(row => {
      !row.metadata.contains("internal") || (row.metadata.contains("internal") && row.metadata.getBoolean("internal") == false)
    }).length

    if (inputColumnCount != cols.length) {
      stage.stageDetail.put("schemaColumnCount", java.lang.Integer.valueOf(cols.length))
      stage.stageDetail.put("inputColumnCount", java.lang.Integer.valueOf(inputColumnCount))

      throw new Exception(s"TypingTransform can only be performed on tables with the same number of columns, but the schema has ${cols.length} columns and the data table has ${inputColumnCount} columns.") with DetailException {
        override val detail = stage.stageDetail
      }
    }

    // initialise statistics accumulators or reset if they exist
    val valueAccumulator = spark.sparkContext.longAccumulator
    val errorAccumulator = spark.sparkContext.longAccumulator

    val transformedDF = try {
      Typing.typeDataFrame(df, cols, stage.failMode, valueAccumulator, errorAccumulator)
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    // repartition to distribute rows evenly
    val repartitionedDF = stage.partitionBy match {
      case Nil => {
        stage.numPartitions match {
          case Some(numPartitions) => transformedDF.repartition(numPartitions)
          case None => transformedDF
        }
      }
      case partitionBy => {
        // create a column array for repartitioning
        val partitionCols = partitionBy.map(col => transformedDF(col))
        stage.numPartitions match {
          case Some(numPartitions) => transformedDF.repartition(numPartitions, partitionCols:_*)
          case None => transformedDF.repartition(partitionCols:_*)
        }
      }
    }
    if (arcContext.immutableViews) repartitionedDF.createTempView(stage.outputView) else repartitionedDF.createOrReplaceTempView(stage.outputView)

    if (!repartitionedDF.isStreaming) {
      stage.stageDetail.put("outputColumns", java.lang.Integer.valueOf(repartitionedDF.schema.length))
      stage.stageDetail.put("numPartitions", java.lang.Integer.valueOf(repartitionedDF.rdd.partitions.length))

      if (stage.persist) {
        repartitionedDF.persist(arcContext.storageLevel)
        stage.stageDetail.put("records", java.lang.Long.valueOf(repartitionedDF.count))
        stage.stageDetail.put("values", java.lang.Long.valueOf(valueAccumulator.value))
        stage.stageDetail.put("errors", java.lang.Long.valueOf(errorAccumulator.value))
      }
    }

    Option(repartitionedDF)
  }

}

object Typing {

  import API._

  /** Phantom Type to enable compiler to find the encoder we want
    */
  type TypedRow = Row

  /** Uses the provided schema to perform a DataFrame map from Row to a TypedRow.
    * We must use the DataFrame map and not RDD as RDD operations break the
    * logical plan which is required for lineage.
    */
  private def performTyping(df: DataFrame, cols: List[ExtractColumn], typedSchema: StructType, failMode: FailMode, valueAccumulator: LongAccumulator, errorAccumulator: LongAccumulator)( implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): Dataset[TypedRow] = {
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
            val col = cols(fieldIdx)
            // Pass through when the incoming type matches the outgoing type
            // except where StringType so that rules like nullableValues can be applied consistently
            if (col.sparkDataType == field.dataType && field.dataType != StringType) {
                (row.get(fieldIdx) :: valuesAccum, errorsAccum)
            } else {
              // TODO: add support for converting between types etc decimal to timestamp, date to timestamp etc
              Typing.typeValue(row.getString(fieldIdx), col) match {
                case (Some(v), Some(err)) => (v :: valuesAccum, err :: errorsAccum)
                case (Some(v), None) => (v :: valuesAccum, errorsAccum)
                case (None, Some(err)) => {
                  if (col.nullable || failMode == FailMode.FailFast) {
                    (null :: valuesAccum, err :: errorsAccum)
                  } else {
                    // this exception is to override the default spark non-nullable error which is not intuitive:
                    // The 0th field '<column name>' of input row cannot be null.
                    throw new Exception(s"""TypingTransform with non-nullable column '${err.field}' cannot continue due to error: ${err.message}.""")
                  }
                }
                case (None, None) => (null :: valuesAccum, errorsAccum)
              }
            }
          }
        }

        val allErrors: List[Row] = errors match {
          case Nil => Nil
          case _ =>
            errors.reverse.map { err =>
              Row(err.field, err.message)
            }
        }

        if (failMode == FailMode.FailFast && allErrors.length != 0) {
          throw new Exception(s"""TypingTransform with failMode equal to '${failMode.sparkString}' cannot continue due to row with error(s): [${allErrors.map(_.toString).mkString(", ")}].""")
        }

        // TODO: added idx column back (if not in the extract)
        //val rowValues = allErrors :: idx :: values
        val rowValues = allErrors :: values

        // record metrics
        errorAccumulator.add(errors.length)
        valueAccumulator.add(values.length)

        // cast to a TypedRow to fit the Dataset map method requirements
        Row(rowValues.reverse:_*).asInstanceOf[TypedRow]
    }
  }

  def typeDataFrame(untypedDataframe: DataFrame, cols: List[ExtractColumn], failMode: FailMode, valueAccumulator: LongAccumulator, errorAccumulator: LongAccumulator)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): DataFrame = {
    val schema = Extract.toStructType(cols)
    val internalFields = untypedDataframe.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).toList

    val typedSchema = StructType(
      schema.fields.toList ::: internalFields ::: Typing.typedFields
    )

    // applies data types but not metadata
    val typedDS = performTyping(untypedDataframe, cols, typedSchema, failMode, valueAccumulator, errorAccumulator)

    // re-attach metadata to result
    var typedDF = typedDS.toDF
    typedSchema.foreach(field => {
      typedDF = typedDF.withColumn(field.name, col(field.name).as(field.name, field.metadata))
    })

    typedDF
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
                case c: BinaryColumn => BinaryTypeable.typeValue(c, nullReplacementValue)
                case c: BooleanColumn => BooleanTypeable.typeValue(c, nullReplacementValue)
                case c: DateColumn => DateTypeable.typeValue(c, nullReplacementValue)
                case c: DecimalColumn => DecimalTypeable.typeValue(c, nullReplacementValue)
                case c: DoubleColumn => DoubleTypeable.typeValue(c, nullReplacementValue)
                case c: TimeColumn => TimeTypeable.typeValue(c, nullReplacementValue)
                case c: IntegerColumn => IntegerTypeable.typeValue(c, nullReplacementValue)
                case c: LongColumn => LongTypeable.typeValue(c, nullReplacementValue)
                case c: StringColumn => StringTypeable.typeValue(c, nullReplacementValue)
                case c: TimestampColumn => TimestampTypeable.typeValue(c, nullReplacementValue)
                case c: StructColumn => throw new Exception("TypingTransform does not support 'StructColumn' type.")
                case c: ArrayColumn => throw new Exception("TypingTransform does not support 'ArrayColumn' type.")
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
        case c: BinaryColumn => BinaryTypeable.typeValue(c, valueToType)
        case c: BooleanColumn => BooleanTypeable.typeValue(c, valueToType)
        case c: DateColumn => DateTypeable.typeValue(c, valueToType)
        case c: DecimalColumn => DecimalTypeable.typeValue(c, valueToType)
        case c: DoubleColumn => DoubleTypeable.typeValue(c, valueToType)
        case c: IntegerColumn => IntegerTypeable.typeValue(c, valueToType)
        case c: LongColumn => LongTypeable.typeValue(c, valueToType)
        case c: StringColumn => StringTypeable.typeValue(c, valueToType)
        case c: TimeColumn => TimeTypeable.typeValue(c, valueToType)
        case c: TimestampColumn => TimestampTypeable.typeValue(c, valueToType)
        case c: StructColumn => throw new Exception("TypingTransform does not support 'StructColumn' type.")
        case c: ArrayColumn => throw new Exception("TypingTransform does not support 'ArrayColumn' type.")
      }
    }
  }

  val errorStructType: StructType =
    StructType(
      StructField("field", StringType, false) ::
      StructField("message", StringType, false) :: Nil
    )

  val typedFields: List[StructField] =
    StructField("_errors", ArrayType(errorStructType), true, new MetadataBuilder().putBoolean("internal", true).putString("description", "An Arc internal field detailing any errors when executing TypingTransform against this row.").build()) :: Nil

  type TypingResult[S] = (Option[S], Option[TypingError])

  sealed trait Typeable[T <: ExtractColumn, S] {
    def typeValue(col: T, value: String): TypingResult[S]
  }

  object Typeable {

    object StringTypeable extends Typeable[StringColumn, String] {

      import scala.collection.mutable.HashMap
      import scala.util.matching.Regex

      case class ValidationResult(valid: Boolean, errorMessage: Option[String] = None)

      sealed trait Validator {
        def validate(value: String): ValidationResult
      }

      case class MinLengthValidator(minLength: Int) extends Validator {
        def validate(value: String): ValidationResult = {
          if (value.length < minLength) {
            val errorMsg = s"String '$value' (${value.length} characters) is less than minLength ($minLength)."
            ValidationResult(false, Option(errorMsg))
          } else {
            ValidationResult(true)
          }
        }
      }

      case class MaxLengthValidator(maxLength: Int) extends Validator {
        def validate(value: String): ValidationResult = {
          if (value.length > maxLength) {
            val errorMsg = s"String '$value' (${value.length} characters) is greater than maxLength ($maxLength)."
            ValidationResult(false, Option(errorMsg))
          } else {
            ValidationResult(true)
          }
        }
      }

      case class RegexValidator(regex: Regex) extends Validator {
        def validate(value: String): ValidationResult = {
          if (regex.pattern.matcher(value).matches) {
            ValidationResult(true)
          } else {
            val errorMsg = s"String '$value' does not match regex '${regex.pattern.toString}'."
            ValidationResult(false, Option(errorMsg))
          }
        }
      }

      private val validatorMemo: HashMap[StringColumn, Seq[Validator]] = HashMap[StringColumn, Seq[Validator]]()

      def colValidators(col: StringColumn): Seq[Validator] = {
        validatorMemo.getOrElseUpdate(col, {
          val minLength = col.minLength.map(MinLengthValidator(_) :: Nil).getOrElse(Nil)
          val maxLength = col.maxLength.map(MaxLengthValidator(_) :: Nil).getOrElse(Nil)
          val regexMatch = col.regex.map(RegexValidator(_) :: Nil).getOrElse(Nil)

          minLength ::: maxLength ::: regexMatch ::: Nil
        })
      }

      def typeValue(col: StringColumn, value: String): (Option[String], Option[TypingError]) = {

        val validationResult = colValidators(col).foldLeft(ValidationResult(true)){ (acc, v) =>
          val res = v.validate(value)
          val valid = acc.valid && res.valid
          val errorMsg = (acc.errorMessage, res.errorMessage) match {
            case (Some(e1), Some(e2)) => Option(e1 + " " + e2)
            case (Some(e1), None) => Option(e1)
            case (None, Some(e2)) => Option(e2)
            case (None, None) => None
          }
          ValidationResult(valid, errorMsg)
        }

        validationResult match {
          case ValidationResult(true, _) => Option(value) -> None
          case ValidationResult(false, errorMsgs) => None -> errorMsgs.map(TypingError.forCol(col, _))
        }
      }

    }

    object NumberUtils {

      // memoizedFormatters not used as DecimalFormat objects are not thread safe and performance
      // cost of using them is not significant enough. Could be opportunity for optimisation if care is taken.

      @scala.annotation.tailrec
      def parseNumber(formatters: List[String], value: String): Option[Number] = {
        formatters match {
          case Nil => None
          case head :: tail =>
            try {
              val formatter = new DecimalFormat(head)
              val pos = new ParsePosition(0)
              val number = formatter.parse(value, pos)

              // ensure all characters from input string have been processed
              // and no errors exist
              if (pos.getIndex != value.length || pos.getErrorIndex != -1) {
                throw new Exception()
              }

              Option(number)
            } catch {
              case e: Exception =>
                // Log Error and occurances?
                parseNumber(tail, value)
            }
        }
      }

      @scala.annotation.tailrec
      def parseBigDecimal(formatters: List[String], value: String): Option[BigDecimal] = {
        formatters match {
          case Nil => None
          case head :: tail =>
            try {
              // get the formatter from memory if available
              val formatter = new DecimalFormat(head)
              formatter.setParseBigDecimal(true)
              val pos = new ParsePosition(0)
              val number = formatter.parse(value, pos).asInstanceOf[java.math.BigDecimal]

              // ensure all characters from input string have been processed
              // and no errors exist
              if (pos.getIndex != value.length || pos.getErrorIndex != -1) {
                throw new Exception
              }

              Option(scala.math.BigDecimal(number))
            } catch {
              case e: Exception =>
                // Log Error and occurances?
                parseBigDecimal(tail, value)
            }
        }
      }

    }

    object IntegerTypeable extends Typeable[IntegerColumn, Int] {
      import NumberUtils._

      def typeValue(col: IntegerColumn, value: String): (Option[Int], Option[TypingError]) = {
        val formatters = col.formatters.getOrElse(List("#,##0;-#,##0"))

        try {
          val v = col.formatters match {
            case Some(fmt) => {
              // number.intValue does not throw exception when < Int.MinValue || > Int.MaxValue
              val number = parseNumber(fmt, value)
              number.map( num => num.toString.toInt )
            }
            case None => Option(value.toInt)
          }
          if(v == None)
            throw new Exception()
          v -> None
        } catch {
          case e: Exception =>
            None -> Some(TypingError.forCol(col, s"""Unable to convert '${value}' to integer using formatters [${formatters.map(c => s"'${c}'").mkString(", ")}]"""))
        }
      }

    }

    object LongTypeable extends Typeable[LongColumn, Long] {
      import NumberUtils._

      def typeValue(col: LongColumn, value: String): (Option[Long], Option[TypingError]) = {
        val formatters = col.formatters.getOrElse(List("#,##0;-#,##0"))

        try {
          val v = col.formatters match {
            case Some(fmt) => {
              // number.longValue does not throw exception when < Long.MinValue || >  Long.MaxValue
              val number = parseNumber(fmt, value)
              number.map( num => num.toString.toLong )
            }
            case None => Option(value.toLong)
          }
          if(v == None)
            throw new Exception()
          v -> None
        } catch {
          case e: Exception =>
            None -> Some(TypingError.forCol(col, s"""Unable to convert '${value}' to long using formatters [${formatters.map(c => s"'${c}'").mkString(", ")}]"""))
        }
      }

    }

    object DoubleTypeable extends Typeable[DoubleColumn, Double] {
      import NumberUtils._

      def typeValue(col: DoubleColumn, value: String): (Option[Double], Option[TypingError]) = {
        val formatters = col.formatters.getOrElse(List("#,##0.###;-#,##0.###"))

        try {
          val v = col.formatters match {
            case Some(fmt) => {
              // number.doubleValue does not throw exception when < Double.MinValue || >  Double.MaxValue
              val number = parseNumber(fmt, value)
              number.map( num => num.toString.toDouble )
            }
            case None => Option(value.toDouble)
          }
          if(v == None)
            throw new Exception()
          if (v.get.isInfinite)
            throw new Exception()
          v -> None
        } catch {
          case e: Exception =>
            None -> Some(TypingError.forCol(col, s"""Unable to convert '${value}' to double using formatters [${formatters.map(c => s"'${c}'").mkString(", ")}]"""))
        }
      }

    }

    object DecimalTypeable extends Typeable[DecimalColumn, Decimal] {
      import NumberUtils._

      def typeValue(col: DecimalColumn, value: String): (Option[Decimal], Option[TypingError]) = {
        val formatters = col.formatters.getOrElse(List("#,##0.###;-#,##0.###"))

        try {
          val v = col.formatters match {
            case Some(fmt) => {
              val number = parseBigDecimal(fmt, value)
              number.map( num => Decimal(num, col.precision, col.scale) )
            }
            case None => {
              val number = Decimal(value)
              if (!number.changePrecision(col.precision, col.scale)) {
                throw new Exception()
              }
              Option(number)
            }
          }
          if(v == None)
            throw new Exception()
          v -> None
        } catch {
          case e: Exception =>
            None -> Some(TypingError.forCol(col, s"""Unable to convert '${value}' to decimal(${col.precision}, ${col.scale}) using formatters [${formatters.map(c => s"'${c}'").mkString(", ")}]"""))
        }
      }

    }

    object BinaryTypeable extends Typeable[BinaryColumn, Array[Byte]] {
      def typeValue(col: BinaryColumn, value: String): (Option[Array[Byte]], Option[TypingError]) = {
          binaryOrError(col, value)
      }

      def binaryOrError(col: BinaryColumn, value: String): TypingResult[Array[Byte]] = {
        try {
          col.encoding match {
            case EncodingTypeBase64 => {
              val valueByteArray = value.getBytes
              if (Base64.isBase64(valueByteArray)) {
                Option(Base64.decodeBase64(value)) -> None
              } else {
                throw new Exception()
              }
            }
            case EncodingTypeHexadecimal => {
              // will throw exception if not valid hexadecimal
              java.lang.Long.parseLong(value, 16)
              Option(org.apache.spark.sql.catalyst.expressions.Hex.unhex(value.getBytes)) -> None
            }
          }
        } catch {
          case e: Exception =>
            None -> Some(TypingError.forCol(col, s"Unable to convert '${value}' to binary using '${col.encoding.sparkString}' decoding."))
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

        dtf.put("ssssssssss:UTC:false:true", epochFormatter)
        dtf.put("ssssssssss:UTC:false:false", epochFormatter)
        dtf.put("sssssssssssss:UTC:false:true", epochMillisFormatter)
        dtf.put("sssssssssssss:UTC:false:false", epochMillisFormatter)
        dtf
      }

      private def zonedDateTimeFormatter(pattern: String, tz: ZoneId, strict: Boolean, caseSensitive: Boolean): DateTimeFormatter = {
        val key = s"${pattern}:${tz.getId}:${strict}:${caseSensitive}"
        // get the existing formatter or add it to memory
        memoizedFormatters.get(key).getOrElse {
          // by default datetimeformatter is case sensitive which can be hard to use
          val dateTimeFormatter = if (caseSensitive) {
            DateTimeFormatter.ofPattern(pattern)
          } else {
            val builder = new DateTimeFormatterBuilder()
            builder.parseCaseInsensitive()
            builder.appendPattern(pattern)
            builder.toFormatter()
          }
          // if formatter does not specify zone information use default
          val withZone = Seq("Offset(", "ZoneId(", "ZoneText(", "LocalizedOffset(").forall { part => !dateTimeFormatter.toString.contains(part) } match {
            case true => dateTimeFormatter.withZone(tz)
            case false => dateTimeFormatter
          }
          // apply resolver style
          val formatter = strict match {
            case true => withZone.withResolverStyle(ResolverStyle.STRICT)
            case false => withZone.withResolverStyle(ResolverStyle.SMART) // smart is default
          }
          memoizedFormatters.put(key, formatter)
          formatter
        }
      }

      private def dateTimeFormatter(pattern: String, strict: Boolean, caseSensitive: Boolean): DateTimeFormatter = {
        val key = s"${pattern}:${strict}:${caseSensitive}"
        // get the existing formatter or add it to memory
        memoizedFormatters.get(key).getOrElse {
          // by default datetimeformatter is case sensitive which can be hard to use
          val dateTimeFormatter = if (caseSensitive) {
            DateTimeFormatter.ofPattern(pattern)
          } else {
            val builder = new DateTimeFormatterBuilder()
            builder.parseCaseInsensitive()
            builder.appendPattern(pattern)
            builder.toFormatter()
          }
          val formatter = strict match {
            case true => dateTimeFormatter.withResolverStyle(ResolverStyle.STRICT)
            case false =>dateTimeFormatter.withResolverStyle(ResolverStyle.SMART) // smart is default
          }
          memoizedFormatters.put(key, formatter)
          formatter
        }
      }

      @scala.annotation.tailrec
      def parseDateTime(formatters: List[String], tz: ZoneId, strict: Boolean, caseSensitive: Boolean, value: String): Option[ZonedDateTime] = {
        formatters match {
          case Nil => None
          case head :: tail =>
            try {
              // get the formatter from memory if available
              val fmt = zonedDateTimeFormatter(head, tz, strict, caseSensitive)
              Option(ZonedDateTime.parse(value, fmt))
            } catch {
              case e: Exception =>
                // Log Error and occurances?
                parseDateTime(tail, tz, strict, caseSensitive, value)
            }
        }
      }

      @scala.annotation.tailrec
      def parseDate(formatters: List[String], strict: Boolean, caseSensitive: Boolean, value: String): Option[LocalDate] = {
        formatters match {
          case Nil => None
          case head :: tail =>
            try {
              // get the formatter from memory if available
              val fmt = dateTimeFormatter(head, strict, caseSensitive)
              Option(LocalDate.parse(value, fmt))
            } catch {
              case e: Exception =>
                // Log Error and occurances?
                parseDate(tail, strict, caseSensitive, value)
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
              val fmt = dateTimeFormatter(head, false, false)
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
            val date = parseDate(col.formatters, col.strict, col.caseSensitive, value)
            date.map( _dt => _dt.atStartOfDay(tz).withHour(time.getHour).withMinute(time.getMinute).withSecond(time.getSecond).withNano(time.getNano))
          }
          case None => parseDateTime(col.formatters, tz, col.strict, col.caseSensitive, value)
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
        val dt = parseDate(col.formatters, col.strict, col.caseSensitive, value)
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


