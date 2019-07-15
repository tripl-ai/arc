package ai.tripl.arc.api

import java.time.LocalTime

import org.apache.spark.sql.{DataFrame, SparkSession}

import org.apache.spark.sql.types._
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.storage.StorageLevel

import ai.tripl.arc.plugins.{DynamicConfigurationPlugin, LifecyclePlugin, PipelineStagePlugin, UDFPlugin}

/** The API defines the model for a pipline. It is made up of stages,
  * extract, transform and load with their respective settings.
  */
object API {

  /** ARCContext is used to define immutable global run parameters.
  */
  case class ARCContext(
    /** the job identifier
      */
    jobId: Option[String],

    /** the name of the job
      */
    jobName: Option[String],

    /** the running environment
      */
    environment: Option[String],

    /** the running environment identifier
      */
    environmentId: Option[String],

    /** the job configuration path
      */
    configUri: Option[String],

    /** whether the job is in structured streaming or batch mode
      */
    isStreaming: Boolean,

    /** whether to ignore environments and process everything
      */
    ignoreEnvironments: Boolean,

    /** the storage level for any dataframe persistence
      */
    storageLevel: StorageLevel,

    /** whether to use createTempView or createOrReplaceTempView at runtime
      */
    immutableViews: Boolean,

    /** the command line arguments
      */
    commandLineArguments: Map[String, String],

    /** a list of dynamic configuration plugins which are resolved before parsing the config
      */
    dynamicConfigurationPlugins: List[DynamicConfigurationPlugin],

    /** a list of lifecycle plugins which are called before and after each stage
      */
    lifecyclePlugins: List[LifecyclePlugin],

    /** a list of active lifecycle plugins which are called before and after each stage
      */
    activeLifecyclePlugins: List[LifecyclePluginInstance],

    /** a list of pipeline stage plugins which are executed in the pipeline
      */
    pipelineStagePlugins: List[PipelineStagePlugin],

    /** a list of udf plugins which are registered before running the pipeline
      */
    udfPlugins: List[UDFPlugin],

    /** a map of objects which can be attached to the context for plugins
      * try to avoid using this as it is hacky
      */
    userData: collection.mutable.Map[String, Object]
  )

  /** ExtractColumns are used to define schemas for typing transforms
    * as well as when extracting from sources that lack a schema such
    * as CSV.
    */
  sealed trait ExtractColumn {

    /** The immutable id for the this column, normally a GUID. Used in
      * constructing the initial hash for lineage as well as a general
      * reference.
      */
    def id(): String

    /** The name of the column, should match the source.
      */
    def name(): String

    def description(): Option[String]

    def nullable(): Boolean

    def sparkDataType(): DataType

    def nullReplacementValue(): Option[String]

    // whether to trim the String value (?)
    def trim(): Boolean

    /** List of possible strings that are equivalent to null e.g. "", "null".
      */
    def nullableValues: List[String]

    def metadata(): Option[String]
  }

  object Extract {
    def toStructType(cols: List[ExtractColumn]): StructType = {
      val fields = cols.map(c => ExtractColumn.toStructField(c))
      StructType(fields)
    }
  }

  object ExtractColumn {

    /** Converts an ExtractColumn to a Spark StructField in order to create a
      * Schema. Adds additional internal metadata that will be persisted in
      * parquet.
      */
    def toStructField(col: ExtractColumn): StructField = {

      val metadataBuilder = col.metadata match {
        case Some(meta) => new MetadataBuilder().withMetadata(Metadata.fromJson(meta))
        case None => new MetadataBuilder()
      }

      for (desc <- col.description) {
        metadataBuilder.putString("description", desc)
      }
      metadataBuilder.putBoolean("nullable", col.nullable)
      metadataBuilder.putBoolean("internal", false)

      StructField(col.name, col.sparkDataType, col.nullable, metadataBuilder.build())
    }
  }

  case class StringColumn(id: String, name: String, description: Option[String], nullable: Boolean, nullReplacementValue: Option[String], trim: Boolean, nullableValues: List[String], metadata: Option[String], minLength: Option[Int], maxLength: Option[Int]) extends ExtractColumn {
    val sparkDataType: DataType = StringType
  }

  /** Formatters is a list of valid Java Time formats. Will attemp to parse in
    * order so most likely match should be first.
    */
  case class DateColumn(id: String, name: String, description: Option[String], nullable: Boolean, nullReplacementValue: Option[String], trim: Boolean, nullableValues: List[String], formatters: List[String], metadata: Option[String], strict: Boolean) extends ExtractColumn {
    val sparkDataType: DataType = DateType
  }

  /** Formatters is a list of valid Java Time formats. Will attemp to parse in
    * order so most likely match should be first.
    */
  case class TimeColumn(id: String, name: String, description: Option[String], nullable: Boolean, nullReplacementValue: Option[String], trim: Boolean, nullableValues: List[String], formatters: List[String], metadata: Option[String]) extends ExtractColumn {
    val sparkDataType: DataType = StringType
  }

  /** Formatters is a list of valid Java Time formats. Will attemp to parse in
    * order so most likely match should be first.
    */
  case class TimestampColumn(id: String, name: String, description: Option[String], nullable: Boolean, nullReplacementValue: Option[String], trim: Boolean, nullableValues: List[String], timezoneId: String, formatters: List[String], time: Option[LocalTime], metadata: Option[String], strict: Boolean) extends ExtractColumn {
    val sparkDataType: DataType = TimestampType
  }

  case class BinaryColumn(id: String, name: String, description: Option[String], nullable: Boolean, nullReplacementValue: Option[String], trim: Boolean, nullableValues: List[String], encoding: EncodingType, metadata: Option[String]) extends ExtractColumn {
    val sparkDataType: DataType = BinaryType
  }

  sealed trait EncodingType {
    def sparkString(): String
  }
  case object EncodingTypeBase64 extends EncodingType { val sparkString = "base64" }
  case object EncodingTypeHexadecimal extends EncodingType { val sparkString = "hexadecimal" }


  /** true / false values are lists of strings that are considered equivalent
    * to true or false e.g. "Y", "yes", "N", "no".
    */
  case class BooleanColumn(id: String, name: String, description: Option[String], nullable: Boolean, nullReplacementValue: Option[String], trim: Boolean, nullableValues: List[String], trueValues: List[String], falseValues: List[String], metadata: Option[String]) extends ExtractColumn {
    val sparkDataType: DataType = BooleanType
  }

  case class IntegerColumn(id: String, name: String, description: Option[String], nullable: Boolean, nullReplacementValue: Option[String], trim: Boolean = true, nullableValues: List[String], metadata: Option[String], formatters: Option[List[String]]) extends ExtractColumn {
    val sparkDataType: DataType = IntegerType
  }

  case class LongColumn(id: String, name: String, description: Option[String], nullable: Boolean, nullReplacementValue: Option[String], trim: Boolean, nullableValues: List[String], metadata: Option[String], formatters: Option[List[String]]) extends ExtractColumn {
    val sparkDataType: DataType = LongType
  }

  case class DoubleColumn(id: String, name: String, description: Option[String], nullable: Boolean, nullReplacementValue: Option[String], trim: Boolean, nullableValues: List[String], metadata: Option[String], formatters: Option[List[String]]) extends ExtractColumn {
    val sparkDataType: DataType = DoubleType
  }

  case class DecimalColumn(id: String, name: String, description: Option[String], nullable: Boolean, nullReplacementValue: Option[String], trim: Boolean, nullableValues: List[String], precision: Int, scale: Int, metadata: Option[String], formatters: Option[List[String]]) extends ExtractColumn {
    val sparkDataType: DataType = DecimalType(precision, scale)
  }

  sealed trait MetadataFormat
  object MetadataFormat {
    case object Json extends MetadataFormat
    case object Avro extends MetadataFormat
  }

  case class MetadataSchema(name: String, format: MetadataFormat)

  // a VersionedPlugin requires the version argument
  trait VersionedPlugin extends Serializable {

    def version: String

  }

  // a ConfigPlugin reads a typesafe config and produces a plugin instance
  trait ConfigPlugin[T] extends VersionedPlugin {

    def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], T]

  }

  // A Pipeline has 1 or more stages
  trait PipelineStage {

    def plugin: PipelineStagePlugin

    def name: String

    def description: Option[String]

    lazy val stageDetail: collection.mutable.Map[String, Object] = {
      val detail = new collection.mutable.HashMap[String, Object]()
      detail.put("type", plugin.getClass.getSimpleName)
      detail.put("plugin", s"${plugin.getClass.getName}:${plugin.version}")
      detail.put("name", name)
      for (d <- description) {
        detail.put("description", d)
      }
      detail
    }

    def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = None

  }

  // A LifecyclePluginInstance executes before and after PipelineStage execution
  trait LifecyclePluginInstance {

    def plugin: LifecyclePlugin

    def before(stage: PipelineStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext)

    def after(stage: PipelineStage, result: Option[DataFrame], isLast: Boolean)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext)

  }


  sealed trait FailModeType {
    def sparkString(): String
  }
  case object FailModeTypePermissive extends FailModeType { val sparkString = "permissive" }
  case object FailModeTypeFailFast extends FailModeType { val sparkString = "failfast" }


  sealed trait OutputModeType {
    def sparkString(): String
  }
  case object OutputModeTypeAppend extends OutputModeType { val sparkString = "append" }
  case object OutputModeTypeComplete extends OutputModeType { val sparkString = "complete" }
  case object OutputModeTypeUpdate extends OutputModeType { val sparkString = "update" }

  sealed trait IsolationLevelType {
    def sparkString(): String
  }
  case object IsolationLevelNone extends IsolationLevelType { val sparkString = "NONE" }
  case object IsolationLevelReadCommitted extends IsolationLevelType { val sparkString = "READ_COMMITTED" }
  case object IsolationLevelReadUncommitted extends IsolationLevelType { val sparkString = "READ_UNCOMMITTED" }
  case object IsolationLevelRepeatableRead extends IsolationLevelType { val sparkString = "REPEATABLE_READ" }
  case object IsolationLevelSerializable extends IsolationLevelType { val sparkString = "SERIALIZABLE" }

  sealed trait ResponseType {
    def sparkString(): String
  }
  case object IntegerResponse extends ResponseType { val sparkString = "integer" }
  case object DoubleResponse extends ResponseType { val sparkString = "double" }
  case object StringResponse extends ResponseType { val sparkString = "string" }

  sealed trait Authentication
  object Authentication {
    case class AmazonAccessKey(accessKeyID: String, secretAccessKey: String, endpoint: Option[String], ssl: Option[Boolean]) extends Authentication
    case class AzureSharedKey(accountName: String, signature: String) extends Authentication
    case class AzureSharedAccessSignature(accountName: String, container: String, token: String) extends Authentication
    case class AzureDataLakeStorageToken(clientID: String, refreshToken: String) extends Authentication
    case class AzureDataLakeStorageGen2AccountKey(accountName: String, accessKey: String) extends Authentication
    case class AzureDataLakeStorageGen2OAuth(clientID: String, secret: String, directoryId: String) extends Authentication
    case class GoogleCloudStorageKeyFile(projectID: String, keyFilePath: String) extends Authentication
  }

  case class ETLPipeline(stages: List[PipelineStage])

  case class TypingError(field: String, message: String)

  object TypingError {

    def forCol(col: ExtractColumn, message: String): TypingError = {
      TypingError(col.name, message)
    }

    def nullErrorForCol(col: ExtractColumn): TypingError = {
      TypingError(col.name, "Non-null value expected")
    }

    def nullReplacementValueNullErrorForCol(col: ExtractColumn): TypingError = {
      TypingError(col.name, "Non-null value expected and no nullReplacementValue value was provided")
    }

  }

  sealed trait ExtractReaderOptions
  case class CsvReaderOptions (
    hasHeader: Boolean,
    ignoreHeader: Boolean,
    delimiter: Char
  ) extends ExtractReaderOptions

  case class ErrorRow(row: String, rowIndex: Long, err:String)

}

/** Spark file reader options.
  */
sealed trait Delimiter {
  def value(): String
}

object Delimiter {
  case object Comma extends Delimiter {
    val value = ","
  }
  case object DefaultHive extends Delimiter {
    val value = s"${0x01 : Char}"
  }
  case object Pipe extends Delimiter {
    val value = "|"
  }
  case object Custom extends Delimiter {
    val value = ""
  }
}

sealed trait QuoteCharacter {
  def value(): String
}

object QuoteCharacter {
  case object Disabled extends QuoteCharacter {
    val value = s"${0x0 : Char}"
  }
  case object DoubleQuote extends QuoteCharacter {
    val value = "\""
  }
  case object SingleQuote extends QuoteCharacter {
    val value = "'"
  }
}

sealed trait SourceType {
  def getDescription(): String
}

case class Parquet(
  mergeSchema: Boolean = false
) extends SourceType {
  val getDescription = "parquet"
}

case class JSON(
  multiLine: Boolean = true
) extends SourceType {
  val getDescription = "JSON"
}

case class Delimited(
  sep: Delimiter = Delimiter.DefaultHive,
  quote: QuoteCharacter = QuoteCharacter.DoubleQuote,
  header: Boolean = false,
  inferSchema: Boolean = false,
  customDelimiter: String = ""
) extends SourceType {
  val getDescription = "Delimited"
}

object Delimited {
  def toSparkOptions(delimited: Delimited): Map[String, String] = {
    import delimited._

    delimited.sep match {
      case Delimiter.Custom => {
        Map(
          "sep" -> customDelimiter,
          "quote" -> quote.value,
          "header" -> header.toString,
          "inferSchema" -> inferSchema.toString
        )
      }
      case _ => {
        Map(
          "sep" -> sep.value,
          "quote" -> quote.value,
          "header" -> header.toString,
          "inferSchema" -> inferSchema.toString
        )
      }
    }
  }
}

object JSON {
  def toSparkOptions(json: JSON): Map[String, String] = {
    import json._
    Map(
      "multiLine" -> multiLine.toString
    )
  }
}
