package ai.tripl.arc.api

import java.time.LocalTime

import scala.util.matching.Regex

import org.apache.spark.sql.{DataFrame, SparkSession}

import org.apache.spark.sql.types._
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.storage.StorageLevel

import ai.tripl.arc.util.SerializableConfiguration
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

    /** whether to allow ipython notebook configuration files
      */
    ipynb: Boolean = true,

    /** whether to allow inline sql submitted to stages like SQLTransform and SQLValidate
      */
    inlineSQL: Boolean = true,    

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
    userData: collection.mutable.Map[String, Object],

    /** a serialized hadoop configuration object so that executors can access it directly
    */ 
    var serializableConfiguration: SerializableConfiguration

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
    def id(): Option[String]

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

      for (id <- col.id) metadataBuilder.putString("id", id)
      for (desc <- col.description) metadataBuilder.putString("description", desc)

      metadataBuilder.putBoolean("nullable", col.nullable)
      metadataBuilder.putBoolean("internal", false)

      StructField(col.name, col.sparkDataType, col.nullable, metadataBuilder.build())
    }
    
  }

  case class StringColumn(id: Option[String], name: String, description: Option[String], nullable: Boolean, nullReplacementValue: Option[String], trim: Boolean, nullableValues: List[String], metadata: Option[String], minLength: Option[Int], maxLength: Option[Int], regex: Option[Regex]) extends ExtractColumn {
    val sparkDataType: DataType = StringType
  }

  /** Formatters is a list of valid Java Time formats. Will attemp to parse in
    * order so most likely match should be first.
    */
  case class DateColumn(id: Option[String], name: String, description: Option[String], nullable: Boolean, nullReplacementValue: Option[String], trim: Boolean, nullableValues: List[String], formatters: List[String], metadata: Option[String], strict: Boolean, caseSensitive: Boolean) extends ExtractColumn {
    val sparkDataType: DataType = DateType
  }

  /** Formatters is a list of valid Java Time formats. Will attemp to parse in
    * order so most likely match should be first.
    */
  case class TimeColumn(id: Option[String], name: String, description: Option[String], nullable: Boolean, nullReplacementValue: Option[String], trim: Boolean, nullableValues: List[String], formatters: List[String], metadata: Option[String]) extends ExtractColumn {
    val sparkDataType: DataType = StringType
  }

  /** Formatters is a list of valid Java Time formats. Will attemp to parse in
    * order so most likely match should be first.
    */
  case class TimestampColumn(id: Option[String], name: String, description: Option[String], nullable: Boolean, nullReplacementValue: Option[String], trim: Boolean, nullableValues: List[String], timezoneId: String, formatters: List[String], time: Option[LocalTime], metadata: Option[String], strict: Boolean, caseSensitive: Boolean) extends ExtractColumn {
    val sparkDataType: DataType = TimestampType
  }

  case class BinaryColumn(id: Option[String], name: String, description: Option[String], nullable: Boolean, nullReplacementValue: Option[String], trim: Boolean, nullableValues: List[String], encoding: EncodingType, metadata: Option[String]) extends ExtractColumn {
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
  case class BooleanColumn(id: Option[String], name: String, description: Option[String], nullable: Boolean, nullReplacementValue: Option[String], trim: Boolean, nullableValues: List[String], trueValues: List[String], falseValues: List[String], metadata: Option[String]) extends ExtractColumn {
    val sparkDataType: DataType = BooleanType
  }

  case class IntegerColumn(id: Option[String], name: String, description: Option[String], nullable: Boolean, nullReplacementValue: Option[String], trim: Boolean = true, nullableValues: List[String], metadata: Option[String], formatters: Option[List[String]]) extends ExtractColumn {
    val sparkDataType: DataType = IntegerType
  }

  case class LongColumn(id: Option[String], name: String, description: Option[String], nullable: Boolean, nullReplacementValue: Option[String], trim: Boolean, nullableValues: List[String], metadata: Option[String], formatters: Option[List[String]]) extends ExtractColumn {
    val sparkDataType: DataType = LongType
  }

  case class DoubleColumn(id: Option[String], name: String, description: Option[String], nullable: Boolean, nullReplacementValue: Option[String], trim: Boolean, nullableValues: List[String], metadata: Option[String], formatters: Option[List[String]]) extends ExtractColumn {
    val sparkDataType: DataType = DoubleType
  }

  case class DecimalColumn(id: Option[String], name: String, description: Option[String], nullable: Boolean, nullReplacementValue: Option[String], trim: Boolean, nullableValues: List[String], precision: Int, scale: Int, metadata: Option[String], formatters: Option[List[String]]) extends ExtractColumn {
    val sparkDataType: DataType = DecimalType(precision, scale)
  }

  case class StructColumn(id: Option[String], name: String, description: Option[String], nullable: Boolean, nullReplacementValue: Option[String], trim: Boolean, nullableValues: List[String], fields: List[ExtractColumn], metadata: Option[String]) extends ExtractColumn {
    val sparkDataType: DataType = StructType(fields.map { child => ExtractColumn.toStructField(child) }.toSeq)
  }    


  case class ArrayColumn(id: Option[String], name: String, description: Option[String], nullable: Boolean, nullReplacementValue: Option[String], trim: Boolean, nullableValues: List[String], elementType: ExtractColumn, metadata: Option[String]) extends ExtractColumn {
    val sparkDataType: DataType = ArrayType(ExtractColumn.toStructField(elementType).dataType, false)
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

    def before(stage: PipelineStage, index: Int, stages: List[PipelineStage])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext) = {
      logger.trace()
        .field("event", "before")
        .field("stage", stage.name)
        .log()      
    }

    def after(result: Option[DataFrame], stage: PipelineStage, index: Int, stages: List[PipelineStage])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
      logger.trace()
        .field("event", "after")
        .field("stage", stage.name)
        .log()

      result
    }

    def runStage(stage: PipelineStage, index: Int, stages: List[PipelineStage])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Boolean = true

  }


  sealed trait FailMode {
    def sparkString(): String
  }
  object FailMode {
    case object Permissive extends FailMode { val sparkString = "permissive" }
    case object FailFast extends FailMode { val sparkString = "failfast" }
  }


  sealed trait OutputModeType {
    def sparkString(): String
  }
  object OutputModeType {
    case object Append extends OutputModeType { val sparkString = "append" }
    case object Complete extends OutputModeType { val sparkString = "complete" }
    case object Update extends OutputModeType { val sparkString = "update" }
  }

  sealed trait IsolationLevel {
    def sparkString(): String
  }
  object IsolationLevel {
    case object None extends IsolationLevel { val sparkString = "NONE" }
    case object ReadCommitted extends IsolationLevel { val sparkString = "READ_COMMITTED" }
    case object ReadUncommitted extends IsolationLevel { val sparkString = "READ_UNCOMMITTED" }
    case object RepeatableRead extends IsolationLevel { val sparkString = "REPEATABLE_READ" }
    case object Serializable extends IsolationLevel { val sparkString = "SERIALIZABLE" }
  }

  sealed trait ResponseType {
    def sparkString(): String
  }
  object ResponseType {
    case object IntegerResponse extends ResponseType { val sparkString = "integer" }
    case object DoubleResponse extends ResponseType { val sparkString = "double" }
    case object StringResponse extends ResponseType { val sparkString = "string" }
  }

  sealed trait Authentication {
    def method(): String
  }
  object Authentication {
    /**
      In the Amazon case, we support encryption options using IAM access only as in order
      to provide access to a KMS key that would need to be done via a role and SSE-S3 should work if
      enable on the bucket as the default. Therefor unless a use case appears for adding encryption
      options to the access key method we will only support encryption options when using IAM for now.

      for arc we are using this dependency resolution order if authentication is not provided:

     */
    case class AmazonAccessKey(bucket: Option[String], accessKeyID: String, secretAccessKey: String, endpoint: Option[String], ssl: Option[Boolean]) extends Authentication {
      def method = "AmazonAccessKey"
    }
    case class AmazonAnonymous(bucket: Option[String]) extends Authentication {
      def method = "AmazonAnonymous"
    }
    case class AmazonEnvironmentVariable(bucket: Option[String]) extends Authentication {
      def method = "AmazonEnvironmentVariable"
    }
    case class AmazonIAM(bucket: Option[String], encryptionType: Option[AmazonS3EncryptionType], keyArn: Option[String], customKey: Option[String]) extends Authentication {
      def method = "AmazonIAM"
    }
    case class AzureSharedKey(accountName: String, signature: String) extends Authentication{
      def method = "AzureSharedKey"
    }
    case class AzureSharedAccessSignature(accountName: String, container: String, token: String) extends Authentication{
      def method = "AzureSharedAccessSignature"
    }
    case class AzureDataLakeStorageToken(clientID: String, refreshToken: String) extends Authentication{
      def method = "AzureDataLakeStorageToken"
    }
    case class AzureDataLakeStorageGen2AccountKey(accountName: String, accessKey: String) extends Authentication{
      def method = "AzureDataLakeStorageGen2AccountKey"
    }
    case class AzureDataLakeStorageGen2OAuth(clientID: String, secret: String, directoryId: String) extends Authentication{
      def method = "AzureDataLakeStorageGen2OAuth"
    }
    case class GoogleCloudStorageKeyFile(projectID: String, keyFilePath: String) extends Authentication{
      def method = "GoogleCloudStorageKeyFile"
    }
  }

  sealed trait AmazonS3EncryptionType
  object AmazonS3EncryptionType {
    case object SSE_S3 extends AmazonS3EncryptionType // AES256
    case object SSE_KMS extends AmazonS3EncryptionType
    case object SSE_C extends AmazonS3EncryptionType // custom

    def fromString(encType: String): Option[AmazonS3EncryptionType] = {
      Option(encType).map(_.trim.toUpperCase) match {
        case Some("SSE-S3") => Some(SSE_S3)
        case Some("SSE-KMS") => Some(SSE_KMS)
        case Some("SSE-C") => Some(SSE_C)
        case _ => None
      }
    }
  }

  // these can be added to and have custom messages as required
  sealed trait ExtractError {
    def getMessage(): String
  }
  case class FileNotFoundExtractError(path: Option[String]) extends ExtractError {
    val getMessage = path match {
      case Some(path) => s"No files matched '${path}' and no schema has been provided to create an empty dataframe."
      case None => "No files matched and no schema has been provided to create an empty dataframe."
    }
  }
  case class PathNotExistsExtractError(path: Option[String]) extends ExtractError {
    val getMessage = path match {
      case Some(path) => s"Path '${path}' does not exist and no schema has been provided to create an empty dataframe."
      case None => "Path does not exist and no schema has been provided to create an empty dataframe."
    }
  }
  case class EmptySchemaExtractError(path: Option[String]) extends ExtractError {
    val getMessage = path match {
      case Some(path) => s"Input '${path}' does not contain any fields and no schema has been provided to create an empty dataframe."
      case None => "Input does not contain any fields and no schema has been provided to create an empty dataframe."
    }
  }

  case class Watermark(eventTime: String, delayThreshold: String)

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
