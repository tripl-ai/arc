package au.com.agl.arc.api

import java.net.URI
import java.time.LocalTime

import au.com.agl.arc.plugins.PipelineStagePlugin
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.SaveMode

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
    environment: String,

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
    ignoreEnvironments: Boolean    

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

  // A Pipeline has 1 or more stages
  sealed trait PipelineStage {
    def name: String

    def getType: String
  }

  case class CustomStage(name: String, params: Map[String, String], stage: PipelineStagePlugin) extends PipelineStage {
    val getType = stage.getClass().getName()
  }

  /** An extract that provides its own schema e.g. parquet
    */
  sealed trait Extract extends PipelineStage {
  }

  object Extract {
    def toStructType(cols: List[ExtractColumn]): StructType = {
      val fields = cols.map(c => ExtractColumn.toStructField(c))
      StructType(fields)
    }
  }  

  /** An extract that is persistable
    */
  sealed trait PersistableExtract extends Extract {
    def persist: Boolean
  }

  /** A columnar extract requires a schema to be provided e.g. parquet vs Delimited.
    */
  sealed trait ColumnarExtract extends PersistableExtract {
    def cols: Either[String, List[ExtractColumn]]
  }

  case class AvroExtract(name: String, description: Option[String], cols: Either[String, List[ExtractColumn]], outputView: String, input: String, authentication: Option[Authentication], params: Map[String, String], persist: Boolean, numPartitions: Option[Int], partitionBy: List[String], contiguousIndex: Boolean, basePath: Option[String]) extends ColumnarExtract { val getType = "AvroExtract" }  

  case class BytesExtract(name: String, description: Option[String], outputView: String, input: Either[String, String], authentication: Option[Authentication], params: Map[String, String], persist: Boolean, numPartitions: Option[Int], contiguousIndex: Boolean) extends Extract { val getType = "BytesExtract" }

  case class DatabricksDeltaExtract(name: String, description: Option[String], outputView: String, input: String, params: Map[String, String], persist: Boolean, numPartitions: Option[Int], partitionBy: List[String]) extends Extract { val getType = "DeltaExtract" }

  case class DelimitedExtract(name: String, description: Option[String], cols: Either[String, List[ExtractColumn]], outputView: String, input: Either[String, String], settings: Delimited, authentication: Option[Authentication], params: Map[String, String], persist: Boolean, numPartitions: Option[Int], partitionBy: List[String], contiguousIndex: Boolean, inputField: Option[String], basePath: Option[String]) extends ColumnarExtract { val getType = "DelimitedExtract" }

  case class ElasticsearchExtract(name: String, description: Option[String], input: String, outputView: String, params: Map[String, String], persist: Boolean, numPartitions: Option[Int], partitionBy: List[String]) extends Extract { val getType = "ElasticsearchExtract" }

  case class HTTPExtract(name: String, description: Option[String], input: Either[String, URI], method: String, headers: Map[String, String], body: Option[String], validStatusCodes: List[Int], outputView: String, params: Map[String, String], persist: Boolean, numPartitions: Option[Int], partitionBy: List[String]) extends Extract { val getType = "HTTPExtract" }

  case class ImageExtract(name: String, description: Option[String], outputView: String, input: String, authentication: Option[Authentication], params: Map[String, String], persist: Boolean, numPartitions: Option[Int], partitionBy: List[String], dropInvalid: Boolean, basePath: Option[String]) extends Extract { val getType = "ImageExtract" }

  case class JDBCExtract(name: String, description: Option[String], cols: Either[String, List[ExtractColumn]], outputView: String, jdbcURL: String, tableName: String, numPartitions: Option[Int], fetchsize: Option[Int], customSchema: Option[String], driver: java.sql.Driver, partitionColumn: Option[String], params: Map[String, String], persist: Boolean, partitionBy: List[String], predicates: List[String]) extends Extract { val getType = "JDBCExtract" }

  case class JSONExtract(name: String, description: Option[String], cols: Either[String, List[ExtractColumn]], outputView: String, input: Either[String, String], settings: JSON, authentication: Option[Authentication], params: Map[String, String], persist: Boolean, numPartitions: Option[Int], partitionBy: List[String], contiguousIndex: Boolean, inputField: Option[String], basePath: Option[String]) extends ColumnarExtract { val getType = "JSONExtract" }

  case class KafkaExtract(name: String, description: Option[String], outputView: String, topic: String, bootstrapServers: String, groupID: String, maxPollRecords: Int, timeout: Long, autoCommit: Boolean, params: Map[String, String], persist: Boolean, numPartitions: Option[Int], partitionBy: List[String]) extends Extract { val getType = "KafkaExtract" }

  case class ORCExtract(name: String, description: Option[String], cols: Either[String, List[ExtractColumn]], outputView: String, input: String, authentication: Option[Authentication], params: Map[String, String], persist: Boolean, numPartitions: Option[Int], partitionBy: List[String], contiguousIndex: Boolean, basePath: Option[String]) extends ColumnarExtract { val getType = "ORCExtract" }

  case class ParquetExtract(name: String, description: Option[String], cols: Either[String, List[ExtractColumn]], outputView: String, input: String, authentication: Option[Authentication], params: Map[String, String], persist: Boolean, numPartitions: Option[Int], partitionBy: List[String], contiguousIndex: Boolean, basePath: Option[String]) extends ColumnarExtract { val getType = "ParquetExtract" }

  case class RateExtract(name: String,  description: Option[String], outputView: String, params: Map[String, String], rowsPerSecond: Int, rampUpTime: Int, numPartitions: Int) extends Extract { val getType = "RateExtract" }

  case class TextExtract(name: String, description: Option[String], cols: Either[String, List[ExtractColumn]], outputView: String, input: String, authentication: Option[Authentication], params: Map[String, String], persist: Boolean, numPartitions: Option[Int], contiguousIndex: Boolean, multiLine: Boolean, basePath: Option[String]) extends ColumnarExtract { val getType = "TextExtract" }

  case class XMLExtract(name: String, description: Option[String], cols: Either[String, List[ExtractColumn]], outputView: String, input: Either[String, String], authentication: Option[Authentication], params: Map[String, String], persist: Boolean, numPartitions: Option[Int], partitionBy: List[String], contiguousIndex: Boolean) extends Extract { val getType = "XMLExtract" }


  sealed trait PersistableTransform extends PipelineStage {
    def persist: Boolean
  }

  case class DiffTransform(name: String, description: Option[String], inputLeftView: String, inputRightView: String, outputIntersectionView: Option[String], outputLeftView: Option[String], outputRightView: Option[String], params: Map[String, String], persist: Boolean) extends PersistableTransform { val getType = "DiffTransform" }

  case class HTTPTransform(name: String, description: Option[String], uri: URI, headers: Map[String, String], validStatusCodes: List[Int], inputView: String, outputView: String, inputField: String, params: Map[String, String], persist: Boolean, batchSize: Int, delimiter: String, numPartitions: Option[Int], partitionBy: List[String], failMode: FailModeType) extends PersistableTransform { val getType = "HTTPTransform" }  
  
  case class JSONTransform(name: String, description: Option[String], inputView: String, outputView: String, params: Map[String, String], persist: Boolean, numPartitions: Option[Int], partitionBy: List[String]) extends PersistableTransform { val getType = "JSONTransform" }

  case class MetadataFilterTransform(name: String, description: Option[String], inputView: String, inputURI: URI, sql: String, outputView:String, params: Map[String, String], sqlParams: Map[String, String], persist: Boolean, numPartitions: Option[Int], partitionBy: List[String]) extends PersistableTransform { val getType = "MetadataFilterTransform" }

  case class MLTransform(name: String, description: Option[String], inputURI: URI, model: Either[PipelineModel, CrossValidatorModel], inputView: String, outputView: String, params: Map[String, String], persist: Boolean, numPartitions: Option[Int], partitionBy: List[String]) extends PersistableTransform { val getType = "MLTransform" }

  case class SQLTransform(name: String, description: Option[String], inputURI: URI, sql: String, outputView:String, params: Map[String, String], sqlParams: Map[String, String], persist: Boolean, numPartitions: Option[Int], partitionBy: List[String]) extends PersistableTransform { val getType = "SQLTransform" }

  case class TensorFlowServingTransform(name: String, description: Option[String], inputView: String, outputView: String, uri: URI, signatureName: Option[String], responseType: ResponseType, batchSize: Int, inputField: String, params: Map[String, String], persist: Boolean, numPartitions: Option[Int], partitionBy: List[String]) extends PersistableTransform { val getType = "TensorFlowServingTransform" }

  case class TypingTransform(name: String, description: Option[String], cols: Either[String, List[ExtractColumn]], inputView: String, outputView: String, params: Map[String, String], persist: Boolean, failMode: FailModeType, numPartitions: Option[Int], partitionBy: List[String]) extends PersistableTransform with ColumnarExtract { val getType = "TypingTransform" }


  sealed trait Load extends PipelineStage

  case class AvroLoad(name: String, description: Option[String], inputView: String, outputURI: URI, partitionBy: List[String], numPartitions: Option[Int], authentication: Option[Authentication], saveMode: SaveMode, params: Map[String, String]) extends Load { val getType = "AvroLoad" }

  case class AzureEventHubsLoad(name: String, description: Option[String], inputView: String, namespaceName: String, eventHubName: String, sharedAccessSignatureKeyName: String, sharedAccessSignatureKey: String, numPartitions: Option[Int], retryMinBackoff: Long, retryMaxBackoff: Long, retryCount: Int, params: Map[String, String]) extends Load { val getType = "AzureEventHubsLoad" }

  case class ConsoleLoad(name: String, description: Option[String], inputView: String, outputMode: OutputModeType, params: Map[String, String]) extends Load { val getType = "ConsoleLoad" }

  case class DatabricksDeltaLoad(name: String, description: Option[String], inputView: String, outputURI: URI, partitionBy: List[String], numPartitions: Option[Int], saveMode: SaveMode, params: Map[String, String]) extends Load { val getType = "ParquetLoad" }

  case class DatabricksSQLDWLoad(name: String, description: Option[String], inputView: String, jdbcURL: String, driver: java.sql.Driver, tempDir: String, dbTable: String, forwardSparkAzureStorageCredentials: Boolean, tableOptions: Option[String], maxStrLength: Int, authentication: Option[Authentication], params: Map[String, String]) extends Load { val getType = "DatabricksSQLDWLoad" }

  case class DelimitedLoad(name: String, description: Option[String], inputView: String, outputURI: URI, settings: Delimited, partitionBy: List[String], numPartitions: Option[Int], authentication: Option[Authentication], saveMode: SaveMode, params: Map[String, String]) extends Load { val getType = "DelimitedLoad" }

  case class ElasticsearchLoad(name: String, description: Option[String], inputView: String, output: String, partitionBy: List[String], numPartitions: Option[Int], saveMode: SaveMode, params: Map[String, String]) extends Load { val getType = "ElasticsearchLoad" }

  case class HTTPLoad(name: String, description: Option[String], inputView: String, outputURI: URI, headers: Map[String, String], validStatusCodes: List[Int], params: Map[String, String]) extends Load { val getType = "HTTPLoad" }

  case class JDBCLoad(name: String, description: Option[String], inputView: String, jdbcURL: String, tableName: String, partitionBy: List[String], numPartitions: Option[Int], isolationLevel: IsolationLevelType, batchsize: Int, truncate: Boolean, createTableOptions: Option[String], createTableColumnTypes: Option[String], saveMode: SaveMode, driver: java.sql.Driver, bulkload: Boolean, tablock: Boolean, params: Map[String, String]) extends Load { val getType = "JDBCLoad" }

  case class JSONLoad(name: String, description: Option[String], inputView: String, outputURI: URI, partitionBy: List[String], numPartitions: Option[Int], authentication: Option[Authentication], saveMode: SaveMode, params: Map[String, String]) extends Load { val getType = "JSONLoad" }

  case class KafkaLoad(name: String, description: Option[String], inputView: String, topic: String, bootstrapServers: String, acks: Int, numPartitions: Option[Int], retries: Int, batchSize: Int, params: Map[String, String]) extends Load { val getType = "KafkaLoad" }
  
  case class ORCLoad(name: String, description: Option[String], inputView: String, outputURI: URI, partitionBy: List[String], numPartitions: Option[Int], authentication: Option[Authentication], saveMode: SaveMode, params: Map[String, String]) extends Load { val getType = "ORCLoad" }

  case class ParquetLoad(name: String, description: Option[String], inputView: String, outputURI: URI, partitionBy: List[String], numPartitions: Option[Int], authentication: Option[Authentication], saveMode: SaveMode, params: Map[String, String]) extends Load { val getType = "ParquetLoad" }

  case class TextLoad(name: String, description: Option[String], inputView: String, outputURI: URI, numPartitions: Option[Int], authentication: Option[Authentication], saveMode: SaveMode, params: Map[String, String], singleFile: Boolean, prefix: String, separator: String, suffix: String) extends Load { val getType = "TextLoad" }

  case class XMLLoad(name: String, description: Option[String], inputView: String, outputURI: URI, partitionBy: List[String], numPartitions: Option[Int], authentication: Option[Authentication], saveMode: SaveMode, params: Map[String, String]) extends Load { val getType = "XMLLoad" }


  sealed trait Execute extends PipelineStage

  case class JDBCExecute(name: String, description: Option[String], inputURI: URI, jdbcURL: String, user: Option[String], password: Option[String], sql: String, sqlParams: Map[String, String], params: Map[String, String]) extends Execute { val getType = "JDBCExecute" }

  case class HTTPExecute(name: String, description: Option[String], uri: URI, headers: Map[String, String], payloads: Map[String, String], validStatusCodes: List[Int], params: Map[String, String]) extends Execute  { val getType = "HTTPExecute" }

  case class KafkaCommitExecute(name: String, description: Option[String], inputView: String, bootstrapServers: String, groupID: String, params: Map[String, String]) extends Execute  { val getType = "KafkaCommitExecute" }

  case class PipelineExecute(name: String, description: Option[String], uri: URI, pipeline: ETLPipeline) extends Execute  { val getType = "PipelineExecute" }


  sealed trait Validate extends PipelineStage

  case class EqualityValidate(name: String, description: Option[String], leftView: String, rightView: String, params: Map[String, String]) extends Validate { val getType = "EqualityValidate" }

  case class SQLValidate(name: String, description: Option[String], inputURI: URI, sql: String, sqlParams: Map[String, String], params: Map[String, String]) extends Validate { val getType = "SQLValidate" }


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
    case class AmazonAccessKey(accessKeyID: String, secretAccessKey: String) extends Authentication
    case class AzureSharedKey(accountName: String, signature: String) extends Authentication
    case class AzureSharedAccessSignature(accountName: String, container: String, token: String) extends Authentication
    case class AzureDataLakeStorageToken(clientID: String, refreshToken: String) extends Authentication
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
