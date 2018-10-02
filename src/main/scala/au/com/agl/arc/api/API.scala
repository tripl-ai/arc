package au.com.agl.arc.api

import java.net.URI
import java.time.LocalTime
import java.sql.Driver

import au.com.agl.arc.plugins.PipelineStagePlugin
import org.apache.spark.sql._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.SaveMode

import org.apache.spark.sql.streaming.OutputMode

import au.com.agl.arc.util._

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
    isStreaming: Boolean

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

  case class StringColumn(id: String, name: String, description: Option[String], nullable: Boolean, nullReplacementValue: Option[String], trim: Boolean, nullableValues: List[String], metadata: Option[String]) extends ExtractColumn {
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

  /** true / false values are lists of strings that are considered equivalent
    * to true or false e.g. "Y", "yes", "N", "no".
    */
  case class BooleanColumn(id: String, name: String, description: Option[String], nullable: Boolean, nullReplacementValue: Option[String], trim: Boolean, nullableValues: List[String], trueValues: List[String], falseValues: List[String], metadata: Option[String]) extends ExtractColumn {
    val sparkDataType: DataType = BooleanType
  }

  case class IntegerColumn(id: String, name: String, description: Option[String], nullable: Boolean, nullReplacementValue: Option[String], trim: Boolean = true, nullableValues: List[String], metadata: Option[String]) extends ExtractColumn {
    val sparkDataType: DataType = IntegerType
  }

  case class LongColumn(id: String, name: String, description: Option[String], nullable: Boolean, nullReplacementValue: Option[String], trim: Boolean, nullableValues: List[String], metadata: Option[String]) extends ExtractColumn {
    val sparkDataType: DataType = LongType
  }

  case class DoubleColumn(id: String, name: String, description: Option[String], nullable: Boolean, nullReplacementValue: Option[String], trim: Boolean, nullableValues: List[String], metadata: Option[String]) extends ExtractColumn {
    val sparkDataType: DataType = DoubleType
  }

  case class DecimalColumn(id: String, name: String, description: Option[String], nullable: Boolean, nullReplacementValue: Option[String], trim: Boolean, nullableValues: List[String], precision: Int, scale: Int, formatter: Option[String], metadata: Option[String]) extends ExtractColumn {
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

  /** A columnar extract requires a schema to be provided e.g. parquet vs Delimited.
    */
  sealed trait ColumnarExtract extends PipelineStage {
    def cols: Either[String, List[ExtractColumn]]
  }

  object Extract {
    def toStructType(cols: List[ExtractColumn]): StructType = {
      val fields = cols.map(c => ExtractColumn.toStructField(c))
      StructType(fields)
    }
  }

  case class AvroExtract(name: String, cols: Either[String, List[ExtractColumn]], outputView: String, input: String, authentication: Option[Authentication], params: Map[String, String], persist: Boolean, numPartitions: Option[Int], partitionBy: List[String], contiguousIndex: Option[Boolean]) extends ColumnarExtract { val getType = "AvroExtract" }  

  case class BytesExtract(name: String, outputView: String, input: Option[String], pathView: Option[String], authentication: Option[Authentication], params: Map[String, String], persist: Boolean, numPartitions: Option[Int], contiguousIndex: Option[Boolean]) extends Extract { val getType = "BytesExtract" }

  case class DelimitedExtract(name: String, cols: Either[String, List[ExtractColumn]], outputView: String, input: Either[String, String], settings: Delimited, authentication: Option[Authentication], params: Map[String, String], persist: Boolean, numPartitions: Option[Int], partitionBy: List[String], contiguousIndex: Option[Boolean]) extends ColumnarExtract { val getType = "DelimitedExtract" }

  case class HTTPExtract(name: String, uri: URI, method: Option[String], headers: Map[String, String], body: Option[String], validStatusCodes: Option[List[Int]], outputView: String, params: Map[String, String], persist: Boolean, numPartitions: Option[Int], partitionBy: List[String]) extends Extract { val getType = "HTTPExtract" }

  case class JDBCExtract(name: String, cols: Either[String, List[ExtractColumn]], outputView: String, jdbcURL: String, tableName: String, numPartitions: Option[Int], fetchsize: Option[Int], customSchema: Option[String], driver: java.sql.Driver, partitionColumn: Option[String], params: Map[String, String], persist: Boolean, partitionBy: List[String], predicates: List[String]) extends Extract { val getType = "JDBCExtract" }

  case class JSONExtract(name: String, cols: Either[String, List[ExtractColumn]], outputView: String, input: Either[String, String], settings: JSON, authentication: Option[Authentication], params: Map[String, String], persist: Boolean, numPartitions: Option[Int], partitionBy: List[String], contiguousIndex: Option[Boolean]) extends ColumnarExtract { val getType = "JSONExtract" }

  case class KafkaExtract(name: String, outputView: String, topic: String, bootstrapServers: String, groupID: String, maxPollRecords: Option[Int], timeout: Option[Long], autoCommit: Option[Boolean], params: Map[String, String], persist: Boolean, numPartitions: Option[Int], partitionBy: List[String]) extends Extract { val getType = "KafkaExtract" }

  case class ORCExtract(name: String, cols: Either[String, List[ExtractColumn]], outputView: String, input: String, authentication: Option[Authentication], params: Map[String, String], persist: Boolean, numPartitions: Option[Int], partitionBy: List[String], contiguousIndex: Option[Boolean]) extends ColumnarExtract { val getType = "ORCExtract" }

  case class ParquetExtract(name: String, cols: Either[String, List[ExtractColumn]], outputView: String, input: String, authentication: Option[Authentication], params: Map[String, String], persist: Boolean, numPartitions: Option[Int], partitionBy: List[String], contiguousIndex: Option[Boolean]) extends ColumnarExtract { val getType = "ParquetExtract" }

  case class RateExtract(name: String,  outputView: String, params: Map[String, String], rowsPerSecond: Option[Int], rampUpTime: Option[Int], numPartitions: Option[Int]) extends Extract { val getType = "RateExtract" }

  case class TextExtract(name: String, cols: Either[String, List[ExtractColumn]], outputView: String, input: String, authentication: Option[Authentication], params: Map[String, String], persist: Boolean, numPartitions: Option[Int], contiguousIndex: Option[Boolean], multiLine: Option[Boolean]) extends ColumnarExtract { val getType = "TextExtract" }

  case class XMLExtract(name: String, cols: Either[String, List[ExtractColumn]], outputView: String, input: Either[String, String], authentication: Option[Authentication], params: Map[String, String], persist: Boolean, numPartitions: Option[Int], partitionBy: List[String], contiguousIndex: Option[Boolean]) extends Extract { val getType = "XMLExtract" }


  sealed trait Transform extends PipelineStage

  case class DiffTransform(name: String, inputLeftView: String, inputRightView: String, outputIntersectionView: Option[String], outputLeftView: Option[String], outputRightView: Option[String], params: Map[String, String], persist: Boolean) extends Transform { val getType = "DiffTransform" }

  case class HTTPTransform(name: String, uri: URI, headers: Map[String, String], validStatusCodes: Option[List[Int]], inputView: String, outputView: String, params: Map[String, String], persist: Boolean) extends Transform { val getType = "HTTPTransform" }  
  
  case class JSONTransform(name: String, inputView: String, outputView: String, params: Map[String, String], persist: Boolean) extends Transform { val getType = "JSONTransform" }

  case class MetadataFilterTransform(name: String, inputView: String, inputURI: URI, sql: String, outputView:String, params: Map[String, String], sqlParams: Map[String, String], persist: Boolean) extends Transform { val getType = "MetadataFilterTransform" }

  case class MLTransform(name: String, inputURI: URI, model: Either[PipelineModel, CrossValidatorModel], inputView: String, outputView: String, params: Map[String, String], persist: Boolean) extends Transform { val getType = "MLTransform" }

  case class SQLTransform(name: String, inputURI: URI, sql: String, outputView:String, params: Map[String, String], sqlParams: Map[String, String], persist: Boolean) extends Transform { val getType = "SQLTransform" }

  case class TensorFlowServingTransform(name: String, inputView: String, outputView: String, uri: URI, signatureName: Option[String], responseType: Option[ReponseType], batchSize: Option[Int], params: Map[String, String], persist: Boolean) extends Transform { val getType = "TensorFlowServingTransform" }

  case class TypingTransform(name: String, cols: Either[String, List[ExtractColumn]], inputView: String, outputView: String, params: Map[String, String], persist: Boolean) extends Transform with ColumnarExtract { val getType = "TypingTransform" }


  sealed trait Load extends PipelineStage

  case class AvroLoad(name: String, inputView: String, outputURI: URI, partitionBy: List[String], numPartitions: Option[Int], authentication: Option[Authentication], saveMode: Option[SaveMode], params: Map[String, String]) extends Load { val getType = "AvroLoad" }

  case class AzureEventHubsLoad(name: String, inputView: String, namespaceName: String, eventHubName: String, sharedAccessSignatureKeyName: String, sharedAccessSignatureKey: String, numPartitions: Option[Int], retryMinBackoff: Option[Long], retryMaxBackoff: Option[Long], retryCount: Option[Int], params: Map[String, String]) extends Load { val getType = "AzureEventHubsLoad" }

  case class ConsoleLoad(name: String, inputView: String, outputMode: Option[OutputModeType], params: Map[String, String]) extends Load { val getType = "ConsoleLoad" }

  case class DelimitedLoad(name: String, inputView: String, outputURI: URI, settings: Delimited, partitionBy: List[String], numPartitions: Option[Int], authentication: Option[Authentication], saveMode: Option[SaveMode], params: Map[String, String]) extends Load { val getType = "DelimitedLoad" }

  case class HTTPLoad(name: String, inputView: String, outputURI: URI, headers: Map[String, String], validStatusCodes: Option[List[Int]], params: Map[String, String]) extends Load { val getType = "HTTPLoad" }

  case class JDBCLoad(name: String, inputView: String, jdbcURL: String, tableName: String, partitionBy: List[String], numPartitions: Option[Int], isolationLevel: Option[String], batchsize: Option[Int], truncate: Option[Boolean], createTableOptions: Option[String], createTableColumnTypes: Option[String], saveMode: Option[SaveMode], driver: java.sql.Driver, bulkload: Option[Boolean], tablock: Option[Boolean], params: Map[String, String]) extends Load { val getType = "JDBCLoad" }

  case class JSONLoad(name: String, inputView: String, outputURI: URI, partitionBy: List[String], numPartitions: Option[Int], authentication: Option[Authentication], saveMode: Option[SaveMode], params: Map[String, String]) extends Load { val getType = "JSONLoad" }

  case class KafkaLoad(name: String, inputView: String, topic: String, bootstrapServers: String, acks: Int, numPartitions: Option[Int], retries: Option[Int], batchSize: Option[Int], params: Map[String, String]) extends Load { val getType = "KafkaLoad" }
  
  case class ORCLoad(name: String, inputView: String, outputURI: URI, partitionBy: List[String], numPartitions: Option[Int], authentication: Option[Authentication], saveMode: Option[SaveMode], params: Map[String, String]) extends Load { val getType = "ORCLoad" }

  case class ParquetLoad(name: String, inputView: String, outputURI: URI, partitionBy: List[String], numPartitions: Option[Int], authentication: Option[Authentication], saveMode: Option[SaveMode], params: Map[String, String]) extends Load { val getType = "ParquetLoad" }

  case class XMLLoad(name: String, inputView: String, outputURI: URI, partitionBy: List[String], numPartitions: Option[Int], authentication: Option[Authentication], saveMode: Option[SaveMode], params: Map[String, String]) extends Load { val getType = "XMLLoad" }


  sealed trait OutputModeType
  case object OutputModeTypeAppend extends OutputModeType { override val toString = "append" }
  case object OutputModeTypeComplete extends OutputModeType { override val toString = "complete" }
  case object OutputModeTypeUpdate extends OutputModeType { override val toString = "update" }

  sealed trait Execute extends PipelineStage

  case class JDBCExecute(name: String, inputURI: URI, url: String,
                        user: Option[String], password: Option[String], 
                        sql: String, sqlParams: Map[String, String], 
                        params: Map[String, String]) extends Execute { val getType = "JDBCExecute" }

  case class HTTPExecute(name: String, uri: URI, headers: Map[String, String], payloads: Map[String, String], validStatusCodes: Option[List[Int]], params: Map[String, String]) extends Execute  { val getType = "HTTPExecute" }

  case class KafkaCommitExecute(name: String, inputView: String, bootstrapServers: String, groupID: String, params: Map[String, String]) extends Execute  { val getType = "KafkaCommitExecute" }

  case class PipelineExecute(name: String, uri: URI, pipeline: ETLPipeline) extends Execute  { val getType = "PipelineExecute" }

  sealed trait Validate extends PipelineStage

  case class EqualityValidate(name: String, leftView: String, rightView: String, params: Map[String, String]) extends Validate { val getType = "EqualityValidate" }

  case class SQLValidate(name: String, inputURI: URI, sql: String, sqlParams: Map[String, String], params: Map[String, String]) extends Validate { val getType = "SQLValidate" }


  sealed trait ReponseType
  case object IntegerResponse extends ReponseType
  case object DoubleResponse extends ReponseType
  case object StringResponse extends ReponseType

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
  inferSchema: Boolean = false
) extends SourceType {
  val getDescription = "Delimited"
}

object Delimited {
  def toSparkOptions(delimited: Delimited): Map[String, String] = {
    import delimited._
    Map(
      "sep" -> sep.value,
      "quote" -> quote.value,
      "header" -> header.toString,
      "inferSchema" -> inferSchema.toString
    )
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