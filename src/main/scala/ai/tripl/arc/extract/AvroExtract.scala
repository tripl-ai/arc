package ai.tripl.arc.extract

import java.io._
import java.lang._
import java.net.URI
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import com.typesafe.config._
import com.fasterxml.jackson.databind.ObjectMapper

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.CloudUtils
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.ExtractUtils
import ai.tripl.arc.util.MetadataUtils
import ai.tripl.arc.util.Utils

class AvroExtract extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def createStage(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "inputView" :: "outputView" :: "authentication" :: "contiguousIndex" :: "numPartitions" :: "partitionBy" :: "persist" :: "schemaURI" :: "schemaView" :: "params" :: "basePath" :: "avroSchemaURI" :: "inputField" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)
    val name = getValue[String]("name")
    val params = readMap("params", c)
    val description = getOptionalValue[String]("description")
    val inputView = if(c.hasPath("inputView")) getValue[String]("inputView") else getValue[String]("inputURI")
    val parsedGlob = if (!c.hasPath("inputView")) {
      inputView.rightFlatMap(glob => parseGlob("inputURI", glob))
    } else {
      Right("")
    }    
    val outputView = getValue[String]("outputView")
    val persist = getValue[Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val authentication = readAuthentication("authentication")
    val contiguousIndex = getValue[Boolean]("contiguousIndex", default = Some(true))
    val uriKey = "schemaURI"
    val stringURI = getOptionalValue[String](uriKey)
    val parsedURI: Either[Errors, Option[URI]] = stringURI.rightFlatMap(optURI => 
      optURI match { 
        case Some(uri) => parseURI(uriKey, uri).rightFlatMap(parsedURI => Right(Option(parsedURI)))
        case None => Right(None)
      }
    )
    val extractColumns = if(!c.hasPath("schemaView")) getExtractColumns(parsedURI, uriKey, authentication) else Right(List.empty)
    val schemaView = if(c.hasPath("schemaView")) getValue[String]("schemaView") else Right("")
    val basePath = getOptionalValue[String]("basePath")
    val inputField = getOptionalValue[String]("inputField")
    val avroSchemaURI = getOptionalValue[String]("avroSchemaURI")
    val avroSchema: Either[Errors, Option[org.apache.avro.Schema]] = avroSchemaURI.rightFlatMap(optAvroSchemaURI => 
      optAvroSchemaURI match { 
        case Some(uri) => {
          parseURI("avroSchemaURI", uri)
          .rightFlatMap(uri => textContentForURI(uri, "avroSchemaURI", Right(None) ))
          .rightFlatMap(schemaString => parseAvroSchema("avroSchemaURI", schemaString))
        }
        case None => Right(None)
      }
    )    

    (name, description, extractColumns, schemaView, inputView, parsedGlob, outputView, persist, numPartitions, partitionBy, authentication, contiguousIndex, invalidKeys, basePath, inputField, avroSchemaURI, avroSchema) match {
      case (Right(name), Right(description), Right(extractColumns), Right(schemaView), Right(inputView), Right(parsedGlob), Right(outputView), Right(persist), Right(numPartitions), Right(partitionBy), Right(authentication), Right(contiguousIndex), Right(invalidKeys), Right(basePath), Right(inputField), Right(avroSchemaURI), Right(avroSchema)) =>
        val input = if(c.hasPath("inputView")) Left(inputView) else Right(parsedGlob)
        val schema = if(c.hasPath("schemaView")) Left(schemaView) else Right(extractColumns)

        val stage = AvroExtractStage(
          plugin=this,
          name=name,
          description=description,
          schema=schema,
          outputView=outputView,
          input=input,
          authentication=authentication,
          params=params,
          persist=persist,
          numPartitions=numPartitions,
          partitionBy=partitionBy,
          basePath=basePath,
          contiguousIndex=contiguousIndex,
          avroSchema=avroSchema,
          inputField=inputField
        )

        stage.stageDetail.put("contiguousIndex", Boolean.valueOf(contiguousIndex))
        stage.stageDetail.put("input", input)  
        stage.stageDetail.put("outputView", outputView)  
        stage.stageDetail.put("persist", Boolean.valueOf(persist))
        for (inputField <- inputField) {
          stage.stageDetail.put("inputField", inputField)  
        }
        
        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, extractColumns, schemaView, inputView, parsedGlob, outputView, persist, numPartitions, partitionBy, authentication, contiguousIndex, extractColumns, invalidKeys, basePath, inputField, avroSchemaURI, avroSchema).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

  def parseAvroSchema(path: String, schemaString: String)(implicit c: Config): Either[Errors, Option[org.apache.avro.Schema]] = {
    def err(lineNumber: Option[Int], msg: String): Either[Errors, Option[org.apache.avro.Schema]] = Left(ConfigError(path, lineNumber, msg) :: Nil)
    try {
      // if schema contains backslash it might have come from the kafka schema registry therefore try to get the data out of the returned object
      // this is hacky but needs to behave well with a registry response
      if (schemaString.contains(""""schema":""") && schemaString.contains("\\")) {
        val objectMapper = new ObjectMapper()
        val metaTree = objectMapper.readTree(schemaString)
        Right(Option(new org.apache.avro.Schema.Parser().parse(metaTree.get("schema").asText)))
      } else {
        // try to parse schema
        Right(Option(new org.apache.avro.Schema.Parser().parse(schemaString)))
      }
    } catch {
      case e: Exception => err(Some(c.getValue(path).origin.lineNumber()), e.getMessage)
    }
  }
}

case class AvroExtractStage(
    plugin: PipelineStagePlugin,
    name: String,
    description: Option[String],
    schema: Either[String, List[ExtractColumn]],
    outputView: String,
    input: Either[String, String],
    authentication: Option[Authentication],
    params: Map[String, String],
    persist: Boolean,
    numPartitions: Option[Int],
    partitionBy: List[String],
    contiguousIndex: Boolean,
    basePath: Option[String],
    avroSchema: Option[org.apache.avro.Schema],
    inputField: Option[String]
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    AvroExtractStage.execute(this)
  }
}

object AvroExtractStage {

  def execute(stage: AvroExtractStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): Option[DataFrame] = {
    import spark.implicits._
    val stageDetail = stage.stageDetail

    // try to get the schema
    val optionSchema = try {
      ExtractUtils.getSchema(stage.schema)(spark, logger)
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stageDetail          
      }      
    }        

    CloudUtils.setHadoopConfiguration(stage.authentication)

    // if incoming dataset is empty create empty dataset with a known schema
    val df = try {
      stage.input match {
        case Right(glob) => {
          stage.basePath match {
            case Some(basePath) => spark.read.format("avro").option("basePath", basePath).load(glob)
            case None => spark.read.format("avro").load(glob)
          }
        }
        case Left(view) => {
          val inputView = spark.table(view)
          stage.avroSchema match {
            case Some(avroSchema) => {
              stage.inputField match {
                case Some(inputField) => inputView.withColumn(inputField, from_avro(col(inputField), avroSchema.toString))
                case None => inputView.withColumn("value", from_avro(col("value"), avroSchema.toString))
              }
            }
            case None => throw new Exception(s"AvroExtract requires the 'avroSchema' to be provided when reading from an 'inputView'.")
          }
        }
      }
    } catch {
        case e: FileNotFoundException => 
          spark.emptyDataFrame
        case e: AnalysisException if (e.getMessage.contains("Path does not exist")) => 
          spark.emptyDataFrame
        case e: Exception => throw new Exception(e) with DetailException {
          override val detail = stageDetail          
        }
    }

    // if incoming dataset has 0 columns then create empty dataset with correct schema
    val emptyDataframeHandlerDF = try {
      if (df.schema.length == 0) {
        stageDetail.put("records", Integer.valueOf(0))
        optionSchema match {
          case Some(s) => spark.createDataFrame(spark.sparkContext.emptyRDD[Row], s)
          case None => throw new Exception(s"AvroExtract has produced 0 columns and no schema has been provided to create an empty dataframe.")
        }
      } else {
        df
      }
    } catch {
      case e: Exception => throw new Exception(e.getMessage) with DetailException {
        override val detail = stageDetail          
      }      
    }    

    // add internal columns data _filename, _index
    val sourceEnrichedDF = ExtractUtils.addInternalColumns(emptyDataframeHandlerDF, stage.contiguousIndex)

    // set column metadata if exists
    val enrichedDF = optionSchema match {
        case Some(schema) => MetadataUtils.setMetadata(sourceEnrichedDF, schema)
        case None => sourceEnrichedDF   
    }
  
    // repartition to distribute rows evenly
    val repartitionedDF = stage.partitionBy match {
      case Nil => { 
        stage.numPartitions match {
          case Some(numPartitions) => enrichedDF.repartition(numPartitions)
          case None => enrichedDF
        }   
      }
      case partitionBy => {
        // create a column array for repartitioning
        val partitionCols = partitionBy.map(col => df(col))
        stage.numPartitions match {
          case Some(numPartitions) => enrichedDF.repartition(numPartitions, partitionCols:_*)
          case None => enrichedDF.repartition(partitionCols:_*)
        }
      }
    } 
    repartitionedDF.createOrReplaceTempView(stage.outputView)

    if (!repartitionedDF.isStreaming) {
      stageDetail.put("inputFiles", Integer.valueOf(repartitionedDF.inputFiles.length))
      stageDetail.put("outputColumns", Integer.valueOf(repartitionedDF.schema.length))
      stageDetail.put("numPartitions", Integer.valueOf(repartitionedDF.rdd.partitions.length))

      if (stage.persist) {
        repartitionedDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
        stageDetail.put("records", Long.valueOf(repartitionedDF.count)) 
      }      
    }

    Option(repartitionedDF)
  }

}

