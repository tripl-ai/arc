package ai.tripl.arc.extract

import java.lang._
import java.net.URI
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import com.typesafe.config._

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

class DelimitedExtract extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def createStage(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "inputURI" :: "outputView" :: "delimiter" :: "quote" :: "header" :: "authentication" :: "contiguousIndex" :: "numPartitions" :: "params" :: "partitionBy" :: "persist" :: "schemaURI" :: "schemaView" :: "customDelimiter" :: "inputField" :: "basePath" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)
    val name = getValue[String]("name")
    val params = readMap("params", c)
    val description = getOptionalValue[String]("description")
    val inputView = if(c.hasPath("inputView")) getValue[String]("inputView") else Right("")
    val parsedGlob = if (!c.hasPath("inputView")) getValue[String]("inputURI").rightFlatMap(glob => parseGlob("inputURI", glob)) else Right("")
    val outputView = getValue[String]("outputView")
    val persist = getValue[Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val authentication = readAuthentication("authentication")
    val contiguousIndex = getValue[Boolean]("contiguousIndex", default = Some(true))
    val extractColumns = if(c.hasPath("schemaURI")) getValue[String]("schemaURI") |> parseURI("schemaURI") _ |> getExtractColumns("schemaURI", authentication) _ else Right(List.empty)
    val schemaView = if(c.hasPath("schemaView")) getValue[String]("schemaView") else Right("")
    val delimiter = getValue[String]("delimiter", default = Some("Comma"), validValues = "Comma" :: "Pipe" :: "DefaultHive" :: "Custom" :: Nil) |> parseDelimiter("delimiter") _
    val quote = getValue[String]("quote", default =  Some("DoubleQuote"), validValues = "DoubleQuote" :: "SingleQuote" :: "None" :: Nil) |> parseQuote("quote") _
    val header = getValue[Boolean]("header", Some(false))
    val customDelimiter = delimiter match {
      case Right(Delimiter.Custom) => {
        getValue[String]("customDelimiter")
      }
      case _ => Right("")
    }
    val inputField = getOptionalValue[String]("inputField")
    val basePath = getOptionalValue[String]("basePath")

    (name, description, inputView, parsedGlob, extractColumns, schemaView, outputView, persist, numPartitions, partitionBy, header, authentication, contiguousIndex, delimiter, quote, invalidKeys, customDelimiter, inputField, basePath) match {
      case (Right(name), Right(description), Right(inputView), Right(parsedGlob), Right(extractColumns), Right(schemaView), Right(outputView), Right(persist), Right(numPartitions), Right(partitionBy), Right(header), Right(authentication), Right(contiguousIndex), Right(delimiter), Right(quote), Right(_), Right(customDelimiter), Right(inputField), Right(basePath)) =>
        val input = if(c.hasPath("inputView")) {
          Left(inputView) 
        } else {
          Right(parsedGlob)
        }
        val schema = if(c.hasPath("schemaView")) Left(schemaView) else Right(extractColumns)
        val stage = DelimitedExtractStage(
          plugin=this,
          name=name,
          description=description,
          schema=schema,
          outputView=outputView,
          input=input,
          settings=new Delimited(header=header, sep=delimiter, quote=quote, customDelimiter=customDelimiter),
          authentication=authentication,
          params=params,
          persist=persist,
          numPartitions=numPartitions,
          partitionBy=partitionBy,
          contiguousIndex=contiguousIndex,
          basePath=basePath,
          inputField=inputField  
        )

        stage.stageDetail.put("contiguousIndex", Boolean.valueOf(contiguousIndex))
        stage.stageDetail.put("outputView", outputView)
        stage.stageDetail.put("persist", Boolean.valueOf(persist))
        val options: Map[String, String] = stage.basePath match {
          case Some(basePath) => Delimited.toSparkOptions(stage.settings) + ("basePath" -> basePath)
          case None => Delimited.toSparkOptions(stage.settings)
        }
        val inputValue = stage.input match {
          case Left(view) => view
          case Right(glob) => glob
        }
        stage.stageDetail.put("input", inputValue)  
        stage.stageDetail.put("options", options.asJava)
        for (inputField <- inputField) {
          stage.stageDetail.put("inputField", inputField)  
        }

        Right(stage)            
      case _ =>
        val allErrors: Errors = List(name, description, inputView, parsedGlob, extractColumns, outputView, persist, numPartitions, partitionBy, header, authentication, contiguousIndex, delimiter, quote, invalidKeys, customDelimiter, inputField, basePath).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

}

case class DelimitedExtractStage(
    plugin: PipelineStagePlugin,
    name: String, 
    description: Option[String], 
    schema: Either[String, List[ExtractColumn]], 
    outputView: String, 
    input: Either[String, String],
    settings: Delimited, 
    authentication: Option[Authentication], 
    params: Map[String, String], 
    persist: Boolean, 
    numPartitions: Option[Int], 
    partitionBy: List[String], 
    contiguousIndex: Boolean, 
    inputField: Option[String], 
    basePath: Option[String]
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    DelimitedExtractStage.execute(this)
  }
}

object DelimitedExtractStage {

  def execute(stage: DelimitedExtractStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    import spark.implicits._

    val options: Map[String, String] = stage.basePath match {
      case Some(basePath) => Delimited.toSparkOptions(stage.settings) + ("basePath" -> basePath)
      case None => Delimited.toSparkOptions(stage.settings)
    }

    val inputValue = stage.input match {
      case Left(view) => view
      case Right(glob) => glob
    }
    
    // try to get the schema
    val optionSchema = try {
      ExtractUtils.getSchema(stage.schema)(spark, logger)
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail          
      }      
    }        

    val df = try {
      if (arcContext.isStreaming) {
        stage.input match {
          case Right(glob) => {
            CloudUtils.setHadoopConfiguration(stage.authentication)

            optionSchema match {
              case Some(schema) => spark.readStream.options(options).schema(schema).csv(glob)
              case None => throw new Exception("CSVExtract requires 'schemaURI' or 'schemaView' to be set if Arc is running in streaming mode.")
            }       
          }
          case Left(view) => throw new Exception("CSVExtract does not support the use of 'inputView' if Arc is running in streaming mode.")
        }
      } else {      
        stage.input match {
          case Right(glob) =>
            CloudUtils.setHadoopConfiguration(stage.authentication)

            try {
              spark.read.options(options).csv(glob)
            } catch {
              // the file that is read is empty
              case e: AnalysisException if (e.getMessage == "Unable to infer schema for CSV. It must be specified manually.;") =>
                spark.emptyDataFrame
              // the file does not exist at all
              case e: AnalysisException if (e.getMessage.contains("Path does not exist")) =>
                spark.emptyDataFrame
              case e: Exception => throw e
            }
            
          case Left(view) => {
            stage.inputField match {
              case Some(inputField) => spark.read.options(options).csv(spark.table(view).select(col(inputField).as("value")).as[String])
              case None => spark.read.options(options).csv(spark.table(view).as[String])
            }
          }
        }   
      }      
    } catch { 
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail          
      }    
    }

    // if incoming dataset has 0 columns then create empty dataset with correct schema
    val emptyDataframeHandlerDF = try {
      if (df.schema.length == 0) {
        stage.stageDetail.put("records", Integer.valueOf(0))
        optionSchema match {
          case Some(schema) => spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
          case None => throw new Exception(s"DelimitedExtract has produced 0 columns and no schema has been provided to create an empty dataframe.")
        }
      } else {
        df
      }
    } catch {
      case e: Exception => throw new Exception(e.getMessage) with DetailException {
        override val detail = stage.stageDetail          
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
      stage.stageDetail.put("inputFiles", Integer.valueOf(repartitionedDF.inputFiles.length))
      stage.stageDetail.put("outputColumns", Integer.valueOf(repartitionedDF.schema.length))
      stage.stageDetail.put("numPartitions", Integer.valueOf(repartitionedDF.rdd.partitions.length))

      if (stage.persist) {
        repartitionedDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
        stage.stageDetail.put("records", java.lang.Long.valueOf(repartitionedDF.count))
      }      
    }

    Option(repartitionedDF)
  }
}