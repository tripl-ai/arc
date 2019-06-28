package ai.tripl.arc.extract

import java.net.URI
import java.util.Properties
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

class JSONExtract extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "inputURI" :: "outputView" :: "authentication" :: "contiguousIndex" :: "multiLine" :: "numPartitions" :: "partitionBy" :: "persist" :: "schemaURI" :: "schemaView" :: "params" :: "inputField" :: "basePath" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = if(c.hasPath("inputView")) getValue[String]("inputView") else Right("")
    val parsedGlob = if (!c.hasPath("inputView")) getValue[String]("inputURI") |> parseGlob("inputURI") _ else Right("")
    val outputView = getValue[String]("outputView")
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val multiLine = getValue[java.lang.Boolean]("multiLine", default = Some(true))
    val authentication = readAuthentication("authentication")
    val contiguousIndex = getValue[java.lang.Boolean]("contiguousIndex", default = Some(true))
    val extractColumns = if(c.hasPath("schemaURI")) getValue[String]("schemaURI") |> parseURI("schemaURI") _ |> getExtractColumns("schemaURI", authentication) _ else Right(List.empty)
    val schemaView = if(c.hasPath("schemaView")) getValue[String]("schemaView") else Right("")
    val inputField = getOptionalValue[String]("inputField")
    val basePath = getOptionalValue[String]("basePath")
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)    

    (name, description, extractColumns, schemaView, inputView, parsedGlob, outputView, persist, numPartitions, multiLine, authentication, contiguousIndex, partitionBy, inputField, basePath, invalidKeys) match {
      case (Right(name), Right(description), Right(extractColumns), Right(schemaView), Right(inputView), Right(parsedGlob), Right(outputView), Right(persist), Right(numPartitions), Right(multiLine), Right(authentication), Right(contiguousIndex), Right(partitionBy), Right(inputField), Right(basePath), Right(invalidKeys)) => 
        val input = if(c.hasPath("inputView")) Left(inputView) else Right(parsedGlob)
        val schema = if(c.hasPath("schemaView")) Left(schemaView) else Right(extractColumns)

        val stage = JSONExtractStage(
          plugin=this,
          name=name,
          description=description,
          schema=schema,
          outputView=outputView,
          input=input,
          settings=new JSON(multiLine=multiLine),
          authentication=authentication,
          params=params,
          persist=persist,
          numPartitions=numPartitions,
          partitionBy=partitionBy,
          contiguousIndex=contiguousIndex,
          basePath=basePath,
          inputField=inputField
        )

        stage.stageDetail.put("contiguousIndex", java.lang.Boolean.valueOf(contiguousIndex))
        stage.stageDetail.put("input", input)  
        stage.stageDetail.put("outputView", outputView)  
        stage.stageDetail.put("persist", java.lang.Boolean.valueOf(persist))
        stage.stageDetail.put("options", JSON.toSparkOptions(stage.settings).asJava)
        for (basePath <- basePath) {
          stage.stageDetail.put("basePath", basePath)  
        }        
        for (inputField <- inputField) {
          stage.stageDetail.put("inputField", inputField)  
        }

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, extractColumns, schemaView, inputView, parsedGlob, outputView, persist, numPartitions, multiLine, authentication, contiguousIndex, partitionBy, invalidKeys, inputField, basePath).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

}

case class JSONExtractStage(
    plugin: JSONExtract,
    name: String, 
    description: Option[String], 
    schema: Either[String, List[ExtractColumn]], 
    outputView: String, 
    input: Either[String, String], 
    settings: JSON, 
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
    JSONExtractStage.execute(this)
  }
}

object JSONExtractStage {

  def execute(stage: JSONExtractStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    import spark.implicits._

    // try to get the schema
    val optionSchema = try {
      ExtractUtils.getSchema(stage.schema)(spark, logger)
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail          
      }      
    }       

    val options = stage.basePath match {
      case Some(basePath) => JSON.toSparkOptions(stage.settings) + ("basePath" -> basePath)
      case None => JSON.toSparkOptions(stage.settings)
    }    

    val df = try {
      if (arcContext.isStreaming) {
        stage.input match {
          case Right(glob) => {
            CloudUtils.setHadoopConfiguration(stage.authentication)

            optionSchema match {
              case Some(schema) => spark.readStream.options(options).schema(schema).json(glob)
              case None => throw new Exception("JSONExtract requires 'schemaURI' or 'schemaView' to be set if Arc is running in streaming mode.")
            }       
          }
          case Left(view) => throw new Exception("JSONExtract does not support the use of 'inputView' if Arc is running in streaming mode.")
        }
      } else {
        stage.input match {
          case Right(glob) =>
            CloudUtils.setHadoopConfiguration(stage.authentication)
            // spark does not cope well reading many small files into json directly from hadoop file systems
            // by reading first as text time drops by ~75%
            // this will not throw an error for empty directory (but will for missing directory)
            try {
              if (stage.settings.multiLine ) {

                // if multiLine then remove the crlf delimiter so it is read as a full object per file
                val oldDelimiter = spark.sparkContext.hadoopConfiguration.get("textinputformat.record.delimiter")
                val newDelimiter = s"${0x0 : Char}"
                // temporarily remove the delimiter so all the data is loaded as a single line
                spark.sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", newDelimiter)              

                // read the file but do not cache. caching will break the input_file_name() function
                val textFile = spark.sparkContext.textFile(glob)

                val json = optionSchema match {
                  case Some(schema) => spark.read.options(options).schema(schema).json(textFile.toDS)
                  case None => spark.read.options(options).json(textFile.toDS)
                }             

                // reset delimiter
                if (oldDelimiter == null) {
                  spark.sparkContext.hadoopConfiguration.unset("textinputformat.record.delimiter")              
                } else {
                  spark.sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", oldDelimiter)        
                }

                json
              } else {
                // read the file but do not cache. caching will break the input_file_name() function              
                val textFile = spark.sparkContext.textFile(glob)

                val json = optionSchema match {
                  case Some(schema) => spark.read.options(options).schema(schema).json(textFile.toDS)
                  case None => spark.read.options(options).json(textFile.toDS)
                }

                json              
              }
            } catch {
              case e: org.apache.hadoop.mapred.InvalidInputException => {
                spark.emptyDataFrame
              }
              case e: Exception => throw e
            }
          case Left(view) => {
            stage.inputField match {
              case Some(inputField) => spark.read.options(options).json(spark.table(view).select(col(inputField).as("value")).as[String])
              case None => spark.read.options(options).json(spark.table(view).as[String])
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
        stage.stageDetail.put("records", java.lang.Integer.valueOf(0))
        optionSchema match {
          case Some(s) => spark.createDataFrame(spark.sparkContext.emptyRDD[Row], s)
          case None => throw new Exception(s"JSONExtract has produced 0 columns and no schema has been provided to create an empty dataframe.")
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

    // // set column metadata if exists
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
      stage.stageDetail.put("inputFiles", java.lang.Integer.valueOf(repartitionedDF.inputFiles.length))
      stage.stageDetail.put("outputColumns", java.lang.Integer.valueOf(repartitionedDF.schema.length))
      stage.stageDetail.put("numPartitions", java.lang.Integer.valueOf(repartitionedDF.rdd.partitions.length))

      if (stage.persist) {
        repartitionedDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
        stage.stageDetail.put("records", java.lang.Long.valueOf(repartitionedDF.count)) 
      }      
    }

    Option(repartitionedDF)
  }

}

