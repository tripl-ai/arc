package ai.tripl.arc.extract

import java.lang._
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

class TextExtract extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def createStage(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "outputView" :: "authentication" :: "contiguousIndex" :: "multiLine" :: "numPartitions" :: "persist" :: "schemaURI" :: "schemaView" :: "params" :: "basePath" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputURI = getValue[String]("inputURI")
    val parsedGlob = inputURI.rightFlatMap(glob => parseGlob("inputURI", glob))
    val outputView = getValue[String]("outputView")
    val persist = getValue[Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val multiLine = getValue[Boolean]("multiLine", default = Some(false))
    val authentication = readAuthentication("authentication")
    val contiguousIndex = getValue[Boolean]("contiguousIndex", default = Some(true))
    val uriKey = "schemaURI"
    val stringURI = getOptionalValue[String](uriKey)
    val parsedURI: Either[Errors, Option[URI]] = stringURI.rightFlatMap(optURI => 
      optURI match { 
        case Some(uri) => parseURI(uriKey)(uri).rightFlatMap(parsedURI => Right(Option(parsedURI)))
        case None => Right(None)
      }
    )
    val extractColumns = if(!c.hasPath("schemaView")) getExtractColumns(parsedURI, uriKey, authentication) else Right(List.empty)
    val basePath = getOptionalValue[String]("basePath")
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, description, extractColumns, inputURI, parsedGlob, outputView, persist, numPartitions, multiLine, authentication, contiguousIndex, basePath, invalidKeys) match {
      case (Right(name), Right(description), Right(extractColumns), Right(inputURI), Right(parsedGlob), Right(outputView), Right(persist), Right(numPartitions), Right(multiLine), Right(authentication), Right(contiguousIndex), Right(basePath), Right(invalidKeys)) => 
        
      val stage = TextExtractStage(
          plugin=this,
          name=name,
          description=description,
          schema=Right(extractColumns),
          outputView=outputView, 
          input=parsedGlob,
          authentication=authentication,
          persist=persist,
          numPartitions=numPartitions,
          contiguousIndex=contiguousIndex,
          multiLine=multiLine,
          basePath=basePath,
          params=params
        )

        stage.stageDetail.put("contiguousIndex", Boolean.valueOf(contiguousIndex))
        stage.stageDetail.put("input", parsedGlob)  
        stage.stageDetail.put("multiLine", Boolean.valueOf(multiLine))
        stage.stageDetail.put("outputView", outputView)  
        stage.stageDetail.put("persist", Boolean.valueOf(persist))
        for (basePath <- basePath) {
          stage.stageDetail.put("basePath", basePath)  
        } 

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, extractColumns, inputURI, parsedGlob, outputView, persist, numPartitions, multiLine, authentication, contiguousIndex, basePath, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

}

case class TextExtractStage(
    plugin: PipelineStagePlugin,
    name: String,
    description: Option[String],
    schema: Either[String, List[ExtractColumn]],
    outputView: String,
    input: String,
    authentication: Option[Authentication],
    params: Map[String, String],
    persist: Boolean,
    numPartitions: Option[Int],
    contiguousIndex: Boolean,
    multiLine: Boolean,
    basePath: Option[String]
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    TextExtractStage.execute(this)
  }
}

object TextExtractStage {

  def execute(stage: TextExtractStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
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

    val df = try {
      if (arcContext.isStreaming) {
        CloudUtils.setHadoopConfiguration(stage.authentication)

        optionSchema match {
          case Some(schema) => spark.readStream.schema(schema).text(stage.input)
          case None => throw new Exception("JSONExtract requires 'schemaURI' to be set if Arc is running in streaming mode.")
        }             
      } else {
        CloudUtils.setHadoopConfiguration(stage.authentication)
        // spark does not cope well reading many small files into json directly from hadoop file systems
        // by reading first as text time drops by ~75%
        // this will not throw an error for empty directory (but will for missing directory)
        try {
          if (stage.multiLine) {
            stage.basePath match {
              case Some(basePath) => spark.read.option("mergeSchema", "true").option("basePath", basePath).parquet(stage.input)
              case None => spark.read.option("wholetext", "true").textFile(stage.input).toDF  
            }  
          } else {
            spark.read.option("wholetext", "false").textFile(stage.input).toDF
          }
        } catch {
          case e: org.apache.spark.sql.AnalysisException if (e.getMessage.contains("Path does not exist")) => {
            spark.emptyDataFrame
          }
          case e: Exception => throw e
        }
      }
    } catch {
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
          case None => throw new Exception(s"TextExtract has produced 0 columns and no schema has been provided to create an empty dataframe.")
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

    // // set column metadata if exists
    val enrichedDF = optionSchema match {
        case Some(schema) => MetadataUtils.setMetadata(sourceEnrichedDF, schema)
        case None => sourceEnrichedDF   
    }

    // repartition to distribute rows evenly
    val repartitionedDF = stage.numPartitions match {
      case Some(numPartitions) => enrichedDF.repartition(numPartitions)
      case None => enrichedDF
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

