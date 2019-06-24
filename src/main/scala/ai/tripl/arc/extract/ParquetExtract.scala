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

class ParquetExtract extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def createStage(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._

    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "outputView" :: "authentication" :: "contiguousIndex" :: "numPartitions" :: "partitionBy" :: "persist" :: "schemaURI" :: "schemaView" :: "params" :: "basePath" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    val name = getValue[String]("name")
    val params = readMap("params", c)
    val environments = if (c.hasPath("environments")) c.getStringList("environments").asScala.toList else Nil

    val description = getOptionalValue[String]("description")

    val inputURI = getValue[String]("inputURI")
    val parsedGlob = inputURI.rightFlatMap(glob => parseGlob("inputURI", glob))
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

    (name, description, extractColumns, schemaView, inputURI, parsedGlob, outputView, persist, numPartitions, authentication, contiguousIndex, partitionBy, invalidKeys, basePath) match {
      case (Right(n), Right(d), Right(cols), Right(sv), Right(in), Right(pg), Right(ov), Right(p), Right(np), Right(auth), Right(ci), Right(pb), Right(_), Right(bp)) => 
        val schema = if(c.hasPath("schemaView")) Left(sv) else Right(cols)
        Right(ParquetExtractStage(this, n, d, schema, ov, pg, auth, params, p, np, pb, ci, bp))
      case _ =>
        val allErrors: Errors = List(name, description, inputURI, schemaView, parsedGlob, outputView, persist, numPartitions, authentication, contiguousIndex, extractColumns, partitionBy, invalidKeys, basePath).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class ParquetExtractStage(plugin: PipelineStagePlugin, 
                          name: String, 
                          description: Option[String], 
                          cols: Either[String, List[ExtractColumn]],
                          outputView: String, 
                          input: String, 
                          authentication: Option[Authentication],
                          params: Map[String, String],
                          persist: Boolean,
                          numPartitions: Option[Int],
                          partitionBy: List[String],
                          contiguousIndex: Boolean,
                          basePath: Option[String]) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    ParquetExtractStage.extract(this)
  }

}

object ParquetExtractStage {

  def extract(stage: ParquetExtractStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    import spark.implicits._
    val stageDetail = stage.stageDetail
    stage.stageDetail.put("input", stage.input) 
    stage.stageDetail.put("outputView", stage.outputView)  
    stage.stageDetail.put("persist", Boolean.valueOf(stage.persist))
    stage.stageDetail.put("contiguousIndex", Boolean.valueOf(stage.contiguousIndex))

    // try to get the schema
    val optionSchema = try {
      ExtractUtils.getSchema(stage.cols)(spark, logger)
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail          
      }      
    } 

    CloudUtils.setHadoopConfiguration(stage.authentication)

    // if incoming dataset is empty create empty dataset with a known schema
    val df = try {
      if (arcContext.isStreaming) {
        optionSchema match {
          case Some(schema) => spark.readStream.option("mergeSchema", "true").schema(schema).parquet(stage.input)
          case None => throw new Exception("ParquetExtract requires 'schemaURI' or 'schemaView' to be set if Arc is running in streaming mode.")
        }       
      } else {    
        stage.basePath match {
          case Some(basePath) => spark.read.option("mergeSchema", "true").option("basePath", basePath).parquet(stage.input)
          case None => spark.read.option("mergeSchema", "true").parquet(stage.input)   
        }             
      }
    } catch {
      case e: AnalysisException if (e.getMessage == "Unable to infer schema for Parquet. It must be specified manually.;") || (e.getMessage.contains("Path does not exist")) => 
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
          case None => throw new Exception(s"ParquetExtract has produced 0 columns and no schema has been provided to create an empty dataframe.")
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