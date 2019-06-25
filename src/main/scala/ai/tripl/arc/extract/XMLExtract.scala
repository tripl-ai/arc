package ai.tripl.arc.extract

import java.lang._
import java.net.URI
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import com.databricks.spark.xml._

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

class XMLExtract extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def createStage(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "inputView" :: "outputView" :: "authentication" :: "contiguousIndex" :: "numPartitions" :: "partitionBy" :: "persist" :: "schemaURI" :: "schemaView" :: "params" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = if(c.hasPath("inputView")) getValue[String]("inputView") else getValue[String]("inputURI")
    val parsedGlob = if (!c.hasPath("inputView")) inputView.rightFlatMap(glob => parseGlob("inputURI", glob)) else Right("")
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
        case Some(uri) => parseURI(uriKey)(uri).rightFlatMap(parsedURI => Right(Option(parsedURI)))
        case None => Right(None)
      }
    )
    val extractColumns = if(!c.hasPath("schemaView")) getExtractColumns(parsedURI, uriKey, authentication) else Right(List.empty)
    val schemaView = if(c.hasPath("schemaView")) getValue[String]("schemaView") else Right("")  
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, description, extractColumns, schemaView, inputView, parsedGlob, outputView, persist, numPartitions, authentication, contiguousIndex, partitionBy, invalidKeys) match {
      case (Right(name), Right(description), Right(extractColumns), Right(schemaView), Right(inputView), Right(parsedGlob), Right(outputView), Right(persist), Right(numPartitions), Right(authentication), Right(contiguousIndex), Right(partitionBy), Right(invalidKeys)) => 
        val input = if(c.hasPath("inputView")) Left(inputView) else Right(parsedGlob)
        val schema = if(c.hasPath("schemaView")) Left(schemaView) else Right(extractColumns)

        val stage = XMLExtractStage(
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
          contiguousIndex=contiguousIndex
        )

        stage.stageDetail.put("input", input)  
        stage.stageDetail.put("outputView", outputView)  
        stage.stageDetail.put("persist", Boolean.valueOf(persist))
        stage.stageDetail.put("contiguousIndex", Boolean.valueOf(contiguousIndex))

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, extractColumns, schemaView, inputView, parsedGlob, outputView, persist, numPartitions, authentication, contiguousIndex, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class XMLExtractStage(
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
    contiguousIndex: Boolean
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    XMLExtractStage.execute(this)
  }
}

object XMLExtractStage {

  def execute(stage: XMLExtractStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): Option[DataFrame] = {
    // force com.sun.xml.* implementation for reading xml to be compatible with spark-xml library
    System.setProperty("javax.xml.stream.XMLInputFactory", "com.sun.xml.internal.stream.XMLInputFactoryImpl")
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
      stage.input match {
        case Right(glob) => {
          CloudUtils.setHadoopConfiguration(stage.authentication)

          // remove the crlf delimiter so it is read as a full object per file
          val oldDelimiter = spark.sparkContext.hadoopConfiguration.get("textinputformat.record.delimiter")
          val newDelimiter = s"${0x0 : Char}"
          // temporarily remove the delimiter so all the data is loaded as a single line
          spark.sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", newDelimiter)              

          // read the file but do not cache. caching will break the input_file_name() function
          val textFile = spark.sparkContext.textFile(glob)

          val xmlReader = new XmlReader
          val xml = xmlReader.xmlRdd(spark, textFile)

          // reset delimiter
          if (oldDelimiter == null) {
            spark.sparkContext.hadoopConfiguration.unset("textinputformat.record.delimiter")              
          } else {
            spark.sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", oldDelimiter)        
          }

          xml            
        }
        case Left(view) => {
          val xmlReader = new XmlReader
          xmlReader.xmlRdd(spark, spark.table(view).as[String].rdd)
        }
      }     
    } catch {
      case e: org.apache.hadoop.mapred.InvalidInputException if (e.getMessage.contains("matches 0 files")) => {
        spark.emptyDataFrame
      }         
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
          case None => throw new Exception(s"XMLExtract has produced 0 columns and no schema has been provided to create an empty dataframe.")
        }
      } else {
        df
      }
    } catch {
      case e: Exception => throw new Exception(e.getMessage) with DetailException {
        override val detail = stageDetail          
      }      
    }

    // try to explode the rows returned by the XML reader
    val flattenedDF = if (emptyDataframeHandlerDF.schema.length == 1) {
      emptyDataframeHandlerDF.schema.fields(0).dataType.typeName match {
        case "array" => emptyDataframeHandlerDF.select(explode(col(emptyDataframeHandlerDF.schema.fieldNames(0)))).select("col.*")
        case "struct" => emptyDataframeHandlerDF.select(s"${emptyDataframeHandlerDF.schema.fieldNames(0)}.*")
        case _ => emptyDataframeHandlerDF
      }
    } else {
      emptyDataframeHandlerDF
    }    

    // add internal columns data _filename, _index
    val sourceEnrichedDF = ExtractUtils.addInternalColumns(flattenedDF, stage.contiguousIndex)

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

