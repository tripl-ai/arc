package ai.tripl.arc.load

import java.net.URI
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.types._

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
import ai.tripl.arc.util.ListenerUtils
import ai.tripl.arc.util.Utils

class XMLLoad extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def createStage(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputURI" :: "authentication" :: "numPartitions" :: "partitionBy" :: "saveMode" :: "params" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = getValue[String]("inputView")
    val outputURI = getValue[String]("outputURI") |> parseURI("outputURI") _
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val authentication = readAuthentication("authentication")  
    val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    (name, description, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys) match {
      case (Right(name), Right(description), Right(inputView), Right(outputURI), Right(numPartitions), Right(authentication), Right(saveMode), Right(partitionBy), Right(invalidKeys)) => 
        
        val stage = XMLLoadStage(
          plugin=this,
          name=name,
          description=description,
          inputView=inputView,
          outputURI=outputURI,
          partitionBy=partitionBy,
          numPartitions=numPartitions,
          authentication=authentication,
          saveMode=saveMode,
          params=params
        )

        stage.stageDetail.put("inputView", inputView)  
        stage.stageDetail.put("outputURI", outputURI.toString)  
        stage.stageDetail.put("partitionBy", partitionBy.asJava)
        stage.stageDetail.put("saveMode", saveMode.toString.toLowerCase)        

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class XMLLoadStage(
    plugin: PipelineStagePlugin,
    name: String, 
    description: Option[String], 
    inputView: String, 
    outputURI: URI, 
    partitionBy: List[String], 
    numPartitions: Option[Int], 
    authentication: Option[Authentication], 
    saveMode: SaveMode, 
    params: Map[String, String]
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    XMLLoadStage.execute(this)
  }
}

object XMLLoadStage {

  def execute(stage: XMLLoadStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    // force com.sun.xml.* implementation for writing xml to be compatible with spark-xml library
    System.setProperty("javax.xml.stream.XMLOutputFactory", "com.sun.xml.internal.stream.XMLOutputFactoryImpl")

    val df = spark.table(stage.inputView) 

    stage.numPartitions match {
      case Some(partitions) => stage.stageDetail.put("numPartitions", Integer.valueOf(partitions))
      case None => stage.stageDetail.put("numPartitions", Integer.valueOf(df.rdd.getNumPartitions))
    }

    // set write permissions
    CloudUtils.setHadoopConfiguration(stage.authentication)
     
    val dropMap = new java.util.HashMap[String, Object]()

    // XML does not need to deal with NullType as it is silenty dropped on write but we want logging to be explicit
    val nulls = df.schema.filter( _.dataType == NullType).map(_.name)
    if (!nulls.isEmpty) {
      dropMap.put("NullType", nulls.asJava)
    }

    stage.stageDetail.put("drop", dropMap) 

    val listener = ListenerUtils.addStageCompletedListener(stage.stageDetail)

    try {
      stage.partitionBy match {
        case Nil => { 
          stage.numPartitions match {
            case Some(n) => df.repartition(n).write.format("com.databricks.spark.xml").mode(stage.saveMode).save(stage.outputURI.toString)
            case None => df.write.format("com.databricks.spark.xml").mode(stage.saveMode).save(stage.outputURI.toString)  
          }   
        }
        case partitionBy => {
          // create a column array for repartitioning
          val partitionCols = partitionBy.map(col => df(col))
          stage.numPartitions match {
            case Some(n) => df.repartition(n, partitionCols:_*).write.format("com.databricks.spark.xml").partitionBy(partitionBy:_*).mode(stage.saveMode).save(stage.outputURI.toString)
            case None => df.repartition(partitionCols:_*).write.format("com.databricks.spark.xml").partitionBy(partitionBy:_*).mode(stage.saveMode).save(stage.outputURI.toString)
          }   
        }
      }    
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail          
      }      
    }        

    spark.sparkContext.removeSparkListener(listener)           

    Option(df)
  }
}