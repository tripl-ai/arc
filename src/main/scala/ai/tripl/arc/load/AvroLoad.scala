package ai.tripl.arc.load

import java.lang._
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

class AvroLoad extends PipelineStagePlugin {

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

      val stage = AvroLoadStage(
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

case class AvroLoadStage(
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
    AvroLoadStage.execute(this)
  }
}

object AvroLoadStage {

  def execute(stage: AvroLoadStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): Option[DataFrame] = {

    val df = spark.table(stage.inputView)      

    stage.numPartitions match {
      case Some(partitions) => stage.stageDetail.put("numPartitions", Integer.valueOf(partitions))
      case None => stage.stageDetail.put("numPartitions", Integer.valueOf(df.rdd.getNumPartitions))
    }

    // set write permissions
    CloudUtils.setHadoopConfiguration(stage.authentication)

    val dropMap = new java.util.HashMap[String, Object]()

    // Avro cannot handle a column of NullType
    val nulls = df.schema.filter( _.dataType == NullType).map(_.name)
    if (!nulls.isEmpty) {
      dropMap.put("NullType", nulls.asJava)
    }

    stage.stageDetail.put("drop", dropMap) 

    val listener = ListenerUtils.addStageCompletedListener(stage.stageDetail)

    // Avro will convert date and times to epoch milliseconds
    val outputDF = try {
      val nonNullDF = df.drop(nulls:_*)
      stage.partitionBy match {
        case Nil =>
          val dfToWrite = stage.numPartitions.map(nonNullDF.repartition(_)).getOrElse(nonNullDF)
          dfToWrite.write.mode(stage.saveMode).format("avro").save(stage.outputURI.toString)
          dfToWrite
        case partitionBy => {
          // create a column array for repartitioning
          val partitionCols = partitionBy.map(col => df(col))
          stage.numPartitions match {
            case Some(n) =>
              val dfToWrite = nonNullDF.repartition(n, partitionCols:_*)
              dfToWrite.write.partitionBy(partitionBy:_*).mode(stage.saveMode).format("avro").save(stage.outputURI.toString)
              dfToWrite
            case None =>
              val dfToWrite = nonNullDF.repartition(partitionCols:_*)
              dfToWrite.write.partitionBy(partitionBy:_*).mode(stage.saveMode).format("avro").save(stage.outputURI.toString)
              dfToWrite
          }
        }
      }
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    spark.sparkContext.removeSparkListener(listener)

    Option(outputDF)
  }
}