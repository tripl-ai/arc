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

class RateExtract extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def createStage(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "outputView" :: "rowsPerSecond" :: "rampUpTime" :: "numPartitions" :: "params" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val outputView = getValue[String]("outputView")
    val rowsPerSecond = getValue[Int]("rowsPerSecond", default = Some(1))
    val rampUpTime = getValue[Int]("rampUpTime", default = Some(1))
    val numPartitions = getValue[Int]("numPartitions", default = Some(spark.sparkContext.defaultParallelism))
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, description, outputView, rowsPerSecond, rampUpTime, numPartitions, invalidKeys) match {
      case (Right(name), Right(description), Right(outputView), Right(rowsPerSecond), Right(rampUpTime), Right(numPartitions), Right(invalidKeys)) => 

        val stage = RateExtractStage(
          plugin=this,
          name=name,
          description=description,
          outputView=outputView, 
          rowsPerSecond=rowsPerSecond,
          rampUpTime=rampUpTime,
          numPartitions=numPartitions,
          params=params
        )

        stage.stageDetail.put("outputView", outputView)  
        stage.stageDetail.put("rowsPerSecond", Integer.valueOf(rowsPerSecond))
        stage.stageDetail.put("rampUpTime", Integer.valueOf(rampUpTime))
        stage.stageDetail.put("numPartitions", Integer.valueOf(numPartitions))

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, outputView, rowsPerSecond, rampUpTime, numPartitions, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

}

case class RateExtractStage(
    plugin: PipelineStagePlugin,
    name: String,
    description: Option[String],
    outputView: String,
    params: Map[String, String],
    rowsPerSecond: Int,
    rampUpTime: Int,
    numPartitions: Int
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    RateExtractStage.execute(this)
  }
}

object RateExtractStage {

  def execute(stage: RateExtractStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    import spark.implicits._
    val stageDetail = stage.stageDetail

    if (!arcContext.isStreaming) {
      throw new Exception("RateExtract can only be executed in streaming mode.") with DetailException {
        override val detail = stageDetail          
      }
    }

    val df = spark.readStream
      .format("rate")
      .option("rowsPerSecond", stage.rowsPerSecond.toString)
      .option("rampUpTime", s"${stage.rampUpTime}s")
      .option("numPartitions", stage.numPartitions.toString)
      .load

    df.createOrReplaceTempView(stage.outputView)

    Option(df)
  }

}

