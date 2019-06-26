package ai.tripl.arc.plugins.lifecycle

import java.util

import org.apache.spark.sql.{DataFrame, SparkSession}

import ai.tripl.arc.api.API._
import ai.tripl.arc.plugins.LifecyclePlugin
import ai.tripl.arc.util.Utils
import ai.tripl.arc.util.log.logger.Logger
import ai.tripl.arc.config.Error._

class DataFramePrinter extends LifecyclePlugin {

  val version = Utils.getFrameworkVersion

  def instantiate[DataFramePrinterInstance](index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], DataFramePrinterInstance] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "numRows" :: "truncate" :: Nil
    val numRows = getValue[Int]("numRows", default = Some(20))
    val truncate = getValue[java.lang.Boolean]("truncate", default = Some(true))
    val invalidKeys = checkValidKeys(c)(expectedKeys)      

    (numRows, truncate, invalidKeys) match {
      case (Right(numRows), Right(truncate), Right(invalidKeys)) => 
        val instance = DataFramePrinterInstance(
          plugin=this,
          numRows=numRows,
          truncate=truncate
        )
        Right(instance)
      case _ =>
        val allErrors: Errors = List(numRows, truncate, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val err = StageError(index, this.getClass.getName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }    
  }
}

case class DataFramePrinterInstance(
    plugin: LifecyclePlugin,
    numRows: Int,
    truncate: Boolean
  ) extends LifecyclePluginInstance {

  override def before(stage: PipelineStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext) {
    DataFramePrinterInstance.before(this, stage)
  }

  override def after(stage: PipelineStage, result: Option[DataFrame], isLast: Boolean)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger) {
    DataFramePrinterInstance.after(this, stage, result, isLast)
  } 
}

object DataFramePrinterInstance {

  def before(instance: DataFramePrinterInstance, stage: PipelineStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger) = {
    logger.trace()        
      .field("event", "before")
      .field("stage", stage.name)
      .log()  
  }

  def after(instance: DataFramePrinterInstance, stage: PipelineStage, result: Option[DataFrame], isLast: Boolean)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger) = {
    logger.trace()        
      .field("event", "after")
      .field("stage", stage.name)
      .field("isLast", java.lang.Boolean.valueOf(isLast))
      .log() 

    result match {
      case Some(df) => df.show(instance.numRows, instance.truncate)
      case None =>
    }
  }

}