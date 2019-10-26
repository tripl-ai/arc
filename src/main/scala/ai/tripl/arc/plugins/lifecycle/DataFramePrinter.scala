package ai.tripl.arc.plugins.lifecycle

import org.apache.spark.sql.{DataFrame, SparkSession}

import ai.tripl.arc.api.API._
import ai.tripl.arc.plugins.LifecyclePlugin
import ai.tripl.arc.util.Utils
import ai.tripl.arc.config.Error._

class DataFramePrinter extends LifecyclePlugin {

  val version = Utils.getFrameworkVersion

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], LifecyclePluginInstance] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "environments" :: "numRows" :: "truncate" :: Nil
    val numRows = getValue[Int]("numRows", default = Some(20))
    val truncate = getValue[java.lang.Boolean]("truncate", default = Some(true))
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (numRows, truncate, invalidKeys) match {
      case (Right(numRows), Right(truncate), Right(invalidKeys)) =>
        Right(DataFramePrinterInstance(
          plugin=this,
          numRows=numRows,
          truncate=truncate
        ))
      case _ =>
        val allErrors: Errors = List(numRows, truncate, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val err = StageError(index, this.getClass.getName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class DataFramePrinterInstance(
    plugin: DataFramePrinter,
    numRows: Int,
    truncate: Boolean
  ) extends LifecyclePluginInstance {

  override def before(stage: PipelineStage, index: Int, stages: List[PipelineStage])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext) {
    logger.trace()
      .field("event", "before")
      .field("stage", stage.name)
      .log()
  }

  override def after(result: Option[DataFrame], stage: PipelineStage, index: Int, stages: List[PipelineStage])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext) {
    logger.trace()
      .field("event", "after")
      .field("stage", stage.name)
      .log()

    result match {
      case Some(df) => df.show(numRows, truncate)
      case None =>
    }
  }
}
