package ai.tripl.arc.plugins
import java.util

import org.apache.spark.sql.{DataFrame, SparkSession}
import ai.tripl.arc.api.API._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.util.Utils
import ai.tripl.arc.util.log.logger.Logger

class LimitLifecyclePlugin extends LifecyclePlugin {

  val version = "0.0.1"

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], LifecyclePluginInstance] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "environments" :: "name" :: "limit" :: "outputView" :: Nil
    val name = getValue[String]("name")
    val limit = getValue[Int]("limit")
    val outputView = getValue[String]("outputView")
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, limit, outputView, invalidKeys) match {
      case (Right(name), Right(limit), Right(outputView), Right(invalidKeys)) =>

        val instance = LimitLifecyclePluginInstance(
          plugin=this,
          name=name,
          limit=limit,
          outputView=outputView
        )

        Right(instance)
      case _ =>
        val allErrors: Errors = List(name, limit, outputView, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val err = StageError(index, this.getClass.getName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class LimitLifecyclePluginInstance(
    plugin: LimitLifecyclePlugin,
    name: String,
    limit: Int,
    outputView: String
  ) extends LifecyclePluginInstance {

  override def before(stage: PipelineStage, index: Int, stages: List[PipelineStage])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext) {
    logger.info()
      .field("event", "before")
      .field("name", name)
      .log()
  }

  override def after(currentValue: Option[DataFrame], stage: PipelineStage, index: Int, stages: List[PipelineStage])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    logger.info()
      .field("event", "after")
      .field("name", name)
      .log()

    currentValue.map(df => {
      val mutatedDF = df.limit(limit)
      mutatedDF.createOrReplaceTempView(outputView)
      mutatedDF
    })
  }
}