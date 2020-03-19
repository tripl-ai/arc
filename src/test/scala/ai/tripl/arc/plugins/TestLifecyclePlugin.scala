package ai.tripl.arc.plugins
import java.util

import org.apache.spark.sql.{DataFrame, SparkSession}
import ai.tripl.arc.api.API._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.util.Utils
import ai.tripl.arc.util.log.logger.Logger

class TestLifecyclePlugin extends LifecyclePlugin {

  val version = "0.0.1"

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], LifecyclePluginInstance] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "environments" :: "name" :: "outputViewBefore" :: "outputViewAfter" :: "value" :: Nil
    val name = getValue[String]("name")
    val outputViewBefore = getValue[String]("outputViewBefore")
    val outputViewAfter = getValue[String]("outputViewAfter")
    val value = getValue[String]("value")
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, outputViewBefore, outputViewAfter, value, invalidKeys) match {
      case (Right(name), Right(outputViewBefore), Right(outputViewAfter), Right(value), Right(invalidKeys)) =>

        val instance = TestLifecyclePluginInstance(
          plugin=this,
          name=name,
          outputViewBefore=outputViewBefore,
          outputViewAfter=outputViewAfter,
          value=value
        )

        Right(instance)
      case _ =>
        val allErrors: Errors = List(name, outputViewBefore, outputViewAfter, value, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val err = StageError(index, this.getClass.getName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class TestLifecyclePluginInstance(
    plugin: LifecyclePlugin,
    name: String,
    outputViewBefore: String,
    outputViewAfter: String,
    value: String
  ) extends LifecyclePluginInstance {

  override def before(stage: PipelineStage, index: Int, stages: List[PipelineStage])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext) {
    import spark.implicits._

    logger.info()
      .field("event", "before")
      .field("name", name)
      .log()

    val df = Seq((stage.name, "before", value)).toDF("stage","when","message")
    df.createOrReplaceTempView(outputViewBefore)
  }

  override def after(currentValue: Option[DataFrame], stage: PipelineStage, index: Int, stages: List[PipelineStage])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    import spark.implicits._

    logger.info()
      .field("event", "after")
      .field("name", name)
      .log()

    val df = Seq((stage.name, "after", value, currentValue.get.count)).toDF("stage","when","message","count")
    df.createOrReplaceTempView(outputViewAfter)

    Option(df)
  }
}