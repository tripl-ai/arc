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

    val expectedKeys = "type" :: "environments" :: "key" :: Nil
    val key = getValue[String]("key")
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (key, invalidKeys) match {
      case (Right(key), Right(invalidKeys)) =>

        val instance = TestLifecyclePluginInstance(
          plugin=this,
          key=key
        )

        Right(instance)
      case _ =>
        val allErrors: Errors = List(key, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val err = StageError(index, this.getClass.getName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class TestLifecyclePluginInstance(
    plugin: LifecyclePlugin,
    key: String
  ) extends LifecyclePluginInstance {

  override def before(index: Int, stages: List[PipelineStage])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext) {
    import spark.implicits._
    val stage= stages(index)
    val df = Seq((stage.name, "before", this.key)).toDF("stage","when","message")
    df.createOrReplaceTempView("before")
  }

  override def after(currentValue: Option[DataFrame], index: Int, stages: List[PipelineStage])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext) {
    import spark.implicits._
    val stage= stages(index)
    val df = Seq((stage.name, "after", this.key, currentValue.get.count)).toDF("stage","when","message","count")
    df.createOrReplaceTempView("after")
  }
}