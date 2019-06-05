package ai.tripl.arc.plugins
import java.util

import org.apache.spark.sql.{DataFrame, SparkSession}
import ai.tripl.arc.api.API._
import ai.tripl.arc.util.Utils
import ai.tripl.arc.util.log.logger.Logger

class LifecyclePluginTest extends LifecyclePlugin {

  var params = Map[String, String]()

  override def setParams(p: Map[String, String]) {
    params = p
  }

  override def before(stage: PipelineStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger) {
    import spark.implicits._
    val df = Seq((stage.name, "before", params.getOrElse("key", "not found"))).toDF("stage","when","message")
    df.createOrReplaceTempView("before")
  }

  override def after(stage: PipelineStage, result: Option[DataFrame], isLast: Boolean)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger) {
    import spark.implicits._
    val df = Seq((stage.name, "after", params.getOrElse("key", "not found"), result.get.count, isLast)).toDF("stage","when","message","count","isLast")
    df.createOrReplaceTempView("after")
  }

}
