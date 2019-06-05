package ai.tripl.arc.plugins

import java.util.ServiceLoader

import scala.collection.JavaConverters._
import org.apache.spark.sql.{DataFrame, SparkSession}
import ai.tripl.arc.api.API.PipelineStage
import ai.tripl.arc.util.Utils

trait LifecyclePlugin {
  def params: Map[String, String]

  def setParams(params: Map[String, String]) 

  def before(stage: PipelineStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger)

  def after(stage: PipelineStage, result: Option[DataFrame], isLast: Boolean)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger)

}

object LifecyclePlugin {

  def resolve(plugin: String, params: Map[String, String]): Option[LifecyclePlugin] = {

    val loader = Utils.getContextOrSparkClassLoader
    val serviceLoader = ServiceLoader.load(classOf[LifecyclePlugin], loader)

    val plugins = for (p <- serviceLoader.iterator().asScala.toList if p.getClass.getName == plugin) yield p
    plugins.headOption match {
      case Some(p) => {
        p.setParams(params)
        Option(p)
      }
      case None => None
    }
  }

}
