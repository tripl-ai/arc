package au.com.agl.arc.plugins

import java.util.ServiceLoader

import scala.collection.JavaConverters._
import org.apache.spark.sql.{DataFrame, SparkSession}
import au.com.agl.arc.api.API.PipelineStage
import au.com.agl.arc.util.Utils

trait LifecyclePlugin {
  def params: Map[String, String]

  def setParams(params: Map[String, String]) 

  def before(stage: PipelineStage)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger)

  def after(stage: PipelineStage, result: Option[DataFrame], isLast: Boolean)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger)

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
