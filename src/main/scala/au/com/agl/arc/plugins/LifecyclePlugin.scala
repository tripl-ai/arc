package au.com.agl.arc.plugins

import java.util.ServiceLoader

import scala.collection.JavaConverters._
import org.apache.spark.sql.{DataFrame, SparkSession}
import au.com.agl.arc.api.API.PipelineStage
import au.com.agl.arc.util.Utils

trait LifecyclePlugin {

  def before(stage: PipelineStage)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger)

  def after(stage: PipelineStage, result: Option[DataFrame], isLast: Boolean)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger)

}

object LifecyclePlugin {

  def plugins(): List[LifecyclePlugin] = {

    val loader = Utils.getContextOrSparkClassLoader
    val serviceLoader = ServiceLoader.load(classOf[LifecyclePlugin], loader)

    val plugins = serviceLoader.iterator().asScala.toList

    plugins
  }

}
