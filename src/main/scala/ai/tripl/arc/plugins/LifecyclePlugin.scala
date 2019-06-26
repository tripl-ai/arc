package ai.tripl.arc.plugins

import java.util.ServiceLoader

import scala.collection.JavaConverters._
import org.apache.spark.sql.{DataFrame, SparkSession}
import ai.tripl.arc.api.API.{ARCContext, ConfigPlugin, LifecyclePluginInstance}
import ai.tripl.arc.util.Utils


trait LifecyclePlugin extends ConfigPlugin {

  def instantiate[LifecyclePluginInstance](index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], LifecyclePluginInstance]

}


