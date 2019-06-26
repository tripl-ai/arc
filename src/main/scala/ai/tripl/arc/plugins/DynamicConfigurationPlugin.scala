package ai.tripl.arc.plugins

import java.util.{ServiceLoader, Map => JMap}

import scala.collection.JavaConverters._

import com.typesafe.config._

import org.apache.spark.sql.SparkSession

import ai.tripl.arc.api.API.{ARCContext, ConfigPlugin}
import ai.tripl.arc.util.Utils

trait DynamicConfigurationPlugin extends ConfigPlugin {

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], Config]

}