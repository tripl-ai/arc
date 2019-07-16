package ai.tripl.arc.plugins

import org.apache.spark.sql.SparkSession

import ai.tripl.arc.api.API.{ARCContext, VersionedPlugin}

trait UDFPlugin extends VersionedPlugin {

  def register()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext)

}

