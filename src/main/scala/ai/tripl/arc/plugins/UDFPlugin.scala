package ai.tripl.arc.plugins

import org.apache.spark.sql.SparkSession

import ai.tripl.arc.api.API.{ARCContext, VersionedPlugin}

trait UDFPlugin extends VersionedPlugin {
  
  case class Deprecation(
    function: String,
    replaceFunction: String
  )

  def register()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext)

  def deprecations()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Seq[Deprecation] = Nil

}

