package au.com.agl.arc.plugins

import org.apache.spark.sql.{DataFrame, SparkSession}

trait PipelineStagePlugin {
  def execute(name: String, params: Map[String, String])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Option[DataFrame]
}

