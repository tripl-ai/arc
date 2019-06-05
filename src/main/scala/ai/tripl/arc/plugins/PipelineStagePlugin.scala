package ai.tripl.arc.plugins

import org.apache.spark.sql.{DataFrame, SparkSession}

trait PipelineStagePlugin {
  def execute(name: String, params: Map[String, String])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): Option[DataFrame]
}

