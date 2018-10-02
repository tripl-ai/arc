package au.com.agl.arc.plugins

import au.com.agl.arc.util.log.logger.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

class ArcCustomPipelineStage extends PipelineStagePlugin {
  override def execute(name: String, params: Map[String, String])(implicit spark: SparkSession, logger: Logger): Option[DataFrame] = {

    None
  }
}
