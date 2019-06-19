package ai.tripl.arc.plugins

import org.apache.spark.sql.{DataFrame, SparkSession}

import com.typesafe.config._

import ai.tripl.arc.api.API.PipelineStage
import ai.tripl.arc.util.ConfigUtils._

trait PipelineStagePlugin {

  def validateConfig(config: Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): Either[List[StageError], PipelineStage]

}

