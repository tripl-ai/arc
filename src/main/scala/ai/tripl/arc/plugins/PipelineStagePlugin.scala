package ai.tripl.arc.plugins

import org.apache.spark.sql.{DataFrame, SparkSession}

import com.typesafe.config._

import ai.tripl.arc.api.API.{ARCContext, PipelineStage}
import ai.tripl.arc.config.Error.StageError

trait PipelineStagePlugin extends Serializable {

  def version: String

  def createStage(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[StageError], PipelineStage]

}

