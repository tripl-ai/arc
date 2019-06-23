package ai.tripl.arc.plugins

import org.apache.spark.sql.{DataFrame, SparkSession}

import com.typesafe.config._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.config.ConfigUtils._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.util.CloudUtils
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.ExtractUtils
import ai.tripl.arc.util.MetadataUtils
import ai.tripl.arc.util.Utils

class ArcCustomStagePlugin extends PipelineStagePlugin {
  
  val simpleName = "ArcCustomStage"

  val version = Utils.getFrameworkVersion

  def validateConfig(index: Int, config: Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): Either[List[StageError], PipelineStage] = {
    Right(ArcCustomStage("ArcCustomStage", None))
  }

}

case class ArcCustomStage(name: String, 
                          description: Option[String]) extends PipelineStage {

  val getType = "ArcCustomStage"

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    ArcCustomStage.extract(this)
  }
}

object ArcCustomStage {
  def extract(extract: ArcCustomStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    None
  }
}
