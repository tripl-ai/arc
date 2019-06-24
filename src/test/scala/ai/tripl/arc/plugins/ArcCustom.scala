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

class ArcCustom extends PipelineStagePlugin {
  
  val version = "1.0.1"

  def createStage(index: Int, config: Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val name = getValue[String]("name")
    val params = readMap("params", c)

    Right(ArcCustomStage(this, name.right.get, None, params))
  }
}

case class ArcCustomStage(plugin: PipelineStagePlugin, 
                          name: String, 
                          description: Option[String],
                          params: Map[String, String]) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    ArcCustomStage.extract(this)
  }
}

object ArcCustomStage {
  def extract(extract: ArcCustomStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    None
  }
}
