package ai.tripl.arc.plugins.pipeline

import java.net.URI
import scala.collection.JavaConverters._

import com.typesafe.config.Config

import com.fasterxml.jackson.databind._

import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

import ai.tripl.arc.api.API._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.config.Plugins
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.SQLUtils
import ai.tripl.arc.util.Utils

class LazyEvaluator extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val id = getOptionalValue[String]("id")
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")

    (id, name, description) match {
      case (Right(id), Right(name), Right(description)) =>

        val stage = LazyEvaluatorStage(
          plugin=this,
          id=id,
          name=name,
          description=description,
          config=config
        )

        Right(stage)
      case _ =>
        val allErrors: Errors = List(id, name, description).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class LazyEvaluatorStage(
    plugin: LazyEvaluator,
    id: Option[String],
    name: String,
    description: Option[String],
    config: Config,
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    LazyEvaluatorStage.execute(this)
  }
}

object LazyEvaluatorStage {

  def execute(stage: LazyEvaluatorStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    try {
      val config = stage.config.resolveWith(arcContext.resolutionConfig).resolve()
      val pluginType = config.getString("type")
      val resolvedOrError = Plugins.resolvePlugin(false, 0, pluginType, config, arcContext.pipelineStagePlugins)
      resolvedOrError match {
        case Left(errors) => throw new Exception(ai.tripl.arc.config.Error.pipelineErrorMsg(errors))
        case Right(s) => {
          val df = s.execute
          stage.stageDetail.put("plugin", s.stageDetail.asJava)
          df
        }
      }
    } catch {
      case e: Exception => throw new Exception(e.getMessage()) with DetailException {
        override val detail = stage.stageDetail
      }
    }

  }

}


