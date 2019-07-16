package ai.tripl.arc.execute

import java.net.URI

import org.apache.spark.sql._

import ai.tripl.arc.api.API._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.Utils

class PipelineExecute extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "uri" :: "authentication" :: "params" :: Nil

    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val uri = getValue[String]("uri") |> parseURI("uri") _
    val authentication = readAuthentication("authentication")
    val textContent = uri |> textContentForURI("uri", authentication) _
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, description, uri, textContent, invalidKeys) match {
      case (Right(name), Right(description), Right(uri), Right(textContent), Right(invalidKeys)) =>

        // try and read the nested pipeline
        val subPipeline = ai.tripl.arc.config.ArcPipeline.parseConfig(Left(textContent), arcContext)

        subPipeline match {
          case Right((pipeline, ctx)) => {

            val stage = PipelineExecuteStage(
              plugin=this,
              name=name,
              description=description,
              uri=uri,
              pipeline=pipeline
            )

            Right(stage)
          }
          case Left(errors) => {
            val stageErrors = errors.collect { case stageError: StageError => stageError }
            Left(stageErrors)
          }
        }
      case _ =>
        val allErrors: Errors = List(name, description, uri, textContent, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class PipelineExecuteStage(
    plugin: PipelineExecute,
    name: String,
    description: Option[String],
    uri: URI,
    pipeline: ETLPipeline
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    None
  }
}
