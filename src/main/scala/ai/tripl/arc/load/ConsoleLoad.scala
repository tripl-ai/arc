package ai.tripl.arc.load

import scala.collection.JavaConverters._

import org.apache.spark.sql._

import ai.tripl.arc.api.API._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.Utils

class ConsoleLoad extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputMode" :: "params" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = getValue[String]("inputView")
    val outputMode = getValue[String]("outputMode", default = Some("Append"), validValues = "Append" :: "Complete" :: "Update" :: Nil) |> parseOutputModeType("outputMode") _
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, description, inputView, outputMode, invalidKeys) match {
      case (Right(name), Right(description), Right(inputView), Right(outputMode), Right(invalidKeys)) =>
        val stage = ConsoleLoadStage(
          plugin=this,
          name=name,
          description=description,
          inputView=inputView,
          outputMode=outputMode,
          params=params
        )

        stage.stageDetail.put("inputView", stage.inputView)
        stage.stageDetail.put("outputMode", stage.outputMode.sparkString)
        stage.stageDetail.put("params", params.asJava)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, outputMode, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

}

case class ConsoleLoadStage(
    plugin: ConsoleLoad,
    name: String,
    description: Option[String],
    inputView: String,
    outputMode: OutputModeType,
    params: Map[String, String]
  ) extends LoadPipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    ConsoleLoadStage.execute(this)
  }
}


object ConsoleLoadStage {

  def execute(stage: ConsoleLoadStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    val df = spark.table(stage.inputView)

    if (!df.isStreaming) {
      throw new Exception("ConsoleLoad can only be executed in streaming mode.") with DetailException {
        override val detail = stage.stageDetail
      }
    }

    df.writeStream
        .format("console")
        .outputMode(stage.outputMode.sparkString)
        .start

    Option(df)
  }
}