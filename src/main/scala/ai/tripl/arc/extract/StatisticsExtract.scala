package ai.tripl.arc.extract

import java.net.URI
import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind._

import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.StatisticsUtils
import ai.tripl.arc.util.SQLUtils
import ai.tripl.arc.util.Utils

class StatisticsExtract extends PipelineStagePlugin with JupyterCompleter {

  val version = Utils.getFrameworkVersion

  def snippet()(implicit arcContext: ARCContext): String = {
    s"""{
    |  "type": "StatisticsExtract",
    |  "name": "StatisticsExtract",
    |  "environments": [${arcContext.completionEnvironments.map { env => s""""${env}""""}.mkString(", ")}],
    |  "inputView": "inputView",
    |  "outputView": "outputView"
    |}""".stripMargin
  }

  val documentationURI = new java.net.URI(s"${baseURI}/extract/#statisticsextract")

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "id" :: "name" :: "description" :: "environments" :: "inputView" :: "outputView" :: "persist" :: "approximate" :: "histogram" :: "params" :: Nil
    val id = getOptionalValue[String]("id")
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = getValue[String]("inputView")
    val outputView = getValue[String]("outputView")
    val approximate = getValue[java.lang.Boolean]("approximate", default = Some(true))
    val histogram = getValue[java.lang.Boolean]("histogram", default = Some(false))
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (id, name, description, inputView, outputView, persist, approximate, histogram, invalidKeys) match {
      case (Right(id), Right(name), Right(description), Right(inputView), Right(outputView), Right(persist), Right(approximate), Right(histogram), Right(invalidKeys)) =>

        val stage = StatisticsExtractStage(
          plugin=this,
          id=id,
          name=name,
          description=description,
          inputView=inputView,
          outputView=outputView,
          params=params,
          persist=persist,
          approximate=approximate,
          histogram=histogram,
        )

        stage.stageDetail.put("inputView", inputView)
        stage.stageDetail.put("outputView", outputView)
        stage.stageDetail.put("params", params.asJava)
        stage.stageDetail.put("persist", java.lang.Boolean.valueOf(persist))
        stage.stageDetail.put("approximate", java.lang.Boolean.valueOf(approximate))
        stage.stageDetail.put("histogram", java.lang.Boolean.valueOf(histogram))

        Right(stage)
      case _ =>
        val allErrors: Errors = List(id, name, description, inputView, outputView, persist, approximate, histogram, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

}

case class StatisticsExtractStage(
    plugin: StatisticsExtract,
    id: Option[String],
    name: String,
    description: Option[String],
    inputView: String,
    outputView: String,
    params: Map[String, String],
    persist: Boolean,
    approximate: Boolean,
    histogram: Boolean,
  ) extends ExtractPipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    StatisticsExtractStage.execute(this)
  }

}

object StatisticsExtractStage {

  def execute(stage: StatisticsExtractStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    val df = spark.table(stage.inputView)

    val statisticsDF = StatisticsUtils.createStatisticsDataframe(df, stage.approximate, stage.histogram)

    if (arcContext.immutableViews) statisticsDF.createTempView(stage.outputView) else statisticsDF.createOrReplaceTempView(stage.outputView)

    if (!statisticsDF.isStreaming) {
      if (stage.persist) {
        spark.catalog.cacheTable(stage.outputView, arcContext.storageLevel)

        // add the statistics to the logs
        val objectMapper = new ObjectMapper()
        val statisticsMap = objectMapper.readValue(StatisticsUtils.createStatisticsJSON(statisticsDF), classOf[java.util.HashMap[String, Object]])
        stage.stageDetail.put("statistics", statisticsMap)
      }
    }

    Option(statisticsDF)
  }

}
