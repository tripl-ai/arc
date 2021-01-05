package ai.tripl.arc.extract

import java.net.URI
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

import com.typesafe.config._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.CloudUtils
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.ExtractUtils
import ai.tripl.arc.util.MetadataUtils
import ai.tripl.arc.util.ListenerUtils
import ai.tripl.arc.util.SQLUtils
import ai.tripl.arc.util.Utils

class MetadataExtract extends PipelineStagePlugin with JupyterCompleter {

  val version = Utils.getFrameworkVersion

  def snippet()(implicit arcContext: ARCContext): String = {
    s"""{
    |  "type": "MetadataExtract",
    |  "name": "MetadataExtract",
    |  "environments": [${arcContext.completionEnvironments.map { env => s""""${env}""""}.mkString(", ")}],
    |  "inputView": "inputView",
    |  "outputView": "outputView"
    |}""".stripMargin
  }

  val documentationURI = new java.net.URI(s"${baseURI}/extract/#metadataextract")

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "id" :: "name" :: "description" :: "environments" :: "inputView" :: "outputView" :: "persist" :: "params" :: "numPartitions" :: "partitionBy" :: Nil
    val id = getOptionalValue[String]("id")
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = getValue[String]("inputView")
    val outputView = getValue[String]("outputView")
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (id, name, description, inputView, outputView, persist, invalidKeys, numPartitions, partitionBy) match {
      case (Right(id), Right(name), Right(description), Right(inputView), Right(outputView), Right(persist), Right(invalidKeys), Right(numPartitions), Right(partitionBy)) =>

        val stage = MetadataExtractStage(
          plugin=this,
          id=id,
          name=name,
          description=description,
          inputView=inputView,
          outputView=outputView,
          params=params,
          persist=persist,
          numPartitions=numPartitions,
          partitionBy=partitionBy
        )

        stage.stageDetail.put("inputView", inputView)
        stage.stageDetail.put("outputView", outputView)
        stage.stageDetail.put("params", params.asJava)
        stage.stageDetail.put("persist", java.lang.Boolean.valueOf(persist))

        Right(stage)
      case _ =>
        val allErrors: Errors = List(id, name, description, inputView, outputView, persist, invalidKeys, numPartitions, partitionBy).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

}

case class MetadataExtractStage(
    plugin: MetadataExtract,
    id: Option[String],
    name: String,
    description: Option[String],
    inputView: String,
    outputView: String,
    params: Map[String, String],
    persist: Boolean,
    numPartitions: Option[Int],
    partitionBy: List[String]
  ) extends ExtractPipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    MetadataExtractStage.execute(this)
  }

}


object MetadataExtractStage {

  def execute(stage: MetadataExtractStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    val df = spark.table(stage.inputView)
    val metadataSchemaDF = MetadataUtils.createMetadataDataframe(df)

    // repartition to distribute rows evenly
    val repartitionedDF = stage.partitionBy match {
      case Nil => {
        stage.numPartitions match {
          case Some(numPartitions) => metadataSchemaDF.repartition(numPartitions)
          case None => metadataSchemaDF
        }
      }
      case partitionBy => {
        // create a column array for repartitioning
        val partitionCols = partitionBy.map(col => metadataSchemaDF(col))
        stage.numPartitions match {
          case Some(numPartitions) => metadataSchemaDF.repartition(numPartitions, partitionCols:_*)
          case None => metadataSchemaDF.repartition(partitionCols:_*)
        }
      }
    }

    if (arcContext.immutableViews) repartitionedDF.createTempView(stage.outputView) else repartitionedDF.createOrReplaceTempView(stage.outputView)

    if (!repartitionedDF.isStreaming) {
      stage.stageDetail.put("outputColumns", java.lang.Integer.valueOf(repartitionedDF.schema.length))
      stage.stageDetail.put("numPartitions", java.lang.Integer.valueOf(repartitionedDF.rdd.partitions.length))

      if (stage.persist) {
        spark.catalog.cacheTable(stage.outputView, arcContext.storageLevel)
        stage.stageDetail.put("records", java.lang.Long.valueOf(repartitionedDF.count))
      }
    }

    Option(repartitionedDF)
  }

}
