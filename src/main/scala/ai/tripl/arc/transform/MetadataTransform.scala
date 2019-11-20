package ai.tripl.arc.transform

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

class MetadataTransform extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputView" :: "schemaView" :: "schemaURI" :: "failMode" :: "persist" :: "params" :: "numPartitions" :: "partitionBy" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = getValue[String]("inputView")
    val outputView = getValue[String]("outputView")
    val authentication = readAuthentication("authentication")
    val extractColumns = if(!c.hasPath("schemaView")) getValue[String]("schemaURI") |> parseURI("schemaURI") _ |> getExtractColumns("schemaURI", authentication) _ else Right(List.empty)
    val schemaURI = if(!c.hasPath("schemaView")) getValue[String]("schemaURI") else Right("")
    val schemaView = if(c.hasPath("schemaView")) getValue[String]("schemaView") else Right("")
    val failMode = getValue[String]("failMode", default = Some("permissive"), validValues = "permissive" :: "failfast" :: Nil) |> parseFailMode("failMode") _
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, description, inputView, outputView, extractColumns, schemaView, schemaURI, failMode, persist, invalidKeys, numPartitions, partitionBy) match {
      case (Right(name), Right(description), Right(inputView), Right(outputView), Right(extractColumns), Right(schemaView), Right(schemaURI), Right(failMode), Right(persist), Right(invalidKeys), Right(numPartitions), Right(partitionBy)) =>
        val schema = if(c.hasPath("schemaView")) Left(schemaView) else Right(extractColumns)
        val _schemaURI = if(!c.hasPath("schemaView")) None else Option(schemaURI)

        val stage = MetadataTransformStage(
          plugin=this,
          name=name,
          description=description,
          inputView=inputView,
          outputView=outputView,
          schemaURI=_schemaURI,
          schema=schema,
          failMode=failMode,
          params=params,
          persist=persist,
          numPartitions=numPartitions,
          partitionBy=partitionBy
        )

        if(c.hasPath("schemaView")) {
          stage.stageDetail.put("schemaView", schemaView)
        } else {
          stage.stageDetail.put("schemaURI", schemaURI)
        }
        stage.stageDetail.put("failMode", failMode.sparkString)
        stage.stageDetail.put("inputView", inputView)
        stage.stageDetail.put("outputView", outputView)
        stage.stageDetail.put("persist", java.lang.Boolean.valueOf(persist))
        stage.stageDetail.put("params", params.asJava)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, outputView, extractColumns, schemaView, failMode, persist, invalidKeys, numPartitions, partitionBy).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

}

case class MetadataTransformStage(
    plugin: MetadataTransform,
    name: String,
    description: Option[String],
    inputView: String,
    outputView: String,
    schemaURI: Option[String],
    schema: Either[String, List[ExtractColumn]],
    failMode: FailModeType,
    params: Map[String, String],
    persist: Boolean,
    numPartitions: Option[Int],
    partitionBy: List[String]
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    MetadataTransformStage.execute(this)
  }
}

object MetadataTransformStage {

  def execute(stage: MetadataTransformStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    val df = spark.table(stage.inputView)

    // try to get the schema
    val schema = try {
      ExtractUtils.getSchema(stage.schema).get
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    // set column metadata
    val enrichedDF = try {
      stage.failMode match {
        // failfast requires all fields in schema to match (by name) fields in inputView
        case FailModeTypeFailFast => {
          val schemaFields = schema.fields.map(_.name).toSet
          val inputFields = df.columns.toSet

          if (schemaFields.diff(inputFields).size != 0 || inputFields.diff(schemaFields).size != 0) {
            stage.schema match {
              case Left(schemaView) => {
                throw new Exception(s"""MetadataTransform with failMode = 'failfast' ensures that the schemaView '${schemaView}' has the same columns as inputView '${stage.inputView}' but schemaView '${schemaView}' has columns: ${schemaFields.map(fieldName => s"'${fieldName}'").mkString("[",", ","]")} and '${stage.inputView}' contains columns: ${inputFields.map(fieldName => s"'${fieldName}'").mkString("[",", ","]")}.""")
              }
              case Right(_) => {
                stage.schemaURI match {
                  case Some(schemaURI) => throw new Exception(s"""MetadataTransform with failMode = 'failfast' ensures that the schema supplied in schemaURI '${schemaURI}' has the same columns as inputView '${stage.inputView}' but schema supplied in '${schemaURI}' has columns: ${schemaFields.map(fieldName => s"'${fieldName}'").mkString("[",", ","]")} and '${stage.inputView}' contains columns: ${inputFields.map(fieldName => s"'${fieldName}'").mkString("[",", ","]")}.""")
                  case None => throw new Exception("Invalid state. Please raise issue.")
                }
              }
            }
          }
        }
        // permissive will not throw issue if no columns match by name
        case FailModeTypePermissive =>
      }

      MetadataUtils.setMetadata(df, schema)
    } catch {
      case e: Exception => throw new Exception(e.getMessage) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    // repartition to distribute rows evenly
    val repartitionedDF = stage.partitionBy match {
      case Nil => {
        stage.numPartitions match {
          case Some(numPartitions) => enrichedDF.repartition(numPartitions)
          case None => enrichedDF
        }
      }
      case partitionBy => {
        // create a column array for repartitioning
        val partitionCols = partitionBy.map(col => enrichedDF(col))
        stage.numPartitions match {
          case Some(numPartitions) => enrichedDF.repartition(numPartitions, partitionCols:_*)
          case None => enrichedDF.repartition(partitionCols:_*)
        }
      }
    }
    if (arcContext.immutableViews) repartitionedDF.createTempView(stage.outputView) else repartitionedDF.createOrReplaceTempView(stage.outputView)

    if (!repartitionedDF.isStreaming) {
      stage.stageDetail.put("outputColumns", java.lang.Integer.valueOf(repartitionedDF.schema.length))
      stage.stageDetail.put("numPartitions", java.lang.Integer.valueOf(repartitionedDF.rdd.partitions.length))

      if (stage.persist) {
        repartitionedDF.persist(arcContext.storageLevel)
        stage.stageDetail.put("records", java.lang.Long.valueOf(repartitionedDF.count))
      }
    }

    Option(repartitionedDF)
  }

}
