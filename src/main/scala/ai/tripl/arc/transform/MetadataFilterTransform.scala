package ai.tripl.arc.transform

import java.net.URI
import scala.collection.JavaConverters._

import org.apache.spark.sql._

import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.MetadataUtils
import ai.tripl.arc.util.SQLUtils
import ai.tripl.arc.util.Utils

class MetadataFilterTransform extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "inputView" :: "outputView" :: "authentication" :: "persist" :: "sqlParams" :: "params" :: "numPartitions" :: "partitionBy" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val parsedURI = getValue[String]("inputURI") |> parseURI("inputURI") _
    val authentication = readAuthentication("authentication")
    val inputSQL = parsedURI |> textContentForURI("inputURI", authentication) _
    val inputView = getValue[String]("inputView")
    val outputView = getValue[String]("outputView")
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val sqlParams = readMap("sqlParams", c)
    val validSQL = inputSQL |> injectSQLParams("inputURI", sqlParams, false) _ |> validateSQL("inputURI") _
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, description, parsedURI, inputSQL, validSQL, inputView, outputView, persist, invalidKeys, numPartitions, partitionBy) match {
      case (Right(name), Right(description), Right(parsedURI), Right(inputSQL), Right(validSQL), Right(inputView), Right(outputView), Right(persist), Right(invalidKeys), Right(numPartitions), Right(partitionBy)) =>

        val stage = MetadataFilterTransformStage(
          plugin=this,
          name=name,
          description=description,
          inputView=inputView,
          inputURI=parsedURI,
          sql=inputSQL,
          outputView=outputView,
          params=params,
          sqlParams=sqlParams,
          persist=persist,
          numPartitions=numPartitions,
          partitionBy=partitionBy
        )

        stage.stageDetail.put("inputURI", parsedURI.toString)
        stage.stageDetail.put("inputView", inputView)
        stage.stageDetail.put("outputView", outputView)
        stage.stageDetail.put("persist", java.lang.Boolean.valueOf(persist))
        stage.stageDetail.put("sql", inputSQL)
        stage.stageDetail.put("sqlParams", sqlParams.asJava)
        stage.stageDetail.put("params", params.asJava)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, parsedURI, inputSQL, validSQL, inputView, outputView, persist, invalidKeys, numPartitions, partitionBy).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

}

case class MetadataFilterTransformStage(
    plugin: MetadataFilterTransform,
    name: String,
    description: Option[String],
    inputView: String,
    inputURI: URI,
    sql: String,
    outputView: String,
    params: Map[String, String],
    sqlParams: Map[String, String],
    persist: Boolean,
    numPartitions: Option[Int],
    partitionBy: List[String]
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    MetadataFilterTransformStage.execute(this)
  }
}


object MetadataFilterTransformStage {

  def execute(stage: MetadataFilterTransformStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    // inject sql parameters
    val stmt = SQLUtils.injectParameters(stage.sql, stage.sqlParams, false)
    stage.stageDetail.put("sql", stmt)

    val df = spark.table(stage.inputView)
    val metadataSchemaDF = MetadataUtils.createMetadataDataframe(df)
    metadataSchemaDF.createOrReplaceTempView("metadata")

    val filterDF = try {
      spark.sql(stmt)
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    if (!filterDF.columns.contains("name")) {
      throw new Exception("result does not contain field 'name' so cannot be filtered") with DetailException {
        override val detail = stage.stageDetail
      }
    }

    // get fields that meet condition from query result
    val inputFields = df.columns.toSet
    val includeColumns = filterDF.collect.map(field => { field.getString(field.fieldIndex("name")) }).toSet
    val excldueColumns = inputFields.diff(includeColumns)

    stage.stageDetail.put("includedColumns", includeColumns.asJava)
    stage.stageDetail.put("excludedColumns", excldueColumns.asJava)

    // drop fields in the excluded set
    val transformedDF = df.drop(excldueColumns.toList:_*)

    // repartition to distribute rows evenly
    val repartitionedDF = stage.partitionBy match {
      case Nil => {
        stage.numPartitions match {
          case Some(numPartitions) => transformedDF.repartition(numPartitions)
          case None => transformedDF
        }
      }
      case partitionBy => {
        // create a column array for repartitioning
        val partitionCols = partitionBy.map(col => transformedDF(col))
        stage.numPartitions match {
          case Some(numPartitions) => transformedDF.repartition(numPartitions, partitionCols:_*)
          case None => transformedDF.repartition(partitionCols:_*)
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

    spark.catalog.dropTempView("metadata")

    Option(repartitionedDF)
  }

}
