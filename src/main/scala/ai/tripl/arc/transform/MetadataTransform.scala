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

class MetadataTransform extends PipelineStagePlugin with JupyterCompleter {

  val version = Utils.getFrameworkVersion

  val snippet = """{
    |  "type": "MetadataTransform",
    |  "name": "MetadataTransform",
    |  "environments": [
    |    "production",
    |    "test"
    |  ],
    |  "inputView": "inputView",
    |  "outputView": "outputView",
    |  "schemaURI": "hdfs://*.json"
    |}""".stripMargin

  val documentationURI = new java.net.URI(s"${baseURI}/transform/#metadatatransform")

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputView" :: "schema" :: "schemaView" :: "schemaURI" :: "failMode" :: "persist" :: "params" :: "numPartitions" :: "partitionBy" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = getValue[String]("inputView")
    val outputView = getValue[String]("outputView")
    val authentication = readAuthentication("authentication")

    val source = checkOneOf(c)(Seq("schema", "schemaURI", "schemaView"))
    val (schema, schemaURI, schemaView) = if (source.isRight) {
      (
        if (c.hasPath("schema")) Right(c.getConfigList("schema").asScala.map { o => o.root().render(ConfigRenderOptions.concise()) }.mkString("[", ",", "]") ) |> verifyInlineSchemaPolicy("schema") _ |> getExtractColumns("schema") _ else Right(List.empty),
        if (c.hasPath("schemaURI")) getValue[String]("schemaURI") |> parseURI("schemaURI") _ |> textContentForURI("schemaURI", authentication) |> getExtractColumns("schemaURI") _ else Right(List.empty),
        if (c.hasPath("schemaView")) getValue[String]("schemaView") else Right("")
      )
    } else {
      (Right(List.empty), Right(List.empty), Right(""))
    }

    val failMode = getValue[String]("failMode", default = Some("permissive"), validValues = "permissive" :: "failfast" :: Nil) |> parseFailMode("failMode") _
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, description, inputView, outputView, source, schema, schemaURI, schemaView, failMode, persist, invalidKeys, numPartitions, partitionBy, authentication) match {
      case (Right(name), Right(description), Right(inputView), Right(outputView), Right(source), Right(schema), Right(schemaURI), Right(schemaView), Right(failMode), Right(persist), Right(invalidKeys), Right(numPartitions), Right(partitionBy), Right(authentication)) =>
        val _schema = if (c.hasPath("schemaView")) {
          Left(schemaView)
        } else if (c.hasPath("schemaURI")) {
          Right(schemaURI)
        } else {
          Right(schema)
        }

        val stage = MetadataTransformStage(
          plugin=this,
          name=name,
          description=description,
          inputView=inputView,
          outputView=outputView,
          schema=_schema,
          failMode=failMode,
          params=params,
          persist=persist,
          numPartitions=numPartitions,
          partitionBy=partitionBy
        )

        if (c.hasPath("schemaView")) {
          stage.stageDetail.put("schemaView", c.getString("schemaView"))
        } else if (c.hasPath("schemaURI")) {
          stage.stageDetail.put("schemaURI", c.getString("schemaURI"))
        }
        authentication.foreach { authentication => stage.stageDetail.put("authentication", authentication.method) }
        numPartitions.foreach { numPartitions => stage.stageDetail.put("numPartitions", Integer.valueOf(numPartitions)) }
        stage.stageDetail.put("failMode", failMode.sparkString)
        stage.stageDetail.put("inputView", inputView)
        stage.stageDetail.put("outputView", outputView)
        stage.stageDetail.put("params", params.asJava)
        stage.stageDetail.put("partitionBy", partitionBy.asJava)
        stage.stageDetail.put("persist", java.lang.Boolean.valueOf(persist))

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, outputView, source, schema, schemaURI, schemaView, failMode, persist, invalidKeys, numPartitions, partitionBy, authentication).collect{ case Left(errs) => errs }.flatten
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
    schema: Either[String, List[ExtractColumn]],
    failMode: FailMode,
    params: Map[String, String],
    persist: Boolean,
    numPartitions: Option[Int],
    partitionBy: List[String]
  ) extends TransformPipelineStage {

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
        case FailMode.FailFast => {
          val schemaFields = schema.fields.map(_.name).toSet
          val inputFields = df.columns.toSet

          if (schemaFields.diff(inputFields).size != 0 || inputFields.diff(schemaFields).size != 0) {
            throw new Exception(s"""MetadataTransform with failMode = 'failfast' ensures that the supplied schema has the same columns as inputView '${stage.inputView}' but the supplied schema has columns: ${schemaFields.map(fieldName => s"'${fieldName}'").mkString("[",", ","]")} and '${stage.inputView}' contains columns: ${inputFields.map(fieldName => s"'${fieldName}'").mkString("[",", ","]")}.""")
          }
        }
        // permissive will not throw issue if no columns match by name
        case FailMode.Permissive =>
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
