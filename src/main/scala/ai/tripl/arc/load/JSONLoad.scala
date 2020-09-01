package ai.tripl.arc.load

import java.net.URI
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.CloudUtils
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.ListenerUtils
import ai.tripl.arc.util.Utils

class JSONLoad extends PipelineStagePlugin with JupyterCompleter {

  val version = Utils.getFrameworkVersion

  val snippet = """{
    |  "type": "JSONLoad",
    |  "name": "JSONLoad",
    |  "environments": [
    |    "production",
    |    "test"
    |  ],
    |  "inputView": "inputView",
    |  "outputURI": "hdfs://*.json"
    |}""".stripMargin

  val documentationURI = new java.net.URI(s"${baseURI}/load/#jsonload")

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "id" :: "name" :: "description" :: "environments" :: "inputView" :: "outputURI" :: "authentication" :: "numPartitions" :: "partitionBy" :: "saveMode" :: "params" :: Nil
    val id = getOptionalValue[String]("id")
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = getValue[String]("inputView")
    val outputURI = getValue[String]("outputURI") |> parseURI("outputURI") _
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val authentication = readAuthentication("authentication")
    val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (id, name, description, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys) match {
      case (Right(id), Right(name), Right(description), Right(inputView), Right(outputURI), Right(numPartitions), Right(authentication), Right(saveMode), Right(partitionBy), Right(invalidKeys)) =>

        val stage = JSONLoadStage(
          plugin=this,
          id=id,
          name=name,
          description=description,
          inputView=inputView,
          outputURI=outputURI,
          partitionBy=partitionBy,
          numPartitions=numPartitions,
          authentication=authentication,
          saveMode=saveMode,
          params=params
        )

        authentication.foreach { authentication => stage.stageDetail.put("authentication", authentication.method) }
        numPartitions.foreach { numPartitions => stage.stageDetail.put("numPartitions", Integer.valueOf(numPartitions)) }
        stage.stageDetail.put("inputView", inputView)
        stage.stageDetail.put("outputURI", outputURI.toString)
        stage.stageDetail.put("params", params.asJava)
        stage.stageDetail.put("partitionBy", partitionBy.asJava)
        stage.stageDetail.put("saveMode", saveMode.toString.toLowerCase)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(id, name, description, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class JSONLoadStage(
    plugin: JSONLoad,
    id: Option[String],
    name: String,
    description: Option[String],
    inputView: String,
    outputURI: URI,
    partitionBy: List[String],
    numPartitions: Option[Int],
    authentication: Option[Authentication],
    saveMode: SaveMode,
    params: Map[String, String]
  ) extends LoadPipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    JSONLoadStage.execute(this)
  }

}

object JSONLoadStage {

  def execute(stage: JSONLoadStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    val df = spark.table(stage.inputView)

    // set write permissions
    CloudUtils.setHadoopConfiguration(stage.authentication)

    // JSONLoad cannot handle a column of NullType
    val nulls = df.schema.filter( _.dataType == NullType).map(_.name)
    val nonNullDF = if (!nulls.isEmpty) {
      val dropMap = new java.util.HashMap[String, Object]()
      dropMap.put("NullType", nulls.asJava)
      if (arcContext.dropUnsupported) {
        stage.stageDetail.put("drop", dropMap)
        df.drop(nulls:_*)
      } else {
        throw new Exception(s"""inputView '${stage.inputView}' contains types ${new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(dropMap)} which are unsupported by JSONLoad and 'dropUnsupported' is set to false.""")
      }
    } else {
      df
    }

    val listener = ListenerUtils.addStageCompletedListener(stage.stageDetail)

    try {
      if (nonNullDF.isStreaming) {
        stage.partitionBy match {
          case Nil => nonNullDF.writeStream.format("json").option("path", stage.outputURI.toString).start
          case partitionBy => {
            nonNullDF.writeStream.partitionBy(partitionBy:_*).format("json").option("path", stage.outputURI.toString).start
          }
        }
      } else {
        stage.partitionBy match {
          case Nil => {
            stage.numPartitions match {
              case Some(n) => nonNullDF.repartition(n).write.mode(stage.saveMode).format("json").save(stage.outputURI.toString)
              case None => nonNullDF.write.mode(stage.saveMode).format("json").save(stage.outputURI.toString)
            }
          }
          case partitionBy => {
            // create a column array for repartitioning
            val partitionCols = partitionBy.map(col => nonNullDF(col))
            stage.numPartitions match {
              case Some(n) => nonNullDF.repartition(n, partitionCols:_*).write.partitionBy(partitionBy:_*).mode(stage.saveMode).format("json").save(stage.outputURI.toString)
              case None => nonNullDF.repartition(partitionCols:_*).write.partitionBy(partitionBy:_*).mode(stage.saveMode).format("json").save(stage.outputURI.toString)
            }
          }
        }
      }
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    spark.sparkContext.removeSparkListener(listener)

    Option(nonNullDF)
  }
}