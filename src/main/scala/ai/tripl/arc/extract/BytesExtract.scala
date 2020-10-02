package ai.tripl.arc.extract

import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.hadoop.mapreduce.lib.input.InvalidInputException

import ai.tripl.arc.api.API._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.datasource.BinaryContent
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.CloudUtils
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.Utils

class BytesExtract extends PipelineStagePlugin with JupyterCompleter {

  val version = Utils.getFrameworkVersion

  val snippet = """{
    |  "type": "BytesExtract",
    |  "name": "BytesExtract",
    |  "environments": [
    |    "production",
    |    "test"
    |  ],
    |  "inputURI": "hdfs://*.jpg",
    |  "outputView": "outputView"
    |}""".stripMargin

  val documentationURI = new java.net.URI(s"${baseURI}/extract/#bytesextract")

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "id" :: "name" :: "description" :: "environments" :: "inputView" :: "inputURI" :: "outputView" :: "authentication" :: "contiguousIndex" :: "numPartitions" :: "persist" :: "params" :: "failMode" :: Nil
    val id = getOptionalValue[String]("id")
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = if(c.hasPath("inputView")) getValue[String]("inputView") else Right("")
    val parsedGlob = if(!c.hasPath("inputView")) getValue[String]("inputURI") |> parseGlob("inputURI") _ else Right("")
    val outputView = getValue[String]("outputView")
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val authentication = readAuthentication("authentication")
    val contiguousIndex = getValue[java.lang.Boolean]("contiguousIndex", default = Some(true))
    val failMode = getValue[String]("failMode", default = Some("failfast"), validValues = "permissive" :: "failfast" :: Nil) |> parseFailMode("failMode") _
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (id, name, description, parsedGlob, inputView, outputView, persist, numPartitions, authentication, contiguousIndex, failMode, invalidKeys) match {
      case (Right(id), Right(name), Right(description), Right(parsedGlob), Right(inputView), Right(outputView), Right(persist), Right(numPartitions), Right(authentication), Right(contiguousIndex), Right(failMode), Right(invalidKeys)) =>
        val input = if(c.hasPath("inputView")) {
          Left(inputView)
        } else {
          Right(parsedGlob)
        }

        val stage = BytesExtractStage(
          plugin=this,
          id=id,
          name=name,
          description=description,
          outputView=outputView,
          input=input,
          authentication=authentication,
          persist=persist,
          numPartitions=numPartitions,
          contiguousIndex=contiguousIndex,
          params=params,
          failMode=failMode
        )

        authentication.foreach { authentication => stage.stageDetail.put("authentication", authentication.method) }
        input match {
          case Left(inputView) => stage.stageDetail.put("inputView", inputView)
          case Right(parsedGlob) => stage.stageDetail.put("inputURI", parsedGlob)
        }
        stage.stageDetail.put("contiguousIndex", java.lang.Boolean.valueOf(contiguousIndex))
        stage.stageDetail.put("failMode", stage.failMode.sparkString)
        stage.stageDetail.put("outputView", outputView)
        stage.stageDetail.put("params", params.asJava)
        stage.stageDetail.put("persist", java.lang.Boolean.valueOf(stage.persist))

        Right(stage)
      case _ =>
        val allErrors: Errors = List(id, name, description, parsedGlob, inputView, outputView, persist, numPartitions, authentication, contiguousIndex, failMode, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class BytesExtractStage(
    plugin: BytesExtract,
    id: Option[String],
    name: String,
    description: Option[String],
    outputView: String,
    input: Either[String, String],
    authentication: Option[Authentication],
    params: Map[String, String],
    persist: Boolean,
    numPartitions: Option[Int],
    contiguousIndex: Boolean,
    failMode: FailMode
  ) extends ExtractPipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    BytesExtractStage.execute(this)
  }

}

object BytesExtractStage {

  def execute(stage: BytesExtractStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    val signature = "BytesExtract requires 'inputView' to be dataset with [value: string] signature."

    CloudUtils.setHadoopConfiguration(stage.authentication)

    val df = try {
      stage.input match {
        case Left(view) => {
          val inputView = spark.table(view)
          val schema = inputView.schema

          val fieldIndex = try {
            schema.fieldIndex("value")
          } catch {
            case e: Exception => throw new Exception(s"""${signature} inputView has: [${inputView.schema.map(_.name).mkString(", ")}].""") with DetailException {
              override val detail = stage.stageDetail
            }
          }

          schema.fields(fieldIndex).dataType match {
            case _: StringType =>
            case _ => throw new Exception(s"""${signature} 'value' is of type: '${schema.fields(fieldIndex).dataType.simpleString}'.""") with DetailException {
              override val detail = stage.stageDetail
            }
          }

          val path = inputView.select(col("value")).collect().map( _.getString(0) ).mkString(",")
          spark.read.format("bytes").load(path)
        }
        case Right(glob) => {
          val bytesDF = spark.read.format("bytes").load(glob)
          // force evaluation so errors can be caught
          bytesDF.take(1)
          bytesDF
        }
      }
    } catch {
      case e: InvalidInputException =>
        if (stage.failMode == FailMode.FailFast) {
          throw new Exception("BytesExtract has found no files and failMode is set to 'failfast' so cannot continue.") with DetailException {
            override val detail = stage.stageDetail
          }
        }
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], BinaryContent.schema)
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    // datasource already has a _filename column so no need to add internal columns

    // repartition to distribute rows evenly
    val repartitionedDF = stage.numPartitions match {
      case Some(numPartitions) => df.repartition(numPartitions)
      case None => df
    }

    if (!arcContext.isStreaming) repartitionedDF.rdd.setName(stage.outputView)
    if (arcContext.immutableViews) repartitionedDF.createTempView(stage.outputView) else repartitionedDF.createOrReplaceTempView(stage.outputView)

    stage.stageDetail.put("inputFiles", java.lang.Integer.valueOf(repartitionedDF.inputFiles.length))
    stage.stageDetail.put("outputColumns", java.lang.Integer.valueOf(repartitionedDF.schema.length))
    stage.stageDetail.put("numPartitions", java.lang.Integer.valueOf(repartitionedDF.rdd.partitions.length))

    if (stage.persist && !repartitionedDF.isStreaming) {
      repartitionedDF.persist(arcContext.storageLevel)
      stage.stageDetail.put("records", java.lang.Long.valueOf(repartitionedDF.count))
    }

    Option(repartitionedDF)
  }

}
