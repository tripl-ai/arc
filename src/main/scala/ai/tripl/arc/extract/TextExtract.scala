package ai.tripl.arc.extract

import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import ai.tripl.arc.api.API._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.CloudUtils
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.ExtractUtils
import ai.tripl.arc.util.MetadataUtils
import ai.tripl.arc.util.Utils

class TextExtract extends PipelineStagePlugin with JupyterCompleter {

  val version = Utils.getFrameworkVersion

  val snippet = """{
    |  "type": "TextExtract",
    |  "name": "TextExtract",
    |  "environments": [
    |    "production",
    |    "test"
    |  ],
    |  "inputURI": "hdfs://*.txt",
    |  "outputView": "outputView"
    |}""".stripMargin

  val documentationURI = new java.net.URI(s"${baseURI}/extract/#textextract")

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "id" :: "name" :: "description" :: "environments" :: "inputURI" :: "inputView" :: "outputView" :: "authentication" :: "contiguousIndex" :: "multiLine" :: "numPartitions" :: "persist" :: "schemaURI" :: "schemaView" :: "params" :: "basePath" :: "watermark" :: Nil
    val id = getOptionalValue[String]("id")
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = if(c.hasPath("inputView")) getValue[String]("inputView") else Right("")
    val parsedGlob = if(!c.hasPath("inputView")) getValue[String]("inputURI") |> parseGlob("inputURI") _ else Right("")
    val outputView = getValue[String]("outputView")
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val multiLine = getValue[java.lang.Boolean]("multiLine", default = Some(false))
    val authentication = readAuthentication("authentication")
    val contiguousIndex = getValue[java.lang.Boolean]("contiguousIndex", default = Some(true))
    val extractColumns = if(c.hasPath("schemaURI")) getValue[String]("schemaURI") |> parseURI("schemaURI") _ |> textContentForURI("schemaURI", authentication) |> getExtractColumns("schemaURI") _ else Right(List.empty)
    val basePath = getOptionalValue[String]("basePath")
    val watermark = readWatermark("watermark")
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (id, name, description, extractColumns, parsedGlob, inputView, outputView, persist, numPartitions, multiLine, authentication, contiguousIndex, basePath, invalidKeys, watermark) match {
      case (Right(id), Right(name), Right(description), Right(extractColumns), Right(parsedGlob), Right(inputView), Right(outputView), Right(persist), Right(numPartitions), Right(multiLine), Right(authentication), Right(contiguousIndex), Right(basePath), Right(invalidKeys), Right(watermark)) =>
        val input = if(c.hasPath("inputView")) {
          Left(inputView)
        } else {
          Right(parsedGlob)
        }

      val stage = TextExtractStage(
          plugin=this,
          id=id,
          name=name,
          description=description,
          schema=Right(extractColumns),
          outputView=outputView,
          input=input,
          authentication=authentication,
          persist=persist,
          numPartitions=numPartitions,
          contiguousIndex=contiguousIndex,
          multiLine=multiLine,
          basePath=basePath,
          params=params,
          watermark=watermark
        )

        authentication.foreach { authentication => stage.stageDetail.put("authentication", authentication.method) }
        input match {
          case Left(inputView) => stage.stageDetail.put("inputView", inputView)
          case Right(parsedGlob) => stage.stageDetail.put("inputURI", parsedGlob)
        }
        basePath.foreach { stage.stageDetail.put("basePath", _) }
        stage.stageDetail.put("contiguousIndex", java.lang.Boolean.valueOf(contiguousIndex))
        stage.stageDetail.put("multiLine", java.lang.Boolean.valueOf(multiLine))
        stage.stageDetail.put("outputView", outputView)
        stage.stageDetail.put("params", params.asJava)
        stage.stageDetail.put("persist", java.lang.Boolean.valueOf(persist))
        watermark.foreach { watermark =>
          val watermarkMap = new java.util.HashMap[String, Object]()
          watermarkMap.put("eventTime", watermark.eventTime)
          watermarkMap.put("delayThreshold", watermark.delayThreshold)
          stage.stageDetail.put("watermark", watermarkMap)
        }

        Right(stage)
      case _ =>
        val allErrors: Errors = List(id, name, description, extractColumns, parsedGlob, inputView, outputView, persist, numPartitions, multiLine, authentication, contiguousIndex, basePath, invalidKeys, watermark).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

}

case class TextExtractStage(
    plugin: TextExtract,
    id: Option[String],
    name: String,
    description: Option[String],
    schema: Either[String, List[ExtractColumn]],
    outputView: String,
    input: Either[String, String],
    authentication: Option[Authentication],
    params: Map[String, String],
    persist: Boolean,
    numPartitions: Option[Int],
    contiguousIndex: Boolean,
    multiLine: Boolean,
    basePath: Option[String],
    watermark: Option[Watermark]
  ) extends ExtractPipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    TextExtractStage.execute(this)
  }

}

object TextExtractStage {

  def execute(stage: TextExtractStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    val signature = "TextExtract requires 'inputView' to be dataset with [value: string] signature."

    // try to get the schema
    val optionSchema = try {
      ExtractUtils.getSchema(stage.schema)(spark, logger)
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    CloudUtils.setHadoopConfiguration(stage.authentication)

    val df = try {

      val path = stage.input match {
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

          inputView.select(col("value")).collect().map( _.getString(0) ).mkString(",")
        }
        case Right(glob) => glob
      }

      if (arcContext.isStreaming) {
        optionSchema match {
          case Some(schema) => {
            stage.watermark match {
              case Some(watermark) => Right(spark.readStream.option("mergeSchema", "true").schema(schema).text(path).withWatermark(watermark.eventTime, watermark.delayThreshold))
              case None => Right(spark.readStream.option("mergeSchema", "true").schema(schema).text(path))
            }
          }
          case None => throw new Exception("TextExtract requires 'schemaURI' to be set if Arc is running in streaming mode.")
        }
      } else {
        try {
          if (stage.multiLine) {
            stage.basePath match {
              case Some(basePath) => Right(spark.read.option("mergeSchema", "true").option("basePath", basePath).textFile(path).toDF)
              case None => Right(spark.read.option("wholetext", "true").textFile(path).toDF)
            }
          } else {
            stage.basePath match {
              case Some(basePath) => Right(spark.read.option("mergeSchema", "false").option("basePath", basePath).textFile(path).toDF)
              case None => Right(spark.read.option("wholetext", "false").textFile(path).toDF)
            }
          }
        } catch {
          case e: AnalysisException if (e.getMessage.contains("Path does not exist")) =>
            Left(PathNotExistsExtractError(Option(path)))
          case e: Exception => throw e
        }
      }
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    // if incoming dataset has 0 columns then try to create empty dataset with correct schema
    // or throw enriched error message
    val emptyDataframeHandlerDF = try {
      df match {
        case Right(df) =>
          if (df.schema.length == 0) {
            optionSchema match {
              case Some(structType) => spark.createDataFrame(spark.sparkContext.emptyRDD[Row], structType)
              case None => throw new Exception(EmptySchemaExtractError(None).getMessage)
            }
          } else {
            df
          }
        case Left(error) => {
          stage.stageDetail.put("records", java.lang.Integer.valueOf(0))
          optionSchema match {
            case Some(s) => spark.createDataFrame(spark.sparkContext.emptyRDD[Row], s)
            case None => throw new Exception(error.getMessage)
          }
        }
      }
    } catch {
      case e: Exception => throw new Exception(e.getMessage) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    // add internal columns data _filename, _index
    val sourceEnrichedDF = ExtractUtils.addInternalColumns(emptyDataframeHandlerDF, stage.contiguousIndex)

    // set column metadata if exists
    val enrichedDF = optionSchema match {
        case Some(schema) => MetadataUtils.setMetadata(sourceEnrichedDF, schema)
        case None => sourceEnrichedDF
    }

    // repartition to distribute rows evenly
    val repartitionedDF = stage.numPartitions match {
      case Some(numPartitions) => enrichedDF.repartition(numPartitions)
      case None => enrichedDF
    }

    if (!arcContext.isStreaming) repartitionedDF.rdd.setName(stage.outputView)
    if (arcContext.immutableViews) repartitionedDF.createTempView(stage.outputView) else repartitionedDF.createOrReplaceTempView(stage.outputView)

    if (!repartitionedDF.isStreaming) {
      stage.stageDetail.put("inputFiles", Integer.valueOf(repartitionedDF.inputFiles.length))
      stage.stageDetail.put("outputColumns", Integer.valueOf(repartitionedDF.schema.length))
      stage.stageDetail.put("numPartitions", Integer.valueOf(repartitionedDF.rdd.partitions.length))

      if (stage.persist) {
        repartitionedDF.persist(arcContext.storageLevel)
        stage.stageDetail.put("records", java.lang.Long.valueOf(repartitionedDF.count))
      }
    }

    Option(repartitionedDF)
  }

}

