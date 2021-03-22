package ai.tripl.arc.extract

import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

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
import ai.tripl.arc.util.Utils

class JSONExtract extends PipelineStagePlugin with JupyterCompleter {

  val version = Utils.getFrameworkVersion

  def snippet()(implicit arcContext: ARCContext): String = {
    s"""{
    |  "type": "JSONExtract",
    |  "name": "JSONExtract",
    |  "environments": [${arcContext.completionEnvironments.map { env => s""""${env}""""}.mkString(", ")}],
    |  "inputURI": "hdfs://*.json",
    |  "outputView": "outputView"
    |}""".stripMargin
  }

  val documentationURI = new java.net.URI(s"${baseURI}/extract/#jsonextract")

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "id" :: "name" :: "description" :: "environments" :: "inputView" :: "inputURI" :: "outputView" :: "authentication" :: "contiguousIndex" :: "multiLine" :: "numPartitions" :: "partitionBy" :: "persist" :: "schemaURI" :: "schemaView" :: "params" :: "inputField" :: "basePath" :: "watermark" :: Nil
    val id = getOptionalValue[String]("id")
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = if(c.hasPath("inputView")) getValue[String]("inputView") else Right("")
    val parsedGlob = if (!c.hasPath("inputView")) getValue[String]("inputURI") |> parseGlob("inputURI") _ else Right("")
    val outputView = getValue[String]("outputView")
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val multiLine = getValue[java.lang.Boolean]("multiLine", default = Some(true))
    val authentication = readAuthentication("authentication")
    val contiguousIndex = getValue[java.lang.Boolean]("contiguousIndex", default = Some(true))
    val extractColumns = if(c.hasPath("schemaURI")) getValue[String]("schemaURI") |> parseURI("schemaURI") _ |> textContentForURI("schemaURI", authentication) |> getExtractColumns("schemaURI") _ else Right(List.empty)
    val schemaView = if(c.hasPath("schemaView")) getValue[String]("schemaView") else Right("")
    val inputField = getOptionalValue[String]("inputField")
    val basePath = getOptionalValue[String]("basePath")
    val watermark = readWatermark("watermark")
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (id, name, description, extractColumns, schemaView, inputView, parsedGlob, outputView, persist, numPartitions, multiLine, authentication, contiguousIndex, partitionBy, inputField, basePath, invalidKeys, watermark) match {
      case (Right(id), Right(name), Right(description), Right(extractColumns), Right(schemaView), Right(inputView), Right(parsedGlob), Right(outputView), Right(persist), Right(numPartitions), Right(multiLine), Right(authentication), Right(contiguousIndex), Right(partitionBy), Right(inputField), Right(basePath), Right(invalidKeys), Right(watermark)) =>
        val input = if(c.hasPath("inputView")) Left(inputView) else Right(parsedGlob)
        val schema = if(c.hasPath("schemaView")) Left(schemaView) else Right(extractColumns)

        val stage = JSONExtractStage(
          plugin=this,
          id=id,
          name=name,
          description=description,
          schema=schema,
          outputView=outputView,
          input=input,
          settings=new JSON(multiLine=multiLine),
          authentication=authentication,
          params=params,
          persist=persist,
          numPartitions=numPartitions,
          partitionBy=partitionBy,
          contiguousIndex=contiguousIndex,
          basePath=basePath,
          inputField=inputField,
          watermark=watermark
        )

        authentication.foreach { authentication => stage.stageDetail.put("authentication", authentication.method) }
        input match {
          case Left(inputView) => stage.stageDetail.put("inputView", inputView)
          case Right(parsedGlob) => stage.stageDetail.put("inputURI", parsedGlob)
        }
        basePath.foreach { stage.stageDetail.put("basePath", _) }
        inputField.foreach { stage.stageDetail.put("inputField", _) }
        stage.stageDetail.put("contiguousIndex", java.lang.Boolean.valueOf(contiguousIndex))
        stage.stageDetail.put("multiLine", java.lang.Boolean.valueOf(multiLine))
        stage.stageDetail.put("options", JSON.toSparkOptions(stage.settings).asJava)
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
        val allErrors: Errors = List(id, name, description, extractColumns, schemaView, inputView, parsedGlob, outputView, persist, numPartitions, multiLine, authentication, contiguousIndex, partitionBy, invalidKeys, inputField, basePath, watermark).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

}

case class JSONExtractStage(
    plugin: JSONExtract,
    id: Option[String],
    name: String,
    description: Option[String],
    schema: Either[String, List[ExtractColumn]],
    outputView: String,
    input: Either[String, String],
    settings: JSON,
    authentication: Option[Authentication],
    params: Map[String, String],
    persist: Boolean,
    numPartitions: Option[Int],
    partitionBy: List[String],
    contiguousIndex: Boolean,
    inputField: Option[String],
    basePath: Option[String],
    watermark: Option[Watermark]
  ) extends ExtractPipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    JSONExtractStage.execute(this)
  }

}

object JSONExtractStage {

  def execute(stage: JSONExtractStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    import spark.implicits._

    CloudUtils.setHadoopConfiguration(stage.authentication)

    // try to get the schema
    val optionSchema = try {
      ExtractUtils.getSchema(stage.schema)(spark, logger)
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    val options = stage.basePath match {
      case Some(basePath) => JSON.toSparkOptions(stage.settings) + ("basePath" -> basePath)
      case None => JSON.toSparkOptions(stage.settings)
    }

    val df = try {
      if (arcContext.isStreaming) {
        stage.input match {
          case Right(glob) => {
            optionSchema match {
              case Some(schema) => {
                stage.watermark match {
                  case Some(watermark) => Right(spark.readStream.options(options).schema(schema).format("json").load(glob).withWatermark(watermark.eventTime, watermark.delayThreshold))
                  case None => Right(spark.readStream.options(options).schema(schema).format("json").load(glob))
                }
              }
              case None => throw new Exception("JSONExtract requires 'schemaURI' or 'schemaView' to be set if Arc is running in streaming mode.")
            }
          }
          case Left(view) => {
            val inputView = spark.table(view)
            if (inputView.isStreaming) {
              throw new Exception("JSONExtract does not support the use of 'inputView' if Arc is running in streaming mode.")
            } else {
              stage.inputField match {
                case Some(inputField) => Right(spark.read.options(options).json(inputView.select(col(inputField).as("value")).as[String]))
                case None => Right(spark.read.options(options).json(inputView.as[String]))
              }
            }
          }
        }
      } else {
        stage.input match {
          case Right(glob) =>
            try {
              optionSchema match {
                case Some(schema) => Right(spark.read.options(options).schema(schema).format("json").load(glob))
                case None => Right(spark.read.options(options).format("json").load(glob))
              }
            } catch {
              case e: AnalysisException if (e.getMessage.contains("Unable to infer schema for JSON")) =>
                Left(FileNotFoundExtractError(Option(glob)))
              case e: AnalysisException if (e.getMessage.contains("Path does not exist")) =>
                Left(PathNotExistsExtractError(Option(glob)))
              case e: Exception => throw e
            }
          case Left(view) => {
            val inputView = spark.table(view)
            stage.inputField match {
              case Some(inputField) => Right(spark.read.options(options).json(inputView.select(col(inputField).as("value")).as[String]))
              case None => Right(spark.read.options(options).json(inputView.as[String]))
            }
          }
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
              case None =>
                stage.input match {
                  case Right(glob) => throw new Exception(EmptySchemaExtractError(Some(glob)).getMessage)
                  case Left(_) => throw new Exception(EmptySchemaExtractError(None).getMessage)
                }
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
      stage.stageDetail.put("inputFiles", java.lang.Integer.valueOf(repartitionedDF.inputFiles.length))
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

