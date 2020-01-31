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

class DelimitedExtract extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "inputURI" :: "outputView" :: "delimiter" :: "quote" :: "header" :: "authentication" :: "contiguousIndex" :: "numPartitions" :: "params" :: "partitionBy" :: "persist" :: "schemaURI" :: "schemaView" :: "customDelimiter" :: "inputField" :: "basePath" :: "watermark" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)
    val name = getValue[String]("name")
    val params = readMap("params", c)
    val description = getOptionalValue[String]("description")
    val inputView = if(c.hasPath("inputView")) getValue[String]("inputView") else Right("")
    val parsedGlob = if(!c.hasPath("inputView")) getValue[String]("inputURI") |> parseGlob("inputURI") _ else Right("")
    val outputView = getValue[String]("outputView")
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val authentication = readAuthentication("authentication")
    val contiguousIndex = getValue[java.lang.Boolean]("contiguousIndex", default = Some(true))
    val extractColumns = if(c.hasPath("schemaURI")) getValue[String]("schemaURI") |> parseURI("schemaURI") _ |> getExtractColumns("schemaURI", authentication) _ else Right(List.empty)
    val schemaView = if(c.hasPath("schemaView")) getValue[String]("schemaView") else Right("")
    val delimiter = getValue[String]("delimiter", default = Some("Comma"), validValues = "Comma" :: "Pipe" :: "DefaultHive" :: "Custom" :: Nil) |> parseDelimiter("delimiter") _
    val quote = getValue[String]("quote", default =  Some("DoubleQuote"), validValues = "DoubleQuote" :: "SingleQuote" :: "None" :: Nil) |> parseQuote("quote") _
    val header = getValue[java.lang.Boolean]("header", Some(false))
    val customDelimiter = delimiter match {
      case Right(Delimiter.Custom) => getValue[String]("customDelimiter")
      case _ => Right("")
    }
    val inputField = getOptionalValue[String]("inputField")
    val basePath = getOptionalValue[String]("basePath")
    val watermark = readWatermark("watermark")

    (name, description, inputView, parsedGlob, extractColumns, schemaView, outputView, persist, numPartitions, partitionBy, header, authentication, contiguousIndex, delimiter, quote, invalidKeys, customDelimiter, inputField, basePath, watermark) match {
      case (Right(name), Right(description), Right(inputView), Right(parsedGlob), Right(extractColumns), Right(schemaView), Right(outputView), Right(persist), Right(numPartitions), Right(partitionBy), Right(header), Right(authentication), Right(contiguousIndex), Right(delimiter), Right(quote), Right(_), Right(customDelimiter), Right(inputField), Right(basePath), Right(watermark)) =>
        val input = if(c.hasPath("inputView")) {
          Left(inputView)
        } else {
          Right(parsedGlob)
        }
        val schema = if(c.hasPath("schemaView")) Left(schemaView) else Right(extractColumns)
        val stage = DelimitedExtractStage(
          plugin=this,
          name=name,
          description=description,
          schema=schema,
          outputView=outputView,
          input=input,
          settings=new Delimited(header=header, sep=delimiter, quote=quote, customDelimiter=customDelimiter),
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

        stage.stageDetail.put("contiguousIndex", java.lang.Boolean.valueOf(contiguousIndex))
        stage.stageDetail.put("outputView", outputView)
        stage.stageDetail.put("persist", java.lang.Boolean.valueOf(persist))
        val options: Map[String, String] = stage.basePath match {
          case Some(basePath) => Delimited.toSparkOptions(stage.settings) + ("basePath" -> basePath)
          case None => Delimited.toSparkOptions(stage.settings)
        }
        input match {
          case Left(inputView) => stage.stageDetail.put("inputView", inputView)
          case Right(parsedGlob) =>stage.stageDetail.put("inputURI", parsedGlob)
        }
        stage.stageDetail.put("options", options.asJava)
        for (inputField <- inputField) {
          stage.stageDetail.put("inputField", inputField)
        }
        stage.stageDetail.put("params", params.asJava)
        for (watermark <- watermark) {
          val watermarkMap = new java.util.HashMap[String, Object]()
          watermarkMap.put("eventTime", watermark.eventTime)
          watermarkMap.put("delayThreshold", watermark.delayThreshold)
          stage.stageDetail.put("watermark", watermarkMap)
        }

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, parsedGlob, extractColumns, outputView, persist, numPartitions, partitionBy, header, authentication, contiguousIndex, delimiter, quote, invalidKeys, customDelimiter, inputField, basePath, watermark).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

}

case class DelimitedExtractStage(
    plugin: DelimitedExtract,
    name: String,
    description: Option[String],
    schema: Either[String, List[ExtractColumn]],
    outputView: String,
    input: Either[String, String],
    settings: Delimited,
    authentication: Option[Authentication],
    params: Map[String, String],
    persist: Boolean,
    numPartitions: Option[Int],
    partitionBy: List[String],
    contiguousIndex: Boolean,
    inputField: Option[String],
    basePath: Option[String],
    watermark: Option[Watermark]
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    DelimitedExtractStage.execute(this)
  }
}

object DelimitedExtractStage {

  def execute(stage: DelimitedExtractStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    import spark.implicits._

    val options: Map[String, String] = stage.basePath match {
      case Some(basePath) => Delimited.toSparkOptions(stage.settings) + ("basePath" -> basePath)
      case None => Delimited.toSparkOptions(stage.settings)
    }

    // try to get the schema
    val optionSchema = try {
      ExtractUtils.getSchema(stage.schema)(spark, logger)
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    val df = try {
      if (arcContext.isStreaming) {
        stage.input match {
          case Right(glob) => {
            CloudUtils.setHadoopConfiguration(stage.authentication)

            optionSchema match {
              case Some(schema) => {
                stage.watermark match {
                  case Some(watermark) => Right(spark.readStream.options(options).schema(schema).csv(glob).withWatermark(watermark.eventTime, watermark.delayThreshold))
                  case None => Right(spark.readStream.options(options).schema(schema).csv(glob))
                }
              }
              case None => throw new Exception("CSVExtract requires 'schemaURI' or 'schemaView' to be set if Arc is running in streaming mode.")
            }
          }
          case Left(view) => {
            val inputView = spark.table(view)
            if (inputView.isStreaming) {
              throw new Exception("CSVExtract does not support the use of 'inputView' if Arc is running in streaming mode.")
            } else {
              stage.inputField match {
                case Some(inputField) => Right(spark.read.options(options).csv(inputView.select(col(inputField).as("value")).as[String]))
                case None => Right(spark.read.options(options).csv(inputView.as[String]))
              }
            }
          }
        }
      } else {
        stage.input match {
          case Right(glob) =>
            CloudUtils.setHadoopConfiguration(stage.authentication)

            try {
              Right(spark.read.options(options).csv(glob))
            } catch {
              // the file that is read is empty
              case e: AnalysisException if (e.getMessage == "Unable to infer schema for CSV. It must be specified manually.;") =>
                Left(FileNotFoundExtractError(Option(glob)))
              // the file does not exist at all
              case e: AnalysisException if (e.getMessage.contains("Path does not exist")) =>
                Left(PathNotExistsExtractError(Option(glob)))
              case e: Exception => throw e
            }

          case Left(view) => {
            stage.inputField match {
              case Some(inputField) => Right(spark.read.options(options).csv(spark.table(view).select(col(inputField).as("value")).as[String]))
              case None => Right(spark.read.options(options).csv(spark.table(view).as[String]))
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