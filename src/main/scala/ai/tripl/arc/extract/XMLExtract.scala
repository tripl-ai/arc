package ai.tripl.arc.extract

import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import com.databricks.spark.xml._

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

import com.typesafe.config._

import java.io.StringReader

import javax.xml.XMLConstants
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.Schema
import javax.xml.validation.SchemaFactory
import javax.xml.validation.Validator

class XMLExtract extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "inputView" :: "inputField" :: "outputView" :: "authentication" :: "contiguousIndex" :: "numPartitions" :: "partitionBy" :: "persist" :: "schemaURI" :: "schemaView" :: "params" :: "xsdURI" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = if(c.hasPath("inputView")) getValue[String]("inputView") else Right("")
    val parsedGlob = if (!c.hasPath("inputView")) getValue[String]("inputURI") |> parseGlob("inputURI") _ else Right("")
    val inputField = getOptionalValue[String]("inputField")
    val authentication = readAuthentication("authentication")
    val outputView = getValue[String]("outputView")
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val contiguousIndex = getValue[java.lang.Boolean]("contiguousIndex", default = Some(true))
    val extractColumns = if(c.hasPath("schemaURI")) getValue[String]("schemaURI") |> parseURI("schemaURI") _ |> getExtractColumns("schemaURI", authentication) _ else Right(List.empty)
    val schemaView = if(c.hasPath("schemaView")) getValue[String]("schemaView") else Right("")
    val xsdURI = if(c.hasPath("xsdURI")) getValue[String]("xsdURI") else Right("")
    val xsd = if(c.hasPath("xsdURI")) xsdURI |> parseURI("xsdURI") _ |> textContentForURI("xsdURI", authentication) _ |> validateXSD("xsdURI") _ else Right("")
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, description, extractColumns, schemaView, inputView, parsedGlob, inputField, outputView, persist, numPartitions, authentication, contiguousIndex, partitionBy, xsdURI, xsd, invalidKeys) match {
      case (Right(name), Right(description), Right(extractColumns), Right(schemaView), Right(inputView), Right(parsedGlob), Right(inputField), Right(outputView), Right(persist), Right(numPartitions), Right(authentication), Right(contiguousIndex), Right(partitionBy), Right(xsdURI), Right(xsd), Right(invalidKeys)) =>
        val input = if(c.hasPath("inputView")) Left(inputView) else Right(parsedGlob)
        val schema = if(c.hasPath("schemaView")) Left(schemaView) else Right(extractColumns)
        val xsdOption = if(c.hasPath("xsdURI")) Option(xsd) else None

        val stage = XMLExtractStage(
          plugin=this,
          name=name,
          description=description,
          schema=schema,
          outputView=outputView,
          input=input,
          inputField=inputField,
          authentication=authentication,
          params=params,
          persist=persist,
          numPartitions=numPartitions,
          partitionBy=partitionBy,
          contiguousIndex=contiguousIndex,
          xsd=xsdOption
        )

        authentication.foreach { authentication => stage.stageDetail.put("authentication", authentication.method) }
        input match {
          case Left(inputView) => stage.stageDetail.put("inputView", inputView)
          case Right(parsedGlob) => stage.stageDetail.put("inputURI", parsedGlob)
        }
        if (c.hasPath("xsdURI")) stage.stageDetail.put("xsdURI", xsdURI)
        inputField.foreach { stage.stageDetail.put("inputField", _) }
        stage.stageDetail.put("contiguousIndex", java.lang.Boolean.valueOf(contiguousIndex))
        stage.stageDetail.put("outputView", outputView)
        stage.stageDetail.put("params", params.asJava)
        stage.stageDetail.put("persist", java.lang.Boolean.valueOf(persist))

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, extractColumns, schemaView, inputView, parsedGlob, inputField, outputView, persist, numPartitions, authentication, contiguousIndex, partitionBy, xsdURI, xsd, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

  def validateXSD(path: String)(xsd: String)(implicit spark: SparkSession, c: Config): Either[Errors, String] = {
    def err(lineNumber: Option[Int], msg: String): Either[Errors, String] = Left(ConfigError(path, lineNumber, msg) :: Nil)

    try {
      // validator is not serialisable so return the input string if success
      val schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
      schemaFactory.newSchema(new StreamSource(new StringReader(xsd))).newValidator
      Right(xsd)
    } catch {
      case e: Exception => err(Some(c.getValue(path).origin.lineNumber()), e.getMessage)
    }
  }

}

case class XMLExtractStage(
    plugin: XMLExtract,
    name: String,
    description: Option[String],
    schema: Either[String, List[ExtractColumn]],
    outputView: String,
    input: Either[String, String],
    inputField: Option[String],
    authentication: Option[Authentication],
    params: Map[String, String],
    persist: Boolean,
    numPartitions: Option[Int],
    partitionBy: List[String],
    contiguousIndex: Boolean,
    xsd: Option[String]
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    XMLExtractStage.execute(this)
  }
}

object XMLExtractStage {

  def execute(stage: XMLExtractStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    import spark.implicits._

    // force com.sun.xml.* implementation for reading xml to be compatible with spark-xml library
    System.setProperty("javax.xml.stream.XMLInputFactory", "com.sun.xml.internal.stream.XMLInputFactoryImpl")

    // try to get the schema
    val optionSchema = try {
      ExtractUtils.getSchema(stage.schema)(spark, logger)
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    val df = try {
      stage.input match {
        case Right(glob) => {
          CloudUtils.setHadoopConfiguration(stage.authentication)

          // read the file but do not cache. caching will break the input_file_name() function
          val textDS = spark.read.option("wholetext", true).text(glob).as[String]

          // if we have an xsd validator
          for (xsd <- stage.xsd) {
            textDS.foreach { xml =>
              // these objects are not serializable so need to be instantiate on each record
              val schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
              val validator = schemaFactory.newSchema(new StreamSource(new StringReader(xsd))).newValidator
              validator.validate(new StreamSource(new StringReader(xml)))
            }
          }

          val xmlReader = new XmlReader
          optionSchema match {
            case Some(schema) => Right(xmlReader.withSchema(schema).xmlRdd(spark, textDS.rdd))
            case None => Right(xmlReader.xmlRdd(spark, textDS.rdd))
          }
        }
        case Left(view) => {
          val xmlReader = new XmlReader

          val textRdd = stage.inputField match {
            case Some(inputField) => spark.table(view).select(col(inputField)).as[String].rdd
            case None => spark.table(view).as[String].rdd
          }

          // if we have an xsd validator
          for (xsd <- stage.xsd) {
            textRdd.foreach { xml =>
              // these objects are not serializable so need to be instantiate on each record
              val schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
              val validator = schemaFactory.newSchema(new StreamSource(new StringReader(xsd))).newValidator
              validator.validate(new StreamSource(new StringReader(xml)))
            }
          }

          optionSchema match {
            case Some(schema) => Right(xmlReader.withSchema(schema).xmlRdd(spark, textRdd))
            case None => Right(xmlReader.xmlRdd(spark, textRdd))
          }
        }
      }
    } catch {
      case e: AnalysisException if (e.getMessage.contains("Path does not exist")) =>
        stage.input match {
          case Right(glob) => Left(PathNotExistsExtractError(Option(glob)))
          case Left(_) => Left(FileNotFoundExtractError(None))
        }
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
        repartitionedDF.persist(arcContext.storageLevel)
        stage.stageDetail.put("records", java.lang.Long.valueOf(repartitionedDF.count))
      }
    }

    Option(repartitionedDF)
  }

}

