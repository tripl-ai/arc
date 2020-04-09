package ai.tripl.arc.load

import java.io.CharArrayWriter
import java.net.URI
import java.nio.charset.StandardCharsets
import javax.xml.stream.XMLOutputFactory
import javax.xml.stream.XMLStreamWriter

import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.CloudUtils
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.ListenerUtils
import ai.tripl.arc.util.Utils

import com.databricks.spark.xml.util._
import com.sun.xml.txw2.output.IndentingXMLStreamWriter

class XMLLoad extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputURI" :: "authentication" :: "numPartitions" :: "partitionBy" :: "saveMode" :: "params" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = getValue[String]("inputView")
    val outputURI = getValue[String]("outputURI") |> parseURI("outputURI") _
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val authentication = readAuthentication("authentication")
    val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _
    val singleFile = getValue[java.lang.Boolean]("singleFile", default = Some(false))
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, description, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, singleFile, invalidKeys) match {
      case (Right(name), Right(description), Right(inputView), Right(outputURI), Right(numPartitions), Right(authentication), Right(saveMode), Right(partitionBy), Right(singleFile), Right(invalidKeys)) =>

        val stage = XMLLoadStage(
          plugin=this,
          name=name,
          description=description,
          inputView=inputView,
          outputURI=outputURI,
          partitionBy=partitionBy,
          numPartitions=numPartitions,
          authentication=authentication,
          saveMode=saveMode,
          singleFile=singleFile,
          params=params
        )

        stage.stageDetail.put("inputView", inputView)
        stage.stageDetail.put("outputURI", outputURI.toString)
        stage.stageDetail.put("partitionBy", partitionBy.asJava)
        stage.stageDetail.put("saveMode", saveMode.toString.toLowerCase)
        stage.stageDetail.put("params", params.asJava)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, singleFile, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class XMLLoadStage(
    plugin: XMLLoad,
    name: String,
    description: Option[String],
    inputView: String,
    outputURI: URI,
    partitionBy: List[String],
    numPartitions: Option[Int],
    authentication: Option[Authentication],
    saveMode: SaveMode,
    singleFile: Boolean,
    params: Map[String, String]
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    XMLLoadStage.execute(this)
  }
}

object XMLLoadStage {

  def execute(stage: XMLLoadStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    // force com.sun.xml.* implementation for writing xml to be compatible with spark-xml library
    System.setProperty("javax.xml.stream.XMLOutputFactory", "com.sun.xml.internal.stream.XMLOutputFactoryImpl")

    val signature = "TextLoad requires input [value: string] or [value: string, filename: string] signature when in singleFile mode."

    val df = spark.table(stage.inputView)

    stage.numPartitions match {
      case Some(partitions) => stage.stageDetail.put("numPartitions", java.lang.Integer.valueOf(partitions))
      case None => stage.stageDetail.put("numPartitions", java.lang.Integer.valueOf(df.rdd.getNumPartitions))
    }

    // set write permissions
    CloudUtils.setHadoopConfiguration(stage.authentication)

    val dropMap = new java.util.HashMap[String, Object]()

    // XML does not need to deal with NullType as it is silenty dropped on write but we want logging to be explicit
    val nulls = df.schema.filter( _.dataType == NullType).map(_.name)
    if (!nulls.isEmpty) {
      dropMap.put("NullType", nulls.asJava)
    }

    stage.stageDetail.put("drop", dropMap)

    val listener = ListenerUtils.addStageCompletedListener(stage.stageDetail)

    try {
      if (stage.singleFile) {
        if (df.schema.length == 0 || df.schema.length > 2 || (df.schema.length == 1 && df.schema.fields(0).dataType != StringType) || (df.schema.length == 2 && df.schema.forall { f => !Seq("filename","value").contains(f.name) || f.dataType != StructType } )) {
          throw new Exception(s"""${signature} Got [${df.schema.map(f => s"""${f.name}: ${f.dataType.simpleString}""").mkString(", ")}].""")
        }        

        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        val hasFilename = df.schema.length == 2
        val rows = df.collect

        // first test for any invalid rules 
        rows.foreach { row => 
          val path = if (hasFilename) {
            new Path(new URI(s"""${stage.outputURI}/${row.getString(row.fieldIndex("filename"))}"""))
          } else {
            new Path(stage.outputURI)
          }
          if (fs.exists(path)) {
            stage.saveMode match {
              case SaveMode.ErrorIfExists => {
                throw new Exception(s"File '${path.toString}' already exists and 'saveMode' equals 'ErrorIfExists' so cannot continue.")
              }
              case _ => 
            }
          }
        }

        // write the records sequentially
        rows.foreach { row => 
          val path = if (hasFilename) {
            new Path(new URI(s"""${stage.outputURI}/${row.getString(row.fieldIndex("filename"))}"""))
          } else {
            new Path(stage.outputURI)
          }          

          // create the outputStream for that file
          val outputStream = if (fs.exists(path)) {
            stage.saveMode match {
              case SaveMode.Overwrite => {
                Option(fs.create(path, true))
          
              }
              case SaveMode.Append => {
                Option(fs.append(path))
              }
              case _ => None
            }
          } else {
            Option(fs.create(path))
          }

          // if has filename drop the filename element so it is not written
          val simpleRow = if (hasFilename) {
            val rowSeq = row.copy.toSeq
            Row.fromSeq(rowSeq.zipWithIndex.collect { case (elem, index) if index != row.fieldIndex("filename") => elem })
          } else {
            row
          }

          // write bytes to the outputStream
          outputStream match {
            case Some(os) => {
              val factory = XMLOutputFactory.newInstance
              val writer = new CharArrayWriter
              val xmlWriter = factory.createXMLStreamWriter(writer)
              val indentingXmlWriter = new IndentingXMLStreamWriter(xmlWriter)
              val xml = {
                StaxXmlGenerator(
                  df.schema,
                  indentingXmlWriter,
                  new XmlOptions(Map[String, String]())
                )(simpleRow)

                indentingXmlWriter.flush
                writer.toString
              }
              writer.reset

              os.writeBytes(xml.trim)
              os.close
            }
            case None =>
          }          
        }       

        fs.close
      } else {
        stage.partitionBy match {
          case Nil => {
            stage.numPartitions match {
              case Some(n) => df.repartition(n).write.format("com.databricks.spark.xml").mode(stage.saveMode).save(stage.outputURI.toString)
              case None => df.write.format("com.databricks.spark.xml").mode(stage.saveMode).save(stage.outputURI.toString)
            }
          }
          case partitionBy => {
            // create a column array for repartitioning
            val partitionCols = partitionBy.map(col => df(col))
            stage.numPartitions match {
              case Some(n) => df.repartition(n, partitionCols:_*).write.format("com.databricks.spark.xml").partitionBy(partitionBy:_*).mode(stage.saveMode).save(stage.outputURI.toString)
              case None => df.repartition(partitionCols:_*).write.format("com.databricks.spark.xml").partitionBy(partitionBy:_*).mode(stage.saveMode).save(stage.outputURI.toString)
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

    Option(df)
  }
}




/**
 * Options for the XML data source.
 */
class XmlOptions(parameters: Map[String, String]) extends Serializable {
  def this() = this(Map.empty)

  val charset = parameters.getOrElse("charset", XmlOptions.DEFAULT_CHARSET)
  val codec = parameters.get("compression").orElse(parameters.get("codec")).orNull
  val rowTag = parameters.getOrElse("rowTag", XmlOptions.DEFAULT_ROW_TAG)
  require(rowTag.nonEmpty, "'rowTag' option should not be empty string.")
  val rootTag = parameters.getOrElse("rootTag", XmlOptions.DEFAULT_ROOT_TAG)
  val samplingRatio = parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0)
  require(samplingRatio > 0, s"samplingRatio ($samplingRatio) should be greater than 0")
  val excludeAttributeFlag = parameters.get("excludeAttribute").map(_.toBoolean).getOrElse(false)
  val treatEmptyValuesAsNulls = parameters.get("treatEmptyValuesAsNulls").map(_.toBoolean).getOrElse(false)
  val attributePrefix = parameters.getOrElse("attributePrefix", XmlOptions.DEFAULT_ATTRIBUTE_PREFIX)
  require(attributePrefix.nonEmpty, "'attributePrefix' option should not be empty string.")
  val valueTag = parameters.getOrElse("valueTag", XmlOptions.DEFAULT_VALUE_TAG)
  require(valueTag.nonEmpty, "'valueTag' option should not be empty string.")
  require(valueTag != attributePrefix, "'valueTag' and 'attributePrefix' options should not be the same.")
  val nullValue = parameters.getOrElse("nullValue", XmlOptions.DEFAULT_NULL_VALUE)
  val columnNameOfCorruptRecord = parameters.getOrElse("columnNameOfCorruptRecord", "_corrupt_record")
  val ignoreSurroundingSpaces = parameters.get("ignoreSurroundingSpaces").map(_.toBoolean).getOrElse(false)
  val parseMode = ParseMode.fromString(parameters.getOrElse("mode", PermissiveMode.name))
  val inferSchema = parameters.get("inferSchema").map(_.toBoolean).getOrElse(true)
  val rowValidationXSDPath = parameters.get("rowValidationXSDPath").orNull
}

object XmlOptions {
  val DEFAULT_ATTRIBUTE_PREFIX = "_"
  val DEFAULT_VALUE_TAG = "_VALUE"
  val DEFAULT_ROW_TAG = "ROW"
  val DEFAULT_ROOT_TAG = "ROWS"
  val DEFAULT_CHARSET: String = StandardCharsets.UTF_8.name
  val DEFAULT_NULL_VALUE: String = null

  def apply(parameters: Map[String, String]): XmlOptions = new XmlOptions(parameters)
}

// This class is borrowed from Spark json datasource.
object StaxXmlGenerator {

  /** Transforms a single Row to XML
    *
    * @param schema the schema object used for conversion
    * @param writer a XML writer object
    * @param options options for XML datasource.
    * @param row The row to convert
    */
  def apply(
      schema: StructType,
      writer: XMLStreamWriter,
      options: XmlOptions)(row: Row): Unit = {
    def writeChildElement(name: String, dt: DataType, v: Any): Unit = (name, dt, v) match {
      // If this is meant to be value but in no child, write only a value
      case (_, _, null) | (_, NullType, _) if options.nullValue == null =>
        // Because usually elements having `null` do not exist, just do not write
        // elements when given values are `null`.
      case (_, _, _) if name == options.valueTag =>
        // If this is meant to be value but in no child, write only a value
        writeElement(dt, v)
      case (_, _, _) =>
        writer.writeStartElement(name)
        writeElement(dt, v)
        writer.writeEndElement()
    }

    def writeChild(name: String, dt: DataType, v: Any): Unit = {
      (dt, v) match {
        // If this is meant to be attribute, write an attribute
        case (_, null) | (NullType, _)
          if name.startsWith(options.attributePrefix) && name != options.valueTag =>
          Option(options.nullValue).foreach {
            writer.writeAttribute(name.substring(options.attributePrefix.length), _)
          }
        case _ if name.startsWith(options.attributePrefix) && name != options.valueTag =>
          writer.writeAttribute(name.substring(options.attributePrefix.length), v.toString)

        // For ArrayType, we just need to write each as XML element.
        case (ArrayType(ty, _), v: Seq[_]) =>
          v.foreach { e =>
            writeChildElement(name, ty, e)
          }
        // For other datatypes, we just write normal elements.
        case _ =>
          writeChildElement(name, dt, v)
      }
    }

    def writeElement(dt: DataType, v: Any): Unit = (dt, v) match {
      case (_, null) | (NullType, _) => writer.writeCharacters(options.nullValue)
      case (StringType, v: String) => writer.writeCharacters(v.toString)
      case (TimestampType, v: java.sql.Timestamp) => writer.writeCharacters(v.toString)
      case (IntegerType, v: Int) => writer.writeCharacters(v.toString)
      case (ShortType, v: Short) => writer.writeCharacters(v.toString)
      case (FloatType, v: Float) => writer.writeCharacters(v.toString)
      case (DoubleType, v: Double) => writer.writeCharacters(v.toString)
      case (LongType, v: Long) => writer.writeCharacters(v.toString)
      case (DecimalType(), v: java.math.BigDecimal) => writer.writeCharacters(v.toString)
      case (ByteType, v: Byte) => writer.writeCharacters(v.toString)
      case (BooleanType, v: Boolean) => writer.writeCharacters(v.toString)
      case (DateType, _) => writer.writeCharacters(v.toString)

      // For the case roundtrip in reading and writing XML files, [[ArrayType]] cannot have
      // [[ArrayType]] as element type. It always wraps the element with [[StructType]]. So,
      // this case only can happen when we convert a normal [[DataFrame]] to XML file.
      // When [[ArrayType]] has [[ArrayType]] as elements, it is confusing what is element name
      // for XML file. Now, it is "item" but this might have to be according the parent field name.
      case (ArrayType(ty, _), v: Seq[_]) =>
        v.foreach { e =>
          writeChild("item", ty, e)
        }

      case (MapType(_, vt, _), mv: Map[_, _]) =>
        val (attributes, elements) = mv.toSeq.partition { case (f, _) =>
          f.toString.startsWith(options.attributePrefix) && f.toString != options.valueTag
        }
        // We need to write attributes first before the value.
        (attributes ++ elements).foreach {
          case (k, v) =>
            writeChild(k.toString, vt, v)
        }

      case (StructType(ty), r: Row) =>
        val (attributes, elements) = ty.zip(r.toSeq).partition { case (f, _) =>
          f.name.startsWith(options.attributePrefix) && f.name != options.valueTag
        }
        // We need to write attributes first before the value.
        (attributes ++ elements).foreach {
          case (field, value) =>
            writeChild(field.name, field.dataType, value)
        }

      case (_, _) =>
        throw new IllegalArgumentException(
          s"Failed to convert value $v (class of ${v.getClass}) in type $dt to XML.")
    }

    val (attributes, elements) = schema.zip(row.toSeq).partition { case (f, _) =>
      f.name.startsWith(options.attributePrefix) && f.name != options.valueTag
    }
    // Writing attributes
    attributes.foreach {
      case (f, v) if v == null || f.dataType == NullType =>
        Option(options.nullValue).foreach {
          writer.writeAttribute(f.name.substring(options.attributePrefix.length), _)
        }
      case (f, v) =>
        writer.writeAttribute(f.name.substring(options.attributePrefix.length), v.toString)
    }
    // Writing elements
    val (names, values) = elements.unzip
    val elementSchema = StructType(schema.filter(names.contains))
    val elementRow = Row.fromSeq(row.toSeq.filter(values.contains))
    writeElement(elementSchema, elementRow)
  }
}
