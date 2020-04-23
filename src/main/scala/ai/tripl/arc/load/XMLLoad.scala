package ai.tripl.arc.load

import java.io.CharArrayWriter
import java.net.URI
import javax.xml.stream.XMLOutputFactory
import javax.xml.stream.XMLStreamWriter

import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
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
import ai.tripl.arc.util.SerializableConfiguration

import com.databricks.spark.xml.util._
import com.sun.xml.txw2.output.IndentingXMLStreamWriter

class XMLLoad extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputURI" :: "authentication" :: "numPartitions" :: "partitionBy" :: "saveMode" :: "singleFile" :: "prefix" :: "params" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = getValue[String]("inputView")
    val outputURI = getOptionalValue[String]("outputURI") |> parseOptionURI("outputURI") _
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val authentication = readAuthentication("authentication")
    val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _
    val singleFile = getValue[java.lang.Boolean]("singleFile", default = Some(false))
    val prefix = getValue[String]("prefix", default = Some(""))
    val singleFileNumPartitions = getValue[Int]("singleFileNumPartitions", default = Some(4096))
    val validOutputURI = (outputURI, singleFile) match {
      case (Right(None), Right(singleFile)) if (singleFile == false) => Left(ConfigError("outputURI", None, "Missing required attribute 'outputURI' when not in 'singleFile' mode.") :: Nil)
      case _ => Right("")
    }
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, description, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, singleFile, prefix, singleFileNumPartitions, validOutputURI, invalidKeys) match {
      case (Right(name), Right(description), Right(inputView), Right(outputURI), Right(numPartitions), Right(authentication), Right(saveMode), Right(partitionBy), Right(singleFile), Right(prefix), Right(singleFileNumPartitions), Right(validOutputURI), Right(invalidKeys)) =>

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
          prefix=prefix,
          singleFileNumPartitions=singleFileNumPartitions,
          params=params
        )

        authentication.foreach { authentication => stage.stageDetail.put("authentication", authentication.method) }
        numPartitions.foreach { numPartitions => stage.stageDetail.put("numPartitions", Integer.valueOf(numPartitions)) }
        outputURI.foreach { outputURI => stage.stageDetail.put("outputURI", outputURI.toString) }
        stage.stageDetail.put("inputView", inputView)
        stage.stageDetail.put("params", params.asJava)
        stage.stageDetail.put("partitionBy", partitionBy.asJava)
        stage.stageDetail.put("prefix", prefix)
        stage.stageDetail.put("saveMode", saveMode.toString.toLowerCase)
        stage.stageDetail.put("singleFile", java.lang.Boolean.valueOf(singleFile))

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, outputURI, numPartitions, authentication, saveMode, partitionBy, singleFile, prefix, validOutputURI, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

object XMLLoad {

  // This class is borrowed from the Spark-XML library
  object StaxXmlGenerator {
    val DEFAULT_ATTRIBUTE_PREFIX = "_"
    val DEFAULT_VALUE_TAG = "_VALUE"
    val DEFAULT_NULL_VALUE: String = null

    /** Transforms a single Row to XML
      *
      * @param schema the schema object used for conversion
      * @param writer a XML writer object
      * @param row The row to convert
      */
    def apply(schema: StructType, writer: XMLStreamWriter)(row: Row): Unit = {
      def writeChildElement(name: String, dt: DataType, v: Any): Unit = (name, dt, v) match {
        // If this is meant to be value but in no child, write only a value
        case (_, _, null) | (_, NullType, _) if DEFAULT_NULL_VALUE == null =>
          // Because usually elements having `null` do not exist, just do not write
          // elements when given values are `null`.
        case (_, _, _) if name == DEFAULT_VALUE_TAG =>
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
            if name.startsWith(DEFAULT_ATTRIBUTE_PREFIX) && name != DEFAULT_VALUE_TAG =>
            Option(DEFAULT_NULL_VALUE).foreach {
              writer.writeAttribute(name.substring(DEFAULT_ATTRIBUTE_PREFIX.length), _)
            }
          case _ if name.startsWith(DEFAULT_ATTRIBUTE_PREFIX) && name != DEFAULT_VALUE_TAG =>
            writer.writeAttribute(name.substring(DEFAULT_ATTRIBUTE_PREFIX.length), v.toString)

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
        case (_, null) | (NullType, _) => writer.writeCharacters(DEFAULT_NULL_VALUE)
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
            f.toString.startsWith(DEFAULT_ATTRIBUTE_PREFIX) && f.toString != DEFAULT_VALUE_TAG
          }
          // We need to write attributes first before the value.
          (attributes ++ elements).foreach {
            case (k, v) =>
              writeChild(k.toString, vt, v)
          }

        case (StructType(ty), r: Row) =>
          val (attributes, elements) = ty.zip(r.toSeq).partition { case (f, _) =>
            f.name.startsWith(DEFAULT_ATTRIBUTE_PREFIX) && f.name != DEFAULT_VALUE_TAG
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
        f.name.startsWith(DEFAULT_ATTRIBUTE_PREFIX) && f.name != DEFAULT_VALUE_TAG
      }
      // Writing attributes
      attributes.foreach {
        case (f, v) if v == null || f.dataType == NullType =>
          Option(DEFAULT_NULL_VALUE).foreach {
            writer.writeAttribute(f.name.substring(DEFAULT_ATTRIBUTE_PREFIX.length), _)
          }
        case (f, v) =>
          writer.writeAttribute(f.name.substring(DEFAULT_ATTRIBUTE_PREFIX.length), v.toString)
      }
      // Writing elements
      val (names, values) = elements.unzip
      val elementSchema = StructType(schema.filter(names.contains))
      val elementRow = Row.fromSeq(row.toSeq.filter(values.contains))
      writeElement(elementSchema, elementRow)
    }
  }

}

case class XMLLoadStage(
    plugin: XMLLoad,
    name: String,
    description: Option[String],
    inputView: String,
    outputURI: Option[URI],
    partitionBy: List[String],
    numPartitions: Option[Int],
    authentication: Option[Authentication],
    saveMode: SaveMode,
    singleFile: Boolean,
    prefix: String,
    singleFileNumPartitions: Int,
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

    val signature = "XMLLoad requires input [value: struct] or [value: struct, filename: string] signature when in singleFile mode."
    val stageOutputURI = stage.outputURI
    val stagePrefix = stage.prefix
    val stageSaveMode = stage.saveMode

    val df = spark.table(stage.inputView)

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
        if (!(
            (df.schema.length == 1 && df.schema.fields(0).dataType.typeName == "struct") ||
            (df.schema.length == 2 && df.schema.fields.map { field => field.dataType.typeName }.toSet == Set("string", "struct")) && df.schema.fieldNames.contains("filename")
          )) {
          throw new Exception(s"""${signature} Got [${df.schema.map(f => s"""${f.name}: ${f.dataType.typeName}""").mkString(", ")}].""")
        }

        val hasFilename = df.schema.length == 2

        if (!hasFilename && stageOutputURI.isEmpty) {
          throw new Exception("XMLLoad requires 'outputURI' to be set if in 'singleFile' mode and no 'filename' column exists.")
        }

        // repartition so that there is a 1:1 mapping of partition:filename
        val repartitionedDF = if (hasFilename) {
          df.repartition(stage.singleFileNumPartitions, col("filename"))
        } else {
          df.repartition(1)
        }

        val outputFileAccumulator = spark.sparkContext.collectionAccumulator[String]

        repartitionedDF.foreachPartition { partition: Iterator[Row] =>
          if (partition.hasNext) {
            val hadoopConf = arcContext.serializableConfiguration.value

            // buffer so first row can be accessed
            val bufferedPartition = partition.buffered

            val firstRow = bufferedPartition.head
            val valueIndex = if (hasFilename) {
              firstRow.schema.fields.zipWithIndex.collect { case (field, index) if (field.name != "filename") => index }.head
            } else {
              0
            }
            val valueSchema = StructType(Seq(firstRow.schema.fields(valueIndex)))

            val path = if (hasFilename) {
              new Path(new URI(firstRow.getString(firstRow.fieldIndex("filename"))))
            } else {
              new Path(stageOutputURI.get)
            }

            val fs = path.getFileSystem(hadoopConf)

            // create the outputStream for that file
            val outputStream = if (fs.exists(path)) {
              stageSaveMode match {
                case SaveMode.ErrorIfExists => {
                  throw new Exception(s"File '${path.toString}' already exists and 'saveMode' equals 'ErrorIfExists' so cannot continue.")
                }
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

            val factory = XMLOutputFactory.newInstance

            // write bytes of the partition to the outputStream
            outputStream match {
              case Some(os) => {
                os.writeBytes(stagePrefix)

                bufferedPartition
                  .map { row =>
                    // remove the filename field
                    if (hasFilename) {
                      Row.fromSeq(Seq(row.getStruct(valueIndex)))
                    } else {
                      row
                    }
                  }
                  .map { row =>
                    val writer = new CharArrayWriter
                    val xmlWriter = factory.createXMLStreamWriter(writer)
                    val indentingXmlWriter = new IndentingXMLStreamWriter(xmlWriter)
                    ai.tripl.arc.load.XMLLoad.StaxXmlGenerator(valueSchema, indentingXmlWriter)(row)
                    indentingXmlWriter.flush
                    writer.toString.trim
                  }
                  .foreach { row =>
                    os.writeBytes(row)
                  }

                os.close
                outputFileAccumulator.add(path.toString)
              }
              case None =>
            }
            fs.close
          }
        }

        stage.stageDetail.put("outputFiles", outputFileAccumulator.value.asScala.toSet.toSeq.asJava)

      } else {
        stage.partitionBy match {
          case Nil => {
            stage.numPartitions match {
              case Some(n) => df.repartition(n).write.format("com.databricks.spark.xml").mode(stage.saveMode).save(stage.outputURI.get.toString)
              case None => df.write.format("com.databricks.spark.xml").mode(stage.saveMode).save(stage.outputURI.get.toString)
            }
          }
          case partitionBy => {
            // create a column array for repartitioning
            val partitionCols = partitionBy.map(col => df(col))
            stage.numPartitions match {
              case Some(n) => df.repartition(n, partitionCols:_*).write.format("com.databricks.spark.xml").partitionBy(partitionBy:_*).mode(stage.saveMode).save(stage.outputURI.get.toString)
              case None => df.repartition(partitionCols:_*).write.format("com.databricks.spark.xml").partitionBy(partitionBy:_*).mode(stage.saveMode).save(stage.outputURI.get.toString)
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

  val validValueFilename = Array(("struct"), ("string"))
}


