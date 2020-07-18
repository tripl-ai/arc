package ai.tripl.arc.load

import java.net.URI
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import ai.tripl.arc.api.API._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.CloudUtils
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.Utils
import ai.tripl.arc.util.SerializableConfiguration

class TextLoad extends PipelineStagePlugin with JupyterCompleter {

  val version = Utils.getFrameworkVersion

  val snippet = """{
    |  "type": "TextLoad",
    |  "name": "TextLoad",
    |  "environments": [
    |    "production",
    |    "test"
    |  ],
    |  "inputView": "inputView",
    |  "outputURI": "hdfs://*.txt"
    |}""".stripMargin

  val documentationURI = new java.net.URI(s"${baseURI}/load/#textload")

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputURI" :: "authentication" :: "numPartitions" :: "partitionBy" :: "saveMode" :: "params" :: "singleFile" :: "prefix" :: "separator" :: "suffix" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = getValue[String]("inputView")
    val outputURI = getOptionalValue[String]("outputURI") |> parseOptionURI("outputURI") _
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val authentication = readAuthentication("authentication")
    val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _
    val singleFile = getValue[java.lang.Boolean]("singleFile", default = Some(false))
    val prefix = getValue[String]("prefix", default = Some(""))
    val separator = getValue[String]("separator", default = Some(""))
    val suffix = getValue[String]("suffix", default = Some(""))
    val singleFileNumPartitions = getValue[Int]("singleFileNumPartitions", default = Some(4096))
    val validOutputURI = (outputURI, singleFile) match {
      case (Right(None), Right(singleFile)) if (singleFile == false) => Left(ConfigError("outputURI", None, "Missing required attribute 'outputURI' when not in 'singleFile' mode.") :: Nil)
      case _ => Right("")
    }
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, description, inputView, outputURI, numPartitions, authentication, saveMode, singleFile, prefix, separator, suffix, singleFileNumPartitions, validOutputURI, invalidKeys) match {
      case (Right(name), Right(description), Right(inputView), Right(outputURI), Right(numPartitions), Right(authentication), Right(saveMode), Right(singleFile), Right(prefix), Right(separator), Right(suffix), Right(singleFileNumPartitions), Right(validOutputURI), Right(invalidKeys)) =>
        val stage = TextLoadStage(
          plugin=this,
          name=name,
          description=description,
          inputView=inputView,
          outputURI=outputURI,
          numPartitions=numPartitions,
          authentication=authentication,
          saveMode=saveMode,
          params=params,
          singleFile=singleFile,
          prefix=prefix,
          separator=separator,
          suffix=suffix,
          singleFileNumPartitions=singleFileNumPartitions
        )

        authentication.foreach { authentication => stage.stageDetail.put("authentication", authentication.method) }
        numPartitions.foreach { numPartitions => stage.stageDetail.put("numPartitions", Integer.valueOf(numPartitions)) }
        outputURI.foreach { outputURI => stage.stageDetail.put("outputURI", outputURI.toString) }
        stage.stageDetail.put("inputView", inputView)
        stage.stageDetail.put("params", params.asJava)
        stage.stageDetail.put("prefix", prefix)
        stage.stageDetail.put("saveMode", saveMode.toString.toLowerCase)
        stage.stageDetail.put("separator", separator)
        stage.stageDetail.put("singleFile", java.lang.Boolean.valueOf(singleFile))
        stage.stageDetail.put("suffix", suffix)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, outputURI, numPartitions, authentication, saveMode, singleFile, prefix, separator, suffix, singleFileNumPartitions, validOutputURI, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class TextLoadStage(
    plugin: TextLoad,
    name: String,
    description: Option[String],
    inputView: String,
    outputURI: Option[URI],
    numPartitions: Option[Int],
    authentication: Option[Authentication],
    saveMode: SaveMode,
    params: Map[String, String],
    singleFile: Boolean,
    prefix: String,
    separator: String,
    suffix: String,
    singleFileNumPartitions: Int
  ) extends LoadPipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    TextLoadStage.execute(this)
  }

}

object TextLoadStage {

  def execute(stage: TextLoadStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    val signature = "TextLoad requires input [value: string], [value: string, filename: string] or [value: string, filename: string, index: integer] signature when in singleFile mode."

    val stageOutputURI = stage.outputURI
    val stagePrefix = stage.prefix
    val stageSeparator = stage.separator
    val stageSuffix = stage.suffix
    val stageSaveMode = stage.saveMode
    val contextSerializableConfiguration = arcContext.serializableConfiguration

    val df = spark.table(stage.inputView)

    // set write permissions
    CloudUtils.setHadoopConfiguration(stage.authentication)

    try {
      if (stage.singleFile) {
        if (!(
            (df.schema.length == 1 && df.schema.fields(0).dataType == StringType) ||
            (df.schema.length == 2 && df.schema.fields.map { field => (field.name, field.dataType) }.forall { field => validValueFilename.contains(field) }) ||
            (df.schema.length == 3 && df.schema.fields.map { field => (field.name, field.dataType) }.forall { field => validValueFilenameIndex.contains(field) })
          )) {
          throw new Exception(s"""${signature} Got [${df.schema.map(f => s"""${f.name}: ${f.dataType.simpleString}""").mkString(", ")}].""")
        }

        val hasFilename = df.schema.length == 2 || df.schema.length == 3
        val hasIndex = df.schema.length == 3

        // repartition so that there is a 1:1 mapping of partition:filename
        val repartitionedDF = if (hasFilename) {
          df.repartition(stage.singleFileNumPartitions, col("filename"))
        } else {
          df.repartition(1)
        }

        if (!hasFilename && stageOutputURI.isEmpty) {
          throw new Exception("TextLoad requires 'outputURI' to be set if in 'singleFile' mode and no 'filename' column exists.")
        }

        val outputFileAccumulator = spark.sparkContext.collectionAccumulator[String]

        repartitionedDF.foreachPartition { partition: Iterator[Row] =>
          if (partition.hasNext) {
            val hadoopConf = contextSerializableConfiguration.value

            // buffer so first row can be accessed
            val bufferedPartition = partition.buffered

            val firstRow = bufferedPartition.head
            val valueIndex = if (hasFilename) {
              firstRow.fieldIndex("value")
            } else {
              0
            }
            val indexIndex = if (hasIndex) {
              firstRow.fieldIndex("index")
            } else {
              0
            }

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

            // write bytes of the partition to the outputStream
            outputStream match {
              case Some(os) => {
                os.writeBytes(stagePrefix)

                // if has index sort
                val iterator = if (hasIndex) {
                  bufferedPartition.toSeq.sortBy { row => row.getInt(indexIndex) }.toIterator
                } else {
                  bufferedPartition
                }

                // use a while loop to add separator only when hasNext
                while (iterator.hasNext) {
                  os.writeBytes(iterator.next.getString(valueIndex))
                  if (iterator.hasNext) {
                    os.writeBytes(stageSeparator)
                  }
                }
                os.writeBytes(stageSuffix)
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
        if (df.schema.length != 1 || df.schema.fields(0).dataType != StringType) {
          throw new Exception(s"""TextLoad supports only a single text column but the input view has ${df.schema.length} columns.""") with DetailException {
            override val detail = stage.stageDetail
          }
        }

        // spark does not allow partitionBy when only single column dataframe
        stage.numPartitions match {
          case Some(n) => df.repartition(n).write.mode(stage.saveMode).text(stage.outputURI.get.toString)
          case None => df.write.mode(stage.saveMode).text(stage.outputURI.get.toString)
        }
      }
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    Option(df)
  }

  val validValueFilename = Array(("value", StringType), ("filename", StringType))
  val validValueFilenameIndex = Array(("value", StringType), ("filename", StringType), ("index", IntegerType))
}
