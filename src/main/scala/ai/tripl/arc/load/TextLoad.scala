package ai.tripl.arc.load

import java.net.URI
import scala.collection.JavaConverters._

import org.apache.spark.sql._
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

class TextLoad extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputURI" :: "authentication" :: "numPartitions" :: "partitionBy" :: "saveMode" :: "params" :: "singleFile" :: "prefix" :: "separator" :: "suffix" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = getValue[String]("inputView")
    val outputURI = getValue[String]("outputURI") |> parseURI("outputURI") _
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val authentication = readAuthentication("authentication")
    val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _
    val singleFile = getValue[java.lang.Boolean]("singleFile", default = Some(false))
    val prefix = getValue[String]("prefix", default = Some(""))
    val separator = getValue[String]("separator", default = Some(""))
    val suffix = getValue[String]("suffix", default = Some(""))
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, description, inputView, outputURI, numPartitions, authentication, saveMode, singleFile, prefix, separator, suffix, invalidKeys) match {
      case (Right(name), Right(description), Right(inputView), Right(outputURI), Right(numPartitions), Right(authentication), Right(saveMode), Right(singleFile), Right(prefix), Right(separator), Right(suffix), Right(invalidKeys)) =>

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
          suffix=suffix
        )

        stage.stageDetail.put("inputView", inputView)
        stage.stageDetail.put("outputURI", outputURI.toString)
        stage.stageDetail.put("saveMode", saveMode.toString.toLowerCase)
        stage.stageDetail.put("params", params.asJava)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, outputURI, numPartitions, authentication, saveMode, singleFile, prefix, separator, suffix, invalidKeys).collect{ case Left(errs) => errs }.flatten
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
    outputURI: URI,
    numPartitions: Option[Int],
    authentication: Option[Authentication],
    saveMode: SaveMode,
    params: Map[String, String],
    singleFile: Boolean,
    prefix: String,
    separator: String,
    suffix: String
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    TextLoadStage.execute(this)
  }
}

object TextLoadStage {

  def execute(stage: TextLoadStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    val signature = "TextLoad requires input [value: string] or [value: string, filename: string] signature when in singleFile mode."

    val df = spark.table(stage.inputView)

    if (!df.isStreaming) {
      stage.numPartitions match {
        case Some(partitions) => stage.stageDetail.put("numPartitions", java.lang.Integer.valueOf(partitions))
        case None => stage.stageDetail.put("numPartitions", java.lang.Integer.valueOf(df.rdd.getNumPartitions))
      }
    }

    // set write permissions
    CloudUtils.setHadoopConfiguration(stage.authentication)

    try {
      if (stage.singleFile) {
        if (df.schema.length == 0 || df.schema.length > 2 || (df.schema.length == 1 && df.schema.fields(0).dataType != StringType) || (df.schema.length == 2 && df.schema.forall { f => !Seq("filename","value").contains(f.name) || f.dataType != StringType } )) {
          throw new Exception(s"""${signature} Got [${df.schema.map(f => s"""${f.name}: ${f.dataType.simpleString}""").mkString(", ")}].""")
        }        

        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

        // group rows by target filename
        val groupedRows = if (df.schema.length == 2) {
          df.collect.groupBy { row => new URI(s"""${stage.outputURI}/${row.getString(row.fieldIndex("filename"))}""") }
        } else {
          df.collect.groupBy { _ => stage.outputURI}
        }


        // first test for any invalid rules 
        groupedRows.foreach { case (outputURI, _) => 
          val path = new Path(outputURI)
          if (fs.exists(path)) {
            stage.saveMode match {
              case SaveMode.ErrorIfExists => {
                throw new Exception(s"File '${path.toString}' already exists and 'saveMode' equals 'ErrorIfExists' so cannot continue.")
              }
              case _ => 
            }
          }
        }

        // write the records for each group sequentially
        groupedRows.foreach { case (outputURI, rowGroup) => 
          val path = new Path(outputURI)

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
            
          // write bytes to the rowgroup to the outputStream
          outputStream match {
            case Some(os) => {
              os.writeBytes(
                rowGroup
                  .map { row => 
                    if (df.schema.length == 1) {
                      row.getString(0) 
                    } else {
                      row.getString(row.fieldIndex("value")) 
                    }
                  }
                  .mkString(stage.prefix, stage.separator, stage.suffix)
              )
              os.close
            }
            case None =>
          }          
        }       

        fs.close
      } else {
        if (df.schema.length != 1 || df.schema.fields(0).dataType != StringType) {
          throw new Exception(s"""TextLoad supports only a single text column but the input view has ${df.schema.length} columns.""") with DetailException {
            override val detail = stage.stageDetail
          }
        }

        // spark does not allow partitionBy when only single column dataframe
        stage.numPartitions match {
          case Some(n) => df.repartition(n).write.mode(stage.saveMode).text(stage.outputURI.toString)
          case None => df.write.mode(stage.saveMode).text(stage.outputURI.toString)
        }
      }
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    Option(df)
  }
}
