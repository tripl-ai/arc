package ai.tripl.arc.extract

import java.lang._
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import org.apache.hadoop.mapreduce.lib.input.InvalidInputException
import com.typesafe.config._

import ai.tripl.arc.datasource.BinaryContent
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

class BytesExtract extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def createStage(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "inputURI" :: "outputView" :: "authentication" :: "contiguousIndex" :: "numPartitions" :: "persist" :: "params" :: "failMode" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = if(c.hasPath("inputView")) getValue[String]("inputView") else Right("")
    val parsedGlob = if(!c.hasPath("inputView")) getValue[String]("inputURI") |> parseGlob("inputURI") _ else Right("")
    val outputView = getValue[String]("outputView")
    val persist = getValue[Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val authentication = readAuthentication("authentication")
    val contiguousIndex = getValue[Boolean]("contiguousIndex", default = Some(true))
    val failMode = getValue[String]("failMode", default = Some("failfast"), validValues = "permissive" :: "failfast" :: Nil) |> parseFailMode("failMode") _
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)    

    (name, description, parsedGlob, inputView, outputView, persist, numPartitions, authentication, contiguousIndex, failMode, invalidKeys) match {
      case (Right(name), Right(description), Right(parsedGlob), Right(inputView), Right(outputView), Right(persist), Right(numPartitions), Right(authentication), Right(contiguousIndex), Right(failMode), Right(invalidKeys)) =>
        val input = if(c.hasPath("inputView")) {
          Left(inputView) 
        } else {
          Right(parsedGlob)
        }

        val stage = BytesExtractStage(
          plugin=this,
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

        stage.stageDetail.put("failMode", stage.failMode.sparkString)
        stage.stageDetail.put("input", if (c.hasPath("inputView")) inputView else parsedGlob)    
        stage.stageDetail.put("outputView", outputView)
        stage.stageDetail.put("persist", Boolean.valueOf(stage.persist))

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, parsedGlob, inputView, outputView, persist, numPartitions, authentication, contiguousIndex, failMode, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class BytesExtractStage(
    plugin: PipelineStagePlugin,
    name: String, 
    description: Option[String],
    outputView: String,
    input: Either[String, String],
    authentication: Option[Authentication],
    params: Map[String, String],
    persist: Boolean,
    numPartitions: Option[Int],
    contiguousIndex: Boolean,
    failMode: FailModeType
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    BytesExtractStage.execute(this)
  }
}

object BytesExtractStage {

  def execute(stage: BytesExtractStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): Option[DataFrame] = {

    val signature = "BytesExtract requires pathView to be dataset with [value: string] signature."

    CloudUtils.setHadoopConfiguration(stage.authentication)

    val df = try {
      stage.input match {
        case Left(view) => {
          val pathView = spark.table(view)
          val schema = pathView.schema

          val fieldIndex = try {
            schema.fieldIndex("value")
          } catch {
            case e: Exception => throw new Exception(s"""${signature} inputView has: [${pathView.schema.map(_.name).mkString(", ")}].""") with DetailException {
              override val detail = stage.stageDetail
            }
          }

          schema.fields(fieldIndex).dataType match {
            case _: StringType =>
            case _ => throw new Exception(s"""${signature} 'value' is of type: '${schema.fields(fieldIndex).dataType.simpleString}'.""") with DetailException {
              override val detail = stage.stageDetail
            }
          }

          val path = pathView.select(col("value")).collect().map( _.getString(0) ).mkString(",")
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
        if (stage.failMode == FailModeTypeFailFast) {
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
    repartitionedDF.createOrReplaceTempView(stage.outputView)

    stage.stageDetail.put("inputFiles", Integer.valueOf(repartitionedDF.inputFiles.length))
    stage.stageDetail.put("outputColumns", Integer.valueOf(repartitionedDF.schema.length))
    stage.stageDetail.put("numPartitions", Integer.valueOf(repartitionedDF.rdd.partitions.length))

    if (stage.persist && !repartitionedDF.isStreaming) {
      repartitionedDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
      stage.stageDetail.put("records", java.lang.Long.valueOf(repartitionedDF.count))
    }

    Option(repartitionedDF)
  }

}
