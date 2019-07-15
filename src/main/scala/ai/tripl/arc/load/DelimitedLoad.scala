package ai.tripl.arc.load

import java.net.URI
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.CloudUtils
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.ListenerUtils
import ai.tripl.arc.util.Utils

class DelimitedLoad extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputURI" :: "authentication" :: "delimiter" :: "header" :: "numPartitions" :: "partitionBy" :: "quote" :: "saveMode" :: "params"  :: "customDelimiter" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = getValue[String]("inputView")
    val outputURI = getValue[String]("outputURI") |> parseURI("outputURI") _
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val authentication = readAuthentication("authentication")  
    val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _
    val delimiter = getValue[String]("delimiter", default = Some("Comma"), validValues = "Comma" :: "Pipe" :: "DefaultHive" :: "Custom" :: Nil) |> parseDelimiter("delimiter") _
    val quote = getValue[String]("quote", default =  Some("DoubleQuote"), validValues = "DoubleQuote" :: "SingleQuote" :: "None" :: Nil) |> parseQuote("quote") _
    val header = getValue[java.lang.Boolean]("header", default = Some(false))   
    val customDelimiter = delimiter match {
      case Right(Delimiter.Custom) => getValue[String]("customDelimiter")
      case _ => Right("")
    }     
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    (name, description, inputView, outputURI, partitionBy, numPartitions, authentication, saveMode, delimiter, quote, header, customDelimiter, invalidKeys) match {
      case (Right(name), Right(description), Right(inputView), Right(outputURI), Right(partitionBy), Right(numPartitions), Right(authentication), Right(saveMode), Right(delimiter), Right(quote), Right(header), Right(customDelimiter), Right(invalidKeys)) => 
        val settings = new Delimited(header=header, sep=delimiter, quote=quote, customDelimiter=customDelimiter)

        val stage = DelimitedLoadStage(
          plugin=this,
          name=name,
          description=description,
          inputView=inputView, 
          outputURI=outputURI, 
          settings=settings, 
          partitionBy=partitionBy, 
          numPartitions=numPartitions, 
          authentication=authentication, 
          saveMode=saveMode, 
          params=params      
        )

        stage.stageDetail.put("inputView", inputView)  
        stage.stageDetail.put("options", Delimited.toSparkOptions(settings).asJava)
        stage.stageDetail.put("outputURI", outputURI.toString)  
        stage.stageDetail.put("partitionBy", partitionBy.asJava)
        stage.stageDetail.put("saveMode", saveMode.toString.toLowerCase)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, outputURI, partitionBy, numPartitions, authentication, saveMode, delimiter, quote, header, customDelimiter, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

  // case class DelimitedLoad() extends Load { val getType = "DelimitedLoad" }
case class DelimitedLoadStage(
    plugin: DelimitedLoad,
    name: String, 
    description: Option[String], 
    inputView: String, 
    outputURI: URI, 
    settings: Delimited, 
    partitionBy: List[String], 
    numPartitions: Option[Int], 
    authentication: Option[Authentication], 
    saveMode: SaveMode, 
    params: Map[String, String]
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    DelimitedLoadStage.execute(this)
  }
}

object DelimitedLoadStage {

  def execute(stage: DelimitedLoadStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    val df = spark.table(stage.inputView)      

    if (!df.isStreaming) {
      stage.numPartitions match {
        case Some(partitions) => stage.stageDetail.put("numPartitions", java.lang.Integer.valueOf(partitions))
        case None => stage.stageDetail.put("numPartitions", java.lang.Integer.valueOf(df.rdd.getNumPartitions))
      }
    }

    // set write permissions
    CloudUtils.setHadoopConfiguration(stage.authentication) 

    val dropMap = new java.util.HashMap[String, Object]()

    // delimited cannot handle a column of ArrayType
    val arrays = df.schema.filter( _.dataType.typeName == "array").map(_.name)
    if (!arrays.isEmpty) {
      dropMap.put("ArrayType", arrays.asJava)
    }    

    // delimited cannot handle a column of NullType
    val nulls = df.schema.filter( _.dataType == NullType).map(_.name)
    if (!nulls.isEmpty) {
      dropMap.put("NullType", nulls.asJava)
    }  

    stage.stageDetail.put("drop", dropMap) 

    val nonNullDF = df.drop(arrays:_*).drop(nulls:_*)

    val options = Delimited.toSparkOptions(stage.settings)

    val listener = ListenerUtils.addStageCompletedListener(stage.stageDetail)

    try {
      if (nonNullDF.isStreaming) {
        stage.partitionBy match {
          case Nil => nonNullDF.writeStream.format("csv").options(options).option("path", stage.outputURI.toString).start
          case partitionBy => {
            nonNullDF.writeStream.partitionBy(partitionBy:_*).format("csv").options(options).option("path", stage.outputURI.toString).start
          }
        }
      } else {
        stage.partitionBy match {
          case Nil => {
            stage.numPartitions match {
              case Some(n) => nonNullDF.repartition(n).write.mode(stage.saveMode).format("csv").options(options).save(stage.outputURI.toString)
              case None => nonNullDF.write.mode(stage.saveMode).format("csv").options(options).save(stage.outputURI.toString)
            }
          }
          case partitionBy => {
            // create a column array for repartitioning
            val partitionCols = partitionBy.map(col => nonNullDF(col))
            stage.numPartitions match {
              case Some(n) => nonNullDF.repartition(n, partitionCols:_*).write.partitionBy(partitionBy:_*).mode(stage.saveMode).format("csv").options(options).save(stage.outputURI.toString)
              case None => nonNullDF.repartition(partitionCols:_*).write.partitionBy(partitionBy:_*).mode(stage.saveMode).format("csv").options(options).save(stage.outputURI.toString)
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

    Option(nonNullDF)
  }
}

