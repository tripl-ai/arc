package ai.tripl.arc.transform

import java.net.URI
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.ml._
import org.apache.spark.ml.classification._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.tuning._
import org.apache.spark.ml.evaluation._
import org.apache.spark.ml.linalg.Vector

import com.typesafe.config._

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
import ai.tripl.arc.util.ListenerUtils
import ai.tripl.arc.util.Utils

class MLTransform extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], ai.tripl.arc.api.API.PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "inputView" :: "outputView" :: "authentication" :: "persist" :: "params" :: "numPartitions" :: "partitionBy" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val authentication = readAuthentication("authentication")
    val inputURI = getValue[String]("inputURI") |> parseURI("inputURI") _
    val model = inputURI |> getModel("inputURI", authentication) _
    val inputView = getValue[String]("inputView")
    val outputView = getValue[String]("outputView")
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))        
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)      

    (name, description, inputURI, model, inputView, outputView, persist, numPartitions, partitionBy, invalidKeys) match {
      case (Right(name), Right(description), Right(inputURI), Right(model), Right(inputView), Right(outputView), Right(persist), Right(numPartitions), Right(partitionBy), Right(invalidKeys)) => 

        val stage = MLTransformStage(
          plugin=this,
          name=name,
          description=description,
          inputURI=inputURI,
          model=model,
          inputView=inputView,
          outputView=outputView,
          params=params,
          persist=persist,
          numPartitions=numPartitions,
          partitionBy=partitionBy
        )

        stage.stageDetail.put("inputURI", inputURI.toString)  
        stage.stageDetail.put("inputView", inputView)  
        stage.stageDetail.put("outputView", outputView)   

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, inputURI, model, inputView, outputView, persist, numPartitions, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

  def getModel(path: String, authentication: Either[Errors, Option[Authentication]])(uri: URI)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, c: Config): Either[Errors, Either[PipelineModel, CrossValidatorModel]] = {
    def err(lineNumber: Option[Int], msg: String): Either[Errors, Either[PipelineModel, CrossValidatorModel]] = Left(ConfigError(path, lineNumber, msg) :: Nil)
    
    authentication.right.map(auth => CloudUtils.setHadoopConfiguration(auth))
    
    try {
      Right(Left(PipelineModel.load(uri.toString)))
    } catch {
      case e: Exception => {
        try{
         Right(Right(CrossValidatorModel.load(uri.toString)))
        } catch {
          case e: Exception => err(Some(c.getValue(path).origin.lineNumber()), e.getMessage)
        }
      }
    }
  }

}

case class MLTransformStage(
    plugin: MLTransform,
    name: String, 
    description: Option[String], 
    inputURI: URI, 
    model: Either[PipelineModel, CrossValidatorModel], 
    inputView: String, 
    outputView: String, 
    params: Map[String, String], 
    persist: Boolean, 
    numPartitions: Option[Int], 
    partitionBy: List[String]
  ) extends ai.tripl.arc.api.API.PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    MLTransformStage.execute(this)
  }
}

object MLTransformStage {
  def execute(stage: MLTransformStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    val df = spark.table(stage.inputView)

    val model = stage.model match {
      case Right(crossValidatorModel) => crossValidatorModel
      case Left(pipelineModel) => pipelineModel
    }    

    val stages = try {
      stage.model match {
        case Right(crossValidatorModel) => crossValidatorModel.bestModel.asInstanceOf[PipelineModel].stages
        case Left(pipelineModel) => pipelineModel.stages
      } 
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail          
      }      
    }             

    // apply model
    val fullTransformedDF = try {
      model.transform(df)
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail          
      }      
    } 

    // select only input fields, predictedCol(s), probabilityCol(s)
    val inputCols = df.schema.fields.map(f => col(f.name)) 
    val predictionCols = stages
      .filter(stage => stage.hasParam("predictionCol"))
      .map(stage => stage.get(stage.getParam("predictionCol")))
      .map(predictionCol => predictionCol.getOrElse("prediction"))
      .map(_.toString)
      .map(col(_))
    val probabilityCols = stages
      .filter(stage => stage.hasParam("probabilityCol"))
      .map(stage => stage.get(stage.getParam("probabilityCol")))
      .map(predictionCol => predictionCol.getOrElse("probability"))
      .map(_.toString)
      .map(col(_))
    var transformedDF = fullTransformedDF.select((inputCols ++ predictionCols ++ probabilityCols): _*)
    
    // if any probability columns exist replace with the max value in the probability vector using a custom UDF
    val maxProbability = udf((v: Vector) => v.toArray.max)
    probabilityCols.foreach(col => {
        transformedDF = transformedDF.withColumn(s"${col}", maxProbability(col))
    })

    // repartition to distribute rows evenly
    val repartitionedDF = stage.partitionBy match {
      case Nil => { 
        stage.numPartitions match {
          case Some(numPartitions) => transformedDF.repartition(numPartitions)
          case None => transformedDF
        }   
      }
      case partitionBy => {
        // create a column array for repartitioning
        val partitionCols = partitionBy.map(col => transformedDF(col))
        stage.numPartitions match {
          case Some(numPartitions) => transformedDF.repartition(numPartitions, partitionCols:_*)
          case None => transformedDF.repartition(partitionCols:_*)
        }
      }
    }

    repartitionedDF.createOrReplaceTempView(stage.outputView)    

    if (!repartitionedDF.isStreaming) {
      stage.stageDetail.put("outputColumns", java.lang.Integer.valueOf(repartitionedDF.schema.length))
      stage.stageDetail.put("numPartitions", java.lang.Integer.valueOf(repartitionedDF.rdd.partitions.length))

      if (stage.persist) {
        repartitionedDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
        stage.stageDetail.put("records", java.lang.Long.valueOf(repartitionedDF.count)) 

        // add percentiles to list for logging
        var approxQuantileMap = new java.util.HashMap[String, Array[java.lang.Double]]()
        probabilityCols.foreach(col => {
            approxQuantileMap.put(col.toString, repartitionedDF.stat.approxQuantile(col.toString, Array(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0), 0.1).map(col => java.lang.Double.valueOf(col)))
        })
        if (approxQuantileMap.size > 0) {
          stage.stageDetail.put("percentiles", approxQuantileMap)
        }        
      }      
    }    

    Option(repartitionedDF)
  }
}
