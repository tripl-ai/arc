package au.com.agl.arc.transform

import java.lang._
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

import au.com.agl.arc.api.API._
import au.com.agl.arc.util._

object MLTransform {

  def transform(transform: MLTransform)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Option[DataFrame] = {
    val startTime = System.currentTimeMillis() 
    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", transform.getType)
    stageDetail.put("name", transform.name)
    stageDetail.put("inputURI", transform.inputURI.toString)  
    stageDetail.put("inputView", transform.inputView)  
    stageDetail.put("outputView", transform.outputView)   

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()      

    val df = spark.table(transform.inputView)

    val model = transform.model match {
      case Right(crossValidatorModel) => crossValidatorModel
      case Left(pipelineModel) => pipelineModel
    }    

    val stages = try {
      transform.model match {
        case Right(crossValidatorModel) => crossValidatorModel.bestModel.asInstanceOf[PipelineModel].stages
        case Left(pipelineModel) => pipelineModel.stages
      } 
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stageDetail          
      }      
    }             

    // apply model
    val fullTransformedDF = try {
      model.transform(df)
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stageDetail          
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

    transformedDF.createOrReplaceTempView(transform.outputView)

    if (transform.persist && !transformedDF.isStreaming) {
      transformedDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
      stageDetail.put("records", Long.valueOf(transformedDF.count))

      // add percentiles to an list for logging
      var approxQuantileMap = new java.util.HashMap[String, Array[Double]]()
      probabilityCols.foreach(col => {
          approxQuantileMap.put(col.toString, transformedDF.stat.approxQuantile(col.toString, Array(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0), 0.1).map(col => Double.valueOf(col)))
      })
      if (approxQuantileMap.size > 0) {
        stageDetail.put("percentiles", approxQuantileMap)
      }
    }

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stageDetail)      
      .log()  

    Option(transformedDF)
  }
}
