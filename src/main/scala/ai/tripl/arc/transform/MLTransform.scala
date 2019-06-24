// package ai.tripl.arc.transform

// import java.lang._
// import scala.collection.JavaConverters._

// import org.apache.spark.sql._
// import org.apache.spark.sql.functions._
// import org.apache.spark.storage.StorageLevel
// import org.apache.spark.ml._
// import org.apache.spark.ml.classification._
// import org.apache.spark.ml.feature._
// import org.apache.spark.ml.tuning._
// import org.apache.spark.ml.evaluation._
// import org.apache.spark.ml.linalg.Vector

// import ai.tripl.arc.api.API._
// import ai.tripl.arc.util._

// object MLTransform {

//   def transform(transform: MLTransform)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): Option[DataFrame] = {
//     val startTime = System.currentTimeMillis() 
//     val stageDetail = new java.util.HashMap[String, Object]()
//     stageDetail.put("type", transform.getType)
//     stageDetail.put("name", transform.name)
//     for (description <- transform.description) {
//       stageDetail.put("description", description)    
//     }    
//     stageDetail.put("inputURI", transform.inputURI.toString)  
//     stageDetail.put("inputView", transform.inputView)  
//     stageDetail.put("outputView", transform.outputView)   

//     logger.info()
//       .field("event", "enter")
//       .map("stage", stageDetail)      
//       .log()      

//     val df = spark.table(transform.inputView)

//     val model = transform.model match {
//       case Right(crossValidatorModel) => crossValidatorModel
//       case Left(pipelineModel) => pipelineModel
//     }    

//     val stages = try {
//       transform.model match {
//         case Right(crossValidatorModel) => crossValidatorModel.bestModel.asInstanceOf[PipelineModel].stages
//         case Left(pipelineModel) => pipelineModel.stages
//       } 
//     } catch {
//       case e: Exception => throw new Exception(e) with DetailException {
//         override val detail = stageDetail          
//       }      
//     }             

//     // apply model
//     val fullTransformedDF = try {
//       model.transform(df)
//     } catch {
//       case e: Exception => throw new Exception(e) with DetailException {
//         override val detail = stageDetail          
//       }      
//     } 

//     // select only input fields, predictedCol(s), probabilityCol(s)
//     val inputCols = df.schema.fields.map(f => col(f.name)) 
//     val predictionCols = stages
//       .filter(stage => stage.hasParam("predictionCol"))
//       .map(stage => stage.get(stage.getParam("predictionCol")))
//       .map(predictionCol => predictionCol.getOrElse("prediction"))
//       .map(_.toString)
//       .map(col(_))
//     val probabilityCols = stages
//       .filter(stage => stage.hasParam("probabilityCol"))
//       .map(stage => stage.get(stage.getParam("probabilityCol")))
//       .map(predictionCol => predictionCol.getOrElse("probability"))
//       .map(_.toString)
//       .map(col(_))
//     var transformedDF = fullTransformedDF.select((inputCols ++ predictionCols ++ probabilityCols): _*)
    
//     // if any probability columns exist replace with the max value in the probability vector using a custom UDF
//     val maxProbability = udf((v: Vector) => v.toArray.max)
//     probabilityCols.foreach(col => {
//         transformedDF = transformedDF.withColumn(s"${col}", maxProbability(col))
//     })

//     // repartition to distribute rows evenly
//     val repartitionedDF = transform.partitionBy match {
//       case Nil => { 
//         transform.numPartitions match {
//           case Some(numPartitions) => transformedDF.repartition(numPartitions)
//           case None => transformedDF
//         }   
//       }
//       case partitionBy => {
//         // create a column array for repartitioning
//         val partitionCols = partitionBy.map(col => transformedDF(col))
//         transform.numPartitions match {
//           case Some(numPartitions) => transformedDF.repartition(numPartitions, partitionCols:_*)
//           case None => transformedDF.repartition(partitionCols:_*)
//         }
//       }
//     }

//     repartitionedDF.createOrReplaceTempView(transform.outputView)    

//     if (!repartitionedDF.isStreaming) {
//       stageDetail.put("outputColumns", Integer.valueOf(repartitionedDF.schema.length))
//       stageDetail.put("numPartitions", Integer.valueOf(repartitionedDF.rdd.partitions.length))

//       if (transform.persist) {
//         repartitionedDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
//         stageDetail.put("records", Long.valueOf(repartitionedDF.count)) 

//         // add percentiles to an list for logging
//         var approxQuantileMap = new java.util.HashMap[String, Array[Double]]()
//         probabilityCols.foreach(col => {
//             approxQuantileMap.put(col.toString, repartitionedDF.stat.approxQuantile(col.toString, Array(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0), 0.1).map(col => Double.valueOf(col)))
//         })
//         if (approxQuantileMap.size > 0) {
//           stageDetail.put("percentiles", approxQuantileMap)
//         }        
//       }      
//     }    

//     logger.info()
//       .field("event", "exit")
//       .field("duration", System.currentTimeMillis() - startTime)
//       .map("stage", stageDetail)      
//       .log()  

//     Option(repartitionedDF)
//   }
// }
