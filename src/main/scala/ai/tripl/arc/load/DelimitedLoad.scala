// package ai.tripl.arc.load

// import scala.collection.JavaConverters._

// import org.apache.spark.sql._
// import org.apache.spark.sql.types._

// import ai.tripl.arc.api._
// import ai.tripl.arc.api.API._
// import ai.tripl.arc.util._

// object DelimitedLoad {

//   def load(load: DelimitedLoad)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): Option[DataFrame] = {
//     import spark.implicits._
//     val startTime = System.currentTimeMillis() 
//     val stageDetail = new java.util.HashMap[String, Object]()
//     stageDetail.put("type", load.getType)
//     stageDetail.put("name", load.name)
//     for (description <- load.description) {
//       stageDetail.put("description", description)    
//     }    
//     stageDetail.put("inputView", load.inputView)  
//     stageDetail.put("outputURI", load.outputURI.toString)  
//     stageDetail.put("partitionBy", load.partitionBy.asJava)
//     stageDetail.put("saveMode", load.saveMode.toString.toLowerCase)

//     val df = spark.table(load.inputView)      

//     if (!df.isStreaming) {
//       load.numPartitions match {
//         case Some(partitions) => stageDetail.put("numPartitions", Integer.valueOf(partitions))
//         case None => stageDetail.put("numPartitions", Integer.valueOf(df.rdd.getNumPartitions))
//       }
//     }

//     val options: Map[String, String] = Delimited.toSparkOptions(load.settings)
//     stageDetail.put("options", options.asJava)

//     logger.info()
//       .field("event", "enter")
//       .map("stage", stageDetail)      
//       .log()

//     // set write permissions
//     CloudUtils.setHadoopConfiguration(load.authentication) 

//     val dropMap = new java.util.HashMap[String, Object]()

//     // delimited cannot handle a column of ArrayType
//     val arrays = df.schema.filter( _.dataType.typeName == "array").map(_.name)
//     if (!arrays.isEmpty) {
//       dropMap.put("ArrayType", arrays.asJava)
//     }    

//     // delimited cannot handle a column of NullType
//     val nulls = df.schema.filter( _.dataType == NullType).map(_.name)
//     if (!nulls.isEmpty) {
//       dropMap.put("NullType", nulls.asJava)
//     }  

//     stageDetail.put("drop", dropMap) 

//     val nonNullDF = df.drop(arrays:_*).drop(nulls:_*)

//     val listener = ListenerUtils.addStageCompletedListener(stageDetail)

//     try {
//       if (nonNullDF.isStreaming) {
//         load.partitionBy match {
//           case Nil => nonNullDF.writeStream.format("csv").options(options).option("path", load.outputURI.toString).start
//           case partitionBy => {
//             val partitionCols = partitionBy.map(col => nonNullDF(col))
//             nonNullDF.writeStream.partitionBy(partitionBy:_*).format("csv").options(options).option("path", load.outputURI.toString).start
//           }
//         }
//       } else {
//         load.partitionBy match {
//           case Nil => {
//             load.numPartitions match {
//               case Some(n) => nonNullDF.repartition(n).write.mode(load.saveMode).format("csv").options(options).save(load.outputURI.toString)
//               case None => nonNullDF.write.mode(load.saveMode).format("csv").options(options).save(load.outputURI.toString)
//             }
//           }
//           case partitionBy => {
//             // create a column array for repartitioning
//             val partitionCols = partitionBy.map(col => nonNullDF(col))
//             load.numPartitions match {
//               case Some(n) => nonNullDF.repartition(n, partitionCols:_*).write.partitionBy(partitionBy:_*).mode(load.saveMode).format("csv").options(options).save(load.outputURI.toString)
//               case None => nonNullDF.repartition(partitionCols:_*).write.partitionBy(partitionBy:_*).mode(load.saveMode).format("csv").options(options).save(load.outputURI.toString)
//             }
//           }
//         }
//       }
//     } catch {
//       case e: Exception => throw new Exception(e) with DetailException {
//         override val detail = stageDetail
//       }
//     }

//     spark.sparkContext.removeSparkListener(listener)           

//     logger.info()
//       .field("event", "exit")
//       .field("duration", System.currentTimeMillis() - startTime)
//       .map("stage", stageDetail)      
//       .log()

//     Option(nonNullDF)
//   }
// }

