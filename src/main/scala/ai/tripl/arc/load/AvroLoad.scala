// package ai.tripl.arc.load

// import java.lang._
// import java.net.URI
// import scala.collection.JavaConverters._

// import org.apache.spark.sql._
// import org.apache.spark.sql.types._

// import ai.tripl.arc.api.API._
// import ai.tripl.arc.util._

// object AvroLoad {

//   def load(load: AvroLoad)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): Option[DataFrame] = {
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

//     load.numPartitions match {
//       case Some(partitions) => stageDetail.put("numPartitions", Integer.valueOf(partitions))
//       case None => stageDetail.put("numPartitions", Integer.valueOf(df.rdd.getNumPartitions))
//     }

//     logger.info()
//       .field("event", "enter")
//       .map("stage", stageDetail)      
//       .log()

//     // set write permissions
//     CloudUtils.setHadoopConfiguration(load.authentication)

//     val dropMap = new java.util.HashMap[String, Object]()

//     // Avro cannot handle a column of NullType
//     val nulls = df.schema.filter( _.dataType == NullType).map(_.name)
//     if (!nulls.isEmpty) {
//       dropMap.put("NullType", nulls.asJava)
//     }

//     stageDetail.put("drop", dropMap) 

//     val listener = ListenerUtils.addStageCompletedListener(stageDetail)

//     // Avro will convert date and times to epoch milliseconds
//     val outputDF = try {
//       val nonNullDF = df.drop(nulls:_*)
//       load.partitionBy match {
//         case Nil =>
//           val dfToWrite = load.numPartitions.map(nonNullDF.repartition(_)).getOrElse(nonNullDF)
//           dfToWrite.write.mode(load.saveMode).format("avro").save(load.outputURI.toString)
//           dfToWrite
//         case partitionBy => {
//           // create a column array for repartitioning
//           val partitionCols = partitionBy.map(col => df(col))
//           load.numPartitions match {
//             case Some(n) =>
//               val dfToWrite = nonNullDF.repartition(n, partitionCols:_*)
//               dfToWrite.write.partitionBy(partitionBy:_*).mode(load.saveMode).format("avro").save(load.outputURI.toString)
//               dfToWrite
//             case None =>
//               val dfToWrite = nonNullDF.repartition(partitionCols:_*)
//               dfToWrite.write.partitionBy(partitionBy:_*).mode(load.saveMode).format("avro").save(load.outputURI.toString)
//               dfToWrite
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

//     Option(outputDF)
//   }
// }