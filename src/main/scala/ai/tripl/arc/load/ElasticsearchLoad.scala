// package ai.tripl.arc.load

// import java.lang._
// import java.net.URI
// import scala.collection.JavaConverters._

// import org.apache.spark.sql._
// import org.apache.spark.sql.types._

// import ai.tripl.arc.api.API._
// import ai.tripl.arc.util._

// import org.elasticsearch.spark.sql._ 

// object ElasticsearchLoad {

//   def load(load: ElasticsearchLoad)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): Option[DataFrame] = {
//     import spark.implicits._
//     val startTime = System.currentTimeMillis() 
//     val stageDetail = new java.util.HashMap[String, Object]()
//     stageDetail.put("type", load.getType)
//     stageDetail.put("name", load.name)
//     for (description <- load.description) {
//       stageDetail.put("description", description)    
//     }
//     stageDetail.put("inputView", load.inputView)  
//     stageDetail.put("output", load.output)  
//     stageDetail.put("partitionBy", load.partitionBy.asJava)
//     stageDetail.put("saveMode", load.saveMode.toString.toLowerCase)
//     stageDetail.put("params", load.params.asJava)

//     val df = spark.table(load.inputView)      

//     load.numPartitions match {
//       case Some(partitions) => stageDetail.put("numPartitions", Integer.valueOf(partitions))
//       case None => stageDetail.put("numPartitions", Integer.valueOf(df.rdd.getNumPartitions))
//     }

//     logger.info()
//       .field("event", "enter")
//       .map("stage", stageDetail)      
//       .log()


//     val dropMap = new java.util.HashMap[String, Object]()

//     // elasticsearch cannot support a column called _index
//     val unsupported = df.schema.filter( _.name == "_index").map(_.name)
//     if (!unsupported.isEmpty) {
//       dropMap.put("Unsupported", unsupported.asJava)
//     }

//     stageDetail.put("drop", dropMap)    
    
//     val nonNullDF = df.drop(unsupported:_*)

//     val listener = ListenerUtils.addStageCompletedListener(stageDetail)

//     // Elasticsearch will convert date and times to epoch milliseconds
//     val outputDF = try {
//       load.partitionBy match {
//         case Nil =>
//           val dfToWrite = load.numPartitions.map(nonNullDF.repartition(_)).getOrElse(nonNullDF)
//           dfToWrite.write.options(load.params).mode(load.saveMode).format("org.elasticsearch.spark.sql").save(load.output)
//           dfToWrite
//         case partitionBy => {
//           // create a column array for repartitioning
//           val partitionCols = partitionBy.map(col => nonNullDF(col))
//           load.numPartitions match {
//             case Some(n) =>
//               val dfToWrite = nonNullDF.repartition(n, partitionCols:_*)
//               dfToWrite.write.options(load.params).partitionBy(partitionBy:_*).mode(load.saveMode).format("org.elasticsearch.spark.sql").save(load.output)
//               dfToWrite
//             case None =>
//               val dfToWrite = nonNullDF.repartition(partitionCols:_*)
//               dfToWrite.write.options(load.params).partitionBy(partitionBy:_*).mode(load.saveMode).format("org.elasticsearch.spark.sql").save(load.output)
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