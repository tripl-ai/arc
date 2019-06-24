// package ai.tripl.arc.transform

// import java.lang._
// import scala.collection.JavaConverters._

// import org.apache.spark.sql._
// import org.apache.spark.storage.StorageLevel

// import ai.tripl.arc.api.API._
// import ai.tripl.arc.util._

// object MetadataFilterTransform {

//   def transform(transform: MetadataFilterTransform)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): Option[DataFrame] = {
//     val startTime = System.currentTimeMillis() 
//     val stageDetail = new java.util.HashMap[String, Object]()
//     stageDetail.put("type", transform.getType)
//     stageDetail.put("name", transform.name)
//     for (description <- transform.description) {
//       stageDetail.put("description", description)    
//     }    

//     // inject sql parameters
//     val stmt = SQLUtils.injectParameters(transform.sql, transform.sqlParams, false)
//     stageDetail.put("sql", stmt)

//     logger.info()
//       .field("event", "enter")
//       .map("stage", stageDetail)      
//       .log()      
    
//     val df = spark.table(transform.inputView)
//     val metadataSchemaDF = MetadataUtils.createMetadataDataframe(df)
//     metadataSchemaDF.createOrReplaceTempView("metadata")

//     val filterDF = try {
//       spark.sql(stmt)
//     } catch {
//       case e: Exception => throw new Exception(e) with DetailException {
//         override val detail = stageDetail          
//       }      
//     }  

//     if (!filterDF.columns.contains("name")) {
//       throw new Exception("result does not contain field 'name' so cannot be filtered") with DetailException {
//         override val detail = stageDetail          
//       }    
//     }

//     // get fields that meet condition from query result
//     val inputFields = df.columns.toSet
//     val includeColumns = filterDF.collect.map(field => { field.getString(field.fieldIndex("name")) }).toSet
//     val excldueColumns = inputFields.diff(includeColumns)

//     stageDetail.put("includedColumns", includeColumns.asJava)
//     stageDetail.put("excludedColumns", excldueColumns.asJava)

//     // drop fields in the excluded set
//     val transformedDF = df.drop(excldueColumns.toList:_*)

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
