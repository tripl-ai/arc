package ai.tripl.arc.extract

import java.lang._
import java.net.URI
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.util._

import org.elasticsearch.spark.sql._ 

object ElasticsearchExtract {

  def extract(extract: ElasticsearchExtract)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    import spark.implicits._
    val startTime = System.currentTimeMillis() 
    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", extract.getType)
    stageDetail.put("name", extract.name)
    for (description <- extract.description) {
      stageDetail.put("description", description)    
    }     
    stageDetail.put("outputView", extract.outputView)  
    stageDetail.put("params", extract.params)

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()

    // if incoming dataset is empty create empty dataset with a known schema
    val df = try {
      if (arcContext.isStreaming) {
        spark.emptyDataFrame
      } else {      
        spark.read.format("org.elasticsearch.spark.sql").options(extract.params).load(extract.input)
      }
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stageDetail          
      }
    }    

    // repartition to distribute rows evenly
    val repartitionedDF = extract.partitionBy match {
      case Nil => { 
        extract.numPartitions match {
          case Some(numPartitions) => df.repartition(numPartitions)
          case None => df
        }   
      }
      case partitionBy => {
        // create a column array for repartitioning
        val partitionCols = partitionBy.map(col => df(col))
        extract.numPartitions match {
          case Some(numPartitions) => df.repartition(numPartitions, partitionCols:_*)
          case None => df.repartition(partitionCols:_*)
        }
      }
    } 
    repartitionedDF.createOrReplaceTempView(extract.outputView)
    
    if (!repartitionedDF.isStreaming) {
      stageDetail.put("inputFiles", Integer.valueOf(repartitionedDF.inputFiles.length))
      stageDetail.put("outputColumns", Integer.valueOf(repartitionedDF.schema.length))
      stageDetail.put("numPartitions", Integer.valueOf(repartitionedDF.rdd.partitions.length))

      if (extract.persist) {
        repartitionedDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
        stageDetail.put("records", Long.valueOf(repartitionedDF.count)) 
      }      
    }

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stageDetail)      
      .log() 

    Option(repartitionedDF)
  }

}