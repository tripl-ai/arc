package au.com.agl.arc.transform

import java.lang._
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

import au.com.agl.arc.api.API._
import au.com.agl.arc.util._

object SQLTransform {

  def transform(transform: SQLTransform)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Option[DataFrame] = {
    val startTime = System.currentTimeMillis() 
    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", transform.getType)
    stageDetail.put("name", transform.name)
    stageDetail.put("inputURI", transform.inputURI.toString)  
    stageDetail.put("outputView", transform.outputView)   
    stageDetail.put("sqlParams", transform.sqlParams.asJava)   
    stageDetail.put("persist", Boolean.valueOf(transform.persist))

    // inject sql parameters
    val stmt = SQLUtils.injectParameters(transform.sql, transform.sqlParams)
    stageDetail.put("sql", stmt)

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()      
    
    val transformedDF = try {
      spark.sql(stmt)
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stageDetail          
      }      
    }  

    transformedDF.createOrReplaceTempView(transform.outputView)

    if (transform.persist) {
      transformedDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
      stageDetail.put("records", Long.valueOf(transformedDF.count)) 
    }    

    // add partition and predicate pushdown detail to logs
    if (transform.persist && !transformedDF.isStreaming) {
      stageDetail.put("partitionFilters", QueryExecutionUtils.getPartitionFilters(transformedDF.queryExecution.executedPlan).toArray)
      stageDetail.put("dataFilters", QueryExecutionUtils.getDataFilters(transformedDF.queryExecution.executedPlan).toArray)
    }

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stageDetail)      
      .log()  

    Option(transformedDF)
  }

}
