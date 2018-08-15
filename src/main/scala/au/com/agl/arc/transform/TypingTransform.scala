package au.com.agl.arc.transform

import java.lang._
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

import au.com.agl.arc.api.API._
import au.com.agl.arc.util._

object TypingTransform {

  def transform(transform: TypingTransform)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Option[DataFrame] = {
    val startTime = System.currentTimeMillis() 
    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", transform.getType)
    stageDetail.put("name", transform.name)
    stageDetail.put("outputView", transform.outputView)   
    stageDetail.put("columns", transform.cols.map(_.name).asJava)
    stageDetail.put("persist", Boolean.valueOf(transform.persist))

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()   
 

    val df = spark.table(transform.inputView)

    // get schema length filtering out any internal fields
    val schemaLength = df.schema.filter(row => { 
      !row.metadata.contains("internal") || (row.metadata.contains("internal") && row.metadata.getBoolean("internal") == false) 
    }).length

    if (schemaLength != transform.cols.length) {
      stageDetail.put("columnCount", Integer.valueOf(transform.cols.length))
      stageDetail.put("schemaLength", Integer.valueOf(schemaLength))

      throw new Exception(s"TypingTransform can only be performed on tables with the same number of columns, but the schema has ${transform.cols.length} columns and the data table has ${schemaLength} columns.") with DetailException {
        override val detail = stageDetail          
      }    
    }

    val transformedDF = try {
      Typing.typeDataFrame(df, transform)
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

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stageDetail)      
      .log()  

    Option(transformedDF)
  }

}
