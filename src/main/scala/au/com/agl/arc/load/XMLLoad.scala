package au.com.agl.arc.load

import java.net.URI
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import au.com.agl.arc.api.API._
import au.com.agl.arc.util._

object XMLLoad {

  def load(load: XMLLoad)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Option[DataFrame] = {
    val startTime = System.currentTimeMillis() 
    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", load.getType)
    stageDetail.put("name", load.name)
    stageDetail.put("inputView", load.inputView)  
    stageDetail.put("outputURI", load.outputURI.toString)  
    stageDetail.put("partitionBy", load.partitionBy.asJava)

    val saveMode = load.saveMode.getOrElse(SaveMode.Overwrite)
    stageDetail.put("saveMode", saveMode.toString)     

    val df = spark.table(load.inputView) 

    load.numPartitions match {
      case Some(partitions) => stageDetail.put("numPartitions", Integer.valueOf(partitions))
      case None => stageDetail.put("numPartitions", Integer.valueOf(df.rdd.getNumPartitions))
    }

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()


    // set write permissions
    CloudUtils.setHadoopConfiguration(load.authentication)
     
    val dropMap = new java.util.HashMap[String, Object]()

    // XML does not need to deal with NullType as it is silenty dropped on write but we want logging to be explicit
    val nulls = df.schema.filter( _.dataType == NullType).map(_.name)
    if (!nulls.isEmpty) {
      dropMap.put("NullType", nulls.asJava)
    }

    stageDetail.put("drop", dropMap) 

    try {
      load.partitionBy match {
        case Nil => { 
          load.numPartitions match {
            case Some(n) => df.repartition(n).write.format("com.databricks.spark.xml").mode(saveMode).save(load.outputURI.toString)
            case None => df.write.format("com.databricks.spark.xml").mode(saveMode).save(load.outputURI.toString)  
          }   
        }
        case partitionBy => {
          // create a column array for repartitioning
          val partitionCols = partitionBy.map(col => df(col))
          load.numPartitions match {
            case Some(n) => df.repartition(n, partitionCols:_*).write.format("com.databricks.spark.xml").partitionBy(partitionBy:_*).mode(saveMode).save(load.outputURI.toString)
            case None => df.repartition(partitionCols:_*).write.format("com.databricks.spark.xml").partitionBy(partitionBy:_*).mode(saveMode).save(load.outputURI.toString)
          }   
        }
      }    
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stageDetail          
      }      
    }           

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stageDetail)      
      .log()

    Option(df)
  }
}