package ai.tripl.arc.extract

import java.lang._
import java.net.URI
import java.util.Properties
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.util._

object RateExtract {

  def extract(extract: RateExtract)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    import spark.implicits._
    val startTime = System.currentTimeMillis() 
    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", extract.getType)
    stageDetail.put("name", extract.name)
    for (description <- extract.description) {
      stageDetail.put("description", description)    
    }  
    stageDetail.put("outputView", extract.outputView)  
    stageDetail.put("rowsPerSecond", Integer.valueOf(extract.rowsPerSecond))
    stageDetail.put("rampUpTime", Integer.valueOf(extract.rampUpTime))
    stageDetail.put("numPartitions", Integer.valueOf(extract.numPartitions))

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()    

    if (!arcContext.isStreaming) {
      throw new Exception("RateExtract can only be executed in streaming mode.") with DetailException {
        override val detail = stageDetail          
      }
    }

    val df = spark.readStream
      .format("rate")
      .option("rowsPerSecond", extract.rowsPerSecond.toString)
      .option("rampUpTime", s"${extract.rampUpTime}s")
      .option("numPartitions", extract.numPartitions.toString)
      .load

    df.createOrReplaceTempView(extract.outputView)

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stageDetail)      
      .log()   

    Option(df)
  }

}

