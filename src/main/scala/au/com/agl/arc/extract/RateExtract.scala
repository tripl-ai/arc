package au.com.agl.arc.extract

import java.lang._
import java.net.URI
import java.util.Properties
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.util._

object RateExtract {

  def extract(extract: RateExtract)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    import spark.implicits._
    val startTime = System.currentTimeMillis() 

    val rowsPerSecond = extract.rowsPerSecond.getOrElse(1)
    val rampUpTime = extract.rampUpTime.getOrElse(0)
    val numPartitions = extract.numPartitions.getOrElse(spark.sparkContext.defaultParallelism)

    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", extract.getType)
    stageDetail.put("name", extract.name)
    stageDetail.put("outputView", extract.outputView)  
    stageDetail.put("rowsPerSecond", Integer.valueOf(rowsPerSecond))
    stageDetail.put("rampUpTime", Integer.valueOf(rampUpTime))
    stageDetail.put("numPartitions", Integer.valueOf(numPartitions))

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
      .option("rowsPerSecond", rowsPerSecond.toString)
      .option("rampUpTime", s"${rampUpTime}s")
      .option("numPartitions", numPartitions.toString)
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

