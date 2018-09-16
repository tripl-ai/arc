package au.com.agl.arc.transform

import java.lang._
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import au.com.agl.arc.api.API._
import au.com.agl.arc.util._

object DiffTransform {

  def transform(transform: DiffTransform)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Option[DataFrame] = {
    val startTime = System.currentTimeMillis() 
    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", transform.getType)
    stageDetail.put("name", transform.name)
    stageDetail.put("inputLeftView", transform.inputLeftView)  
    stageDetail.put("inputRightView", transform.inputRightView)   
    stageDetail.put("persist", Boolean.valueOf(transform.persist))

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()      
    
    val inputLeftDF = spark.table(transform.inputLeftView)   
    val inputRightDF = spark.table(transform.inputRightView)   

    // do a full join on a calculated hash of all values in row on each dataset
    // trying to calculate the hash value inside the joinWith method produced an inconsistent result

    
    val leftHashDF = inputLeftDF.withColumn("_hash", sha2(to_json(struct(inputLeftDF.columns.map(col):_*)),512))
    val rightHashDF = inputRightDF.withColumn("_hash", sha2(to_json(struct(inputRightDF.columns.map(col):_*)),512))
    val transformedDF = leftHashDF.joinWith(rightHashDF, leftHashDF("_hash") === rightHashDF("_hash"), "full")

    if (transform.persist && !transformedDF.isStreaming) {
      transformedDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
    }   

    val outputIntersectionDF = transformedDF.filter(col("_1").isNotNull).filter(col("_2").isNotNull).select(col("_1.*")).drop("_hash")
    val outputLeftDF = transformedDF.filter(col("_2").isNull).select(col("_1.*")).drop("_hash")
    val outputRightDF = transformedDF.filter(col("_1").isNull).select(col("_2.*")).drop("_hash")

    // register views
    transform.outputIntersectionView match {
      case Some(oiv) => outputIntersectionDF.createOrReplaceTempView(oiv)
      case None => 
    }
    transform.outputLeftView match {
      case Some(olv) => outputLeftDF.createOrReplaceTempView(olv)
      case None => 
    }
    transform.outputRightView match {
      case Some(orv) => outputRightDF.createOrReplaceTempView(orv)
      case None => 
    }    

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stageDetail)      
      .log()  

    Option(outputIntersectionDF)
  }

}
