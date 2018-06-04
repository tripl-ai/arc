package au.com.agl.arc.extract

import java.lang._
import java.net.URI
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.util._

object XMLExtract {

  def extract(extract: XMLExtract)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): DataFrame = {
    import spark.implicits._
    val startTime = System.currentTimeMillis() 
    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", extract.getType)
    stageDetail.put("name", extract.name)
    stageDetail.put("input", extract.input.toString)  
    stageDetail.put("outputView", extract.outputView)  
    stageDetail.put("persist", Boolean.valueOf(extract.persist))

    val options = ConfigUtils.paramsToOptions(extract.params, "rowTag" :: Nil)

    stageDetail.put("options", options.asJava)

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()    

    // the xml reader does not yet support loading from a string dataset
    CloudUtils.setHadoopConfiguration(extract.authentication)
    val df = try {
      spark.read.format("com.databricks.spark.xml").options(options).load(extract.input.toString)     
    } catch {
      case e: org.apache.hadoop.mapreduce.lib.input.InvalidInputException if (e.getMessage.contains("matches 0 files")) => {
        spark.emptyDataFrame
      }         
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stageDetail          
      }      
    }

    // if incoming dataset has 0 columns then create empty dataset with correct schema
    val emptyDataframeHandlerDF = if (df.schema.length == 0) {
      val schema = extract.cols match {
        case Nil => throw new Exception(s"XMLExtract has produced 0 columns and no schema has been provided to create an empty dataframe.") with DetailException {
          override val detail = stageDetail          
        }
        case cols => Extract.toStructType(cols)
      }
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    } else {
      df
    }

    // add meta columns
    val enrichedDF = emptyDataframeHandlerDF
      .withColumn("_index",monotonically_increasing_id().as("_index", new MetadataBuilder().putBoolean("internal", true).build()))
      .withColumn("_filename", input_file_name().as("_filename", new MetadataBuilder().putBoolean("internal", true).build()))
         
    // repartition to distribute rows evenly
    val repartitionedDF = enrichedDF.repartition(spark.sparkContext.defaultParallelism * 4)  
    repartitionedDF.createOrReplaceTempView(extract.outputView)

    stageDetail.put("inputFiles", Integer.valueOf(repartitionedDF.inputFiles.length))
    stageDetail.put("outputColumns", Integer.valueOf(repartitionedDF.schema.length))

    if (extract.persist) {
      repartitionedDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
      stageDetail.put("records", Long.valueOf(repartitionedDF.count)) 
    }    

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stageDetail)      
      .log() 

    repartitionedDF
  }

}

