package au.com.agl.arc.extract

import java.lang._
import java.net.URI
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.util._

object DelimitedExtract {

  def extract(extract: DelimitedExtract)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): DataFrame = {
    import spark.implicits._
    val startTime = System.currentTimeMillis() 
    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", extract.getType)
    stageDetail.put("name", extract.name)
    stageDetail.put("persist", Boolean.valueOf(extract.persist))
    stageDetail.put("outputView", extract.outputView)

    val options: Map[String, String] = Delimited.toSparkOptions(extract.settings)

    val inputValue = extract.input match {
      case Right(uri) => uri.toString
      case Left(view) => view
    }

    stageDetail.put("input", inputValue)  
    stageDetail.put("options", options.asJava)

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()    
    
    val df = try {
      extract.input match {
        case Right(uri) =>
          CloudUtils.setHadoopConfiguration(extract.authentication)

          try {
            spark.read.options(options).csv(uri.toString)
          } catch {
            case e: AnalysisException if (e.getMessage == "Unable to infer schema for CSV. It must be specified manually.;") || (e.getMessage.contains("Path does not exist")) => 
              spark.emptyDataFrame
            case e: Exception => throw e
          }
        case Left(view) => spark.read.options(options).csv(spark.table(view).as[String])
      }         
    } catch { 
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stageDetail          
      }    
    }

    // if incoming dataset has 0 columns then create empty dataset with correct schema
    val emptyDataframeHandlerDF = if (df.schema.length == 0) {
      val schema = extract.cols match {
        case Nil => throw new Exception(s"DelimitedExtract has produced 0 columns and no schema has been provided to create an empty dataframe.") with DetailException {
          override val detail = stageDetail          
        }
        case cols => Extract.toStructType(cols)
      }
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    } else {
      df
    }        

    // add meta columns including sequential index
    // if schema already has metadata columns ignore
    val enrichedDF = if (!emptyDataframeHandlerDF.schema.map(_.name).contains("_index")) {
      val window = Window.partitionBy("_filename").orderBy("_monotonically_increasing_id")
      emptyDataframeHandlerDF
        .withColumn("_monotonically_increasing_id", monotonically_increasing_id())
        .withColumn("_filename", input_file_name().as("_filename", new MetadataBuilder().putBoolean("internal", true).build()))
        .withColumn("_index", row_number().over(window).as("_index", new MetadataBuilder().putBoolean("internal", true).build()))
        .drop("_monotonically_increasing_id")
    } else {
      emptyDataframeHandlerDF
    }
         
    // repartition to distribute rows evenly
    val repartitionedDF = extract.numPartitions match {
      case Some(numPartitions) => enrichedDF.repartition(numPartitions)
      case None => enrichedDF
    }
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