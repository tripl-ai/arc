package au.com.agl.arc.extract

import java.lang._
import java.net.URI
import java.util.Properties
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.util._

object JSONExtract {

  def extract(extract: JSONExtract)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): DataFrame = {
    import spark.implicits._
    val startTime = System.currentTimeMillis() 
    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", extract.getType)
    stageDetail.put("name", extract.name)
    stageDetail.put("input", extract.input.toString)  
    stageDetail.put("outputView", extract.outputView)  
    stageDetail.put("persist", Boolean.valueOf(extract.persist))

    val options: Map[String, String] = JSON.toSparkOptions(extract.settings)

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
          // spark does not cope well reading many small files into json directly from hadoop file systems
          // by reading first as text time drops by ~75%
          // this will not throw an error for empty directory (but will for missing directory)
          try {
            if (extract.settings.multiLine ) {

              // if multiLine then remove the crlf delimiter so it is read as a full object per file
              val oldDelimiter = spark.sparkContext.hadoopConfiguration.get("textinputformat.record.delimiter")
              val newDelimiter = s"${0x0 : Char}"
              // temporarily remove the delimiter so all the data is loaded as a single line
              spark.sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", newDelimiter)              

              // read the file but do not cache. caching will break the input_file_name() function
              val textFile = spark.sparkContext.textFile(uri.toString)
              val json = spark.read.options(options).json(textFile.toDS)

              // reset delimiter
              if (oldDelimiter == null) {
                spark.sparkContext.hadoopConfiguration.unset("textinputformat.record.delimiter")              
              } else {
                spark.sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", oldDelimiter)        
              }

              json
            } else {
              // read the file but do not cache. caching will break the input_file_name() function              
              val textFile = spark.sparkContext.textFile(uri.toString)
              val json = spark.read.options(options).json(textFile.toDS)

              json              
            }
          } catch {
            case e: org.apache.hadoop.mapred.InvalidInputException => {
              spark.emptyDataFrame
            }
            case e: Exception => throw e
          }
        case Left(view) => spark.read.options(options).json(spark.table(view).as[String])
      }
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stageDetail          
      }
    }

    // if incoming dataset has 0 columns then create empty dataset with correct schema
    val emptyDataframeHandlerDF = if (df.schema.length == 0) {
      val schema = extract.cols match {
        case Nil => throw new Exception(s"JSONExtract has produced 0 columns and no schema has been provided to create an empty dataframe.") with DetailException {
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
    val repartitionedDF = extract.numPartitions match {
      case Some(numPartitions) => enrichedDF.repartition(numPartitions)
      case None => enrichedDF.repartition(spark.sparkContext.defaultParallelism * 4)
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

