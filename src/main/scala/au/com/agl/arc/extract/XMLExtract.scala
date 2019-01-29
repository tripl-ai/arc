package au.com.agl.arc.extract

import java.lang._
import java.net.URI
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import com.databricks.spark.xml._

import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.util._

object XMLExtract {

  def extract(extract: XMLExtract)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Option[DataFrame] = {
    // force com.sun.xml.* implementation for reading xml to be compatible with spark-xml library
    System.setProperty("javax.xml.stream.XMLInputFactory", "com.sun.xml.internal.stream.XMLInputFactoryImpl")

    import spark.implicits._
    val startTime = System.currentTimeMillis() 
    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", extract.getType)
    stageDetail.put("name", extract.name)
    stageDetail.put("input", extract.input)  
    stageDetail.put("outputView", extract.outputView)  
    stageDetail.put("persist", Boolean.valueOf(extract.persist))
    stageDetail.put("contiguousIndex", Boolean.valueOf(extract.contiguousIndex))

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()    

    // try to get the schema
    val optionSchema = try {
      ExtractUtils.getSchema(extract.cols)(spark, logger)
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stageDetail          
      }      
    } 

    val df = try {
      extract.input match {
        case Right(glob) => {
          CloudUtils.setHadoopConfiguration(extract.authentication)

          // remove the crlf delimiter so it is read as a full object per file
          val oldDelimiter = spark.sparkContext.hadoopConfiguration.get("textinputformat.record.delimiter")
          val newDelimiter = s"${0x0 : Char}"
          // temporarily remove the delimiter so all the data is loaded as a single line
          spark.sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", newDelimiter)              

          // read the file but do not cache. caching will break the input_file_name() function
          val textFile = spark.sparkContext.textFile(glob)

          val xmlReader = new XmlReader
          val xml = xmlReader.xmlRdd(spark.sqlContext, textFile)

          // reset delimiter
          if (oldDelimiter == null) {
            spark.sparkContext.hadoopConfiguration.unset("textinputformat.record.delimiter")              
          } else {
            spark.sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", oldDelimiter)        
          }

          xml            
        }
        case Left(view) => {
          val xmlReader = new XmlReader
          xmlReader.xmlRdd(spark.sqlContext, spark.table(view).as[String].rdd)
        }
      }     
    } catch {
      case e: org.apache.hadoop.mapred.InvalidInputException if (e.getMessage.contains("matches 0 files")) => {
        spark.emptyDataFrame
      }         
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stageDetail          
      }      
    }

    // if incoming dataset has 0 columns then create empty dataset with correct schema
    val emptyDataframeHandlerDF = try {
      if (df.schema.length == 0) {
        stageDetail.put("records", Integer.valueOf(0))
        optionSchema match {
          case Some(s) => spark.createDataFrame(spark.sparkContext.emptyRDD[Row], s)
          case None => throw new Exception(s"XMLExtract has produced 0 columns and no schema has been provided to create an empty dataframe.")
        }
      } else {
        df
      }
    } catch {
      case e: Exception => throw new Exception(e.getMessage) with DetailException {
        override val detail = stageDetail          
      }      
    }

    // try to explode the rows returned by the XML reader
    val flattenedDF = if (emptyDataframeHandlerDF.schema.length == 1) {
      emptyDataframeHandlerDF.schema.fields(0).dataType.typeName match {
        case "array" => emptyDataframeHandlerDF.select(explode(col(emptyDataframeHandlerDF.schema.fieldNames(0)))).select("col.*")
        case "struct" => emptyDataframeHandlerDF.select(s"${emptyDataframeHandlerDF.schema.fieldNames(0)}.*")
        case _ => emptyDataframeHandlerDF
      }
    } else {
      emptyDataframeHandlerDF
    }    

    // add internal columns data _filename, _index
    val sourceEnrichedDF = ExtractUtils.addInternalColumns(flattenedDF, extract.contiguousIndex)

    // set column metadata if exists
    val enrichedDF = optionSchema match {
        case Some(schema) => MetadataUtils.setMetadata(sourceEnrichedDF, schema)
        case None => sourceEnrichedDF   
    }
         
    // repartition to distribute rows evenly
    val repartitionedDF = extract.partitionBy match {
      case Nil => { 
        extract.numPartitions match {
          case Some(numPartitions) => enrichedDF.repartition(numPartitions)
          case None => enrichedDF
        }   
      }
      case partitionBy => {
        // create a column array for repartitioning
        val partitionCols = partitionBy.map(col => df(col))
        extract.numPartitions match {
          case Some(numPartitions) => enrichedDF.repartition(numPartitions, partitionCols:_*)
          case None => enrichedDF.repartition(partitionCols:_*)
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

