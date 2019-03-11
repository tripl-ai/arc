package au.com.agl.arc.load

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import au.com.agl.arc.api.API._
import au.com.agl.arc.util._

object TextLoad {

  def load(load: TextLoad)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Option[DataFrame] = {
    val startTime = System.currentTimeMillis() 
    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", load.getType)
    stageDetail.put("name", load.name)
    for (description <- load.description) {
      stageDetail.put("description", description)    
    }    
    stageDetail.put("inputView", load.inputView)  
    stageDetail.put("outputURI", load.outputURI.toString)  
    stageDetail.put("saveMode", load.saveMode.toString.toLowerCase)

    val df = spark.table(load.inputView)      

    if (!df.isStreaming) {
      load.numPartitions match {
        case Some(partitions) => stageDetail.put("numPartitions", Integer.valueOf(partitions))
        case None => stageDetail.put("numPartitions", Integer.valueOf(df.rdd.getNumPartitions))
      }
    }

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()

    if (df.schema.length != 1 || df.schema.fields(0).dataType != StringType) {
      throw new Exception(s"""TextLoad supports only a single text column but the input view has ${df.schema.length} columns.""") with DetailException {
        override val detail = stageDetail          
      } 
    }      

    // set write permissions
    CloudUtils.setHadoopConfiguration(load.authentication)

    try {
      if (load.singleFile) {
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        val path = new Path(load.outputURI)

        val outputStream = if (fs.exists(path)) {
          load.saveMode match {
            case SaveMode.ErrorIfExists => {
              throw new Exception(s"File '${path}' already exists and 'saveMode' equals 'ErrorIfExists' so cannot continue.")
            }
            case SaveMode.Ignore => {
              None
            }          
            case SaveMode.Overwrite => {
              fs.delete(path, false)
              Option(fs.create(path))
            }
            case SaveMode.Append => {
              throw new Exception(s"File '${path}' already exists and 'saveMode' equals 'Append' which is not supported with 'singleFile' mode.")
            }
          }
        } else {
          Option(fs.create(path))
        }

        outputStream match {
          case Some(os) => {
            os.writeBytes(df.collect.map(_.getString(0)).mkString(load.prefix, load.separator, load.suffix))
            os.close
          }
          case None =>
        }

        fs.close
      } else {
        // spark does not allow partitionBy when only single column dataframe
        load.numPartitions match {
          case Some(n) => df.repartition(n).write.mode(load.saveMode).text(load.outputURI.toString)
          case None => df.write.mode(load.saveMode).text(load.outputURI.toString)
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
