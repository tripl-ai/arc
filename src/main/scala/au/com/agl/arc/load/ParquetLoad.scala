package au.com.agl.arc.load

import java.net.URI
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import au.com.agl.arc.api.API._
import au.com.agl.arc.util._

object ParquetLoad {

  def load(load: ParquetLoad)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Option[DataFrame] = {
    val startTime = System.currentTimeMillis() 
    var stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", load.getType)
    stageDetail.put("name", load.name)
    for (description <- load.description) {
      stageDetail.put("description", description)    
    }
    stageDetail.put("inputView", load.inputView)  
    stageDetail.put("outputURI", load.outputURI.toString)  
    stageDetail.put("partitionBy", load.partitionBy.asJava)
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

    // set write permissions
    CloudUtils.setHadoopConfiguration(load.authentication)

    val dropMap = new java.util.HashMap[String, Object]()

    // Parquet cannot handle a column of NullType
    val nulls = df.schema.filter( _.dataType == NullType).map(_.name)
    if (!nulls.isEmpty) {
      dropMap.put("NullType", nulls.asJava)
    }

    stageDetail.put("drop", dropMap)

    val nonNullDF = df.drop(nulls:_*)

    val listener = ListenerUtils.addStageCompletedListener(stageDetail)

    try {
      if (nonNullDF.isStreaming) {
        load.partitionBy match {
          case Nil => nonNullDF.writeStream.format("parquet").option("path", load.outputURI.toString).start
          case partitionBy => {
            val partitionCols = partitionBy.map(col => nonNullDF(col))
            nonNullDF.writeStream.partitionBy(partitionBy:_*).format("parquet").option("path", load.outputURI.toString).start
          }
        }
      } else {
        load.partitionBy match {
          case Nil => {
            load.numPartitions match {
              case Some(n) => nonNullDF.repartition(n).write.mode(load.saveMode).parquet(load.outputURI.toString)
              case None => nonNullDF.write.mode(load.saveMode).parquet(load.outputURI.toString)
            }
          }
          case partitionBy => {
            // create a column array for repartitioning
            val partitionCols = partitionBy.map(col => nonNullDF(col))
            load.numPartitions match {
              case Some(n) => nonNullDF.repartition(n, partitionCols:_*).write.partitionBy(partitionBy:_*).mode(load.saveMode).parquet(load.outputURI.toString)
              case None => nonNullDF.repartition(partitionCols:_*).write.partitionBy(partitionBy:_*).mode(load.saveMode).parquet(load.outputURI.toString)
            }
          }
        }
      }
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stageDetail
      }
    }

    spark.sparkContext.removeSparkListener(listener)           

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stageDetail)      
      .log()

    Option(nonNullDF)
  }
}