package au.com.agl.arc.extract

import java.lang._

import org.apache.spark.sql.{DataFrame, SparkSession}
import au.com.agl.arc.api.API._
import au.com.agl.arc.util.{CloudUtils, DetailException, ExtractUtils}
import org.apache.spark.sql.types.{BinaryType, StringType}
import org.apache.spark.storage.StorageLevel

object BytesExtract {

  def extract(extract: BytesExtract)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Option[DataFrame] = {
    import spark.implicits._
    val startTime = System.currentTimeMillis()
    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", extract.getType)
    stageDetail.put("name", extract.name)
    for (description <- extract.description) {
      stageDetail.put("description", description)    
    }     
    stageDetail.put("outputView", extract.outputView)
    stageDetail.put("persist", Boolean.valueOf(extract.persist))

    val inputValue = extract.input match {
      case Left(view) => view
      case Right(glob) => glob
    }

    stageDetail.put("input", inputValue)  

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)
      .log()

    val signature = "BytesExtract requires pathView to be dataset with [value: string] signature."

    CloudUtils.setHadoopConfiguration(extract.authentication)

    val df = try {
      extract.input match {
        case Left(view) => {
          val pathView = spark.table(view)
          val schema = pathView.schema

          val fieldIndex = try {
            schema.fieldIndex("value")
          } catch {
            case e: Exception => throw new Exception(s"""${signature} inputView has: [${pathView.schema.map(_.name).mkString(", ")}].""") with DetailException {
              override val detail = stageDetail
            }
          }

          schema.fields(fieldIndex).dataType match {
            case _: StringType =>
            case _ => throw new Exception(s"""${signature} 'value' is of type: '${schema.fields(fieldIndex).dataType.simpleString}'.""") with DetailException {
              override val detail = stageDetail
            }
          }

          val path = pathView.select($"value").collect().map( _.getString(0) ).mkString(",")
          spark.read.format("bytes").load(path)
        }
        case Right(glob) => {
          spark.read.format("bytes").load(glob)          
        }
      }
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stageDetail
      }
    }

    // datasource already has a _filename column so no need to add internal columns

    // repartition to distribute rows evenly
    val repartitionedDF = extract.numPartitions match {
      case Some(numPartitions) => df.repartition(numPartitions)
      case None => df
    }
    repartitionedDF.createOrReplaceTempView(extract.outputView)

    stageDetail.put("inputFiles", Integer.valueOf(repartitionedDF.inputFiles.length))
    stageDetail.put("outputColumns", Integer.valueOf(repartitionedDF.schema.length))
    stageDetail.put("numPartitions", Integer.valueOf(repartitionedDF.rdd.partitions.length))

    if (extract.persist && !repartitionedDF.isStreaming) {
      repartitionedDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
      stageDetail.put("records", java.lang.Long.valueOf(repartitionedDF.count))
    }

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stageDetail)
      .log()

    Option(repartitionedDF)
  }

}
