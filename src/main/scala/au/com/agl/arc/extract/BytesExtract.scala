package au.com.agl.arc.extract

import org.apache.spark.sql.{DataFrame, SparkSession}
import au.com.agl.arc.api.API._
import au.com.agl.arc.util.{CloudUtils, DetailException, ExtractUtils}
import org.apache.spark.storage.StorageLevel

object BytesExtract {

  def extract(extract: BytesExtract)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Option[DataFrame] = {

    val startTime = System.currentTimeMillis()
    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", extract.getType)
    stageDetail.put("name", extract.name)
    stageDetail.put("input", extract.input.toString)
    stageDetail.put("outputView", extract.outputView)
    stageDetail.put("persist", java.lang.Boolean.valueOf(extract.persist))

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)
      .log()

    CloudUtils.setHadoopConfiguration(extract.authentication)

    val df = try {
      spark.read.format("bytes").load(extract.input)
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stageDetail
      }
    }

    // add source data including index
    val enrichedDF = ExtractUtils.addSourceMetadata(df, extract.contiguousIndex.getOrElse(true))

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
