package ai.tripl.arc.util

import org.apache.spark.sql._
import org.apache.spark.scheduler._
import ai.tripl.arc.util.log.logger.JsonLogger

import org.apache.spark.scheduler.SparkListenerInterface

object ListenerUtils {

  def addExecutorListener()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): SparkListener = {

    val listener = new SparkListener() {
      
      override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded) {
      val executorInfo = executorAdded.executorInfo
        logger.debug()
          .field("event", "SparkListenerExecutorAdded")
          .field("type", "SparkListener")
          .field("executorId", executorAdded.executorId)
          .field("totalCores", executorInfo.totalCores)
          .field("executorHost", executorInfo.executorHost)
          .field("defaultParallelism", spark.sparkContext.defaultParallelism)
          .log()       
      }

      override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved) {
        logger.debug()
          .field("event", "SparkListenerExecutorRemoved")
          .field("type", "SparkListener")
          .field("executorId", executorRemoved.executorId)
          .field("reason", executorRemoved.reason)
          .log()       
      }

    }

    spark.sparkContext.addSparkListener(listener)
    listener
  }

  def addStageCompletedListener(stageDetail: scala.collection.mutable.Map[String,Object])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): SparkListener = {

    val listener = new SparkListener() {
              
      override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
        val stageInfo = stageCompleted.stageInfo
        val taskMetrics = stageInfo.taskMetrics
        val inputMetrics = taskMetrics.inputMetrics
        val outputMetrics = taskMetrics.outputMetrics

        // report nested Executor Memory Stats
        val executorMemoryStatus = spark.sparkContext.getExecutorMemoryStatus
        val executorMemoryStatusMap = new java.util.HashMap[String, java.util.HashMap[String, Long]]()
        for ((k,(v1, v2)) <- executorMemoryStatus) {
          val executorMemoryStatusMemoryMap = new java.util.HashMap[String, Long]
          executorMemoryStatusMemoryMap.put("total", v1)
          executorMemoryStatusMemoryMap.put("available", v2)
          executorMemoryStatusMap.put(k, executorMemoryStatusMemoryMap)
        }

        val inputMetricsMap = new java.util.HashMap[String, Long]()
        inputMetricsMap.put("bytesRead", inputMetrics.bytesRead)
        inputMetricsMap.put("recordsRead", inputMetrics.recordsRead)        

        val outputMetricsMap = new java.util.HashMap[String, Long]()
        outputMetricsMap.put("bytesWritten", outputMetrics.bytesWritten)
        outputMetricsMap.put("recordsWritten", outputMetrics.recordsWritten)               

        stageDetail.put("inputMetrics", inputMetricsMap)
        stageDetail.put("outputMetrics", outputMetricsMap)
        stageDetail.put("executorMemoryStatus", executorMemoryStatusMap)

        // logger.info()
        //   .field("event", "exit")
        //   .map("stage", stageDetail)      
        //   .log()        
      }

    }

    spark.sparkContext.addSparkListener(listener)
    listener
  } 

  def addTaskCompletedListener(stageDetail: java.util.HashMap[String, Object])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): SparkListener = {

    val listener = new SparkListener() {

      override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
        val taskInfo = taskEnd.taskInfo
        val taskMetrics = taskEnd.taskMetrics
        val inputMetrics = taskMetrics.inputMetrics
        val outputMetrics = taskMetrics.outputMetrics

        val inputMetricsMap = new java.util.HashMap[String, Long]()
        inputMetricsMap.put("bytesRead", inputMetrics.bytesRead)
        inputMetricsMap.put("recordsRead", inputMetrics.recordsRead)        

        val outputMetricsMap = new java.util.HashMap[String, Long]()
        outputMetricsMap.put("bytesWritten", outputMetrics.bytesWritten)
        outputMetricsMap.put("recordsWritten", outputMetrics.recordsWritten)       

        logger.debug()
          .field("event", "SparkListenerTaskEnd")
          .field("type", "SparkListener")
          .field("stageId", taskEnd.stageId)
          .field("executorId", taskInfo.executorId)
          .field("host", taskInfo.host)
          .field("status", taskInfo.status)
          .field("attemptNumber", taskInfo.attemptNumber)
          .map("inputMetrics", inputMetricsMap)
          .map("outputMetrics", outputMetricsMap)
          .log()  
      }

    }

    spark.sparkContext.addSparkListener(listener)
    listener    
  }   

}
