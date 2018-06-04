package au.com.agl.arc.util

import org.apache.spark.sql._
import org.apache.spark.scheduler._

object ListenerUtils {
  def addListeners()(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger) = {

    spark.sparkContext.addSparkListener(new SparkListener() {
      
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

        logger.debug()
          .field("event", "SparkListenerStageCompleted")
          .field("type", "SparkListener")
          .field("name", stageInfo.name)
          .field("numTasks", stageInfo.numTasks)
          .field("executorCpuTime", taskMetrics.executorCpuTime)
          .map("inputMetrics", inputMetricsMap)
          .map("outputMetrics", outputMetricsMap)
          .map("executorMemoryStatus", executorMemoryStatusMap)
          .log()  
      }

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

    })
  }
}
