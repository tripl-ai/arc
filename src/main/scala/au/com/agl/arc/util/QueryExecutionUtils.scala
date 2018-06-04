package au.com.agl.arc.util

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.FileSourceScanExec

import org.apache.spark.sql._

object QueryExecutionUtils {

  def getPartitionFilters(plan: SparkPlan): List[String] = {
    plan.collect { case a: FileSourceScanExec => a }
      .flatMap(fileSourceScanExec => 
        fileSourceScanExec
          .partitionFilters
          .toList
      ).toList
      .map(_.toString)
  }

  def getDataFilters(plan: SparkPlan): List[String] = {
    plan.collect { case a: FileSourceScanExec => a }
      .flatMap(fileSourceScanExec => fileSourceScanExec.dataFilters)
      .toList
      .map(_.toString)
  }

}
