package ai.tripl.arc.plugins.lifecycle

import java.util

import org.apache.spark.sql.{DataFrame, SparkSession}

import ai.tripl.arc.api.API._
import ai.tripl.arc.plugins.LifecyclePlugin
import ai.tripl.arc.util.Utils
import ai.tripl.arc.util.log.logger.Logger

class DataFramePrinterLifecyclePlugin extends LifecyclePlugin {

  var params = Map[String, String]()

  override def setParams(p: Map[String, String]) {
    params = p
  }

  override def before(stage: PipelineStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger) {
    logger.trace()        
      .field("event", "before")
      .field("stage", stage.name)
      .field("stageType", stage.getType)
      .log()  
  }

  override def after(stage: PipelineStage, result: Option[DataFrame], isLast: Boolean)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger) {
    logger.trace()        
      .field("event", "after")
      .field("stage", stage.name)
      .field("stageType", stage.getType)
      .field("isLast", java.lang.Boolean.valueOf(isLast))
      .log() 

    result match {
      case Some(df) => {
        val numRows = params.get("numRows") match {
          case Some(n) => n.toInt
          case None => 20
        }

        val truncate = params.get("truncate") match {
          case Some(t) => t.toBoolean
          case None => true
        }  

        df.show(numRows, truncate)
      }
      case None =>
    }
  }

}