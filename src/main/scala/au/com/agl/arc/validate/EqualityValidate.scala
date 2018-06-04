package au.com.agl.arc.validate

import org.apache.spark.sql._
import java.lang._

import au.com.agl.arc.api.API._ 
import au.com.agl.arc.util._

object EqualityValidate {

  def validate(validate: EqualityValidate)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Unit = {
    val startTime = System.currentTimeMillis() 
    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", validate.getType)
    stageDetail.put("name", validate.name)
    stageDetail.put("leftView", validate.leftView)      
    stageDetail.put("rightView", validate.rightView) 

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)
      .log()   

    val rawLeftDF = spark.table(validate.leftView)   
    val rawRightDF = spark.table(validate.rightView)   

    // remove any internal fields as some will be added by things like the ParquetExtract which includes filename
    val leftInternalFields = rawLeftDF.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)
    val rightInternalFields = rawRightDF.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)
    val leftDF = rawLeftDF.drop(leftInternalFields:_*) 
    val rightDF = rawRightDF.drop(rightInternalFields:_*) 

    if (leftDF.columns.length != rightDF.columns.length) {
      val leftExceptRightColumns = leftDF.columns diff rightDF.columns
      val rightExceptLeftColumns = rightDF.columns diff leftDF.columns

      stageDetail.put("leftExceptRightColumns", leftExceptRightColumns)
      stageDetail.put("rightExceptLeftColumns", rightExceptLeftColumns)

      throw new Exception(s"""EqualityValidate ensures the two input datasets are the same, but '${validate.leftView}' (${leftDF.columns.length} columns) contains columns: [${leftExceptRightColumns.map(fieldName => s"'${fieldName}'").mkString(", ")}] that are not in '${validate.rightView}' and '${validate.rightView}' (${rightDF.columns.length} columns) contains columns: [${rightExceptLeftColumns.map(fieldName => s"'${fieldName}'").mkString(", ")}] that are not in '${validate.leftView}'. Columns are not equal so cannot the data be compared.""") with DetailException {
        override val detail = stageDetail
      }      
    }      

    val leftExceptRight = leftDF.except(rightDF)
    val rightExceptLeft = rightDF.except(leftDF)
    val leftExceptRightCount = leftExceptRight.count
    val rightExceptLeftCount = rightExceptLeft.count     

    if (leftExceptRightCount != 0 || rightExceptLeftCount != 0) {
      stageDetail.put("leftExceptRightCount", Long.valueOf(leftExceptRightCount))
      stageDetail.put("rightExceptLeftCount", Long.valueOf(rightExceptLeftCount))

      throw new Exception(s"EqualityValidate ensures the two input datasets are the same, but '${validate.leftView}' (${leftDF.count} rows) contains ${leftExceptRightCount} rows that are not in '${validate.rightView}' and '${validate.rightView}' (${rightDF.count} rows) contains ${rightExceptLeftCount} rows which are not in '${validate.leftView}'.") with DetailException {
        override val detail = stageDetail
      }
    }    

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stageDetail)
      .log()    
  }
}


