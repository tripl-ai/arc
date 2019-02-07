package au.com.agl.arc.validate

import java.lang._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import au.com.agl.arc.api.API._ 
import au.com.agl.arc.util._

object EqualityValidate {

  def validate(validate: EqualityValidate)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Option[DataFrame] = {
    val startTime = System.currentTimeMillis() 
    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", validate.getType)
    stageDetail.put("name", validate.name)
    for (description <- validate.description) {
      stageDetail.put("description", description)    
    }    
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

    // test column count equality
    val leftExceptRightColumns = leftDF.columns diff rightDF.columns
    val rightExceptLeftColumns = rightDF.columns diff leftDF.columns
    if (leftExceptRightColumns.length != 0 || rightExceptLeftColumns.length != 0) {
      stageDetail.put("leftExceptRightColumns", leftExceptRightColumns)
      stageDetail.put("rightExceptLeftColumns", rightExceptLeftColumns)

      throw new Exception(s"""EqualityValidate ensures the two input datasets are the same (including column order), but '${validate.leftView}' (${leftDF.columns.length} columns) contains columns: [${leftExceptRightColumns.map(fieldName => s"'${fieldName}'").mkString(", ")}] that are not in '${validate.rightView}' and '${validate.rightView}' (${rightDF.columns.length} columns) contains columns: [${rightExceptLeftColumns.map(fieldName => s"'${fieldName}'").mkString(", ")}] that are not in '${validate.leftView}'. Columns are not equal so cannot the data be compared.""") with DetailException {
        override val detail = stageDetail
      }      
    }      

    // test column order equality
    if (leftDF.schema.map(_.name).toArray.deep != rightDF.schema.map(_.name).toArray.deep) {
      stageDetail.put("leftColumns", leftDF.schema.map(_.name).toArray)
      stageDetail.put("rightColumns", rightDF.schema.map(_.name).toArray)

      throw new Exception(s"""EqualityValidate ensures the two input datasets are the same (including column order), but '${validate.leftView}' contains columns (ordered): [${leftDF.columns.map(fieldName => s"'${fieldName}'").mkString(", ")}] and '${validate.rightView}' contains columns (ordered): [${rightDF.columns.map(fieldName => s"'${fieldName}'").mkString(", ")}]. Columns are not equal so cannot the data be compared.""") with DetailException {
        override val detail = stageDetail
      }      
    }      

    // test column type equality
    if (leftDF.schema.map(_.dataType).toArray.deep != rightDF.schema.map(_.dataType).toArray.deep) {
      stageDetail.put("leftColumnsTypes", leftDF.schema.map(_.dataType.typeName).toArray)
      stageDetail.put("rightColumnsTypes", rightDF.schema.map(_.dataType.typeName).toArray)

      throw new Exception(s"""EqualityValidate ensures the two input datasets are the same (including column order), but '${validate.leftView}' contains column types (ordered): [${leftDF.schema.map(_.dataType.typeName).toArray.map(fieldType => s"'${fieldType}'").mkString(", ")}] and '${validate.rightView}' contains column types (ordered): [${rightDF.schema.map(_.dataType.typeName).toArray.map(fieldType => s"'${fieldType}'").mkString(", ")}]. Columns are not equal so cannot the data be compared.""") with DetailException {
        override val detail = stageDetail
      }      
    }   

    // do not test column nullable equality

    // do a full join on a calculated hash of all values in row on each dataset
    // trying to calculate the hash value inside the joinWith method produced an inconsistent result
    val leftHashDF = leftDF.withColumn("_hash", sha2(to_json(struct(leftDF.columns.map(col):_*)),512))
    val rightHashDF = rightDF.withColumn("_hash", sha2(to_json(struct(rightDF.columns.map(col):_*)),512))
    val transformedDF = leftHashDF.joinWith(rightHashDF, leftHashDF("_hash") === rightHashDF("_hash"), "full")

    val leftExceptRight = transformedDF.filter(col("_2").isNull)
    val rightExceptLeft = transformedDF.filter(col("_1").isNull)
    val leftExceptRightCount = leftExceptRight.count
    val rightExceptLeftCount = rightExceptLeft.count     

    if (leftExceptRightCount != 0 || rightExceptLeftCount != 0) {
      stageDetail.put("leftExceptRightCount", Long.valueOf(leftExceptRightCount))
      stageDetail.put("rightExceptLeftCount", Long.valueOf(rightExceptLeftCount))

      throw new Exception(s"EqualityValidate ensures the two input datasets are the same (including column order), but '${validate.leftView}' (${leftDF.count} rows) contains ${leftExceptRightCount} rows that are not in '${validate.rightView}' and '${validate.rightView}' (${rightDF.count} rows) contains ${rightExceptLeftCount} rows which are not in '${validate.leftView}'.") with DetailException {
        override val detail = stageDetail
      }
    }    

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stageDetail)
      .log()

    None
  }
}