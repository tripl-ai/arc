package au.com.agl.arc.validate

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.node._

import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

import au.com.agl.arc.api.API._ 
import au.com.agl.arc.util._

object SQLValidate {

  def validate(validate: SQLValidate)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Option[DataFrame] = {
    val startTime = System.currentTimeMillis()
    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", validate.getType)
    stageDetail.put("name", validate.name)
    stageDetail.put("sqlParams", validate.sqlParams.asJava)

    val signature = "SQLValidate requires query to return 1 row with [outcome: boolean, message: string] signature."
    
    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)
      .log()   

    // replace sql parameters
    val stmt = SQLUtils.injectParameters(validate.sql, validate.sqlParams)

    val df = try {
      spark.sql(stmt)
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stageDetail          
      } 
    }
    val count = df.persist(StorageLevel.MEMORY_AND_DISK_SER).count

    if (df.count != 1 || df.schema.length != 2) {
      throw new Exception(s"""${signature} Query returned ${count} rows of type [${df.schema.map(f => f.dataType.simpleString).mkString(", ")}].""") with DetailException {
        override val detail = stageDetail          
      }
    }

    var messageMap = new java.util.HashMap[String, Object]()

    try {
      val row = df.first
      val resultIsNull = row.isNullAt(0)
      val messageIsNull = row.isNullAt(1)

      if (resultIsNull) {
        throw new Exception(s"""${signature} Query returned [null, ${if (messageIsNull) "null" else "not null"}].""") with DetailException {
          override val detail = stageDetail
        }
      }

      val message = row.getString(1)
      
      // try to parse to json
      try {
        val objectMapper = new ObjectMapper()
        messageMap = objectMapper.readValue(message, classOf[java.util.HashMap[String, Object]])
        stageDetail.put("message", messageMap)
      } catch {
        case e: Exception => 
          stageDetail.put("message", message)
      }  

      val result = row.getBoolean(0)

      // if result is false throw exception to exit job
      if (result == false) {
        throw new Exception(s"SQLValidate failed with message: '${message}'.") with DetailException {
          override val detail = stageDetail
        }
      }
    } catch {
      case e: ClassCastException =>
        throw new Exception(s"${signature} Query returned ${count} rows of type [${df.schema.map(f => f.dataType.simpleString).mkString(", ")}].") with DetailException {
          override val detail = stageDetail          
        }     
      case e: Exception with DetailException => throw e
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stageDetail          
      } 
    }

    df.unpersist

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stageDetail)
      .log()

    Option(df)
  }
}


