package au.com.agl.arc.extract

import java.lang._
import java.net.URI
import java.util.Properties
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.util._

object JDBCExtract {

  def extract(extract: JDBCExtract)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): DataFrame = {
    import spark.implicits._
    val startTime = System.currentTimeMillis() 
    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", extract.getType)
    stageDetail.put("name", extract.name)
    stageDetail.put("outputView", extract.outputView)  
    stageDetail.put("persist", Boolean.valueOf(extract.persist))
    stageDetail.put("jdbcURL", extract.jdbcURL)
    stageDetail.put("driver", extract.driver.getClass.toString)  
    stageDetail.put("tableName", extract.tableName)

    // override defaults https://spark.apache.org/docs/latest/sql-programming-guide.html#jdbc-to-other-databases
    val connectionProperties = new Properties()
    connectionProperties.put("user", extract.params.get("user").getOrElse(""))
    connectionProperties.put("password", extract.params.get("password").getOrElse(""))

    for (numPartitions <- extract.numPartitions) {
      connectionProperties.put("numPartitions", Integer.valueOf(numPartitions))    
    }

    for (fetchsize <- extract.fetchsize) {
      connectionProperties.put("fetchsize", Integer.valueOf(fetchsize))    
      stageDetail.put("fetchsize", Integer.valueOf(fetchsize))
    }     

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()  

    val df = try {
      spark.read.jdbc(extract.jdbcURL, extract.tableName, connectionProperties)
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stageDetail          
      }      
    }

    // repartition to distribute rows evenly
    val repartitionedDF = extract.numPartitions match {
      case Some(numPartitions) => df.repartition(numPartitions)
      case None => df
    }
    repartitionedDF.createOrReplaceTempView(extract.outputView)
    
    stageDetail.put("outputColumns", Integer.valueOf(repartitionedDF.schema.length))
    stageDetail.put("numPartitions", Integer.valueOf(repartitionedDF.rdd.partitions.length))

    if (extract.persist) {
      repartitionedDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
      stageDetail.put("records", Long.valueOf(repartitionedDF.count)) 
    }    

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stageDetail)      
      .log()

    repartitionedDF
  }

}

