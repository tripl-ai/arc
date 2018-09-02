package au.com.agl.arc.extract

import java.lang._
import java.net.URI
import java.sql.DriverManager
import java.util.Properties
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.util._
import au.com.agl.arc.util.ControlUtils._

object JDBCExtract {

  def extract(extract: JDBCExtract)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Option[DataFrame] = {
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
      connectionProperties.put("numPartitions", numPartitions.toString)    
    }

    for (fetchsize <- extract.fetchsize) {
      connectionProperties.put("fetchsize", fetchsize.toString)    
      stageDetail.put("fetchsize", Integer.valueOf(fetchsize))
    }     

    extract.predicates match {
      case Nil =>
      case predicates => stageDetail.put("predicates", predicates.asJava)
    }

    for (partitionColumn <- extract.partitionColumn) {
      connectionProperties.put("partitionColumn", partitionColumn)    
      stageDetail.put("partitionColumn", partitionColumn)

      // automatically set the lowerBound and upperBound
      try {
        using(DriverManager.getConnection(extract.jdbcURL, connectionProperties)) { connection =>
          using(connection.createStatement) { statement =>
            val res = statement.execute(s"SELECT MIN(${partitionColumn}), MAX(${partitionColumn}) FROM ${extract.tableName}")
            // try to get results to throw error if one exists
            if (res) {
              statement.getResultSet.next

              val lowerBound = statement.getResultSet.getLong(1)
              val upperBound = statement.getResultSet.getLong(2)

              connectionProperties.put("lowerBound", lowerBound.toString)    
              stageDetail.put("lowerBound", Long.valueOf(lowerBound))
              connectionProperties.put("upperBound", upperBound.toString)    
              stageDetail.put("upperBound", Long.valueOf(upperBound))
            }
          }
        }
      } catch {
        case e: Exception => throw new Exception(e) with DetailException {
          override val detail = stageDetail          
        } 
      }
    }  

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()  

    // try to get the schema
    val optionSchema = try {
      ExtractUtils.getSchema(extract.cols)(spark)
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stageDetail          
      }      
    }         

    val df = try {
      extract.predicates match {
        case Nil => spark.read.jdbc(extract.jdbcURL, extract.tableName, connectionProperties)
        case predicates => spark.read.jdbc(extract.jdbcURL, extract.tableName, predicates.toArray, connectionProperties)
      }
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stageDetail          
      }      
    }

    // set column metadata if exists
    val enrichedDF = optionSchema match {
        case Some(schema) => MetadataUtils.setMetadata(df, schema)
        case None => df   
    }    

    // repartition to distribute rows evenly
    val repartitionedDF = extract.partitionBy match {
      case Nil => { 
        extract.numPartitions match {
          case Some(numPartitions) => enrichedDF.repartition(numPartitions)
          case None => enrichedDF
        }   
      }
      case partitionBy => {
        // create a column array for repartitioning
        val partitionCols = partitionBy.map(col => df(col))
        extract.numPartitions match {
          case Some(numPartitions) => df.repartition(numPartitions, partitionCols:_*)
          case None => df.repartition(partitionCols:_*)
        }
      }
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

    Option(repartitionedDF)
  }

}

