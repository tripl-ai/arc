package au.com.agl.arc.load

import java.lang._
import java.net.URI
import java.sql.DriverManager
import java.util.Properties
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.types._

// sqlserver bulk import - not limited to azure hosted sqlserver instances
import com.microsoft.azure.sqldb.spark.connect._

import au.com.agl.arc.api.API._
import au.com.agl.arc.util._

object JDBCLoad {

  def load(load: JDBCLoad)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Unit = {
    val startTime = System.currentTimeMillis() 
    val stageDetail = new java.util.HashMap[String, Object]()
    val saveMode = load.saveMode.getOrElse(SaveMode.Overwrite)

    stageDetail.put("type", load.getType)
    stageDetail.put("name", load.name)
    stageDetail.put("inputView", load.inputView)  
    stageDetail.put("jdbcURL", load.jdbcURL)  
    stageDetail.put("driver", load.driver.getClass.toString)  
    stageDetail.put("tableName", load.tableName)  
    stageDetail.put("bulkload", Boolean.valueOf(load.bulkload.getOrElse(false)))
    stageDetail.put("saveMode", saveMode.toString)

    val df = spark.table(load.inputView)

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()

    // force cache the table so that when write verification is performed any upstream calculations are not executed twice
    if (!spark.catalog.isCached(load.inputView)) {
      df.cache
    }
    val sourceCount = df.count
    stageDetail.put("count", Long.valueOf(sourceCount))

    val dropMap = new java.util.HashMap[String, Object]()

    // many jdbc targets cannot handle a column of ArrayType
    // drop these columns before write
    val arrays = df.schema.filter( _.dataType.typeName == "array").map(_.name)
    if (!arrays.isEmpty) {
      dropMap.put("ArrayType", arrays.asJava)
    }    

    // JDBC cannot handle a column of NullType
    val nulls = df.schema.filter( _.dataType == NullType).map(_.name)
    if (!nulls.isEmpty) {
      dropMap.put("NullType", nulls.asJava)
    }

    stageDetail.put("drop", dropMap) 

    try {
      // override defaults https://spark.apache.org/docs/latest/sql-programming-guide.html#jdbc-to-other-databases
      // Properties is a Hashtable<Object,Object> but gets mapped to <String,String> so ensure all values are strings
      val connectionProperties = new Properties()
      connectionProperties.put("user", load.params.get("user").getOrElse(""))
      connectionProperties.put("password", load.params.get("password").getOrElse(""))

      // switch to custom jdbc actions based on driver
      load.driver match {
        // switch to custom sqlserver bulkloader
        case _: com.microsoft.sqlserver.jdbc.SQLServerDriver if (load.bulkload.getOrElse(false)) => {

          // remove the "jdbc:" prefix
          val uri = new URI(load.jdbcURL.substring(5))

          for (batchsize <- load.batchsize) {
            stageDetail.put("batchsize", Integer.valueOf(batchsize))
          } 

          // ensure table name appears correct
          val tablePath = load.tableName.split("\\.")
          if (tablePath.length != 3) {
            throw new Exception(s"tableName should contain 3 components database.schema.table currently has ${tablePath.length} component(s).")    
          }

          val bulkCopyConfig = com.microsoft.azure.sqldb.spark.config.Config(Map(
            "url"               -> s"${uri.getHost}:${uri.getPort}",
            "user"              -> load.params.get("user").getOrElse(""),
            "password"          -> load.params.get("password").getOrElse(""),
            "databaseName"      -> tablePath(0).replace("[", "").replace("]", ""),
            "dbTable"           -> s"${tablePath(1)}.${tablePath(2)}",
            "bulkCopyBatchSize" -> load.batchsize.getOrElse(10000).toString,
            "bulkCopyTableLock" -> "true",
            "bulkCopyTimeout"   -> "600"
          ))

          df.bulkCopyToSqlDB(bulkCopyConfig)
        }

        // default spark jdbc 
        case _ => {
          load.numPartitions match {
            case Some(partitions) => {
              connectionProperties.put("numPartitions", String.valueOf(partitions))          
              stageDetail.put("numPartitions", Integer.valueOf(partitions))
            }
            case None => stageDetail.put("numPartitions", Integer.valueOf(df.rdd.getNumPartitions))
          }

          for (isolationLevel <- load.isolationLevel) {
            connectionProperties.put("isolationLevel", isolationLevel)            
            stageDetail.put("isolationLevel", isolationLevel)  
          }
          for (batchsize <- load.batchsize) {
            connectionProperties.put("batchsize", String.valueOf(batchsize))    
            stageDetail.put("batchsize", Integer.valueOf(batchsize))
          } 
          for (truncate <- load.truncate) {
            connectionProperties.put("truncate", truncate.toString) 
            stageDetail.put("truncate", Boolean.valueOf(truncate))
          }     
          for (createTableOptions <- load.createTableOptions) {
            connectionProperties.put("createTableOptions", createTableOptions)
            stageDetail.put("createTableOptions", createTableOptions)
          } 
          for (createTableColumnTypes <- load.createTableColumnTypes) {
            connectionProperties.put("createTableColumnTypes", createTableColumnTypes)
            stageDetail.put("createTableColumnTypes", createTableColumnTypes)
          }     

          load.partitionBy match {
            case Nil => { 
              df.drop(arrays:_*).drop(nulls:_*).write.mode(saveMode).jdbc(load.jdbcURL, load.tableName, connectionProperties)
            }
            case partitionBy => {
              df.drop(arrays:_*).drop(nulls:_*).write.partitionBy(partitionBy:_*).mode(saveMode).jdbc(load.jdbcURL, load.tableName, connectionProperties)
            }
          }           
        }
      }


      // execute a count query on target db to ensure correct number of rows
      val connection = DriverManager.getConnection(load.jdbcURL, connectionProperties)
      // theoretically vulnerable to sql injection but should fail in jdbc write stage
      val resultSet = connection.createStatement().executeQuery(s"SELECT COUNT(*) AS count FROM ${load.tableName}")
      resultSet.next()
      val targetCount = resultSet.getInt("count")

      // log counts
      stageDetail.put("sourceCount", Long.valueOf(sourceCount))
      stageDetail.put("targetCount", Long.valueOf(targetCount))

      if (sourceCount != targetCount) {
        throw new Exception(s"JDBCLoad should create same number of records in the target ('${load.tableName}') as exist in source ('${load.inputView}') but source has ${sourceCount} records and target has ${targetCount} records.")
      }      
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
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
