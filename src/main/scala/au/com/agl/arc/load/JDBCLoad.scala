package au.com.agl.arc.load

import java.lang._
import java.net.URI
import java.sql.DriverManager
import java.sql.Connection
import java.util.Properties
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.execution.datasources.jdbc._

// sqlserver bulk import - not limited to azure hosted sqlserver instances
import com.microsoft.azure.sqldb.spark.connect._

import au.com.agl.arc.api.API._
import au.com.agl.arc.util._
import au.com.agl.arc.util.ControlUtils._


object JDBCLoad {

  val SaveModeIgnore = -1

  def load(load: JDBCLoad)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Unit = {
    val startTime = System.currentTimeMillis 
    val stageDetail = new java.util.HashMap[String, Object]()
    val saveMode = load.saveMode.getOrElse(SaveMode.Overwrite)
    val truncate = load.truncate.getOrElse(false)

    stageDetail.put("type", load.getType)
    stageDetail.put("name", load.name)
    stageDetail.put("inputView", load.inputView)  
    stageDetail.put("jdbcURL", load.jdbcURL)  
    stageDetail.put("driver", load.driver.getClass.toString)  
    stageDetail.put("tableName", load.tableName)  
    stageDetail.put("bulkload", Boolean.valueOf(load.bulkload.getOrElse(false)))
    stageDetail.put("saveMode", saveMode.toString)
    stageDetail.put("truncate", Boolean.valueOf(truncate))

    val df = spark.table(load.inputView)

    val numPartitions = load.numPartitions match {
      case Some(numPartitions) => numPartitions
      case None => df.rdd.getNumPartitions
    }
    stageDetail.put("numPartitions", Integer.valueOf(numPartitions))

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()

    // ensure table name appears correct
    val tablePath = load.tableName.split("\\.")
    if (tablePath.length != 3) {
      throw new Exception(s"tableName should contain 3 components database.schema.table currently has ${tablePath.length} component(s).")    
    }

    // force cache the table so that when write verification is performed any upstream calculations are not executed twice
    if (!spark.catalog.isCached(load.inputView)) {
      df.cache
    }

    val sourceCount = df.count
    stageDetail.put("count", Long.valueOf(sourceCount))

    // override defaults https://spark.apache.org/docs/latest/sql-programming-guide.html#jdbc-to-other-databases
    // Properties is a Hashtable<Object,Object> but gets mapped to <String,String> so ensure all values are strings
    val connectionProperties = new Properties
    connectionProperties.put("user", load.params.get("user").getOrElse(""))
    connectionProperties.put("password", load.params.get("password").getOrElse(""))    

    // build spark JDBCOptions object so we can utilise their inbuilt dialect support
    val jdbcOptions = new JDBCOptions(Map("url"-> load.jdbcURL, "dbtable" -> load.tableName))

    // execute a count query on target db to get intial count
    val targetPreCount = try {
      using(DriverManager.getConnection(load.jdbcURL, connectionProperties)) { connection =>
        // check if table exists
        if (JdbcUtils.tableExists(connection, jdbcOptions)) {
          saveMode match {
            case SaveMode.ErrorIfExists => {
              throw new Exception(s"Table '${load.tableName}' already exists and 'saveMode' equals 'ErrorIfExists' so cannot continue.")
            }
            case SaveMode.Ignore => {
              // return a constant if table exists and SaveMode.Ignore
              SaveModeIgnore
            }          
            case SaveMode.Overwrite => {
              if (truncate) {
                JdbcUtils.truncateTable(connection, jdbcOptions)
              } else {
                using(connection.createStatement) { statement =>
                  statement.executeUpdate(s"DELETE FROM ${load.tableName}")
                }
              }
              0
            }
            case SaveMode.Append => {
              using(connection.createStatement) { statement =>
                val resultSet = statement.executeQuery(s"SELECT COUNT(*) AS count FROM ${load.tableName}")
                resultSet.next
                resultSet.getInt("count")                  
              }              
            }
          }
        } else {
          if (load.bulkload.getOrElse(false)) {
            throw new Exception(s"Table '${load.tableName}' does not exist and 'bulkLoad' equals 'true' so cannot continue.")
          }
          0
        }
      }
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stageDetail         
      }
    }

    // if not table exists and SaveMode.Ignore
    if (targetPreCount != SaveModeIgnore) {

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
        // switch to custom jdbc actions based on driver
        load.driver match {
          // switch to custom sqlserver bulkloader
          case _: com.microsoft.sqlserver.jdbc.SQLServerDriver if (load.bulkload.getOrElse(false)) => {      

            // remove the "jdbc:" prefix
            val uri = new URI(load.jdbcURL.substring(5))
            
            val batchsize = load.batchsize.getOrElse(10000)
            stageDetail.put("batchsize", Integer.valueOf(batchsize))

            val tablock = load.tablock.getOrElse(true)
            stageDetail.put("tablock", Boolean.valueOf(tablock))            

            val bulkCopyConfig = com.microsoft.azure.sqldb.spark.config.Config(Map(
              "url"               -> s"${uri.getHost}:${uri.getPort}",
              "user"              -> load.params.get("user").getOrElse(""),
              "password"          -> load.params.get("password").getOrElse(""),
              "databaseName"      -> tablePath(0).replace("[", "").replace("]", ""),
              "dbTable"           -> s"${tablePath(1)}.${tablePath(2)}",
              "bulkCopyBatchSize" -> batchsize.toString,
              "bulkCopyTableLock" -> tablock.toString,
              "bulkCopyTimeout"   -> "42300"
            ))

            load.numPartitions match {
              case Some(numPartitions) => df.drop(arrays:_*).drop(nulls:_*).repartition(numPartitions).bulkCopyToSqlDB(bulkCopyConfig)
              case None => df.drop(arrays:_*).drop(nulls:_*).bulkCopyToSqlDB(bulkCopyConfig)
            }
          }

          // default spark jdbc 
          case _ => {
            for (numPartitions <- load.numPartitions) {
              connectionProperties.put("numPartitions", String.valueOf(numPartitions))    
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
        val targetPostCount = try {
          using(DriverManager.getConnection(load.jdbcURL, connectionProperties)) { connection =>
            val resultSet = connection.createStatement.executeQuery(s"SELECT COUNT(*) AS count FROM ${load.tableName}")
            resultSet.next
            resultSet.getInt("count")
          }
        } catch {
          case e: Exception => throw new Exception(e) with DetailException {
            override val detail = stageDetail          
          }      
        }

        // log counts
        stageDetail.put("sourceCount", Long.valueOf(sourceCount))
        stageDetail.put("targetPreCount", Long.valueOf(targetPreCount))
        stageDetail.put("targetPostCount", Long.valueOf(targetPostCount))

        if (sourceCount != targetPostCount - targetPreCount) {
          throw new Exception(s"JDBCLoad should create same number of records in the target ('${load.tableName}') as exist in source ('${load.inputView}') but source has ${sourceCount} records and target created ${targetPostCount-targetPreCount} records.")
        }      
      } catch {
        case e: Exception => throw new Exception(e) with DetailException {
          override val detail = stageDetail          
        }      
      } 
    }

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stageDetail)      
      .log()  
  }
}
