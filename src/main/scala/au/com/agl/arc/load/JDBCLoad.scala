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

  def load(load: JDBCLoad)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
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
    stageDetail.put("saveMode", saveMode.toString.toLowerCase)
    stageDetail.put("truncate", Boolean.valueOf(truncate))
    stageDetail.put("partitionBy", load.partitionBy.asJava)

    val df = spark.table(load.inputView)

    if (!df.isStreaming) {
      load.numPartitions match {
        case Some(partitions) => stageDetail.put("numPartitions", Integer.valueOf(partitions))
        case None => stageDetail.put("numPartitions", Integer.valueOf(df.rdd.getNumPartitions))
      }
    } 

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()

    // ensure table name appears correct
    val tablePath = load.tableName.split("\\.")
    if (tablePath.length != 3) {
      throw new Exception(s"tableName should contain 3 components database.schema.table currently has ${tablePath.length} component(s).")    
    }

    val databaseName = tablePath(0).replace("[", "").replace("]", "")
    val tableName = s"${tablePath(1)}.${tablePath(2)}"

    // force cache the table so that when write verification is performed any upstream calculations are not executed twice
    if (!df.isStreaming && !spark.catalog.isCached(load.inputView)) {
      df.cache
    }

    // override defaults https://spark.apache.org/docs/latest/sql-programming-guide.html#jdbc-to-other-databases
    // Properties is a Hashtable<Object,Object> but gets mapped to <String,String> so ensure all values are strings
    val connectionProperties = new Properties
    connectionProperties.put("user", load.params.get("user").getOrElse(""))
    connectionProperties.put("password", load.params.get("password").getOrElse(""))    
    connectionProperties.put("databaseName", databaseName)    
    connectionProperties.put("dbtable", tableName)    

    // build spark JDBCOptions object so we can utilise their inbuilt dialect support
    val jdbcOptions = new JdbcOptionsInWrite(Map("url"-> load.jdbcURL, "dbtable" -> tableName, "user" -> load.params.get("user").getOrElse(""), "password" -> load.params.get("password").getOrElse(""), "databaseName" -> databaseName))

    // execute a count query on target db to get intial count
    val targetPreCount = try {
      using(DriverManager.getConnection(load.jdbcURL, connectionProperties)) { connection =>
        // check if table exists
        if (JdbcUtils.tableExists(connection, jdbcOptions)) {
          saveMode match {
            case SaveMode.ErrorIfExists => {
              throw new Exception(s"Table '${tableName}' already exists in database '${databaseName}' and 'saveMode' equals 'ErrorIfExists' so cannot continue.")
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
                  statement.executeUpdate(s"DELETE FROM ${tableName}")
                }
              }
              0
            }
            case SaveMode.Append => {
              using(connection.createStatement) { statement =>
                val resultSet = statement.executeQuery(s"SELECT COUNT(*) AS count FROM ${tableName}")
                resultSet.next
                resultSet.getInt("count")                  
              }              
            }
          }
        } else {
          if (load.bulkload.getOrElse(false)) {
            throw new Exception(s"Table '${tableName}' does not exist in database '${databaseName}' and 'bulkLoad' equals 'true' so cannot continue.")
          }
          0
        }
      }
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stageDetail         
      }
    }

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
    
    val nonNullDF = df.drop(arrays:_*).drop(nulls:_*)            

    val listener = ListenerUtils.addStageCompletedListener(stageDetail)

    // if not table exists and SaveMode.Ignore
    val outputDF = if (nonNullDF.isStreaming) {
      val jdbcSink = new JDBCSink(load.jdbcURL, connectionProperties)
      load.partitionBy match {
        case Nil => nonNullDF.writeStream.foreach(jdbcSink).start
        case partitionBy => {
          val partitionCols = partitionBy.map(col => nonNullDF(col))
          nonNullDF.writeStream.partitionBy(partitionBy:_*).foreach(jdbcSink).start
        }
      }
      None
    } else {
      if (targetPreCount != SaveModeIgnore) {
        val sourceCount = df.count
        stageDetail.put("count", Long.valueOf(sourceCount))

        val writtenDF =
          try {
            // switch to custom jdbc actions based on driver
            val resultDF = load.driver match {
              // switch to custom sqlserver bulkloader
              case _: com.microsoft.sqlserver.jdbc.SQLServerDriver if (load.bulkload.getOrElse(false)) => {              

                // ensure schemas align after dropping invalid columns
                using(DriverManager.getConnection(load.jdbcURL, connectionProperties)) { connection =>
                  val inputSchema = nonNullDF.schema
                  JdbcUtils.getSchemaOption(connection, jdbcOptions) match {
                    case Some(targetSchema) => {
                      // ensure column names, types and order on input and target align
                      if (!inputSchema.zip(targetSchema).forall({ case (input, target) => input.name.toLowerCase == target.name.toLowerCase && input.dataType == target.dataType })) {
                        throw new Exception(s"""Input dataset '${load.inputView}' has schema [${inputSchema.map(field => s"${field.name}: ${field.dataType.simpleString}").mkString(", ")}] which does not match target table '${tableName}' which has schema [${targetSchema.map(field => s"${field.name}: ${field.dataType.simpleString}").mkString(", ")}]. Ensure names, types and field orders align.""")
                      }                  
                    }
                    case None => throw new Exception(s"Table '${tableName}' does not exist in database '${databaseName}' and 'bulkload' equals 'true' so cannot continue.")
                  }
                }  

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
                  "databaseName"      -> databaseName,
                  "dbTable"           -> tableName,
                  "bulkCopyBatchSize" -> batchsize.toString,
                  "bulkCopyTableLock" -> tablock.toString,
                  "bulkCopyTimeout"   -> "42300"
                ))

                val dfToWrite = load.numPartitions.map(nonNullDF.repartition(_)).getOrElse(nonNullDF)
                dfToWrite.bulkCopyToSqlDB(bulkCopyConfig)
                dfToWrite
              }

              // default spark jdbc
              case _ => {
                for (numPartitions <- load.numPartitions) {
                  connectionProperties.put("numPartitions", numPartitions.toString)
                }
                for (isolationLevel <- load.isolationLevel) {
                  connectionProperties.put("isolationLevel", isolationLevel)
                  stageDetail.put("isolationLevel", isolationLevel)
                }
                for (batchsize <- load.batchsize) {
                  connectionProperties.put("batchsize", batchsize.toString)
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
                    nonNullDF.write.mode(saveMode).jdbc(load.jdbcURL, tableName, connectionProperties)
                  }
                  case partitionBy => {
                    nonNullDF.write.partitionBy(partitionBy:_*).mode(saveMode).jdbc(load.jdbcURL, tableName, connectionProperties)
                  }
                }
                nonNullDF
              }
            }

            // execute a count query on target db to ensure correct number of rows
            val targetPostCount = using(DriverManager.getConnection(load.jdbcURL, connectionProperties)) { connection =>
              val resultSet = connection.createStatement.executeQuery(s"SELECT COUNT(*) AS count FROM ${tableName}")
              resultSet.next
              resultSet.getInt("count")
            }

            // log counts
            stageDetail.put("sourceCount", Long.valueOf(sourceCount))
            stageDetail.put("targetPreCount", Long.valueOf(targetPreCount))
            stageDetail.put("targetPostCount", Long.valueOf(targetPostCount))

            if (sourceCount != targetPostCount - targetPreCount) {
              throw new Exception(s"JDBCLoad should create same number of records in the target ('${tableName}') as exist in source ('${load.inputView}') but source has ${sourceCount} records and target created ${targetPostCount-targetPreCount} records.")
            }

            resultDF
          } catch {
            case e: Exception => throw new Exception(e) with DetailException {
              override val detail = stageDetail
            }
          }
        Option(writtenDF)
      } else {
        Option(df)
      }
    }

    spark.sparkContext.removeSparkListener(listener)    

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stageDetail)
      .log()

    outputDF
  }
}
