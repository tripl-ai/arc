package au.com.agl.arc.execute

import scala.collection.JavaConverters._

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException
import java.util.concurrent.Executors
import java.util.concurrent.Future

import com.microsoft.sqlserver.jdbc.SQLServerDataSource

import org.apache.spark.sql._

import au.com.agl.arc.api.API._
import au.com.agl.arc.util._

object JDBCExecute {

  def execute(exec: JDBCExecute)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Unit = {
    val startTime = System.currentTimeMillis() 
    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", exec.getType)
    stageDetail.put("name", exec.name)
    stageDetail.put("inputURI", exec.inputURI.toString)     
    stageDetail.put("sqlParams", exec.sqlParams.asJava)

    // replace sql parameters
    val stmt = SQLUtils.injectParameters(exec.sql, exec.sqlParams)

    stageDetail.put("sql", stmt)     

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()

    val conn = exec.params.get("jdbcType") match {
      case Some("AzureSQLServerDW") => Option(getAzureSQLServerDWConnection(exec.params))
      case Some("Derby") => Option(getDerbyConnection(exec.params))        
      case _ => None
    }
    try {
      for (c <- conn) {
        val result = c.createStatement().execute(stmt)
      }
    } catch {
      case e: Exception =>
        throw e       
    } finally {
      try {
        conn.foreach(_.close())
      } catch {
        case e: Exception =>
          throw e
      }
    } 

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stageDetail)      
      .log()   
  }

  private def getAzureSQLServerDWConnection(params: Map[String, String]): Connection = {
    val ds = new SQLServerDataSource()

    for (url <- params.get("url")) {
      ds.setURL(url)    
    }        
    for (user <- params.get("user")) {
      ds.setUser(user)    
    }    
    for (password <- params.get("password")) {
      ds.setPassword(password)    
    }        
    for (serverPort <- params.get("serverPort")) {
      ds.setPortNumber(serverPort.toInt)
    }
    for (databaseName <- params.get("databaseName")) {
      ds.setDatabaseName(databaseName)    
    }         

    ds.getConnection()
  }

  private def getDerbyConnection(params: Map[String, String]): Connection = {
    DriverManager.getConnection(params.get("serverName").getOrElse(""))    
  }  
}


