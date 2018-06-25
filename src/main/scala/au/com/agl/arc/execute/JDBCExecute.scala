package au.com.agl.arc.execute

import scala.collection.JavaConverters._

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.Properties

import com.microsoft.sqlserver.jdbc.SQLServerDataSource

import org.apache.spark.sql._

import au.com.agl.arc.api.API._
import au.com.agl.arc.util._
import au.com.agl.arc.util.ControlUtils._

object JDBCExecute {

  def getConnection(url: String, user: Option[String], password: Option[String], params: Map[String, String]): Connection = {
    val _user = user.orElse(params.get("user"))
    val _password = password.orElse(params.get("password"))

    (_user, _password) match {
      case (Some(u), Some(p)) =>
        if (params.isEmpty) {
          DriverManager.getConnection(url, u, p)
        } else {
          val props = new Properties()
          props.setProperty("user", u)
          props.setProperty("password", p)
          for ( (k,v) <- params) {
            props.setProperty(k, v)
          }
          DriverManager.getConnection(url, props)
        }
      case _ =>
        DriverManager.getConnection(url)
    }
  }

  def execute(exec: JDBCExecute)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Unit = {
    import exec._

    val startTime = System.currentTimeMillis() 
    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", getType)
    stageDetail.put("name", name)
    stageDetail.put("inputURI", inputURI.toString)     
    stageDetail.put("sqlParams", sqlParams.asJava)

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()

    try {
      // replace sql parameters
      val sqlToExecute = SQLUtils.injectParameters(sql, sqlParams)
      stageDetail.put("sql", sqlToExecute)     

      using(getConnection(url, user, password, params)) { conn =>
        using(conn.createStatement) { stmt =>
          stmt.execute(sqlToExecute)
        }
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