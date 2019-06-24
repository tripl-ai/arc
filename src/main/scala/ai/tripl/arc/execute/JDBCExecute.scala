// package ai.tripl.arc.execute

// import scala.collection.JavaConverters._

// import java.sql.Connection
// import java.sql.DriverManager
// import java.sql.ResultSet
// import java.sql.SQLException
// import java.util.concurrent.Executors
// import java.util.concurrent.Future
// import java.util.Properties

// import com.microsoft.sqlserver.jdbc.SQLServerDataSource

// import org.apache.spark.sql._

// import ai.tripl.arc.api.API._
// import ai.tripl.arc.util._
// import ai.tripl.arc.util.ControlUtils._

// object JDBCExecute {

//   def execute(exec: JDBCExecute)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): Option[DataFrame] = {
//     val startTime = System.currentTimeMillis() 
//     val stageDetail = new java.util.HashMap[String, Object]()
//     stageDetail.put("type", exec.getType)
//     stageDetail.put("name", exec.name)
//     for (description <- exec.description) {
//       stageDetail.put("description", description)    
//     }    
//     stageDetail.put("inputURI", exec.inputURI.toString)     
//     stageDetail.put("sqlParams", exec.sqlParams.asJava)

//     // replace sql parameters
//     val sqlToExecute = SQLUtils.injectParameters(exec.sql, exec.sqlParams, false)
//     stageDetail.put("sql", sqlToExecute)     

//     logger.info()
//       .field("event", "enter")
//       .map("stage", stageDetail)      
//       .log()

//     try {
//       using(getConnection(exec.jdbcURL, exec.user, exec.password, exec.params)) { conn =>
//         using(conn.createStatement) { stmt =>
//           val res = stmt.execute(sqlToExecute)
//           // try to get results to throw error if one exists
//           if (res) {
//             stmt.getResultSet.next
//           }
//         }
//       }

//     } catch {
//       case e: Exception => throw new Exception(e) with DetailException {
//         override val detail = stageDetail         
//       }
//     }

//     logger.info()
//       .field("event", "exit")
//       .field("duration", System.currentTimeMillis() - startTime)
//       .map("stage", stageDetail)      
//       .log()

//     None
//   }

//   def getConnection(url: String, user: Option[String], password: Option[String], params: Map[String, String]): Connection = {
//     val _user = user.orElse(params.get("user"))
//     val _password = password.orElse(params.get("password"))


//     val props = new Properties()

//     (_user, _password) match {
//       case (Some(u), Some(p)) =>
//         props.setProperty("user", u)
//         props.setProperty("password", p)
//       case _ =>
//     }

//     for ( (k,v) <- params) {
//       props.setProperty(k, v)
//     }
    
//     DriverManager.getConnection(url, props)
//   }

// }