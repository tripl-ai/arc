package au.com.agl.arc.load

import java.lang._
import java.net.URI
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import au.com.agl.arc.api.API._
import au.com.agl.arc.util._

object DatabricksSQLDWLoad {

  def load(load: DatabricksSQLDWLoad)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Option[DataFrame] = {
    import spark.implicits._
    val startTime = System.currentTimeMillis() 
    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", load.getType)
    stageDetail.put("name", load.name)
    for (description <- load.description) {
      stageDetail.put("description", description)    
    }
    stageDetail.put("inputView", load.inputView)  
    stageDetail.put("jdbcURL", load.jdbcURL)  
    stageDetail.put("dbTable", load.dbTable)    
    for (tableOptions <- load.tableOptions) {
      stageDetail.put("tableOptions", tableOptions)    
    }     
    stageDetail.put("maxStrLength", Integer.valueOf(load.maxStrLength)) 
    stageDetail.put("tempDir", load.tempDir)
    stageDetail.put("forwardSparkAzureStorageCredentials", load.forwardSparkAzureStorageCredentials.toString)    

    val df = spark.table(load.inputView)      
    stageDetail.put("numPartitions", Integer.valueOf(df.rdd.getNumPartitions))

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()

    // set write permissions
    // for this stage these credentials are sent to the sql server see: forwardSparkAzureStorageCredentials
    CloudUtils.setHadoopConfiguration(load.authentication)

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

    // try to write the inputView
    val outputDF = try {
      val writer = nonNullDF.write
        .format("com.databricks.spark.sqldw")
        .option("url", load.jdbcURL)
        .option("forwardSparkAzureStorageCredentials", load.forwardSparkAzureStorageCredentials.toString)
        .option("tempDir", load.tempDir)
        .option("maxStrLength", load.maxStrLength)
        .option("dbTable", load.dbTable)
        
      // set the optional parameters
      for (user <- load.params.get("user")) {
        writer.option("user", user)    
      }      
      for (password <- load.params.get("password")) {
        writer.option("password", password)    
      }                
      for (tableOptions <- load.tableOptions) {
        writer.option("tableOptions", tableOptions)    
      }          

      writer.save()    
      nonNullDF    
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

    Option(outputDF)
  }
}