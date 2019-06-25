package ai.tripl.arc.plugins

import org.apache.spark.sql.SQLContext

trait UDFPlugin {

  def version: String
  
  // return the list of udf names that were registered for logging
  def register(sqlContext: SQLContext)(implicit logger: ai.tripl.arc.util.log.logger.Logger): Seq[String]

}

