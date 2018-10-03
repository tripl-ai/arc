package au.com.agl.arc.plugins

import org.apache.spark.sql.SQLContext

trait UDFPlugin extends Serializable {

  // return the list of udf names that were registered for logging
  def register(sqlContext: SQLContext)(implicit logger: au.com.agl.arc.util.log.logger.Logger): Seq[String]

}

