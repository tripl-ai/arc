package au.com.agl.arc.plugins

import org.apache.spark.sql.SQLContext

trait UDFPlugin {

  def register(sqlContext: SQLContext)(implicit logger: au.com.agl.arc.util.log.logger.Logger): Seq[String]

}

