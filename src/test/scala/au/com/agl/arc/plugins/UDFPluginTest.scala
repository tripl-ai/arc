package au.com.agl.arc.plugins
import java.util

import org.apache.spark.sql.SQLContext

import au.com.agl.arc.util.log.logger.Logger

class UDFPluginTest extends UDFPlugin {

  // add 10 to an incoming integer - DO NOT DO THIS IN PRODUCTION INSTEAD USE SPARK SQL DIRECTLY
  def addTen(input: Int): Int = {
    input + 10
  }

  // add 20 to an incoming integer  - DO NOT DO THIS IN PRODUCTION INSTEAD USE SPARK SQL DIRECTLY
  def addTwenty(input: Int): Int = {
    input + 20
  }


    // one udf plugin can register multiple user defined functions
  override def register(sqlContext: SQLContext)(implicit logger: au.com.agl.arc.util.log.logger.Logger): Seq[String] = {

    // register the functions so they can be accessed via Spark SQL
    sqlContext.udf.register("add_ten", addTen _ )           // SELECT add_ten(1) AS one_plus_ten
    sqlContext.udf.register("add_twenty", addTwenty _ )     // SELECT add_twenty(1) AS one_plus_twenty
    
    // return the list of udf names that were registered for logging
    Seq("add_ten", "add_twenty")

  }

}
