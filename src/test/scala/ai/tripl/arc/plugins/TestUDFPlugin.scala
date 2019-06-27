package ai.tripl.arc.plugins
import java.util

import org.apache.spark.sql.SparkSession
import ai.tripl.arc.api.API.ARCContext

import ai.tripl.arc.util.log.logger.Logger

class TestUDFPlugin extends UDFPlugin {

  val version = "0.0.1"

  // one udf plugin can register multiple user defined functions
  override def register()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext) = {

    // register the functions so they can be accessed via Spark SQL
    spark.sqlContext.udf.register("add_ten", TestUDFPlugin.addTen _ )           // SELECT add_ten(1) AS one_plus_ten
    spark.sqlContext.udf.register("add_twenty", TestUDFPlugin.addTwenty _ )     // SELECT add_twenty(1) AS one_plus_twenty

  }
}

object TestUDFPlugin {
  // add 10 to an incoming integer - DO NOT DO THIS IN PRODUCTION INSTEAD USE SPARK SQL DIRECTLY
  def addTen(input: Int): Int = {
    input + 10
  }

  // add 20 to an incoming integer  - DO NOT DO THIS IN PRODUCTION INSTEAD USE SPARK SQL DIRECTLY
  def addTwenty(input: Int): Int = {
    input + 20
  }
}