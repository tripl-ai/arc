package au.com.agl.arc.udf

import org.apache.spark.sql.SQLContext

object UDF {

  def registerUDFs(sqlContext: SQLContext): Unit = {
    // register custom UDFs via sqlContext.udf.register("funcName", func )

  }

}
