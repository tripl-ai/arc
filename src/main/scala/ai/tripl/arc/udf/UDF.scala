package ai.tripl.arc.udf

import java.util.ServiceLoader
import scala.collection.JavaConverters._

import ai.tripl.arc.plugins.UDFPlugin
import ai.tripl.arc.util.Utils
import ai.tripl.arc.api.API.ARCContext

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions

object UDF {

  def registerUDFs()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): java.util.HashMap[String, Object] = {
    import spark.implicits._

    val logData = new java.util.HashMap[String, Object]()

    for (plugin <- arcContext.udfPlugins) {
      val beforeFunctions = spark.catalog.listFunctions.map(_.name).collect.toSet

      val pluginUDFs = plugin.register

      val afterFunctions = spark.catalog.listFunctions.map(_.name).collect.toSet
      val addedFunctions = afterFunctions.diff(beforeFunctions)

      logData.put(s"${plugin.getClass.getName}:${plugin.version}", addedFunctions.asJava)
    }

    logData
  }

}
