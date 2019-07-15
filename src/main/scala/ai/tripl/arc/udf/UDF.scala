package ai.tripl.arc.udf

import scala.collection.JavaConverters._

import ai.tripl.arc.api.API.ARCContext

import org.apache.spark.sql.SparkSession

object UDF {

  def registerUDFs()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): java.util.HashMap[String, Object] = {
    import spark.implicits._

    val logData = new java.util.HashMap[String, Object]()

    for (plugin <- arcContext.udfPlugins) {
      val beforeFunctions = spark.catalog.listFunctions.map(_.name).collect.toSet

      plugin.register

      val afterFunctions = spark.catalog.listFunctions.map(_.name).collect.toSet
      val addedFunctions = afterFunctions.diff(beforeFunctions)

      logData.put(s"${plugin.getClass.getName}:${plugin.version}", addedFunctions.asJava)
    }

    logData
  }

}
