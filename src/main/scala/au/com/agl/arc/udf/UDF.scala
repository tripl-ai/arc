package au.com.agl.arc.udf

import java.util.ServiceLoader

import au.com.agl.arc.plugins.UDFPlugin
import au.com.agl.arc.util.Utils

import scala.collection.JavaConverters._
import com.fasterxml.jackson.databind._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions

object UDF {

  // extract the object from the json string
  def jsonPath(json: String, path: String): List[JsonNode] = {
    if (!path.startsWith("$")) {
      throw new Exception(s"""path '${path}' must start with '$$'.""")
    }
    val objectMapper = new ObjectMapper()
    val rootNode = objectMapper.readTree(json)
    val node = rootNode.at(path.substring(1).replace(".", "/"))

    if (node.getNodeType.toString!= "ARRAY") {
      throw new Exception(s"""value at '${path}' must be 'array' type.""")
    }

    node.asScala.toList
  }

  // get json array cast as double
  def getJSONDoubleArray(json: String, path: String): Array[Double] = {
    val node = jsonPath(json, path)
    node.map(_.asDouble).toArray
  }

  // get json array cast as integer
  def getJSONIntArray(json: String, path: String): Array[Int] = {
    val node = jsonPath(json, path)
    node.map(_.asInt).toArray
  }
  
  // get json array cast as long
  def getJSONLongArray(json: String, path: String): Array[Long] = {
    val node = jsonPath(json, path)
    node.map(_.asLong).toArray
  }

  def getRandom(): Double = {
    scala.util.Random.nextDouble
  }

  def registerUDFs(sqlContext: SQLContext)(implicit logger: au.com.agl.arc.util.log.logger.Logger): Unit = {
    // register custom UDFs via sqlContext.udf.register("funcName", func )
    sqlContext.udf.register("get_json_double_array", getJSONDoubleArray _ )
    sqlContext.udf.register("get_json_integer_array", getJSONIntArray _ )
    sqlContext.udf.register("get_json_long_array", getJSONLongArray _ )
    sqlContext.udf.register("random", getRandom _ )

    val loader = Utils.getContextOrSparkClassLoader
    val serviceLoader = ServiceLoader.load(classOf[UDFPlugin], loader)

    for (p <- serviceLoader.iterator().asScala) {
      val pluginUDFs = p.register(sqlContext)

      val name = p.getClass.getName

      val logData = new java.util.HashMap[String, Object]()
      logData.put("name", name)
      logData.put("udfs", pluginUDFs.asJava)

      logger.info().message(s"Registered UDF Plugin $name").field("udfPlugin", logData).log()
    }
  }

}
