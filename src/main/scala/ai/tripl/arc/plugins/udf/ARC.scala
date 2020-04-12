package ai.tripl.arc.plugins.udf

import java.io.CharArrayWriter
import java.net.URI
import javax.xml.stream.XMLOutputFactory
import javax.xml.stream.XMLStreamWriter

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind._

import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import ai.tripl.arc.util.log.logger.Logger
import ai.tripl.arc.api.API.ARCContext
import ai.tripl.arc.util.Utils

import com.databricks.spark.xml.util._
import com.sun.xml.txw2.output.IndentingXMLStreamWriter

class ARC extends ai.tripl.arc.plugins.UDFPlugin {

  val version = Utils.getFrameworkVersion

  // one udf plugin can register multiple user defined functions
  override def register()(implicit spark: SparkSession, logger: Logger, arcContext: ARCContext) = {

    // register custom UDFs via sqlContext.udf.register("funcName", func )
    spark.sqlContext.udf.register("get_json_double_array", ARCPlugin.getJSONDoubleArray _ )
    spark.sqlContext.udf.register("get_json_integer_array", ARCPlugin.getJSONIntArray _ )
    spark.sqlContext.udf.register("get_json_long_array", ARCPlugin.getJSONLongArray _ )
    spark.sqlContext.udf.register("random", ARCPlugin.getRandom _ )
    spark.sqlContext.udf.register("to_xml", ARCPlugin.toXML _ )

  }
}

object ARCPlugin {
  // extract the object from the json string
  def jsonPath(json: String, path: String): List[JsonNode] = {
    if (!path.startsWith("$")) {
      throw new Exception(s"""path '${path}' must start with '$$'.""")
    }
    val objectMapper = new ObjectMapper()
    val rootNode = objectMapper.readTree(json)
    val node = rootNode.at(path.substring(1).replace(".", "/"))

    if (node.getNodeType.toString != "ARRAY") {
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

  // convert structtype to xml
  def toXML(input: Row): String = {
    val factory = XMLOutputFactory.newInstance
    val writer = new CharArrayWriter
    val xmlWriter = factory.createXMLStreamWriter(writer)
    val indentingXmlWriter = new IndentingXMLStreamWriter(xmlWriter)
    ai.tripl.arc.load.XMLLoad.StaxXmlGenerator(input.schema, indentingXmlWriter)(input)
    indentingXmlWriter.flush
    writer.toString.trim    
  }  

}
