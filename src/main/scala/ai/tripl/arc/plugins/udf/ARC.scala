package ai.tripl.arc.plugins.udf

import java.io.CharArrayWriter
import java.net.URI
import java.io.ByteArrayInputStream
import javax.xml.stream.XMLOutputFactory
import javax.xml.stream.XMLStreamWriter

import scala.collection.JavaConverters._
import com.fasterxml.jackson.databind._

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import org.apache.commons.io.IOUtils

import org.apache.http.client.methods.{HttpGet}
import org.apache.http.impl.client.HttpClients

import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import ai.tripl.arc.util.log.logger.Logger
import ai.tripl.arc.api.API.ARCContext
import ai.tripl.arc.util.Utils
import ai.tripl.arc.util.ControlUtils.using

import ai.tripl.arc.util.SerializableConfiguration
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
    spark.sqlContext.udf.register("get_uri", ARCPlugin.getURI _ )    
    spark.sqlContext.udf.register("random", ARCPlugin.getRandom _ )
    spark.sqlContext.udf.register("to_xml", ARCPlugin.toXML _ )
    spark.sqlContext.udf.register("struct_keys", ARCPlugin.structKeys _ )
  }

  override def deprecations()(implicit spark: SparkSession, logger: Logger, arcContext: ARCContext) = {
    Seq(
      Deprecation("get_json_double_array", "get_json_object"),
      Deprecation("get_json_integer_array", "get_json_object"),
      Deprecation("get_json_long_array", "get_json_object")
    )
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

  def structKeys(input: Row): Array[String] = {
    input.schema.fieldNames
  }

  // get byte array content of uri
  def getURI(uri: String)(implicit spark: SparkSession, arcContext: ARCContext): Array[Byte] = {

    val (glob, inputStream) = uri match {
      case uri: String if (uri.startsWith("http") || uri.startsWith("https")) => {
        val client = HttpClients.createDefault
        try {
          val httpGet = new HttpGet(uri);
          val response = client.execute(httpGet);
          try {
            val statusCode = response.getStatusLine.getStatusCode
            val reasonPhrase = response.getStatusLine.getReasonPhrase

            if (statusCode != 200) {
              throw new Exception(s"""Expected StatusCode = 200 when GET '${uri}' but server responded with ${statusCode} (${reasonPhrase}).""")
            }
            (uri, new ByteArrayInputStream(IOUtils.toByteArray(response.getEntity.getContent)))
          } finally {
            response.close
          }
        } finally {
          client.close
        }
      }
      case _ => {
        val hadoopConf = arcContext.serializableConfiguration.value
        val path = new Path(uri)
        val fs = path.getFileSystem(hadoopConf)

        // resolve input uri to Path as it may be a glob pattern
        val globStatus = fs.globStatus(path)
        globStatus.length match {
          case 0 => throw new Exception(s"no files found for uri '${uri}'")
          case 1 => (globStatus(0).getPath.toString, fs.open(globStatus(0).getPath))
          case _ => throw new Exception(s"more than one file found for uri '${uri}'")
        }
      }
    }

    // create inputStream matching compression if possible
    val compressionHandler = glob.toString match {
      case u: String if (u.endsWith(".gzip") || u.endsWith(".gz")) => new java.util.zip.GZIPInputStream(inputStream)
      case u: String if (u.endsWith(".bzip2") || u.endsWith(".bz2")) => new org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream(inputStream)
      case u: String if u.endsWith(".deflate") => new java.util.zip.DeflaterInputStream(inputStream)
      case u: String if u.endsWith(".lz4") => new net.jpountz.lz4.LZ4FrameInputStream(inputStream)
      case _ => inputStream
    }

    IOUtils.toByteArray(compressionHandler)
  }

}
