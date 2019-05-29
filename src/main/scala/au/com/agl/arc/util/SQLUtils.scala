package au.com.agl.arc.util

import scala.collection.JavaConverters._

import java.io.File
import java.net.URI

import org.apache.commons.io.IOUtils
import org.apache.commons.io.FileUtils

object SQLUtils {

  private lazy val clazz = getClass

  def sqlForResource(name: String): String = {
    val file = clazz.getResourceAsStream(s"/sql/$name")
    IOUtils.toString(file, "UTF-8")
  }

  def sqlForURI(uri: URI): String = {
    val file = new File(uri.getPath())
    FileUtils.readFileToString(file, "UTF-8")
  }

  def injectParameters(sql: String, sqlParams: Map[String, String], allowMissing: Boolean)(implicit logger: au.com.agl.arc.util.log.logger.Logger): String = {
    // replace sql parameters
    // using regex from the apache zeppelin project
    val stmt = sqlParams.foldLeft(sql) {
      case (sql, (k,v)) => { 
          val placeholderRegex = "[$][{]\\s*" + k + "\\s*(?:=[^}]+)?[}]"

          // throw error if no match found
          if (!allowMissing && placeholderRegex.r.findAllIn(sql).length == 0) {
            throw new Exception(s"No placeholder found in SQL statement for sqlParam: '${k}'.")
          }           

          placeholderRegex.r.replaceAllIn(sql, v)
      }
    }

    // find missing replacements
    val missingRegex = "[$][{](\\w*)(?:=[^}]+)?[}]"
    val missingValues = missingRegex.r.findAllIn(stmt).toList
    if (missingValues.length != 0) {
      throw new Exception(s"""No replacement value found in sqlParams ${sqlParams.keys.toList.mkString("[", ", ", "]")} for placeholders: ${missingValues.mkString("[", ", ", "]")}.""")
    }

    logger.trace()
      .field("type", "SQLTransform")
      .field("sql", sql)
      .map("sqlParams", sqlParams.asJava)          
      .field("stmt", stmt)          
      .log()
    
    stmt
  }
}
