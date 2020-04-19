package ai.tripl.arc.util

import java.time.LocalTime
import java.time.format.DateTimeFormatter

import scala.collection.JavaConverters._

import org.apache.spark.sql._

import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.config.ConfigUtils._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.util.EitherUtils._

import com.typesafe.config._
import org.apache.commons.lang.StringEscapeUtils

object MetadataSchema {

  @deprecated("MetadataSchema.parseDataFrameMetadata(source: String) is deprecated. please use ArcSchema.parseArcSchemaDataFrame(source: String).", "2.10.0")
  def parseDataFrameMetadata(source: DataFrame)(implicit logger: ai.tripl.arc.util.log.logger.Logger): ArcSchema.ParseResult = {
    logger.warn()
      .field("event", "deprecation")
      .field("message", s"MetadataSchema.parseDataFrameMetadata(source: String) is deprecated. please use ArcSchema.parseArcSchemaDataFrame(source: String).")
      .log()  

    ArcSchema.parseArcSchemaDataFrame(source)
  }  

  @deprecated("MetadataSchema.parseJsonMetadata(source: String) is deprecated. please use ArcSchema.parseArcSchema(source: String).", "2.10.0")
  def parseJsonMetadata(source: String)(implicit logger: ai.tripl.arc.util.log.logger.Logger): ArcSchema.ParseResult = {
    logger.warn()
      .field("event", "deprecation")
      .field("message", s"MetadataSchema.parseJsonMetadata(source: String) is deprecated. please use ArcSchema.parseArcSchema(source: String).")
      .log()  

    ArcSchema.parseArcSchema(source)
  }


  

}