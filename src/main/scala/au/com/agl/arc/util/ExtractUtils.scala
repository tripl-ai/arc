package au.com.agl.arc.util

import java.net.URI
import java.time.Instant

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import au.com.agl.arc.api.API._

object ExtractUtils {

  def getSchema(schema: Either[String, List[ExtractColumn]])(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Option[StructType] = {
    schema match {
      case Right(cols) => {
        cols match {
          case Nil => None
          case c => Option(Extract.toStructType(c))
        }
      }
      case Left(view) => {
        val parseResult: au.com.agl.arc.util.MetadataSchema.ParseResult = au.com.agl.arc.util.MetadataSchema.parseDataFrameMetadata(spark.table(view))(logger)
        parseResult match {
          case Right(cols) => Option(Extract.toStructType(cols))
          case Left(errors) => throw new Exception(s"""Schema view '${view}' to cannot be parsed as it has errors: ${errors.mkString(", ")}.""")
        }
      }
    }
  }   
  
  def addInternalColumns(input: DataFrame, contiguousIndex: Boolean): DataFrame = {
    if (!input.isStreaming) {
      // add meta columns including sequential index
      // if schema already has metadata any columns ignore
      if (!input.columns.intersect(List("_index","_monotonically_increasing_id")).nonEmpty) {
        if (contiguousIndex) {
          // the window function will break partition pushdown
          val window = Window.partitionBy("_filename").orderBy("_monotonically_increasing_id")

          input
            .withColumn("_monotonically_increasing_id", monotonically_increasing_id())
            .withColumn("_filename", input_file_name().as("_filename", new MetadataBuilder().putBoolean("internal", true).build()))
            .withColumn("_index", row_number().over(window).as("_index", new MetadataBuilder().putBoolean("internal", true).build()))
            .drop("_monotonically_increasing_id")
        } else {
          input
            .withColumn("_monotonically_increasing_id", monotonically_increasing_id().as("_monotonically_increasing_id", new MetadataBuilder().putBoolean("internal", true).build()))
            .withColumn("_filename", input_file_name().as("_filename", new MetadataBuilder().putBoolean("internal", true).build()))
        }
      } else {
        input
      }
    } else {
      input
    }
  }
}
