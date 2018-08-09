package au.com.agl.arc.util

import java.net.URI
import java.time.Instant

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

object MetadataUtils {

  // turns a schema into a dataframe
  def createMetadataDataframe(input: DataFrame)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): DataFrame = {
    import spark.implicits._

    // this is a hack but having to deal with StructTypes and StructFields
    val schemaDataframe = spark.sparkContext.parallelize(Seq(input.schema.json)).toDF.as[String]
    val parsedSchema = spark.read.json(schemaDataframe)
    parsedSchema.createOrReplaceTempView("parsedSchema")

    val schema = spark.sql("""
    SELECT 
      field.name AS name
      ,field.type AS type
      ,field.nullable AS nullable
      ,field.metadata AS metadata
    FROM (
      SELECT 
        EXPLODE(fields) AS field 
      FROM parsedSchema
    ) subquery
    """)

    schema.cache.count
    schema
  }

}

