package ai.tripl.arc.util

import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.sql.functions._

// todo
// remove this code and replace with https://issues.apache.org/jira/browse/SPARK-32511 once available
object DataFrameUtils {

  def getSourceField(df: DataFrame, source: String): Option[StructField] = {
    df.schema.fields.filter(_.name == source).headOption
  }

  def getType(sourceField: StructField): StructType = {
    sourceField.dataType match {
      case x: StructType => x
      case _ => throw new Exception("source must be StructType column")
    }
  }

  def genOutputCol(names: Array[String], source: String): Column = {
    struct(names.map(x => col(source).getItem(x).alias(x)): _*)
  }

  def dropFrom(df: DataFrame, source: String, toDrop: List[String]): DataFrame = {
    getSourceField(df, source)
      .map(getType)
      .map(_.fieldNames.diff(toDrop))
      .map(genOutputCol(_, source))
      .map(df.withColumn(source, _))
      .getOrElse(df)
  }

}