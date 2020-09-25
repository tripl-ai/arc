package ai.tripl.arc.util

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DataFrameUtils {

 def dropSubColumn(col: Column, colType: DataType, fullColName: String, dropColName: String): Option[Column] = {
    if (fullColName.equals(dropColName)) {
      None
    } else if (dropColName.startsWith(s"$fullColName.")) {
      colType match {
        case colType: StructType =>
          Some(struct(
            colType.fields
                .flatMap(f =>
                  dropSubColumn(col.getField(f.name), f.dataType, s"$fullColName.${f.name}", dropColName) match {
                    case Some(x) => Some(x.alias(f.name))
                    case None => None
                  })
                : _*))
        case colType: ArrayType =>
          colType.elementType match {
            case innerType: StructType =>
              // we are potentially dropping a column from within a struct, that is itself inside an array
              // Spark has some very strange behavior in this case, which they insist is not a bug
              // see https://issues.apache.org/jira/browse/SPARK-31779 and associated comments
              // and also the thread here: https://stackoverflow.com/a/39943812/375670
              // this is a workaround for that behavior

              // first, get all struct fields
              val innerFields = innerType.fields
              // next, create a new type for all the struct fields EXCEPT the column that is to be dropped
              // we will need this later
              val preserveNamesStruct = ArrayType(StructType(
                innerFields.filterNot(f => s"$fullColName.${f.name}".equals(dropColName))
              ))
              // next, apply dropSubColumn recursively to build up the new values after dropping the column
              val filteredInnerFields = innerFields.flatMap(f =>
                dropSubColumn(col.getField(f.name), f.dataType, s"$fullColName.${f.name}", dropColName) match {
                    case Some(x) => Some(x.alias(f.name))
                    case None => None
                }
              )
              // finally, use arrays_zip to unwrap the arrays that were introduced by building up the new. filtered
              // struct in this way (see comments in SPARK-31779), and then cast to the StructType we created earlier
              // to get the original names back
              Some(arrays_zip(filteredInnerFields:_*).cast(preserveNamesStruct))
          }

        case _ => Some(col)
      }
    } else {
      Some(col)
    }
  }

  def dropColumn(df: DataFrame, colName: String): DataFrame = {
    df.schema.fields.flatMap(f => {
      if (colName.startsWith(s"${f.name}.")) {
        dropSubColumn(col(f.name), f.dataType, f.name, colName) match {
          case Some(x) => Some((f.name, x))
          case None => None
        }
      } else {
        None
      }
    }).foldLeft(df.drop(colName)) {
      case (df, (colName, column)) => df.withColumn(colName, column)
    }
  }

}