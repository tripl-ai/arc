package ai.tripl.arc.util

import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.node._

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.util.{GenericArrayData, QuantileSummaries}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object StatisticsUtils {

  // used to map statistics function to output column name
  case class Statistic(
    name: String,
    expr: Expression => Expression
  )

  // converts the input dataframe to a statistics dataframe.
  // this is pivoted compared with the spark internal statistics dataframe
  // all output columns are StringType as different data types may be present in the aggregate columns
  // based heavily on org.apache.spark.sql.execution.stat.StatFunctions.summary
  def createStatisticsDataframe(input: DataFrame, approximate: Boolean, histogram: Boolean)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): DataFrame = {

    val inputColumns = input.queryExecution.sparkPlan.output.filter(field =>
      field.dataType.isInstanceOf[BooleanType]
      || field.dataType.isInstanceOf[DateType]
      || field.dataType.isInstanceOf[DecimalType]
      || field.dataType.isInstanceOf[DoubleType]
      || field.dataType.isInstanceOf[IntegerType]
      || field.dataType.isInstanceOf[LongType]
      || field.dataType.isInstanceOf[StringType]
      || field.dataType.isInstanceOf[TimestampType]
      || field.dataType.isInstanceOf[NullType]
    )

    // create generic expressions
    val countExpr = (child: Expression) => Count(child).toAggregateExpression()
    val distinctCountExpr = (child: Expression) => Count(child).toAggregateExpression(isDistinct = true)
    val approxDistinctCountExpr = (child: Expression) => HyperLogLogPlusPlus(child, 0.01).toAggregateExpression()
    val nullCountExpr = (child: Expression) => CountIf(IsNull(child)).toAggregateExpression()
    val meanExpr = (child: Expression) => Average(child).toAggregateExpression()
    val stddevExpr = (child: Expression) => StddevPop(child).toAggregateExpression()
    val approxStddevExpr = (child: Expression) => StddevSamp(child).toAggregateExpression()
    val minExpr = (child: Expression) => Min(child).toAggregateExpression()
    val maxExpr = (child: Expression) => Max(child).toAggregateExpression()
    val minColLengthExpr = (child: Expression) => Min(Length(child)).toAggregateExpression()
    val avgColLengthExpr = (child: Expression) => Floor(Average(Length(child)).toAggregateExpression())
    val maxColLengthExpr = (child: Expression) => Max(Length(child)).toAggregateExpression()
    val percentile25Expr = (child: Expression) => {
      GetArrayItem(
        new Percentile(child,
          Literal(new GenericArrayData(List(0.25)), ArrayType(DoubleType, false))
        ).toAggregateExpression(),
        Literal(0)
      )
    }
    val approxPercentile25Expr = (child: Expression) => {
      GetArrayItem(
        new ApproximatePercentile(child,
          Literal(new GenericArrayData(List(0.25)), ArrayType(DoubleType, false))
        ).toAggregateExpression(),
        Literal(0)
      )
    }
    val percentile50Expr = (child: Expression) => {
      GetArrayItem(
        new Percentile(child,
          Literal(new GenericArrayData(List(0.50)), ArrayType(DoubleType, false))
        ).toAggregateExpression(),
        Literal(0)
      )
    }
    val approxPercentile50Expr = (child: Expression) => {
      GetArrayItem(
        new ApproximatePercentile(child,
          Literal(new GenericArrayData(List(0.50)), ArrayType(DoubleType, false))
        ).toAggregateExpression(),
        Literal(0)
      )
    }
    val percentile75Expr = (child: Expression) => {
      GetArrayItem(
        new Percentile(child,
          Literal(new GenericArrayData(List(0.75)), ArrayType(DoubleType, false))
        ).toAggregateExpression(),
        Literal(0)
      )
    }
    val approxPercentile75Expr = (child: Expression) => {
      GetArrayItem(
        new ApproximatePercentile(child,
          Literal(new GenericArrayData(List(0.75)), ArrayType(DoubleType, false))
        ).toAggregateExpression(),
        Literal(0)
      )
    }
    val nullExpr = (child: Expression) => Literal(null)

    // map the expressions to column names
    val statistics = if (histogram) {
      Seq(
        Statistic("count", countExpr),
        Statistic("num_nulls", nullCountExpr),
        Statistic("distinct_count", if (approximate) approxDistinctCountExpr else distinctCountExpr),
        Statistic("mean", meanExpr),
        Statistic("stddev", if (approximate) approxStddevExpr else stddevExpr),
        Statistic("min_col_len", minColLengthExpr),
        Statistic("avg_col_len", avgColLengthExpr),
        Statistic("max_col_len", maxColLengthExpr),
        Statistic("min", minExpr),
        Statistic("25%", if (approximate) approxPercentile25Expr else percentile25Expr),
        Statistic("50%", if (approximate) approxPercentile50Expr else percentile50Expr),
        Statistic("75%", if (approximate) approxPercentile75Expr else percentile75Expr),
        Statistic("max", maxExpr),
      )
    } else {
      Seq(
        Statistic("count", countExpr),
        Statistic("num_nulls", nullCountExpr),
        Statistic("distinct_count", if (approximate) approxDistinctCountExpr else distinctCountExpr),
        Statistic("mean", meanExpr),
        Statistic("stddev", if (approximate) approxStddevExpr else stddevExpr),
        Statistic("min_col_len", minColLengthExpr),
        Statistic("avg_col_len", avgColLengthExpr),
        Statistic("max_col_len", maxColLengthExpr),
        Statistic("min", minExpr),
        Statistic("max", maxExpr),
      )
    }
    // generate the aggregate statement
    // override any dataType / statistics that need extra logic
    // this statement will produce a single row dataset with (n statistics * n input columns) columns
    val pivotAgg = inputColumns.flatMap { column =>
      statistics.map { statistic =>
        (column.dataType, statistic.name) match {
          // null boolean columns
          case (BooleanType, "mean") => new Column(Cast(nullExpr(column), StringType))
          case (BooleanType, "stddev") => new Column(Cast(nullExpr(column), StringType))
          case (BooleanType, "min_col_len") => new Column(Cast(nullExpr(column), StringType))
          case (BooleanType, "avg_col_len") => new Column(Cast(nullExpr(column), StringType))
          case (BooleanType, "max_col_len") => new Column(Cast(nullExpr(column), StringType))
          case (BooleanType, "25%") => new Column(Cast(nullExpr(column), StringType))
          case (BooleanType, "50%") => new Column(Cast(nullExpr(column), StringType))
          case (BooleanType, "75%") => new Column(Cast(nullExpr(column), StringType))

          // null datetype columns
          // datetype percentiles can only be approximated
          case (DateType, "mean") => new Column(Cast(nullExpr(column), StringType))
          case (DateType, "stddev") => new Column(Cast(nullExpr(column), StringType))
          case (DateType, "min_col_len") => new Column(Cast(nullExpr(column), StringType))
          case (DateType, "avg_col_len") => new Column(Cast(nullExpr(column), StringType))
          case (DateType, "max_col_len") => new Column(Cast(nullExpr(column), StringType))
          case (DateType, "25%") => new Column(Cast(approxPercentile25Expr(column), StringType))
          case (DateType, "50%") => new Column(Cast(approxPercentile50Expr(column), StringType))
          case (DateType, "75%") => new Column(Cast(approxPercentile75Expr(column), StringType))

          case (DecimalType(), "min_col_len") => new Column(Cast(nullExpr(column), StringType))
          case (DecimalType(), "avg_col_len") => new Column(Cast(nullExpr(column), StringType))
          case (DecimalType(), "max_col_len") => new Column(Cast(nullExpr(column), StringType))

          case (DoubleType, "min_col_len") => new Column(Cast(nullExpr(column), StringType))
          case (DoubleType, "avg_col_len") => new Column(Cast(nullExpr(column), StringType))
          case (DoubleType, "max_col_len") => new Column(Cast(nullExpr(column), StringType))

          case (IntegerType, "min_col_len") => new Column(Cast(nullExpr(column), StringType))
          case (IntegerType, "avg_col_len") => new Column(Cast(nullExpr(column), StringType))
          case (IntegerType, "max_col_len") => new Column(Cast(nullExpr(column), StringType))

          // floor the mean to return 'LongType'
          case (LongType, "mean") => new Column(Cast(Floor(meanExpr(column)), StringType))
          case (LongType, "min_col_len") => new Column(Cast(nullExpr(column), StringType))
          case (LongType, "avg_col_len") => new Column(Cast(nullExpr(column), StringType))
          case (LongType, "max_col_len") => new Column(Cast(nullExpr(column), StringType))
          case (LongType, "25%") => if (approximate) {
            new Column(Cast(Floor(approxPercentile25Expr(column)), StringType))
          } else {
            new Column(Cast(Floor(percentile25Expr(column)), StringType))
          }
          case (LongType, "50%") => if (approximate) {
            new Column(Cast(Floor(approxPercentile50Expr(column)), StringType))
          } else {
            new Column(Cast(Floor(percentile50Expr(column)), StringType))
          }
          case (LongType, "75%") => if (approximate) {
            new Column(Cast(Floor(approxPercentile75Expr(column)), StringType))
          } else {
            new Column(Cast(Floor(percentile75Expr(column)), StringType))
          }

          // null timestamp columns
          case (TimestampType, "mean") => new Column(Cast(nullExpr(column), StringType))
          case (TimestampType, "stddev") => new Column(Cast(nullExpr(column), StringType))
          case (TimestampType, "min_col_len") => new Column(Cast(nullExpr(column), StringType))
          case (TimestampType, "avg_col_len") => new Column(Cast(nullExpr(column), StringType))
          case (TimestampType, "max_col_len") => new Column(Cast(nullExpr(column), StringType))

          // apply explicit output formatting
          // timestamptype percentiles can only be approximated
          case (TimestampType, "min") => new Column(Cast(new DateFormatClass(minExpr(column), Literal("yyyy-MM-dd HH:mm:ss'Z'")), StringType))
          case (TimestampType, "25%") => new Column(Cast(new DateFormatClass(approxPercentile25Expr(column), Literal("yyyy-MM-dd HH:mm:ss'Z'")), StringType))
          case (TimestampType, "50%") => new Column(Cast(new DateFormatClass(approxPercentile50Expr(column), Literal("yyyy-MM-dd HH:mm:ss'Z'")), StringType))
          case (TimestampType, "75%") => new Column(Cast(new DateFormatClass(approxPercentile75Expr(column), Literal("yyyy-MM-dd HH:mm:ss'Z'")), StringType))
          case (TimestampType, "max") => new Column(Cast(new DateFormatClass(maxExpr(column), Literal("yyyy-MM-dd HH:mm:ss'Z'")), StringType))

          // do not leak string data
          case (StringType, "min") => new Column(Cast(nullExpr(column), StringType))
          case (StringType, "max") => new Column(Cast(nullExpr(column), StringType))

          // override strange count behavior
          case (NullType, "count") => new Column(Cast(nullExpr(column), StringType))
          case (NullType, "countDistinct") => new Column(Cast(nullExpr(column), StringType))
          case (NullType, "min_col_len") => new Column(Cast(nullExpr(column), StringType))
          case (NullType, "avg_col_len") => new Column(Cast(nullExpr(column), StringType))
          case (NullType, "max_col_len") => new Column(Cast(nullExpr(column), StringType))

          // otherwise apply generic function
          case _ => new Column(Cast(statistic.expr(column), StringType))
        }
      }
    }

    // deterministic timezone
    val previousSessionTimeZone = spark.conf.getOption("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    // execute the aggregation
    val aggResult = input.select(pivotAgg: _*).collect.head

    // reset timezone
    previousSessionTimeZone match {
      case Some(tz) => spark.conf.set("spark.sql.session.timeZone", tz)
      case None => spark.conf.unset("spark.sql.session.timeZone")
    }

    // wrap the results back into a list of rows
    // one row for each input column
    // adds input column name to first value in row
    val results = (0 to (aggResult.size / statistics.length) - 1).map { rowIndex =>
      Row.fromSeq(
        Seq(inputColumns(rowIndex).name, inputColumns(rowIndex).dataType.catalogString) ++ (0 to statistics.length - 1).map { columnIndex =>
          aggResult.getString(rowIndex * statistics.length + columnIndex)
        }
      )
    }

    spark.createDataFrame(
      spark.sparkContext.makeRDD(results),
      StructType(Seq(StructField("col_name", StringType, true), StructField("data_type", StringType, true)) ++ statistics.map { statistic => StructField(statistic.name, StringType, true) })
    )
  }

  // converts the statistics dataframe produced by createStatisticsDataframe to a JSON string
  def createStatisticsJSON(input: DataFrame)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): String = {
    val jsonNodeFactory = new JsonNodeFactory(true)
    val objectMapper = new ObjectMapper()
    val node = jsonNodeFactory.objectNode

    input.collect.toList.foreach { case (row) =>
      val schema = row.schema
      val statisticsNode = jsonNodeFactory.objectNode

      // loop through each row and create an object with only the statistics values
      // try to convert them to numeric/boolean values if possible
      (1 to row.size - 1).foreach { column =>
        statisticsNode.set[ObjectNode](schema(column).name,
          if (!row.isNullAt(column)) {
            scala.util.Try {
              jsonNodeFactory.numberNode(row.getString(column).toLong)
            }.orElse {
              scala.util.Try {
                jsonNodeFactory.numberNode(row.getString(column).toDouble)
              }
            }.orElse {
              scala.util.Try {
                jsonNodeFactory.booleanNode(row.getString(column).toBoolean)
              }
            }
            .getOrElse(jsonNodeFactory.textNode(row.getString(column)))
          } else {
            jsonNodeFactory.nullNode
          }
        )
      }
      // add the statistics to the output node keyed by column name
      node.set[ObjectNode](row.getString(0), statisticsNode)
    }

    // convert to JSON string
    objectMapper.writeValueAsString(node)
  }

}