package ai.tripl.arc.util

import org.apache.spark.TaskContext

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import ai.tripl.arc.api.API._

object ExtractUtils {

  type IndexRow = Row

  case class Partition (
      filename: String,
      partitionId: Integer,
      min: Long,
      max: Long,
  )

  def getSchema(schema: Either[String, List[ExtractColumn]])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): Option[StructType] = {
    schema match {
      case Right(cols) => {
        cols match {
          case Nil => None
          case c => Option(Extract.toStructType(c))
        }
      }
      case Left(view) => {
        val parseResult: ai.tripl.arc.util.ArcSchema.ParseResult = ai.tripl.arc.util.ArcSchema.parseArcSchemaDataFrame(spark.table(view))(logger)
        parseResult match {
          case Right(cols) => Option(Extract.toStructType(cols))
          case Left(errors) => throw new Exception(s"""Schema view '${view}' to cannot be parsed as it has errors: ${errors.mkString(", ")}.""")
        }
      }
    }
  }

  def addInternalColumns(input: DataFrame, contiguousIndex: Boolean)(implicit spark: SparkSession, arcContext: ARCContext): DataFrame = {
    if (!input.isStreaming && !arcContext.isStreaming) {
      // add meta columns including sequential index
      // if schema already has metadata any columns ignore
      if (input.columns.intersect(List("_filename", "_index", "_monotonically_increasing_id")).isEmpty) {

        // add these as they are required for both _monotonically_increasing_id and _index
        val enrichedInput = input
          .withColumn("_monotonically_increasing_id", monotonically_increasing_id().as("_monotonically_increasing_id", new MetadataBuilder().putBoolean("internal", true).build()))
          .withColumn("_filename", input_file_name().as("_filename", new MetadataBuilder().putBoolean("internal", true).build()))
          .withColumn("_partition_id",spark_partition_id().as("_partition_id", new MetadataBuilder().putBoolean("internal", true).build()))

        if (contiguousIndex) {
          /*
          this code logically performs a row_number over filename order by _monotonically_increasing_id
          it does this the hard way as the window function will force all data to be moved to a single partition and run on a single core
          so is very slow/inefficient on large datasets.

          simple code:

          val window = Window.partitionBy("_filename").orderBy("_monotonically_increasing_id")
          input
            .withColumn("_monotonically_increasing_id", monotonically_increasing_id())
            .withColumn("_filename", input_file_name().as("_filename", new MetadataBuilder().putBoolean("internal", true).build()))
            .withColumn("_index", row_number().over(window).as("_index", new MetadataBuilder().putBoolean("internal", true).build()))
            .drop("_monotonically_increasing_id")
          */

          // calculate the min and max for each _filename, partition combination
          // create a map[filename]map[partitionId]Partition
          // Partition holds the actual start and end index for that partition (derived by iterating over the partitions after calculations)
          // summary is then distributed to the executors so a map operation can calculate the new index
          val summary = {
            enrichedInput
              .groupBy(col("_filename"), col("_partition_id"))
              .agg(min(col("_monotonically_increasing_id")), max(col("_monotonically_increasing_id")))
              .collect
              .map { row => Partition(filename=row.getString(0), partitionId=row.getInt(1), min=row.getLong(2), max=row.getLong(3)) }
              .groupBy { _.filename }
              .map { case (filename, partitions) =>
                (filename, partitions.sortBy { _.partitionId }.scanLeft(Partition("", 0, 0, 0)) {
                    case (previousPartition, partition) => {
                      Partition(partition.filename, partition.partitionId, previousPartition.max + 1, previousPartition.max + 1 + (partition.max - partition.min))
                    }
                  }
                  .drop(1)
                  .groupBy { _.partitionId}
                  .map { case (partitionId, partitionGroup) => (partitionId, partitionGroup.head) }
                )
              }
          }

          // unfortunately using an enriched encoder here with metadata attached loses enriched metadata after the mapPartitions
          // have to overwrite the fields with .withColumn and .drop below to ensure metadata is correctly attached.
          implicit val typedEncoder: Encoder[IndexRow] = org.apache.spark.sql.catalyst.encoders.RowEncoder(enrichedInput.schema)

          // map each partition and calculate the actual row_number offset
          enrichedInput.mapPartitions[IndexRow] {  partition: Iterator[Row] =>
            val bufferedPartition = partition.buffered
            bufferedPartition.hasNext match {
              case false => partition
              case true => {
                val head = bufferedPartition.head
                val filename = head.getString(head.fieldIndex("_filename"))
                val partitionId = head.getInt(head.fieldIndex("_partition_id"))
                val monotonically_increasing_id_index = head.fieldIndex("_monotonically_increasing_id")
                val partitionSummary = summary.get(filename).get.get(partitionId).get
                bufferedPartition.map { row => {
                    // The current implementation puts the partition ID in the upper 31 bits, and the lower 33 bits represent the record number within each partition.
                    // this code gets the last 33 bits as a long (i.e. the record number portion)
                    val rowNumber = row.getLong(monotonically_increasing_id_index) & (1L << 33) - 1
                    Row.fromSeq(row.toSeq.updated(monotonically_increasing_id_index, rowNumber + partitionSummary.min))
                  }
                }
              }
            }
          }
          .withColumn("_index", col("_monotonically_increasing_id").as("_index", new MetadataBuilder().putBoolean("internal", true).build()))
          .withColumn("_filename", col("_filename").as("_filename", new MetadataBuilder().putBoolean("internal", true).build()))
          .drop("_monotonically_increasing_id")
          .drop("_partition_id")
        } else {
          enrichedInput.drop("_partition_id")
        }
      } else {
        input
      }
    } else {
      input
    }
  }
}
