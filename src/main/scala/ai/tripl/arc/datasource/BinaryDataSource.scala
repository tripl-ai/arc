package ai.tripl.arc.datasource

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider, TableScan}
import org.apache.spark.sql.types._

import scala.util.Try

object BinaryContent {
  val schema = StructType(
    Seq(
      StructField("value", BinaryType, true),
      StructField("_filename", StringType, false)
    )
  )
}

class BinaryContentRelation(val sqlContext: SQLContext, val path: String) extends BaseRelation with TableScan {

  override def schema: StructType = {
    BinaryContent.schema
  }

  override def buildScan(): RDD[Row] = {
    sqlContext.sparkContext.binaryFiles(path).map { case (k, pds) => Row(Try(pds.toArray()).toOption, k) }
  }

}

class BinaryContentDataSource extends DataSourceRegister with RelationProvider {

  override def shortName(): String = "bytes"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {

    new BinaryContentRelation(sqlContext, parameters("path"))
  }

}
