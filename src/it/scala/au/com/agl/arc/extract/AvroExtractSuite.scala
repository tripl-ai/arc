package au.com.agl.arc

import java.net.URI

import scala.io.Source

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.avro.Schema
import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.util.log.LoggerFactory 

import au.com.agl.arc.util._

class AvroExtractSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  
  val targetFile = getClass.getResource("/avro/users.avro").toString 
  val targetBinaryFile = getClass.getResource("/avro/users.avrobinary").toString 
  val schemaFile = getClass.getResource("/avro/user.avsc").toString 
  val outputView = "dataset"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    session = spark
    import spark.implicits._    
  }

  after {
    session.stop()
  }

  // test("AvroExtract: Schema Included Avro") {
  //   implicit val spark = session
  //   import spark.implicits._
  //   implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)  

  //   val extractDataset = extract.AvroExtract.extract(
  //     AvroExtract(
  //       name=outputView,
  //       description=None,
  //       cols=Right(Nil),
  //       outputView=outputView,
  //       input=targetFile,
  //       authentication=None,
  //       params=Map.empty,
  //       persist=false,
  //       numPartitions=None,
  //       partitionBy=Nil,
  //       basePath=None,
  //       contiguousIndex=true,
  //       avroSchema=None
  //     )
  //   ).get

  //   extractDataset.show(false)
  // }  

  test("AvroExtract: Binary only Avro") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)  


    val schema = new Schema.Parser().parse(CloudUtils.getTextBlob(schemaFile))

    extract.BytesExtract.extract(
      BytesExtract(
        name="dataset",
        description=None,
        outputView=outputView, 
        input=Right(targetBinaryFile),
        authentication=None,
        persist=false,
        numPartitions=None,
        contiguousIndex=true,
        params=Map.empty
      )
    )

    val extractDataset = extract.AvroExtract.extract(
      AvroExtract(
        name=outputView,
        description=None,
        cols=Right(Nil),
        outputView=outputView,
        input=Left(outputView),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        basePath=None,
        contiguousIndex=true,
        avroSchema=Option(schema),
        inputField=None
      )
    ).get
    

    spark.sql(s"""SELECT value.* FROM ${outputView}""").show(false)
  }    
}
