package ai.tripl.arc

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.util.log.LoggerFactory 

import ai.tripl.arc.util._

class AvroExtractSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  
  val targetFile = FileUtils.getTempDirectoryPath() + "extract.avro" 
  val targetFileGlob = FileUtils.getTempDirectoryPath() + "ex{t,a,b,c}ract.avro" 
  val emptyDirectory = FileUtils.getTempDirectoryPath() + "empty.avro" 
  val emptyWildcardDirectory = FileUtils.getTempDirectoryPath() + "*.avro.gz" 
  val outputView = "dataset"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")    

    session = spark
    import spark.implicits._    

    // recreate test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile)) 
    FileUtils.deleteQuietly(new java.io.File(emptyDirectory)) 
    FileUtils.forceMkdir(new java.io.File(emptyDirectory))
    // avro does not support writing NullType
    TestUtils.getKnownDataset.drop($"nullDatum").write.format("com.databricks.spark.avro").save(targetFile)
  }

  after {
    session.stop()

    // clean up test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile))     
    FileUtils.deleteQuietly(new java.io.File(emptyDirectory))     
  }

  test("AvroExtract") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    // parse json schema to List[ExtractColumn]
    val cols = ai.tripl.arc.util.MetadataSchema.parseJsonMetadata(TestUtils.getKnownDatasetMetadataJson)    

    val dataset = extract.AvroExtractStage.extract(
      extract.AvroExtractStage(
        plugin=new extract.AvroExtract,
        name=outputView,
        description=None,
        cols=Right(cols.right.getOrElse(Nil)),
        outputView=outputView,
        input=Right(targetFileGlob),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        basePath=None,
        contiguousIndex=true,
        avroSchema=None,
        inputField=None
      )
    ).get

    // test that the filename is correctly populated
    assert(dataset.filter($"_filename".contains(targetFile)).count != 0)  

    val internal = dataset.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)

    val expected = TestUtils.getKnownDataset
      .drop($"nullDatum")
    val actual = dataset.drop(internal:_*)

    assert(TestUtils.datasetEquality(expected, actual))

    // test metadata
    val timestampDatumMetadata = actual.schema.fields(actual.schema.fieldIndex("timestampDatum")).metadata    
    assert(timestampDatumMetadata.getLong("securityLevel") == 7)
  }  

  test("AvroExtract Caching") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    // no cache
    extract.AvroExtractStage.extract(
      extract.AvroExtractStage(
        plugin=new extract.AvroExtract,
        name=outputView,
        description=None,
        cols=Right(Nil),
        outputView=outputView,
        input=Right(targetFile),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        basePath=None,
        contiguousIndex=true,
        avroSchema=None,
        inputField=None        
      )
    )
    assert(spark.catalog.isCached(outputView) === false)

    // cache
    extract.AvroExtractStage.extract(
      extract.AvroExtractStage(
        plugin=new extract.AvroExtract,
        name=outputView,
        description=None,
        cols=Right(Nil),
        outputView=outputView,
        input=Right(targetFile),
        authentication=None,
        params=Map.empty,
        persist=true,
        numPartitions=None,
        partitionBy=Nil,
        basePath=None,
        contiguousIndex=true,
        avroSchema=None,
        inputField=None        
      )
    )
    assert(spark.catalog.isCached(outputView) === true)     
  }  

  test("AvroExtract Empty Dataset") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val cols = 
      BooleanColumn(
        id="1",
        name="booleanDatum",
        description=None,
        nullable=true,
        nullReplacementValue=None,
        trim=false,
        nullableValues=Nil, 
        trueValues=Nil, 
        falseValues=Nil,
        metadata=None
      ) :: Nil    

    // try with wildcard
    val thrown0 = intercept[Exception with DetailException] {
      extract.AvroExtractStage.extract(
        extract.AvroExtractStage(
          plugin=new extract.AvroExtract,
          name=outputView,
          description=None,
          cols=Right(Nil),
          outputView=outputView,
          input=Right(emptyWildcardDirectory),
          authentication=None,
          params=Map.empty,
          persist=false,
          numPartitions=None,
          partitionBy=Nil,
          basePath=None,
          contiguousIndex=true,
          avroSchema=None,
          inputField=None          
        )
      )
    }
    assert(thrown0.getMessage === "AvroExtract has produced 0 columns and no schema has been provided to create an empty dataframe.")
    
    // try without providing column metadata
    val thrown1 = intercept[Exception with DetailException] {
      extract.AvroExtractStage.extract(
        extract.AvroExtractStage(
          plugin=new extract.AvroExtract,
          name=outputView,
          description=None,
          cols=Right(Nil),
          outputView=outputView,
          input=Right(emptyDirectory),
          authentication=None,
          params=Map.empty,
          persist=false,
          numPartitions=None,
          partitionBy=Nil,
          basePath=None,
          contiguousIndex=true,
          avroSchema=None,
          inputField=None          
        )
      )
    }
    assert(thrown1.getMessage === "AvroExtract has produced 0 columns and no schema has been provided to create an empty dataframe.")
    
    // try with column
    val dataset = extract.AvroExtractStage.extract(
      extract.AvroExtractStage(
        plugin=new extract.AvroExtract,
        name=outputView,
        description=None,
        cols=Right(cols),
        outputView=outputView,
        input=Right(emptyDirectory),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        basePath=None,
        contiguousIndex=true,
        avroSchema=None,
        inputField=None        
      )
    ).get

    val internal = dataset.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)
    val actual = dataset.drop(internal:_*)

    val expected = TestUtils.getKnownDataset.select($"booleanDatum").limit(0)

    assert(TestUtils.datasetEquality(expected, actual))
  }  
}
