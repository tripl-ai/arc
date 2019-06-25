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

import ai.tripl.arc.util.TestUtils

class XMLExtractSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  
  val targetFile = FileUtils.getTempDirectoryPath() + "extract.xml" 
  val targetFileGlob = FileUtils.getTempDirectoryPath() + "ex{t,a,b,c}ract.xml" 
  val emptyDirectory = FileUtils.getTempDirectoryPath() + "empty.xml" 
  val emptyWildcardDirectory = FileUtils.getTempDirectoryPath() + "*.xml.gz" 
  val zipSingleRecord = getClass.getResource("/note.xml.zip").toString
  val zipMultipleRecord =  getClass.getResource("/notes.xml.zip").toString
  val inputView = "dataset"
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
    spark.sparkContext.hadoopConfiguration.set("io.compression.codecs", classOf[ai.tripl.arc.util.ZipCodec].getName)

    // recreate test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile)) 
    FileUtils.deleteQuietly(new java.io.File(emptyDirectory)) 
    FileUtils.forceMkdir(new java.io.File(emptyDirectory))

    // force com.sun.xml.* implementation for writing xml to be compatible with spark-xml library
    System.setProperty("javax.xml.stream.XMLOutputFactory", "com.sun.xml.internal.stream.XMLOutputFactoryImpl")    
    // XML will silently drop NullType on write
    TestUtils.getKnownDataset.write.option("rowTag", "testRow").format("com.databricks.spark.xml").save(targetFile)
  }

  after {
    session.stop()

    // clean up test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile))     
    FileUtils.deleteQuietly(new java.io.File(emptyDirectory))     
  }

  test("XMLExtract") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    // parse json schema to List[ExtractColumn]
    val schema = ai.tripl.arc.util.MetadataSchema.parseJsonMetadata(TestUtils.getKnownDatasetMetadataJson)    

    val dataset = extract.XMLExtractStage.execute(
      extract.XMLExtractStage(
        plugin=new extract.XMLExtract,
        name=outputView,
        description=None,
        schema=Right(schema.right.getOrElse(Nil)),
        outputView=outputView,
        input=Right(targetFileGlob),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=true
      )
    ).get

    // test that the filename is correctly populated
    assert(dataset.filter($"_filename".contains(targetFile)).count != 0)    

    val expected = TestUtils.getKnownDataset
      .withColumn("decimalDatum", col("decimalDatum").cast("double"))
      .drop($"nullDatum")
  
    val internal = dataset.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)
    val actual = dataset.drop(internal:_*)

    assert(TestUtils.datasetEquality(expected, actual))

    // test metadata
    val timestampDatumMetadata = actual.schema.fields(actual.schema.fieldIndex("timestampDatum")).metadata    
    assert(timestampDatumMetadata.getLong("securityLevel") == 7)        
  }  

  test("XMLExtract: Caching") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    // no cache
    extract.XMLExtractStage.execute(
      extract.XMLExtractStage(
        plugin=new extract.XMLExtract,
        name=outputView,
        description=None,
        schema=Right(Nil),
        outputView=outputView,
        input=Right(targetFile),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=true
      )
    )
    assert(spark.catalog.isCached(outputView) === false)

    // cache
    extract.XMLExtractStage.execute(
      extract.XMLExtractStage(
        plugin=new extract.XMLExtract,
        name=outputView,
        description=None,
        schema=Right(Nil),
        outputView=outputView,
        input=Right(targetFile),
        authentication=None,
        params=Map.empty,
        persist=true,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=true
      )
    )
    assert(spark.catalog.isCached(outputView) === true)     
  }  

  test("XMLExtract: Empty Dataset") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val schema = 
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
      extract.XMLExtractStage.execute(
        extract.XMLExtractStage(
          plugin=new extract.XMLExtract,
          name=outputView,
          description=None,
          schema=Right(Nil),
          outputView=outputView,
          input=Right(emptyWildcardDirectory),
          authentication=None,
          params=Map.empty,
          persist=false,
          numPartitions=None,
          partitionBy=Nil,
          contiguousIndex=true
        )
      )
    }
    assert(thrown0.getMessage === "XMLExtract has produced 0 columns and no schema has been provided to create an empty dataframe.")
    
    // try without providing column metadata
    val thrown1 = intercept[Exception with DetailException] {
      extract.XMLExtractStage.execute(
        extract.XMLExtractStage(
          plugin=new extract.XMLExtract,
          name=outputView,
          description=None,
          schema=Right(Nil),
          outputView=outputView,
          input=Right(emptyDirectory),
          authentication=None,
          params=Map.empty,
          persist=false,
          numPartitions=None,
          partitionBy=Nil,
          contiguousIndex=true
        )
      )
    }
    assert(thrown1.getMessage === "XMLExtract has produced 0 columns and no schema has been provided to create an empty dataframe.")
    
    // try with column
    val dataset = extract.XMLExtractStage.execute(
      extract.XMLExtractStage(
        plugin=new extract.XMLExtract,
        name=outputView,
        description=None,
        schema=Right(schema),
        outputView=outputView,
        input=Right(emptyDirectory),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=true
      )
    ).get

    val expected = TestUtils.getKnownDataset.select($"booleanDatum").limit(0)

    val internal = dataset.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)
    val actual = dataset.drop(internal:_*)    

    assert(TestUtils.datasetEquality(expected, actual))
  }  

  test("XMLExtract: .zip single record") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val dataset = extract.XMLExtractStage.execute(
      extract.XMLExtractStage(
        plugin=new extract.XMLExtract,
        name=outputView,
        description=None,
        schema=Right(Nil),
        outputView=outputView,
        input=Right(zipSingleRecord),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=true
      )
    ).get

    // test that the filename is correctly populated
    assert(dataset.filter($"_filename".contains(zipSingleRecord)).count != 0)    
    assert(dataset.schema.fieldNames.contains("body"))
  }  

  test("XMLExtract: .zip multiple record") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val dataset = extract.XMLExtractStage.execute(
      extract.XMLExtractStage(
        plugin=new extract.XMLExtract,
        name=outputView,
        description=None,
        schema=Right(Nil),
        outputView=outputView,
        input=Right(zipMultipleRecord),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=true
      )
    ).get

    // test that the filename is correctly populated
    assert(dataset.filter($"_filename".contains(zipMultipleRecord)).count != 0)    
    assert(dataset.schema.fieldNames.contains("body"))
    assert(dataset.count == 2)

  } 

  test("XMLExtract: Dataframe") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    // temporarily remove the delimiter so all the data is loaded as a single line
    spark.sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", s"${0x0 : Char}")       

    val textFile = spark.sparkContext.textFile(targetFileGlob)
    textFile.toDF.createOrReplaceTempView(inputView)

    val dataset = extract.XMLExtractStage.execute(
      extract.XMLExtractStage(
        plugin=new extract.XMLExtract,
        name=outputView,
        description=None,
        schema=Right(Nil),
        outputView=outputView,
        input=Left(inputView),
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        contiguousIndex=true
      )
    ).get

    val expected = TestUtils.getKnownDataset
      .withColumn("decimalDatum", col("decimalDatum").cast("double"))
      .drop($"nullDatum")
  
    val internal = dataset.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)
    val actual = dataset.drop(internal:_*)

    assert(TestUtils.datasetEquality(expected, actual))

  }  

}