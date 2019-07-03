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
import ai.tripl.arc.util._

import ai.tripl.arc.util.TestUtils

class ParquetExtractSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  
  val targetFile = FileUtils.getTempDirectoryPath() + "extract.parquet" 
  val targetFileGlob = FileUtils.getTempDirectoryPath() + "ex{t,a,b,c}ract.parquet" 
  val emptyDirectory = FileUtils.getTempDirectoryPath() + "empty.parquet" 
  val emptyWildcardDirectory = FileUtils.getTempDirectoryPath() + "*.parquet.gz" 
  val outputView = "dataset"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")    

    session = spark
    import spark.implicits._    

    // recreate test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile)) 
    FileUtils.deleteQuietly(new java.io.File(emptyDirectory)) 
    FileUtils.forceMkdir(new java.io.File(emptyDirectory))
    // parquet does not support writing NullType
    TestUtils.getKnownDataset.drop($"nullDatum").write.parquet(targetFile)
  }

  after {
    session.stop()

    // clean up test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile))     
    FileUtils.deleteQuietly(new java.io.File(emptyDirectory))     
  }

  test("ParquetExtract: end-to-end") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val conf = s"""{
      "stages": [
        {
          "type": "ParquetExtract",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "${targetFile}",
          "outputView": "${outputView}"
        }
      ]
    }"""
    
    val pipelineEither = ConfigUtils.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => {
        println(err)
        assert(false)
      }
      case Right((pipeline, _)) => {
        val df = ARC.run(pipeline)(spark, logger, arcContext)
      }
    }  
  }

  test("ParquetExtract: Metadata from columns") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    // parse json schema to List[ExtractColumn]
    val schema = ai.tripl.arc.util.MetadataSchema.parseJsonMetadata(TestUtils.getKnownDatasetMetadataJson)    

    val dataset = extract.ParquetExtractStage.execute(
      extract.ParquetExtractStage(
        plugin=new extract.ParquetExtract,
        name=outputView,
        description=None,
        schema=Right(schema.right.getOrElse(Nil)),
        outputView=outputView,
        input=targetFileGlob,
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        basePath=None,
        contiguousIndex=true
      )
    ).get

    // test that the filename is correctly populated
    assert(dataset.filter($"_filename".contains(targetFile)).count != 0)

    val internal = dataset.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)
    val actual = dataset.drop(internal:_*)

    val expected = TestUtils.getKnownDataset.drop($"nullDatum")

    assert(TestUtils.datasetEquality(expected, actual))

    // test metadata
    val timestampDatumMetadata = actual.schema.fields(actual.schema.fieldIndex("timestampDatum")).metadata    
    assert(timestampDatumMetadata.getLong("securityLevel") == 7)        
  }  

  test("ParquetExtract: Caching") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    // no cache
    extract.ParquetExtractStage.execute(
      extract.ParquetExtractStage(
        plugin=new extract.ParquetExtract,
        name=outputView,
        description=None,
        schema=Right(Nil),
        outputView=outputView,
        input=targetFile,
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        basePath=None,
        contiguousIndex=true
      )
    )
    assert(spark.catalog.isCached(outputView) === false)

    // cache
    extract.ParquetExtractStage.execute(
      extract.ParquetExtractStage(
        plugin=new extract.ParquetExtract,
        name=outputView,
        description=None,
        schema=Right(Nil),
        outputView=outputView,
        input=targetFile,
        authentication=None,
        params=Map.empty,
        persist=true,
        numPartitions=None,
        partitionBy=Nil,
        basePath=None,
        contiguousIndex=true
      )
    )
    assert(spark.catalog.isCached(outputView) === true)     
  }  

  test("ParquetExtract: Empty Dataset") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
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
      extract.ParquetExtractStage.execute(
        extract.ParquetExtractStage(
          plugin=new extract.ParquetExtract,
          name=outputView,
          description=None,
          schema=Right(Nil),
          outputView=outputView,
          input=emptyWildcardDirectory,
          authentication=None,
          params=Map.empty,
          persist=false,
          numPartitions=None,
          partitionBy=Nil,
          basePath=None,
          contiguousIndex=true
        )
      )
    }
    assert(thrown0.getMessage === "ParquetExtract has produced 0 columns and no schema has been provided to create an empty dataframe.")
    
    // try without providing column metadata
    val thrown1 = intercept[Exception with DetailException] {
      extract.ParquetExtractStage.execute(
        extract.ParquetExtractStage(
          plugin=new extract.ParquetExtract,
          name=outputView,
          description=None,
          schema=Right(Nil),
          outputView=outputView,
          input=emptyDirectory,
          authentication=None,
          params=Map.empty,
          persist=false,
          numPartitions=None,
          partitionBy=Nil,
          basePath=None,
          contiguousIndex=true
        )
      )
    }
    assert(thrown1.getMessage === "ParquetExtract has produced 0 columns and no schema has been provided to create an empty dataframe.")
    
    // try with column
    val dataset = extract.ParquetExtractStage.execute(
      extract.ParquetExtractStage(
        plugin=new extract.ParquetExtract,
        name=outputView,
        description=None,
        schema=Right(schema),
        outputView=outputView,
        input=emptyDirectory,
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        basePath=None,
        contiguousIndex=true
      )
    ).get

    val internal = dataset.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)
    val actual = dataset.drop(internal:_*)

    val expected = TestUtils.getKnownDataset.select($"booleanDatum").limit(0)

    assert(TestUtils.datasetEquality(expected, actual))
  }  

  test("ParquetExtract: Structured Streaming") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=true)

    // parse json schema to List[ExtractColumn]
    val schema = ai.tripl.arc.util.MetadataSchema.parseJsonMetadata(TestUtils.getKnownDatasetMetadataJson)    

    val dataset = extract.ParquetExtractStage.execute(
      extract.ParquetExtractStage(
        plugin=new extract.ParquetExtract,
        name=outputView,
        description=None,
        schema=Right(schema.right.getOrElse(Nil)),
        outputView=outputView,
        input=targetFileGlob,
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        basePath=None,
        contiguousIndex=true
      )
    ).get

    val writeStream = dataset
      .writeStream
      .queryName("extract") 
      .format("memory")
      .start

    val df = spark.table("extract")

    try {
      Thread.sleep(2000)
      // will fail if parsing does not work
      df.first.getBoolean(0)
    } finally {
      writeStream.stop
    }  
  }    
}
