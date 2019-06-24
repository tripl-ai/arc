// package ai.tripl.arc

// import java.net.URI

// import org.scalatest.FunSuite
// import org.scalatest.BeforeAndAfter

// import collection.JavaConverters._

// import org.apache.commons.io.FileUtils
// import org.apache.commons.io.IOUtils
// import org.apache.spark.sql._
// import org.apache.spark.sql.functions._

// import ai.tripl.arc.api._
// import ai.tripl.arc.api.API._
// import ai.tripl.arc.datasource.BinaryContent
// import ai.tripl.arc.util.log.LoggerFactory 

// import ai.tripl.arc.util._
// import ai.tripl.arc.util.ControlUtils._

// class BytesExtractSuite extends FunSuite with BeforeAndAfter {

//   var session: SparkSession = _  

//   val pathView = "pathView"
//   val outputView = "outputView"
//   val targetFile = getClass.getResource("/notes.xml.zip").toString
//   val emptyDirectory = FileUtils.getTempDirectoryPath() + "missing.binary" 
//   val missingDirectory = FileUtils.getTempDirectoryPath() + "/missing/missing.binary" 
//   val emptyWildcardDirectory = FileUtils.getTempDirectoryPath() + "*.binary" 

//   before {
//     implicit val spark = SparkSession
//                   .builder()
//                   .master("local[*]")
//                   .config("spark.ui.port", "9999")
//                   .appName("Spark ETL Test")
//                   .getOrCreate()
//     spark.sparkContext.setLogLevel("FATAL")

//     // set for deterministic timezone
//     spark.conf.set("spark.sql.session.timeZone", "UTC")        

//     session = spark
//   }


//   after {
//     session.stop
//   }

//   test("BytesExtract: input") {
//     implicit val spark = session
//     import spark.implicits._
//     implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
//     implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false, lifecyclePlugins=Nil, disableDependencyValidation=false)

//     val extractDataset = extract.BytesExtract.extract(
//       BytesExtract(
//         name="dataset",
//         description=None,
//         outputView=outputView, 
//         input=Right(targetFile),
//         authentication=None,
//         persist=false,
//         numPartitions=None,
//         contiguousIndex=true,
//         params=Map.empty,
//         failMode=FailModeTypeFailFast
//       )
//     ).get

//     assert(extractDataset.filter($"_filename".contains(targetFile)).count != 0)
//     assert(extractDataset.count == 1)
//   }    

//   test("BytesExtract: pathView") {
//     implicit val spark = session
//     import spark.implicits._
//     implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
//     implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false, lifecyclePlugins=Nil, disableDependencyValidation=false)

//     val input = Seq(targetFile, targetFile).toDF("value")
//     input.createOrReplaceTempView(pathView)

//     val extractDataset = extract.BytesExtract.extract(
//       BytesExtract(
//         name="dataset",
//         description=None,
//         outputView=outputView, 
//         input=Left(pathView),
//         authentication=None,
//         persist=false,
//         numPartitions=None,
//         contiguousIndex=true,
//         params=Map.empty,
//         failMode=FailModeTypeFailFast
//       )
//     ).get

//     assert(extractDataset.count == 2)
//   }    

//   test("BytesExtract: FailModeTypeFailFast") {
//     implicit val spark = session
//     import spark.implicits._
//     implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

//     // try with wildcard
//     val thrown0 = intercept[Exception with DetailException] {
//       val extractDataset = extract.BytesExtract.extract(
//         BytesExtract(
//           name="dataset",
//           description=None,
//           outputView=outputView, 
//           input=Right(emptyWildcardDirectory),
//           authentication=None,
//           persist=false,
//           numPartitions=None,
//           contiguousIndex=true,
//           params=Map.empty,
//           failMode=FailModeTypeFailFast
//         )        
//       )
//     }
//     assert(thrown0.getMessage === "BytesExtract has found no files and failMode is set to 'failfast' so cannot continue.")
    
//     // try without providing column metadata
//     val thrown1 = intercept[Exception with DetailException] {
//       val extractDataset = extract.BytesExtract.extract(
//         BytesExtract(
//           name="dataset",
//           description=None,
//           outputView=outputView, 
//           input=Right(emptyDirectory),
//           authentication=None,
//           persist=false,
//           numPartitions=None,
//           contiguousIndex=true,
//           params=Map.empty,
//           failMode=FailModeTypeFailFast
//         )  
//       )
//     }
//     assert(thrown1.getMessage === "BytesExtract has found no files and failMode is set to 'failfast' so cannot continue.")
    
//     // try without providing column metadata
//     val thrown2 = intercept[Exception with DetailException] {
//       val extractDataset = extract.BytesExtract.extract(
//         BytesExtract(
//           name="dataset",
//           description=None,
//           outputView=outputView, 
//           input=Right(missingDirectory),
//           authentication=None,
//           persist=false,
//           numPartitions=None,
//           contiguousIndex=true,
//           params=Map.empty,
//           failMode=FailModeTypeFailFast
//         )  
//       )
//     }
//     assert(thrown2.getMessage === "BytesExtract has found no files and failMode is set to 'failfast' so cannot continue.")

//     // try with column
//     val actual = extract.BytesExtract.extract(
//       BytesExtract(
//         name="dataset",
//         description=None,
//         outputView=outputView, 
//         input=Right(emptyWildcardDirectory),
//         authentication=None,
//         persist=false,
//         numPartitions=None,
//         contiguousIndex=true,
//         params=Map.empty,
//         failMode=FailModeTypePermissive
//       )  
//     ).get

//     val expected = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], BinaryContent.schema)
//     assert(TestUtils.datasetEquality(expected, actual))

//   }  

// }
