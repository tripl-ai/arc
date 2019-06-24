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
// import ai.tripl.arc.util.log.LoggerFactory 

// import ai.tripl.arc.util._
// import ai.tripl.arc.util.ControlUtils._

// class RateExtractSuite extends FunSuite with BeforeAndAfter {

//   var session: SparkSession = _  

//   val outputView = "outputView"

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

//   test("RateExtract: Structured Streaming") {
//     implicit val spark = session
//     import spark.implicits._
//     implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
//     implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=true, ignoreEnvironments=false, lifecyclePlugins=Nil, disableDependencyValidation=false)

//     val extractDataset = extract.RateExtract.extract(
//       RateExtract(
//         name="dataset",
//         description=None,
//         outputView=outputView, 
//         rowsPerSecond=10,
//         rampUpTime=0,
//         numPartitions=1,
//         params=Map.empty
//       )
//     ).get

//     val writeStream = extractDataset
//       .writeStream
//       .queryName("extract") 
//       .format("memory")
//       .start

//     val df = spark.table("extract")

//     try {
//       Thread.sleep(2000)
//       assert(df.count != 0)
//     } finally {
//       writeStream.stop
//     }  
//   }    
// }
