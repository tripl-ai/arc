// package ai.tripl.arc

// import java.net.URI

// import org.scalatest.FunSuite
// import org.scalatest.BeforeAndAfter

// import org.apache.commons.lang3.exception.ExceptionUtils
// import scala.collection.JavaConverters._

// import org.apache.commons.io.FileUtils
// import org.apache.commons.io.IOUtils

// import org.apache.spark.ml.{Pipeline, PipelineModel}
// import org.apache.spark.ml.classification.LogisticRegression
// import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
// import org.apache.spark.sql._
// import org.apache.spark.sql.functions._

// import ai.tripl.arc.util._
// import ai.tripl.arc.api._
// import ai.tripl.arc.api.API._
// import ai.tripl.arc.util.log.LoggerFactory 

// import ai.tripl.arc.util.TestUtils

// class ARCSuite extends FunSuite with BeforeAndAfter {

//   var session: SparkSession = _  
//   val targetFile = FileUtils.getTempDirectoryPath()
//   val inputView = "inputView"
//   val outputView = "outputView"

//   before {
//     implicit val spark = SparkSession
//       .builder()
//       .master("local[*]")
//       .config("spark.ui.port", "9999")
//       .appName("Spark ETL Test")
//       .getOrCreate()
//     spark.sparkContext.setLogLevel("ERROR")

//     // set for deterministic timezone
//     spark.conf.set("spark.sql.session.timeZone", "UTC")       

//     session = spark
//   }

//   after {
//     session.stop()
//   }

//   test("ARCSuite") {
//     implicit val spark = session
//     import spark.implicits._
//     implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
//     implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false, lifecyclePlugins=Nil)

//     val dataset = TestUtils.getKnownDataset
//     dataset.createOrReplaceTempView(inputView)

//     val pipeline = ETLPipeline(
//         SQLTransform(
//           name="SQLTransformName", 
//           description=None,
//           inputURI=new URI(targetFile),
//           sql=s"SELECT * FROM ${inputView} WHERE booleanDatum = fasdf",
//           outputView=outputView,
//           persist=false,
//           sqlParams=Map.empty,
//           params=Map.empty,
//           numPartitions=None,
//           partitionBy=Nil             
//         ) :: Nil
//     )

//     val thrown = intercept[Exception with DetailException] {
//         ARC.run(pipeline)
//     }

//     assert(thrown.getMessage.contains("cannot resolve '`fasdf`' given input columns"))
//   }    
// }


