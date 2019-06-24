// package ai.tripl.arc

// import java.net.URI

// import org.scalatest.FunSuite
// import org.scalatest.BeforeAndAfter

// import org.apache.commons.io.FileUtils
// import org.apache.commons.io.IOUtils
// import org.apache.spark.sql._
// import org.apache.spark.sql.functions._

// import ai.tripl.arc.api._
// import ai.tripl.arc.api.API._
// import ai.tripl.arc.util.log.LoggerFactory 
// import ai.tripl.arc.util._

// import com.fasterxml.jackson.databind._

// import ai.tripl.arc.util.TestUtils

// class EqualityValidateSuite extends FunSuite with BeforeAndAfter {

//   var session: SparkSession = _  
//   var testName = "EqualityValidate"
//   val leftView = "leftViewName"
//   val rightView = "rightViewName"
//   val objectMapper = new ObjectMapper()

//   before {
//     implicit val spark = SparkSession
//                   .builder()
//                   .master("local[*]")
//                   .config("spark.ui.port", "9999")
//                   .appName("Spark ETL Test")
//                   .getOrCreate()
//     spark.sparkContext.setLogLevel("ERROR")

//     // set for deterministic timezone
//     spark.conf.set("spark.sql.session.timeZone", "UTC")   

//     session = spark
//   }

//   after {
//     session.stop()
//   }

//   test("EqualityValidate: different number of columns") {
//     implicit val spark = session
//     import spark.implicits._
//     implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

//     val dataset = TestUtils.getKnownDataset
//     dataset.createOrReplaceTempView(leftView)
//     dataset.drop($"booleanDatum").createOrReplaceTempView(rightView)

//     val thrown = intercept[Exception with DetailException] {
//       validate.EqualityValidate.validate(
//         EqualityValidate(
//           name=testName, 
//           description=None,
//           leftView=leftView,
//           rightView=rightView,
//           params=Map.empty
//         )
//       )
//     }

//     assert(thrown.getMessage === s"""EqualityValidate ensures the two input datasets are the same (including column order), but '${leftView}' (10 columns) contains columns: ['booleanDatum'] that are not in '${rightView}' and '${rightView}' (9 columns) contains columns: [] that are not in '${leftView}'. Columns are not equal so cannot the data be compared.""")
//   }    

//   test("EqualityValidate: different order of columns") {
//     implicit val spark = session
//     import spark.implicits._
//     implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

//     val dataset = TestUtils.getKnownDataset
//     dataset.createOrReplaceTempView(leftView)
//     dataset.select($"dateDatum", $"decimalDatum", $"doubleDatum", $"integerDatum", $"longDatum", $"stringDatum", $"timeDatum", $"timestampDatum", $"nullDatum", $"booleanDatum").createOrReplaceTempView(rightView)

//     val thrown = intercept[Exception with DetailException] {
//       validate.EqualityValidate.validate(
//         EqualityValidate(
//           name=testName, 
//           description=None,
//           leftView=leftView,
//           rightView=rightView,
//           params=Map.empty
//         )
//       )
//     }

//     assert(thrown.getMessage === s"""EqualityValidate ensures the two input datasets are the same (including column order), but '${leftView}' contains columns (ordered): ['booleanDatum', 'dateDatum', 'decimalDatum', 'doubleDatum', 'integerDatum', 'longDatum', 'stringDatum', 'timeDatum', 'timestampDatum', 'nullDatum'] and '${rightView}' contains columns (ordered): ['dateDatum', 'decimalDatum', 'doubleDatum', 'integerDatum', 'longDatum', 'stringDatum', 'timeDatum', 'timestampDatum', 'nullDatum', 'booleanDatum']. Columns are not equal so cannot the data be compared.""")
//   }   

//   test("EqualityValidate: different type of columns") {
//     implicit val spark = session
//     import spark.implicits._
//     implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

//     val dataset = TestUtils.getKnownDataset
//     dataset.createOrReplaceTempView(leftView)
//     dataset.select($"booleanDatum".cast("string"), $"dateDatum", $"decimalDatum", $"doubleDatum", $"integerDatum", $"longDatum", $"stringDatum", $"timeDatum", $"timestampDatum", $"nullDatum").createOrReplaceTempView(rightView)

//     val thrown = intercept[Exception with DetailException] {
//       validate.EqualityValidate.validate(
//         EqualityValidate(
//           name=testName, 
//           description=None,
//           leftView=leftView,
//           rightView=rightView,
//           params=Map.empty
//         )
//       )
//     }

//     assert(thrown.getMessage === s"""EqualityValidate ensures the two input datasets are the same (including column order), but '${leftView}' contains column types (ordered): ['boolean', 'date', 'decimal(38,18)', 'double', 'integer', 'long', 'string', 'string', 'timestamp', 'null'] and '${rightView}' contains column types (ordered): ['string', 'date', 'decimal(38,18)', 'double', 'integer', 'long', 'string', 'string', 'timestamp', 'null']. Columns are not equal so cannot the data be compared.""")
//   }    

//   test("EqualityValidate: value") {
//     implicit val spark = session
//     import spark.implicits._
//     implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

//     val dataset = TestUtils.getKnownDataset
//     dataset.createOrReplaceTempView(leftView)
//     dataset.withColumn("booleanDatum", lit(true)).createOrReplaceTempView(rightView)

//     val thrown = intercept[Exception with DetailException] {
//       validate.EqualityValidate.validate(
//         EqualityValidate(
//           name=testName, 
//           description=None,
//           leftView=leftView,
//           rightView=rightView,
//           params=Map.empty
//         )
//       )
//     }

//     assert(thrown.getMessage === s"""EqualityValidate ensures the two input datasets are the same (including column order), but '${leftView}' (2 rows) contains 1 rows that are not in '${rightView}' and '${rightView}' (2 rows) contains 1 rows which are not in '${leftView}'.""")
//   }   

// }
