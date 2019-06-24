// package ai.tripl.arc

// import java.net.URI
// import java.sql.DriverManager

// import org.scalatest.FunSuite
// import org.scalatest.BeforeAndAfter

// import org.apache.commons.io.FileUtils
// import org.apache.commons.io.IOUtils
// import org.apache.spark.sql._
// import org.apache.spark.sql.functions._

// import ai.tripl.arc.api._
// import ai.tripl.arc.api.API._
// import ai.tripl.arc.util.log.LoggerFactory 

// import ai.tripl.arc.util.TestUtils

// class JDBCExtractSuite extends FunSuite with BeforeAndAfter {

//   var session: SparkSession = _  
//   var connection: java.sql.Connection = _

//   val url = "jdbc:derby:memory:JDBCExtractSuite"
//   val dbtable = "known"
//   val outputView = "dataset"

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
//     import spark.implicits._

//     // need to disable SecurityManager to allow a derby instance
//     System.setSecurityManager(null)
//     DriverManager.registerDriver(new org.apache.derby.jdbc.EmbeddedDriver())
//     connection = DriverManager.getConnection(s"${url};create=true")

//     // create known table
//     // JDBC does not support creating a table with NullType column (understandably)
//     TestUtils.getKnownDataset.drop($"nullDatum")
//       .write
//       .format("jdbc")
//       .option("url", url)
//       .option("dbtable", dbtable)
//       .save()
//   }

//   after {
//     session.stop()
//     connection.close()
//     // dropping table will throw a good exception by design
//     // see Removing an in-memory database derby docs
//     try {
//       DriverManager.getConnection(s"${url};drop=true")
//     } catch {
//       case e: Exception =>
//     }
//   }

//   test("JDBCExtract: Table") {
//     implicit val spark = session
//     import spark.implicits._
//     implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
//     implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false, lifecyclePlugins=Nil, disableDependencyValidation=false)

//     // parse json schema to List[ExtractColumn]
//     val cols = ai.tripl.arc.util.MetadataSchema.parseJsonMetadata(TestUtils.getKnownDatasetMetadataJson)    

//     val result = extract.JDBCExtract.extract(
//       JDBCExtract(
//         name=outputView, 
//         description=None,
//         cols=Right(cols.right.getOrElse(Nil)),
//         outputView=dbtable, 
//         jdbcURL=url,
//         driver=DriverManager.getDriver(url),
//         tableName=dbtable, 
//         numPartitions=None, 
//         partitionBy=Nil,
//         fetchsize=None, 
//         customSchema=None,
//         partitionColumn=None,
//         predicates=Nil,
//         params=Map.empty, 
//         persist=false
//       )
//     ).get

//     var actual = result.withColumn("decimalDatum", col("decimalDatum").cast("decimal(38,18)"))
//     val expected = TestUtils.getKnownDataset.drop($"nullDatum")

//     assert(TestUtils.datasetEquality(expected, actual))

//     // test metadata
//     val timestampDatumMetadata = actual.schema.fields(actual.schema.fieldIndex("timestampDatum")).metadata    
//     assert(timestampDatumMetadata.getLong("securityLevel") == 7)        
//   }     

//   test("JDBCExtract: Query") {
//     implicit val spark = session
//     import spark.implicits._
//     implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
//     implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false, lifecyclePlugins=Nil, disableDependencyValidation=false)

//     val result = extract.JDBCExtract.extract(
//       JDBCExtract(
//         name=outputView, 
//         description=None,
//         cols=Right(Nil),
//         outputView=dbtable, 
//         jdbcURL=url, 
//         driver=DriverManager.getDriver(url),
//         tableName=s"(SELECT * FROM ${dbtable}) dbtable", 
//         numPartitions=None,
//         partitionBy=Nil,
//         fetchsize=None, 
//         customSchema=None,
//         partitionColumn=None,
//         predicates=Nil,
//         params=Map.empty, 
//         persist=false
//       )
//     ).get

//     var actual = result.withColumn("decimalDatum", col("decimalDatum").cast("decimal(38,18)"))
//     val expected = TestUtils.getKnownDataset.drop($"nullDatum")

//     assert(TestUtils.datasetEquality(expected, actual))
//   }   

//   test("JDBCExtract: Query returning Empty Dataset") {
//     implicit val spark = session
//     import spark.implicits._
//     implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
//     implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false, lifecyclePlugins=Nil, disableDependencyValidation=false)

//     val result = extract.JDBCExtract.extract(
//       JDBCExtract(
//         name=outputView, 
//         description=None,
//         cols=Right(Nil),
//         outputView=dbtable, 
//         jdbcURL=url, 
//         driver=DriverManager.getDriver(url),
//         tableName=s"(SELECT * FROM ${dbtable} WHERE false) dbtable", 
//         numPartitions=None,
//         partitionBy=Nil,
//         fetchsize=None, 
//         customSchema=None,
//         partitionColumn=None,
//         predicates=Nil,
//         params=Map.empty, 
//         persist=false
//       )
//     ).get

//     var actual = result.withColumn("decimalDatum", col("decimalDatum").cast("decimal(38,18)"))
//     val expected = TestUtils.getKnownDataset.drop($"nullDatum")

//     // data types will mismatch due to derby but test that we have at least same column names
//     assert(actual.schema.map(_.name) === expected.schema.map(_.name))
//   }   
// }
