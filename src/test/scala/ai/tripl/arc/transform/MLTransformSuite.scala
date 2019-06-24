// package ai.tripl.arc

// import java.net.URI

// import org.scalatest.FunSuite
// import org.scalatest.BeforeAndAfter

// import org.apache.commons.io.FileUtils

// import org.apache.spark.ml.{Pipeline, PipelineModel}
// import org.apache.spark.ml.classification.LogisticRegression
// import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
// import org.apache.spark.ml.tuning._
// import org.apache.spark.ml.evaluation._
// import org.apache.spark.sql._
// import org.apache.spark.sql.functions._

// import ai.tripl.arc.api.API._
// import ai.tripl.arc.util.log.LoggerFactory 

// import ai.tripl.arc.util.TestUtils

// class MLTransformSuite extends FunSuite with BeforeAndAfter {

//   var session: SparkSession = _  
//   val pipelineModelTargetFile = FileUtils.getTempDirectoryPath() + "spark-logistic-regression-model-pipelinemodel" 
//   val crossValidatorModelTargetFile = FileUtils.getTempDirectoryPath() + "spark-logistic-regression-model-crossvalidatormodel" 
//   val inputView = "inputView"
//   val outputView = "outputView"

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

//     // ensure targets removed
//     FileUtils.deleteQuietly(new java.io.File(pipelineModelTargetFile))     
//     FileUtils.deleteQuietly(new java.io.File(crossValidatorModelTargetFile))   

//     // Train an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
//     val training = spark.createDataFrame(Seq(
//       (0L, "a b c d e spark", 1.0),
//       (1L, "b d", 0.0),
//       (2L, "spark f g h", 1.0),
//       (3L, "hadoop mapreduce", 0.0)
//     )).toDF("id", "text", "label")

//     val tokenizer = new Tokenizer()
//       .setInputCol("text")
//       .setOutputCol("words")

//     val hashingTF = new HashingTF()
//       .setNumFeatures(1000)
//       .setInputCol(tokenizer.getOutputCol)
//       .setOutputCol("features")

//     val lr = new LogisticRegression()
//       .setMaxIter(10)
//       .setRegParam(0.001)

//     val pipeline = new Pipeline()
//       .setStages(Array(tokenizer, hashingTF, lr))

//     val pipelineModel = pipeline.fit(training)
//     pipelineModel.write.overwrite().save(pipelineModelTargetFile)    

//     val paramGrid = new ParamGridBuilder().build()

//     val crossValidator = new CrossValidator()
//       .setEstimator(pipeline)
//       .setEvaluator(new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("f1"))
//       .setEstimatorParamMaps(paramGrid)
//       .setNumFolds(2) // Use 3+ in practice

//     val crossValidatorModel = crossValidator.fit(training)
//     crossValidatorModel.write.overwrite().save(crossValidatorModelTargetFile) 
//   }

//   after {
//     session.stop()

//     // clean up test dataset
//     FileUtils.deleteQuietly(new java.io.File(pipelineModelTargetFile))     
//     FileUtils.deleteQuietly(new java.io.File(crossValidatorModelTargetFile))     
//   }

//   test("MLTransform: pipelineModel") {
//     implicit val spark = session
//     import spark.implicits._
//     implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

//     val expected = spark.createDataFrame(Seq(
//       (4L, "spark i j k", 1.0, 0.8),
//       (5L, "l m n", 0.0, 0.8),
//       (6L, "spark hadoop spark", 1.0, 0.9),
//       (7L, "apache hadoop", 0.0, 1.0)
//     )).toDF("id", "text", "prediction", "probability")
//     expected.drop("prediction").drop("probability").createOrReplaceTempView(inputView)

//     val transformed = transform.MLTransform.transform(
//       MLTransform(
//         name="MLTransform", 
//         description=None,
//         inputURI=new URI(pipelineModelTargetFile),
//         model=Left(PipelineModel.load(pipelineModelTargetFile)),
//         inputView=inputView,
//         outputView=outputView,
//         persist=true,
//         params=Map.empty,
//         numPartitions=None,
//         partitionBy=Nil           
//       )
//     ).get

//     // round due to random seed changing
//     val actual = transformed.withColumn("probability", round($"probability", 1))

//     assert(TestUtils.datasetEquality(expected, actual))
//   }  

//   test("MLTransform: crossValidatorModelTargetFile") {
//     implicit val spark = session
//     import spark.implicits._
//     implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

//     val expected = spark.createDataFrame(Seq(
//       (4L, "spark i j k", 1.0, 0.8),
//       (5L, "l m n", 0.0, 0.8),
//       (6L, "spark hadoop spark", 1.0, 0.9),
//       (7L, "apache hadoop", 0.0, 1.0)
//     )).toDF("id", "text", "prediction", "probability")
//     expected.drop("prediction").drop("probability").createOrReplaceTempView(inputView)

//     val transformed = transform.MLTransform.transform(
//       MLTransform(
//         name="MLTransform", 
//         description=None,
//         inputURI=new URI(crossValidatorModelTargetFile),
//         model=Right(CrossValidatorModel.load(crossValidatorModelTargetFile)),
//         inputView=inputView,
//         outputView=outputView,
//         persist=true,
//         params=Map.empty,
//         numPartitions=None,
//         partitionBy=Nil           
//       )
//     ).get

//     // round due to random seed changing
//     val actual = transformed.withColumn("probability", round($"probability", 1))

//     assert(TestUtils.datasetEquality(expected, actual))
//   }    

//   test("MLTransform: Structured Streaming") {
//     implicit val spark = session
//     import spark.implicits._
//     implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

//     val readStream = spark
//       .readStream
//       .format("rate")
//       .option("rowsPerSecond", "1")
//       .load

//     readStream.createOrReplaceTempView("readstream")

//     val input = spark.sql(s"""
//     SELECT 
//       readStream.value AS id
//       ,"spark hadoop spark" AS text
//     FROM readstream 
//     """)

//     input.createOrReplaceTempView(inputView)

//     val transformDataset = transform.MLTransform.transform(
//       MLTransform(
//         name="MLTransform", 
//         description=None,
//         inputURI=new URI(crossValidatorModelTargetFile),
//         model=Right(CrossValidatorModel.load(crossValidatorModelTargetFile)),
//         inputView=inputView,
//         outputView=outputView,
//         persist=false,
//         params=Map.empty,
//         numPartitions=None,
//         partitionBy=Nil           
//       )
//     ).get

//     val writeStream = transformDataset
//       .writeStream
//       .queryName("transformed") 
//       .format("memory")
//       .start

//     val df = spark.table("transformed")

//     try {
//       Thread.sleep(2000)
//       assert(df.first.getDouble(2) == 1.0)
//     } finally {
//       writeStream.stop
//     }    
//   }   
// }
