package au.com.agl.arc

import java.net.URI
import java.util.UUID
import java.util.Properties

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.util.log.LoggerFactory 
import au.com.agl.arc.util._

class KafkaExtractSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  
  val inputView = "inputView"
  val outputView = "outputView"
  val bootstrapServers = "localhost:29092"
  val timeout = 3000L
  val checkPointPath = "/tmp/checkpoint"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .config("spark.sql.streaming.checkpointLocation", checkPointPath)
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    session = spark
    import spark.implicits._  

    FileUtils.deleteQuietly(new java.io.File(checkPointPath)) 
  }

  after {
    session.stop()
    FileUtils.deleteQuietly(new java.io.File(checkPointPath)) 
  }

  test("KafkaExtract: String") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false)

    val topic = UUID.randomUUID.toString
    val groupId = UUID.randomUUID.toString

    // insert 100 records
    val dataset = spark.sqlContext.range(0, 100)
      .select("id")
      .withColumn("uniform", rand(seed=10))
      .withColumn("normal", randn(seed=27))
      .repartition(10)
      .toJSON

    dataset.createOrReplaceTempView(inputView)

    load.KafkaLoad.load(
      KafkaLoad(
        name="df", 
        description=None,
        inputView=inputView, 
        topic=topic,
        bootstrapServers=bootstrapServers,
        acks= -1,
        numPartitions=None, 
        batchSize=16384, 
        retries=0, 
        params=Map.empty
      )
    )   

    val extractDataset = extract.KafkaExtract.extract(
      KafkaExtract(
        name="df", 
        description=None,
        outputView=outputView, 
        topic=topic,
        bootstrapServers=bootstrapServers,
        groupID=groupId,
        maxPollRecords=10000, 
        timeout=timeout, 
        autoCommit=false, 
        persist=true, 
        numPartitions=None, 
        partitionBy=Nil,
        params=Map.empty,
        keyType=StringType,
        valueType=StringType
      )
    ).get

    val expected = dataset
    val actual = extractDataset.select(col("value")).as[String]

    val actualExceptExpectedCount = actual.except(expected).count
    val expectedExceptActualCount = expected.except(actual).count
    if (actualExceptExpectedCount != 0 || expectedExceptActualCount != 0) {
      println("actual")
      actual.show(false)
      println("expected")
      expected.show(false)  
    }
    assert(actual.except(expected).count === 0)
    assert(expected.except(actual).count === 0)
  }  


  test("KafkaExtract: Binary") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false)

    val topic = UUID.randomUUID.toString
    val groupId = UUID.randomUUID.toString

    // insert 100 records
    val dataset = spark.sqlContext.range(0, 100)
      .select("id")
      .withColumn("uniform", rand(seed=10))
      .withColumn("normal", randn(seed=27))
      .repartition(10)
      .toJSON
      .select(col("value").cast(BinaryType))

    dataset.createOrReplaceTempView(inputView)

    load.KafkaLoad.load(
      KafkaLoad(
        name="df", 
        description=None,
        inputView=inputView, 
        topic=topic,
        bootstrapServers=bootstrapServers,
        acks= -1,
        numPartitions=None, 
        batchSize=16384, 
        retries=0, 
        params=Map.empty
      )
    )   

    val extractDataset = extract.KafkaExtract.extract(
      KafkaExtract(
        name="df", 
        description=None,
        outputView=outputView, 
        topic=topic,
        bootstrapServers=bootstrapServers,
        groupID=groupId,
        maxPollRecords=10000, 
        timeout=timeout, 
        autoCommit=false, 
        persist=true, 
        numPartitions=None, 
        partitionBy=Nil,
        params=Map.empty,
        keyType=BinaryType,
        valueType=BinaryType
      )
    ).get

    val expected = dataset
    val actual = extractDataset.select(col("value"))

    val actualExceptExpectedCount = actual.except(expected).count
    val expectedExceptActualCount = expected.except(actual).count
    if (actualExceptExpectedCount != 0 || expectedExceptActualCount != 0) {
      println("actual")
      actual.show(false)
      println("expected")
      expected.show(false)  
    }
    assert(actual.except(expected).count === 0)
    assert(expected.except(actual).count === 0)
  }  

  test("KafkaExtract: autoCommit = false") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false)

    val topic = UUID.randomUUID.toString
    val groupId = UUID.randomUUID.toString

    // insert 100 records
    val dataset = spark.sqlContext.range(0, 100)
      .select("id")
      .withColumn("uniform", rand(seed=10))
      .withColumn("normal", randn(seed=27))
      .repartition(10)
      .toJSON
    dataset.createOrReplaceTempView(inputView)

    load.KafkaLoad.load(
      KafkaLoad(
        name="df", 
        description=None,
        inputView=inputView, 
        topic=topic,
        bootstrapServers=bootstrapServers,
        acks= -1,
        numPartitions=None, 
        batchSize=16384, 
        retries=0, 
        params=Map.empty
      )
    )   

    val extractDataset0 = extract.KafkaExtract.extract(
      KafkaExtract(
        name="df", 
        description=None,
        outputView=outputView, 
        topic=topic,
        bootstrapServers=bootstrapServers,
        groupID=groupId,
        maxPollRecords=10000, 
        timeout=timeout, 
        autoCommit=false, 
        persist=true, 
        numPartitions=None, 
        partitionBy=Nil,
        params=Map.empty,
        keyType=StringType,
        valueType=StringType        
      )
    ).get

    val extractDataset1 = extract.KafkaExtract.extract(
      KafkaExtract(
        name="df", 
        description=None,
        outputView=outputView, 
        topic=topic,
        bootstrapServers=bootstrapServers,
        groupID=groupId,
        maxPollRecords=10000, 
        timeout=timeout, 
        autoCommit=false, 
        persist=true, 
        numPartitions=None, 
        partitionBy=Nil,
        params=Map.empty,
        keyType=StringType,
        valueType=StringType   
      )
    ).get

    assert(spark.catalog.isCached(outputView) === true)

    val expected = extractDataset0.select($"value").as[String]
    val actual = extractDataset1.select($"value").as[String]

    val actualExceptExpectedCount = actual.except(expected).count
    val expectedExceptActualCount = expected.except(actual).count
    if (actualExceptExpectedCount != 0 || expectedExceptActualCount != 0) {
      println("actual")
      actual.show(false)
      println("expected")
      expected.show(false)  
    }
    assert(actual.except(expected).count === 0)
    assert(expected.except(actual).count === 0)
  }  

  test("KafkaExtract: autoCommit = true") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false)

    val topic = UUID.randomUUID.toString
    val groupId = UUID.randomUUID.toString

    // insert 100 records
    val dataset = spark.sqlContext.range(0, 100)
      .select("id")
      .withColumn("uniform", rand(seed=10))
      .withColumn("normal", randn(seed=27))
      .repartition(10)
      .toJSON
    dataset.createOrReplaceTempView(inputView)

    load.KafkaLoad.load(
      KafkaLoad(
        name="df", 
        description=None,
        inputView=inputView, 
        topic=topic,
        bootstrapServers=bootstrapServers,
        acks= -1,
        numPartitions=None, 
        batchSize=16384, 
        retries=0, 
        params=Map.empty
      )
    )   

    val extractDataset0 = extract.KafkaExtract.extract(
      KafkaExtract(
        name="df", 
        description=None,
        outputView=outputView, 
        topic=topic,
        bootstrapServers=bootstrapServers,
        groupID=groupId,
        maxPollRecords=10000, 
        timeout=timeout, 
        autoCommit=true, 
        persist=true, 
        numPartitions=None, 
        partitionBy=Nil,
        params=Map.empty,
        keyType=StringType,
        valueType=StringType   
      )
    ).get

    val extractDataset1 = extract.KafkaExtract.extract(
      KafkaExtract(
        name="df", 
        description=None,
        outputView=outputView, 
        topic=topic,
        bootstrapServers=bootstrapServers,
        groupID=groupId,
        maxPollRecords=10000, 
        timeout=timeout, 
        autoCommit=true, 
        persist=true, 
        numPartitions=None, 
        partitionBy=Nil,
        params=Map.empty,
        keyType=StringType,
        valueType=StringType   
      )
    ).get

    assert(spark.catalog.isCached(outputView) === true)

    val expected = extractDataset0.select($"value").as[String]
    val actual = extractDataset1.select($"value").as[String]

    assert(expected.count === dataset.count)
    assert(actual.count === 0)
  }    

  test("KafkaExtract: Structured Streaming") {
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=true, ignoreEnvironments=false)

    val topic = UUID.randomUUID.toString
    val groupId = UUID.randomUUID.toString

    val extractDataset = extract.KafkaExtract.extract(
      KafkaExtract(
        name="df", 
        description=None,
        outputView=outputView, 
        topic=topic,
        bootstrapServers=bootstrapServers,
        groupID=groupId,
        maxPollRecords=10000, 
        timeout=timeout, 
        autoCommit=false, 
        persist=true, 
        numPartitions=None, 
        partitionBy=Nil,
        params=Map.empty,
        keyType=StringType,
        valueType=StringType   
      )
    ).get

    val writeStream0 = extractDataset
      .writeStream
      .queryName("extract") 
      .format("memory")
      .start

    val readStream = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", "1")
      .load

    val writeStream1 = readStream
      .selectExpr("CAST(timestamp AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("topic", topic)
      .start()

    val df = spark.table("extract")

    try {
      // enough time for both producer and consumer to begin
      Thread.sleep(5000)
      assert(df.count > 0)
    } finally {
      writeStream0.stop
      writeStream1.stop
    }    
  }   
}
