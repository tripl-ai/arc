package au.com.agl.arc

import java.net.URI
import java.util.UUID
import java.util.Properties

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition

import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.util.log.LoggerFactory 

class KafkaLoadSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  
  val inputView = "inputView"
  val outputView = "outputView"
  val bootstrapServers = "localhost:29092"
  val timeout = Option(3000L)
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
    FileUtils.deleteQuietly(new java.io.File(checkPointPath)) 
  }

  after {
    session.stop()
    FileUtils.deleteQuietly(new java.io.File(checkPointPath)) 
  }

  test("KafkaLoad: (value)") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false)

    val topic = UUID.randomUUID.toString
    val groupId = UUID.randomUUID.toString

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
        inputView=inputView, 
        topic=topic,
        bootstrapServers=bootstrapServers,
        acks= -1,
        numPartitions=None, 
        batchSize=None, 
        retries=None, 
        params=Map.empty
      )
    )   
  
    val extractDataset = extract.KafkaExtract.extract(
      KafkaExtract(
        name="df", 
        outputView=outputView, 
        topic=topic,
        bootstrapServers=bootstrapServers,
        groupID=groupId,
        maxPollRecords=None, 
        timeout=timeout, 
        autoCommit=Option(false), 
        persist=true, 
        numPartitions=None, 
        partitionBy=Nil,
        params=Map.empty
      )
    ).get 

    val expected = dataset
    val actual = extractDataset.select($"value").as[String]

    val actualExceptExpectedCount = actual.except(expected).count
    val expectedExceptActualCount = expected.except(actual).count
    if (actualExceptExpectedCount != 0 || expectedExceptActualCount != 0) {
      println("actual")
      actual.show(false)
      println("expected")
      expected.show(false)  
    }
    assert(actualExceptExpectedCount === 0)
    assert(expectedExceptActualCount === 0)
    // ensure partitions are utilised
    // note the default number of partitions is set in the KAFKA_NUM_PARTITIONS environment variable in the docker-compose file
    assert(extractDataset.agg(countDistinct("partition")).take(1)(0).getLong(0) === 10)    
  } 

  test("KafkaLoad: (key, value)") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false)

    val topic = UUID.randomUUID.toString
    val groupId = UUID.randomUUID.toString

    val dataset = spark.sqlContext.range(0, 100)
      .select("id")
      .withColumn("key", $"id".cast("string"))
      .withColumn("uniform", rand(seed=10))
      .withColumn("normal", randn(seed=27))
      .withColumn("value", to_json(struct($"uniform", $"normal")))
      .select("key", "value")
      .repartition(10)
    dataset.createOrReplaceTempView(inputView)

    load.KafkaLoad.load(
      KafkaLoad(
        name="df", 
        inputView=inputView, 
        topic=topic,
        bootstrapServers=bootstrapServers,
        acks= -1,
        numPartitions=None, 
        batchSize=None, 
        retries=None, 
        params=Map.empty
      )
    )   
  
    val extractDataset = extract.KafkaExtract.extract(
      KafkaExtract(
        name="df", 
        outputView=outputView, 
        topic=topic,
        bootstrapServers=bootstrapServers,
        groupID=groupId,
        maxPollRecords=None, 
        timeout=timeout, 
        autoCommit=Option(false), 
        persist=true, 
        numPartitions=None, 
        partitionBy=Nil,
        params=Map.empty
      )
    ).get

    val expected = dataset
    val actual = extractDataset.select($"key", $"value")

    val actualExceptExpectedCount = actual.except(expected).count
    val expectedExceptActualCount = expected.except(actual).count
    if (actualExceptExpectedCount != 0 || expectedExceptActualCount != 0) {
      println("actual")
      actual.show(false)
      println("expected")
      expected.show(false)  
    }
    assert(actualExceptExpectedCount === 0)
    assert(expectedExceptActualCount === 0)

    // ensure partitions are utilised
    // note the default number of partitions is set in the KAFKA_NUM_PARTITIONS environment variable in the docker-compose file
    assert(extractDataset.agg(countDistinct("partition")).take(1)(0).getLong(0) === 10)    
  }     

  test("KafkaLoad: Structured Streaming") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit var arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=true, ignoreEnvironments=false)

    val topic = UUID.randomUUID.toString
    val groupId = UUID.randomUUID.toString

    val readStream = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", "1")
      .load

    readStream.createOrReplaceTempView(inputView)

    val output = spark.sql(s"""
    SELECT 
      CAST(timestamp AS STRING) AS key
      ,CAST(value AS STRING) as value
    FROM ${inputView}
    """)

    output.createOrReplaceTempView(inputView)

    load.KafkaLoad.load(
      KafkaLoad(
        name="df", 
        inputView=inputView, 
        topic=topic,
        bootstrapServers=bootstrapServers,
        acks= -1,
        numPartitions=None, 
        batchSize=None, 
        retries=None, 
        params=Map.empty
      )
    ) 

    Thread.sleep(2000)
    spark.streams.active.foreach(streamingQuery => streamingQuery.stop)

    // use batch mode to check whether data was loaded
    arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false)
    val actual = extract.KafkaExtract.extract(
      KafkaExtract(
        name="df", 
        outputView=outputView, 
        topic=topic,
        bootstrapServers=bootstrapServers,
        groupID=groupId,
        maxPollRecords=None, 
        timeout=timeout, 
        autoCommit=Option(false), 
        persist=true, 
        numPartitions=None, 
        partitionBy=Nil,
        params=Map.empty
      )
    ).get

    assert(actual.count > 0)
  }    
}
