package au.com.agl.arc

import java.net.URI
import java.util.UUID
import java.util.Properties

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.util.log.LoggerFactory 
import au.com.agl.arc.util._

class KafkaExtractSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  
  val outputView = "dataset"
  val bootstrapServers = "localhost:29092"
  val timeout = Option(1000L)

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    session = spark
    import spark.implicits._  
  }

  after {
    session.stop()
  }

  test("KafkaExtract") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val topic = UUID.randomUUID.toString
    val groupId = UUID.randomUUID.toString

    // insert 100 records
    val props = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ProducerConfig.ACKS_CONFIG, "-1")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    val kafkaProducer = new KafkaProducer[String, String](props)
    val dataset = spark.sqlContext.range(0, 100)
      .select("id")
      .withColumn("uniform", rand(seed=10))
      .withColumn("normal", randn(seed=27))
      .repartition(10)
      .toJSON

    try {
        dataset.collect.foreach(value => {
          val producerRecord = new ProducerRecord[String, String](topic, value)
          kafkaProducer.send(producerRecord)
        })    
    } finally {
      kafkaProducer.close
    } 

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
        params=Map.empty
      )
    ) 

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
    assert(actual.except(expected).count === 0)
    assert(expected.except(actual).count === 0)
  }  

  test("KafkaExtract: autoCommit = false") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val topic = UUID.randomUUID.toString
    val groupId = UUID.randomUUID.toString

    // insert 100 records
    val props = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ProducerConfig.ACKS_CONFIG, "-1")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    val kafkaProducer = new KafkaProducer[String, String](props)
    val dataset = spark.sqlContext.range(0, 100)
      .select("id")
      .withColumn("uniform", rand(seed=10))
      .withColumn("normal", randn(seed=27))
      .repartition(10)
      .toJSON

    try {
        dataset.collect.foreach(value => {
          val producerRecord = new ProducerRecord[String, String](topic, value)
          kafkaProducer.send(producerRecord)
        })    
    } finally {
      kafkaProducer.close
    } 

    val extractDataset0 = extract.KafkaExtract.extract(
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
        params=Map.empty
      )
    ) 

    val extractDataset1 = extract.KafkaExtract.extract(
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
        params=Map.empty
      )
    )

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

    val topic = UUID.randomUUID.toString
    val groupId = UUID.randomUUID.toString

    // insert 100 records
    val props = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ProducerConfig.ACKS_CONFIG, "-1")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    val kafkaProducer = new KafkaProducer[String, String](props)
    val dataset = spark.sqlContext.range(0, 100)
      .select("id")
      .withColumn("uniform", rand(seed=10))
      .withColumn("normal", randn(seed=27))
      .repartition(10)
      .toJSON

    try {
        dataset.collect.foreach(value => {
          val producerRecord = new ProducerRecord[String, String](topic, value)
          kafkaProducer.send(producerRecord)
        })    
    } finally {
      kafkaProducer.close
    } 

    val extractDataset0 = extract.KafkaExtract.extract(
      KafkaExtract(
        name="df", 
        outputView=outputView, 
        topic=topic,
        bootstrapServers=bootstrapServers,
        groupID=groupId,
        maxPollRecords=None, 
        timeout=timeout, 
        autoCommit=Option(true), 
        persist=true, 
        numPartitions=None, 
        params=Map.empty
      )
    ) 

    val extractDataset1 = extract.KafkaExtract.extract(
      KafkaExtract(
        name="df", 
        outputView=outputView, 
        topic=topic,
        bootstrapServers=bootstrapServers,
        groupID=groupId,
        maxPollRecords=None, 
        timeout=timeout, 
        autoCommit=Option(true), 
        persist=true, 
        numPartitions=None, 
        params=Map.empty
      )
    )

    assert(spark.catalog.isCached(outputView) === true)

    val expected = extractDataset0.select($"value").as[String]
    val actual = extractDataset1.select($"value").as[String]

    assert(expected.count === dataset.count)
    assert(actual.count === 0)
  }    
}
