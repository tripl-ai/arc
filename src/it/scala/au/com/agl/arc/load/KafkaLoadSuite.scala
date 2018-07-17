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
  val topic = UUID.randomUUID.toString
  val bootstrapServers = "localhost:29092"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    session = spark
  }

  after {
    session.stop()
  }

  test("KafkaLoad") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

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
  
    // get and sum offsets 
    val props = new Properties
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5000")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID.toString)


    val kafkaConsumer = new KafkaConsumer[String, String](props)
    val actual = try {
      val partitions = kafkaConsumer.partitionsFor(topic).asScala
      val topicPartitions = partitions.map(partition => { new TopicPartition(partition.topic, partition.partition) })
      val offsets = kafkaConsumer.endOffsets(topicPartitions.asJava).asScala
      offsets.map({ case (key,value) => {value}}).filter(_ != 0)
    } finally {
      kafkaConsumer.close
    }

    // ensure row counts match
    assert(dataset.count === actual.reduce(_ + _))
    // ensure partitions are utilised
    // note the default number of partitions is set in the KAFKA_NUM_PARTITIONS environment variable in the docker-compose file
    assert(actual.size === 10)    
  }   
}
