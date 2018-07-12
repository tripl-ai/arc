package au.com.agl.arc.extract

import java.lang._
import java.net.URI
import java.util.{Collections, Properties}

import scala.annotation.tailrec
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.TaskContext

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition


import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.util._

case class KafkaRecord (
  topic: String,
  partition: Int,
  offset: Long,
  timestamp: Long,
  key: String,
  value: String
)

object KafkaExtract {

  def extract(extract: KafkaExtract)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): DataFrame = {
    import spark.implicits._
    val startTime = System.currentTimeMillis() 
    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", extract.getType)
    stageDetail.put("name", extract.name)
    stageDetail.put("outputView", extract.outputView)
    stageDetail.put("bootstrapServers", extract.bootstrapServers)
    stageDetail.put("groupID", extract.groupID)
    stageDetail.put("topic", extract.topic)
    stageDetail.put("maxPollRecords", Integer.valueOf(extract.maxPollRecords.getOrElse(10000)))
    stageDetail.put("timeout", Long.valueOf(extract.timeout.getOrElse(10000L)))
    stageDetail.put("persist", Boolean.valueOf(extract.persist))

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()

    // KafkaConsumer properties
    // https://kafka.apache.org/documentation/#consumerconfigs
    val props = new Properties
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, extract.bootstrapServers)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, extract.groupID)

    // first get the number of partitions via the driver process so it can be used for mapPartition
    val kafkaDriverConsumer = new KafkaConsumer[String, String](props)
    val numPartitions = try {
      kafkaDriverConsumer.partitionsFor(extract.topic).size
    } finally {
      kafkaDriverConsumer.close
    }

    val df = try {
      spark.sqlContext.emptyDataFrame.repartition(numPartitions).mapPartitions(partition => {
        // get the partition of this executor which maps 1:1 with Kafka partition
        val partitionId = TaskContext.getPartitionId

        val props = new Properties
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, extract.bootstrapServers)
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, extract.maxPollRecords.getOrElse(10000).toString)
        props.put(ConsumerConfig.GROUP_ID_CONFIG, s"${extract.groupID}-${partitionId}")

        // try to assign records based on partitionId and extract 
        val kafkaConsumer = new KafkaConsumer[String, String](props)
        val topicPartition = new TopicPartition(extract.topic, partitionId)

        def getKafkaRecord(): List[KafkaRecord] = {
          kafkaConsumer.poll(extract.timeout.getOrElse(10000L)).records(extract.topic).asScala.map(consumerRecord => {
            KafkaRecord(consumerRecord.topic, consumerRecord.partition, consumerRecord.offset, consumerRecord.timestamp, consumerRecord.key, consumerRecord.value)
          }).toList
        }

        @tailrec
        def getAllKafkaRecords(kafkaRecords: List[KafkaRecord], kafkaRecordsAccumulator: List[KafkaRecord]): List[KafkaRecord] = {
            kafkaRecords match {
                case Nil => kafkaRecordsAccumulator
                case _ => getAllKafkaRecords(getKafkaRecord, kafkaRecordsAccumulator ::: kafkaRecords)
            }
        }

        try {
          kafkaConsumer.assign(List(topicPartition).asJava)

          // recursively get batches of records until finished
          val dataset = getAllKafkaRecords(getKafkaRecord, Nil)

          // only commit offset once consumerRecords are succesfully mapped to case classes
          kafkaConsumer.commitSync

          dataset.toIterator
        } finally {
          kafkaConsumer.close
        }
      }).toDF
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stageDetail          
      }
    } 

    // repartition to distribute rows evenly
    val repartitionedDF = extract.numPartitions match {
      case Some(numPartitions) => df.repartition(numPartitions)
      case None => df
    }
    repartitionedDF.createOrReplaceTempView(extract.outputView)

    stageDetail.put("outputColumns", Integer.valueOf(repartitionedDF.schema.length))
    stageDetail.put("numPartitions", Integer.valueOf(repartitionedDF.rdd.partitions.length))

    if (extract.persist) {
      repartitionedDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
      stageDetail.put("records", Long.valueOf(repartitionedDF.count)) 
    }    

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stageDetail)      
      .log() 

    repartitionedDF
  }

}
