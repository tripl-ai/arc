package ai.tripl.arc.extract

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

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.util._

case class KafkaRecord (
  topic: String,
  partition: Int,
  offset: Long,
  timestamp: Long,
  key: Array[Byte],
  value: Array[Byte]
)

object KafkaExtract {

  def extract(extract: KafkaExtract)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    import spark.implicits._
    val startTime = System.currentTimeMillis() 
    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", extract.getType)
    stageDetail.put("name", extract.name)
    for (description <- extract.description) {
      stageDetail.put("description", description)    
    }     
    stageDetail.put("outputView", extract.outputView)
    stageDetail.put("bootstrapServers", extract.bootstrapServers)
    stageDetail.put("groupID", extract.groupID)
    stageDetail.put("topic", extract.topic)
    stageDetail.put("maxPollRecords", java.lang.Integer.valueOf(extract.maxPollRecords))
    stageDetail.put("timeout", java.lang.Long.valueOf(extract.timeout))
    stageDetail.put("autoCommit", java.lang.Boolean.valueOf(extract.autoCommit))
    stageDetail.put("persist", java.lang.Boolean.valueOf(extract.persist))

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()

    val df = if (arcContext.isStreaming) {
      spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", extract.bootstrapServers)
        .option("subscribe", extract.topic)
        .load()
    } else {
      // KafkaConsumer properties
      // https://kafka.apache.org/documentation/#consumerconfigs


      val commonProps = new Properties
      commonProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, extract.bootstrapServers)
      commonProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
      commonProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
      commonProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      commonProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      commonProps.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, extract.timeout.toString)
      commonProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Math.min(10000, extract.timeout-1).toString)
      commonProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, Math.min(500, extract.timeout-1).toString)
      commonProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, Math.min(3000, extract.timeout-2).toString)

      val props = new Properties
      props.putAll(commonProps)
      props.put(ConsumerConfig.GROUP_ID_CONFIG, extract.groupID)

      // first get the number of partitions via the driver process so it can be used for mapPartition
      val numPartitions = try {
        val kafkaDriverConsumer = new KafkaConsumer[Array[Byte], Array[Byte]](props)
        try {
          kafkaDriverConsumer.partitionsFor(extract.topic).size
        } finally {
          kafkaDriverConsumer.close
        }
      } catch {
        case e: Exception => throw new Exception(e) with DetailException {
          override val detail = stageDetail          
        }  
      }

      try {
        spark.sqlContext.emptyDataFrame.repartition(numPartitions).mapPartitions(partition => {
          // get the partition of this task which maps 1:1 with Kafka partition
          val partitionId = TaskContext.getPartitionId
          
          val props = new Properties
          props.putAll(commonProps)
          props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, extract.maxPollRecords.toString)
          props.put(ConsumerConfig.GROUP_ID_CONFIG, s"${extract.groupID}-${partitionId}")

          // try to assign records based on partitionId and extract 
          val kafkaConsumer = new KafkaConsumer[Array[Byte], Array[Byte]](props)
          val topicPartition = new TopicPartition(extract.topic, partitionId)

          def getKafkaRecord(): List[KafkaRecord] = {
            kafkaConsumer.poll(extract.timeout).records(extract.topic).asScala.map(consumerRecord => {
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
            // assign only current partition to this task
            kafkaConsumer.assign(List(topicPartition).asJava)

            // recursively get batches of records until finished
            val dataset = getAllKafkaRecords(getKafkaRecord, Nil)

            // only commit offset once consumerRecords are succesfully mapped to case classes
            if (extract.autoCommit) {
              kafkaConsumer.commitSync
            }

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
    }

    // repartition to distribute rows evenly
    val repartitionedDF = extract.partitionBy match {
      case Nil => { 
        extract.numPartitions match {
          case Some(numPartitions) => df.repartition(numPartitions)
          case None => df
        }   
      }
      case partitionBy => {
        // create a column array for repartitioning
        val partitionCols = partitionBy.map(col => df(col))
        extract.numPartitions match {
          case Some(numPartitions) => df.repartition(numPartitions, partitionCols:_*)
          case None => df.repartition(partitionCols:_*)
        }
      }
    } 
    repartitionedDF.createOrReplaceTempView(extract.outputView)

    if (!repartitionedDF.isStreaming) {
      stageDetail.put("outputColumns", java.lang.Integer.valueOf(repartitionedDF.schema.length))
      stageDetail.put("numPartitions", java.lang.Integer.valueOf(repartitionedDF.rdd.partitions.length))
    }

    // force persistence if autoCommit=false to prevent double KafkaExtract execution and different offsets
    if ((extract.persist || !extract.autoCommit) && !repartitionedDF.isStreaming) {
      repartitionedDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
      stageDetail.put("records", java.lang.Long.valueOf(repartitionedDF.count)) 
    }    

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stageDetail)      
      .log() 

    Option(repartitionedDF)
  }

}