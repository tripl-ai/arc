package au.com.agl.arc.extract

import java.lang._
import java.net.URI
import java.util.{Collections, Properties}
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

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
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, extract.groupID)

    val kafkaConsumer = new KafkaConsumer[String, String](props)
    kafkaConsumer.subscribe(List(extract.topic).asJava)

    // create a dataframe with correct value
    val df = try {
      val kafkaRecords = kafkaConsumer.poll(10000).records(extract.topic).asScala.map(consumerRecord => {
        KafkaRecord(consumerRecord.topic, consumerRecord.partition, consumerRecord.offset, consumerRecord.timestamp, consumerRecord.key, consumerRecord.value)
      }).toSeq

      // spark will use the KafkaRecord case class even if the kafkaConsumer returns 0 records
      spark.sparkContext.parallelize(kafkaRecords).toDF
    } catch {
        case e: Exception => throw new Exception(e) with DetailException {
          override val detail = stageDetail          
        }
    } finally {
      kafkaConsumer.close
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
