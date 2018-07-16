package au.com.agl.arc.execute

import java.lang._
import java.util.Properties

import scala.collection.JavaConverters._

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import au.com.agl.arc.api.API._
import au.com.agl.arc.util._

object KafkaCommitExecute {

  def execute(exec: KafkaCommitExecute)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Unit = {
    val startTime = System.currentTimeMillis() 

    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", exec.getType)
    stageDetail.put("name", exec.name)
    stageDetail.put("inputView", exec.inputView)  
    stageDetail.put("bootstrapServers", exec.bootstrapServers)

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()

    val df = spark.table(exec.inputView)     

    val offsetsLogMap = new java.util.HashMap[String, Object]()

    try {
      // get the aggregation
      val offset = df.groupBy(df("topic"), df("partition")).agg(max(df("offset")))

      val props = new Properties
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, exec.bootstrapServers)
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

      offset.collect.foreach(row => {
        val topic = row.getString(0)
        val partitionId = row.getInt(1)
        val offset = row.getLong(2) + 1 // note the +1 which is required to set offset as correct value for next read

        props.put(ConsumerConfig.GROUP_ID_CONFIG, s"${exec.groupID}-${partitionId}")
        val kafkaConsumer = new KafkaConsumer[String, String](props)

        // set the offset
        try {
          val offsetsMap = new java.util.HashMap[TopicPartition,OffsetAndMetadata]()
          offsetsMap.put(new TopicPartition(topic, partitionId), new OffsetAndMetadata(offset))
          kafkaConsumer.commitSync(offsetsMap)

          // update logs
          offsetsLogMap.put(s"${exec.groupID}-${partitionId}", Long.valueOf(offset))
          stageDetail.put("offsets", offsetsLogMap) 
        } finally {
          kafkaConsumer.close
        }
      })
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stageDetail          
      }
    }

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stageDetail)      
      .log()   
  }
}