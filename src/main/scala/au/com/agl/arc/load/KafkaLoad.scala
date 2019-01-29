package au.com.agl.arc.load

import java.lang._
import java.net.URI
import java.util.Properties
import scala.collection.JavaConverters._

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.io.Source

import au.com.agl.arc.api.API._
import au.com.agl.arc.util._

case class SimpleType(name: String, dataType: DataType)

object KafkaLoad {

  def load(load: KafkaLoad)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Option[DataFrame] = {
    import spark.implicits._
    val startTime = System.currentTimeMillis() 
    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", load.getType)
    stageDetail.put("name", load.name)
    stageDetail.put("inputView", load.inputView)  
    stageDetail.put("topic", load.topic)  
    stageDetail.put("bootstrapServers", load.bootstrapServers)
    stageDetail.put("acks", Integer.valueOf(load.acks))
    stageDetail.put("retries", Integer.valueOf(load.retries))
    stageDetail.put("batchSize", Integer.valueOf(load.batchSize))

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()

    val signature = "KafkaLoad requires inputView to be dataset with [key: string, value: string] or [value: string] signature."

    val df = spark.table(load.inputView)     

      // enforce schema layout
      val simpleSchema = df.schema.fields.map(field => {
          SimpleType(field.name, field.dataType)
      })
      simpleSchema match {
        case Array(SimpleType("key", StringType), SimpleType("value", StringType)) => 
        case Array(SimpleType("value", StringType)) => 
        case _ => { 
          throw new Exception(s"${signature} inputView '${load.inputView}' has ${df.schema.length} columns of type [${df.schema.map(f => f.dataType.simpleString).mkString(", ")}].") with DetailException {
            override val detail = stageDetail          
          }      
        }
      }

    val outputDF = if (df.isStreaming) {
      df.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", load.bootstrapServers)
        .option("topic", load.topic)
        .start

      df
    } else {

      val repartitionedDF = load.numPartitions match {
        case Some(partitions) => {
          stageDetail.put("numPartitions", Integer.valueOf(partitions))
          df.repartition(partitions)
        }
        case None => {
          stageDetail.put("numPartitions", Integer.valueOf(df.rdd.getNumPartitions))
          df
        }
      }      

      // initialise statistics accumulators
      val recordAccumulator = spark.sparkContext.longAccumulator
      val bytesAccumulator = spark.sparkContext.longAccumulator
      val outputMetricsMap = new java.util.HashMap[String, Long]()

      try {
        repartitionedDF.schema.map(_.dataType) match {
          case List(StringType) => {
            repartitionedDF.foreachPartition(partition => {
              // KafkaProducer properties 
              // https://kafka.apache.org/documentation/#producerconfigs
              val props = new Properties
              props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, load.bootstrapServers)
              props.put(ProducerConfig.ACKS_CONFIG, String.valueOf(load.acks))
              props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
              props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
              props.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(load.retries))    
              props.put(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(load.batchSize))    

              // create producer
              val kafkaProducer = new KafkaProducer[String, String](props)
              try {
                // send each message via producer
                partition.foreach(row => {
                  // create payload and send sync
                  val producerRecord = new ProducerRecord[String,String](load.topic, row.getString(0))
                  kafkaProducer.send(producerRecord)
                  recordAccumulator.add(1)
                }) 
              } finally {
                kafkaProducer.close
              }          
            })
          }
          case List(StringType, StringType) => {
            repartitionedDF.foreachPartition(partition => {
              // KafkaProducer properties 
              // https://kafka.apache.org/documentation/#producerconfigs
              val props = new Properties
              props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, load.bootstrapServers)
              props.put(ProducerConfig.ACKS_CONFIG, String.valueOf(load.acks))
              props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
              props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
              props.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(load.retries))    
              props.put(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(load.batchSize))     

              // create producer
              val kafkaProducer = new KafkaProducer[String, String](props)
              try {
                // send each message via producer
                partition.foreach(row => {
                  // create payload and send sync
                  val key = row.getString(0)
                  val value = row.getString(1)

                  val producerRecord = new ProducerRecord[String,String](load.topic, key, value)
                  kafkaProducer.send(producerRecord)
                  recordAccumulator.add(1)
                  bytesAccumulator.add(key.getBytes.length + value.getBytes.length)
                }) 
              } finally {
                kafkaProducer.close
              }          
            })
          }
        }
      } catch {
        case e: Exception => throw new Exception(e) with DetailException {
          outputMetricsMap.put("recordsWritten", Long.valueOf(recordAccumulator.value))         
          outputMetricsMap.put("bytesWritten", Long.valueOf(bytesAccumulator.value))
          stageDetail.put("outputMetrics", outputMetricsMap)

          override val detail = stageDetail          
        }
      }

      outputMetricsMap.put("recordsWritten", Long.valueOf(recordAccumulator.value))         
      outputMetricsMap.put("bytesWritten", Long.valueOf(bytesAccumulator.value))
      stageDetail.put("outputMetrics", outputMetricsMap)

      repartitionedDF
    }

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stageDetail)      
      .log()

    Option(outputDF)
  }
}