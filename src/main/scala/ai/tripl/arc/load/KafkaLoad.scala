// package ai.tripl.arc.load

// import java.net.URI
// import java.util.Properties
// import scala.collection.JavaConverters._

// import org.apache.kafka.clients.producer.KafkaProducer
// import org.apache.kafka.clients.producer.Producer
// import org.apache.kafka.clients.producer.ProducerConfig
// import org.apache.kafka.clients.producer.ProducerRecord

// import org.apache.spark.sql._
// import org.apache.spark.sql.functions._
// import org.apache.spark.sql.types._
// import org.apache.spark.TaskContext

// import scala.io.Source

// import ai.tripl.arc.api.API._
// import ai.tripl.arc.util._

// case class SimpleType(name: String, dataType: DataType)

// object KafkaLoad {

//   def load(load: KafkaLoad)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): Option[DataFrame] = {
//     import spark.implicits._
//     val startTime = System.currentTimeMillis() 
//     val stageDetail = new java.util.HashMap[String, Object]()
//     stageDetail.put("type", load.getType)
//     stageDetail.put("name", load.name)
//     for (description <- load.description) {
//       stageDetail.put("description", description)    
//     }    
//     stageDetail.put("inputView", load.inputView)  
//     stageDetail.put("topic", load.topic)  
//     stageDetail.put("bootstrapServers", load.bootstrapServers)
//     stageDetail.put("acks", java.lang.Integer.valueOf(load.acks))
//     stageDetail.put("retries", java.lang.Integer.valueOf(load.retries))
//     stageDetail.put("batchSize", java.lang.Integer.valueOf(load.batchSize))

//     logger.info()
//       .field("event", "enter")
//       .map("stage", stageDetail)      
//       .log()

//     val signature = "KafkaLoad requires inputView to be dataset with [key: string, value: string], [value: string], [key: binary, value: binary] or [value: binary] signature."

//     val df = spark.table(load.inputView)     

//       // enforce schema layout
//       val simpleSchema = df.schema.fields.map(field => {
//           SimpleType(field.name, field.dataType)
//       })
//       simpleSchema match {
//         case Array(SimpleType("key", StringType), SimpleType("value", StringType)) => 
//         case Array(SimpleType("value", StringType)) => 
//         case Array(SimpleType("key", BinaryType), SimpleType("value", BinaryType)) => 
//         case Array(SimpleType("value", BinaryType)) =>         
//         case _ => { 
//           throw new Exception(s"${signature} inputView '${load.inputView}' has ${df.schema.length} columns of type [${df.schema.map(f => f.dataType.simpleString).mkString(", ")}].") with DetailException {
//             override val detail = stageDetail          
//           }      
//         }
//       }

//     val outputDF = if (df.isStreaming) {
//       df.writeStream
//         .format("kafka")
//         .option("kafka.bootstrap.servers", load.bootstrapServers)
//         .option("topic", load.topic)
//         .start

//       df
//     } else {

//       val repartitionedDF = load.numPartitions match {
//         case Some(partitions) => {
//           stageDetail.put("numPartitions", java.lang.Integer.valueOf(partitions))
//           df.repartition(partitions)
//         }
//         case None => {
//           stageDetail.put("numPartitions", java.lang.Integer.valueOf(df.rdd.getNumPartitions))
//           df
//         }
//       }      

//       // initialise statistics accumulators
//       val recordAccumulator = spark.sparkContext.longAccumulator
//       val bytesAccumulator = spark.sparkContext.longAccumulator
//       val outputMetricsMap = new java.util.HashMap[java.lang.String, java.lang.Long]()

//       // KafkaProducer properties 
//       // https://kafka.apache.org/documentation/#producerconfigs
//       val commonProps = new Properties
//       commonProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, load.bootstrapServers)
//       commonProps.put(ProducerConfig.ACKS_CONFIG, String.valueOf(load.acks))      
//       commonProps.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(load.retries))    
//       commonProps.put(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(load.batchSize))    

//       try {
//         repartitionedDF.schema.map(_.dataType) match {
//           case List(StringType) => {
//             repartitionedDF.foreachPartition { partition: Iterator[org.apache.spark.sql.Row] => 
//               val props = new Properties
//               props.putAll(commonProps)
//               props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
//               props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

//               // create producer
//               val kafkaProducer = new KafkaProducer[java.lang.String, java.lang.String](props)
//               try {
//                 // send each message via producer
//                 partition.foreach(row => {
//                   // create payload and send sync
//                   val value = row.getString(0)

//                   val producerRecord = new ProducerRecord[java.lang.String, java.lang.String](load.topic, value)
//                   kafkaProducer.send(producerRecord)
//                   recordAccumulator.add(1)
//                   bytesAccumulator.add(value.getBytes.length)
//                 }) 
//               } finally {
//                 kafkaProducer.close
//               }          
//             }
//           }
//           case List(BinaryType) => {
//             repartitionedDF.foreachPartition { partition: Iterator[org.apache.spark.sql.Row] => 
//               val props = new Properties
//               props.putAll(commonProps)
//               props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
//               props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")  

//               // create producer
//               val kafkaProducer = new KafkaProducer[Array[Byte], Array[Byte]](props)
//               try {
//                 // send each message via producer
//                 partition.foreach(row => {
//                   // create payload and send sync
//                   val value = row.get(0).asInstanceOf[Array[Byte]]

//                   val producerRecord = new ProducerRecord[Array[Byte],Array[Byte]](load.topic, value)
//                   kafkaProducer.send(producerRecord)
//                   recordAccumulator.add(1)
//                   bytesAccumulator.add(value.length)
//                 }) 
//               } finally {
//                 kafkaProducer.close
//               }          
//             }
//           }          
//           case List(StringType, StringType) => {
//             repartitionedDF.foreachPartition { partition: Iterator[org.apache.spark.sql.Row] => 
//               val props = new Properties
//               props.putAll(commonProps)
//               props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
//               props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")  

//               // create producer
//               val kafkaProducer = new KafkaProducer[String, String](props)
//               try {
//                 // send each message via producer
//                 partition.foreach(row => {
//                   // create payload and send sync
//                   val key = row.getString(0)
//                   val value = row.getString(1)

//                   val producerRecord = new ProducerRecord[String,String](load.topic, key, value)
//                   kafkaProducer.send(producerRecord)
//                   recordAccumulator.add(1)
//                   bytesAccumulator.add(key.getBytes.length + value.getBytes.length)
//                 }) 
//               } finally {
//                 kafkaProducer.close
//               }          
//             }
//           }
//           case List(BinaryType, BinaryType) => {
//             repartitionedDF.foreachPartition { partition: Iterator[org.apache.spark.sql.Row] => 
//               // KafkaProducer properties 
//               // https://kafka.apache.org/documentation/#producerconfigs
//               val props = new Properties
//               props.putAll(commonProps)
//               props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
//               props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")

//               // create producer
//               val kafkaProducer = new KafkaProducer[Array[Byte], Array[Byte]](props)
//               try {
//                 // send each message via producer
//                 partition.foreach(row => {
//                   // create payload and send sync
//                   val key = row.get(0).asInstanceOf[Array[Byte]]
//                   val value = row.get(1).asInstanceOf[Array[Byte]]

//                   val producerRecord = new ProducerRecord[Array[Byte], Array[Byte]](load.topic, key, value)
//                   kafkaProducer.send(producerRecord)
//                   recordAccumulator.add(1)
//                   bytesAccumulator.add(key.length + value.length)
//                 }) 
//               } finally {
//                 kafkaProducer.close
//               }          
//             }
//           }          
//         }
//       } catch {
//         case e: Exception => throw new Exception(e) with DetailException {
//           outputMetricsMap.put("recordsWritten", java.lang.Long.valueOf(recordAccumulator.value))         
//           outputMetricsMap.put("bytesWritten", java.lang.Long.valueOf(bytesAccumulator.value))
//           stageDetail.put("outputMetrics", outputMetricsMap)

//           override val detail = stageDetail          
//         }
//       }

//       outputMetricsMap.put("recordsWritten", java.lang.Long.valueOf(recordAccumulator.value))         
//       outputMetricsMap.put("bytesWritten", java.lang.Long.valueOf(bytesAccumulator.value))
//       stageDetail.put("outputMetrics", outputMetricsMap)

//       repartitionedDF
//     }

//     logger.info()
//       .field("event", "exit")
//       .field("duration", System.currentTimeMillis() - startTime)
//       .map("stage", stageDetail)      
//       .log()

//     Option(outputDF)
//   }
// }