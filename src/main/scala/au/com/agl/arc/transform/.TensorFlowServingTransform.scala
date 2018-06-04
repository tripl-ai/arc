package au.com.agl.arc.transform

import java.lang._
import scala.collection.JavaConverters._

import io.grpc.netty.NettyChannelBuilder
import io.grpc.internal.DnsNameResolverProvider
import java.net.URI
import java.util.concurrent.TimeUnit
import org.tensorflow.framework.DataType
import org.tensorflow.framework.TensorProto
import org.tensorflow.framework.TensorShapeProto
import tensorflow.serving.Model.ModelSpec
import tensorflow.serving.Predict.PredictRequest
import tensorflow.serving.PredictionServiceGrpc

import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import au.com.agl.arc.api.API._
import au.com.agl.arc.util._

object TensorFlowServingTransform {

  def transform(transform: TensorFlowServingTransform)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): DataFrame = {
    val startTime = System.currentTimeMillis() 
    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", transform.getType)
    stageDetail.put("name", transform.name)
    stageDetail.put("inputView", transform.inputView)  
    stageDetail.put("outputView", transform.outputView)  
    stageDetail.put("inputFields", transform.inputFields.asJava) 
    stageDetail.put("outputFields", transform.outputFields.asJava) 

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()      


    val inputStructFields = transform.inputFields.map { case (k,v) => { 
      TensorFlowUtils.dataTypeToStructField(k, v) 
    }}

    val outputStructFields = transform.outputFields.map { case (k,v) => { 
      TensorFlowUtils.dataTypeToStructField(k, v) 
    }}

    val df = spark.table(transform.inputView)

    inputStructFields.foreach(field => {
      if (!df.schema.toList.contains(field)) {
        throw new Exception(s"Input view '${transform.inputView}' requires column '${field.name}' of type '${field.dataType.toString}' to meet TensorFlow Serving Model requirement.")
      }
    })

    // create a type to output the transformed dataframe to
    type TransformedRow = Row
    val outputSchema = StructType(df.schema.toList ::: outputStructFields.toList)
    implicit val typedEncoder: Encoder[TransformedRow] = org.apache.spark.sql.catalyst.encoders.RowEncoder(outputSchema)

    val transformedDF = try {
      
      df.mapPartitions(partition => {

        trait TensorValue[A] {
          def tensorProto(value: A): TensorProto = {
            
            val dim = TensorShapeProto.Dim.newBuilder().setSize(1)
            val shape = TensorShapeProto.newBuilder().addDim(dim)

            val builder = TensorProto.newBuilder().setDtype(dataType()).setTensorShape(shape)

            setValue(builder, value)
            
            val tensor = builder.build()

            tensor
          }

          def dataType(): org.tensorflow.framework.DataType

          def setValue(builder: TensorProto.Builder, v: A)
        }

        object TensorValue {

          def tensorProto[A](value: A)(implicit tv: TensorValue[A]) = tv.tensorProto(value)

          // type class instances
          implicit val intTensorValue: TensorValue[Int] = new TensorValue[Int] {
            val dataType = org.tensorflow.framework.DataType.DT_INT32
            def setValue(builder: TensorProto.Builder, v: Int) = builder.addIntVal(v)
          }

          implicit val floatTensorValue: TensorValue[Float] = new TensorValue[Float] {
            val dataType = org.tensorflow.framework.DataType.DT_FLOAT
            def setValue(builder: TensorProto.Builder, v: Float) = builder.addFloatVal(v)
          }  

          implicit val doubleTensorValue: TensorValue[Double] = new TensorValue[Double] {
            val dataType = org.tensorflow.framework.DataType.DT_DOUBLE
            def setValue(builder: TensorProto.Builder, v: Double) = builder.addDoubleVal(v)
          }  
        }

        val managedChannel = { NettyChannelBuilder.forAddress(transform.hostname, transform.port)
          .nameResolverFactory(new DnsNameResolverProvider())
          .usePlaintext(true) // by default SSL/TLS is used
          .build()
        }

        val blockingStub = PredictionServiceGrpc.newBlockingStub(managedChannel)

        partition.map(row => {

          val modelSpec = { ModelSpec.newBuilder()
            .setName(transform.modelName)
            .setSignatureName(transform.signatureName)
          }

          val predictRequest = { PredictRequest.newBuilder()
            .setModelSpec(modelSpec)
          }

          inputStructFields.foreach(field => {
            field.dataType match {
              case _: IntegerType => predictRequest.putInputs(field.name, TensorValue.tensorProto(row.getInt(row.fieldIndex(field.name))))
            }
          })
        
          val prediction = blockingStub.predict(predictRequest.build()).getOutputsMap

          // append output fields by name
          val rs = row.toSeq ++ outputStructFields.map(field => {
            field.dataType match {
              case _: IntegerType => prediction.get(field.name).getIntVal(0)
              case _: FloatType => prediction.get(field.name).getFloatVal(0)
              case _: DoubleType => prediction.get(field.name).getDoubleVal(0)
            }
          })

          Row.fromSeq(rs).asInstanceOf[TransformedRow]
        })
      })

    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stageDetail          
      }      
    }  

    transformedDF.createOrReplaceTempView(transform.outputView)

    if (transform.persist) {
      transformedDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
      stageDetail.put("records", Long.valueOf(transformedDF.count)) 
    }    

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stageDetail)      
      .log()  

    transformedDF
  }
}
