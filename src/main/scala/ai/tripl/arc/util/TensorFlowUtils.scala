package ai.tripl.arc.util

import org.apache.spark.sql.types._

object TensorFlowUtils {

    def dataTypeToStructField(name: String, dataType: String): StructField = {

      // todo add type safety to dataType
      val sparkDataType = dataType match {
        case "DT_INT32" => IntegerType
        case "DT_FLOAT" => FloatType
        case "DT_DOUBLE" => DoubleType
        case _ => throw new Exception(s"Unable to match '${dataType}' with Spark internal type. Allowed values: [DT_INT32, DT_FLOAT, DT_DOUBLE].")
      }

      StructField(name, sparkDataType, true)
    }

}
    
// DT_BFLOAT16             DT_COMPLEX128             DT_DOUBLE             DT_HALF              DT_INT32             DT_INT8             DT_QINT16_REF_VALUE   DT_QINT8_REF_VALUE     DT_QUINT8_REF_VALUE     DT_STRING_REF_VALUE   DT_UINT8_REF_VALUE     getDescriptor         
// DT_BFLOAT16_REF         DT_COMPLEX128_REF         DT_DOUBLE_REF         DT_HALF_REF          DT_INT32_REF         DT_INT8_REF         DT_QINT16_VALUE       DT_QINT8_VALUE         DT_QUINT8_VALUE         DT_STRING_VALUE       DT_UINT8_VALUE         internalGetValueMap   
// DT_BFLOAT16_REF_VALUE   DT_COMPLEX128_REF_VALUE   DT_DOUBLE_REF_VALUE   DT_HALF_REF_VALUE    DT_INT32_REF_VALUE   DT_INT8_REF_VALUE   DT_QINT32             DT_QUINT16             DT_RESOURCE             DT_UINT16             DT_VARIANT             valueOf               
// DT_BFLOAT16_VALUE       DT_COMPLEX128_VALUE       DT_DOUBLE_VALUE       DT_HALF_VALUE        DT_INT32_VALUE       DT_INT8_VALUE       DT_QINT32_REF         DT_QUINT16_REF         DT_RESOURCE_REF         DT_UINT16_REF         DT_VARIANT_REF         values                
// DT_BOOL                 DT_COMPLEX64              DT_FLOAT              DT_INT16             DT_INT64             DT_INVALID          DT_QINT32_REF_VALUE   DT_QUINT16_REF_VALUE   DT_RESOURCE_REF_VALUE   DT_UINT16_REF_VALUE   DT_VARIANT_REF_VALUE                         
// DT_BOOL_REF             DT_COMPLEX64_REF          DT_FLOAT_REF          DT_INT16_REF         DT_INT64_REF         DT_INVALID_VALUE    DT_QINT32_VALUE       DT_QUINT16_VALUE       DT_RESOURCE_VALUE       DT_UINT16_VALUE       DT_VARIANT_VALUE                             
// DT_BOOL_REF_VALUE       DT_COMPLEX64_REF_VALUE    DT_FLOAT_REF_VALUE    DT_INT16_REF_VALUE   DT_INT64_REF_VALUE   DT_QINT16           DT_QINT8              DT_QUINT8              DT_STRING               DT_UINT8              UNRECOGNIZED                                 
// DT_BOOL_VALUE           DT_COMPLEX64_VALUE        DT_FLOAT_VALUE        DT_INT16_VALUE       DT_INT64_VALUE       DT_QINT16_REF       DT_QINT8_REF          DT_QUINT8_REF          DT_STRING_REF           DT_UINT8_REF 

