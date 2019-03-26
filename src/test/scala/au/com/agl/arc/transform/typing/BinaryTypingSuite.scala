package au.com.agl.arc.util

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import au.com.agl.arc.api.API._
import au.com.agl.arc.util._

class BinaryTypingSuite extends FunSuite with BeforeAndAfter {

  before {
  }

  after {
  }

  // input: arctest
  // base64: YXJjdGVzdA==
  // hex: 61726374657374
  // bytes: Array[Byte](97, 114, 99, 116, 101, 115, 116)

  test("BinaryTyping: value has leading spaces") {
    val col = BinaryColumn(id="1", name="name", description=Some("description"), nullable=true, nullReplacementValue=Some(""), trim=true, nullableValues=""::Nil, encoding=EncodingTypeBase64, metadata=None)
    Typing.typeValue(" YXJjdGVzdA==", col) match {
      case (Some(res), err) => {
        assert(res === Array[Byte](97, 114, 99, 116, 101, 115, 116))
        assert(err === None)
      }
      case (_,_) => assert(false)
    }
  }

  test("BinaryTyping: value has trailing spaces") {
    val col = BinaryColumn(id="1", name="name", description=Some("description"), nullable=true, nullReplacementValue=Some(""), trim=true, nullableValues=""::Nil, encoding=EncodingTypeBase64, metadata=None)
    Typing.typeValue("YXJjdGVzdA==  ", col) match {
      case (Some(res), err) => {
        assert(res === Array[Byte](97, 114, 99, 116, 101, 115, 116))
        assert(err === None)
      }
      case (_,_) => assert(false)
    }
  }

  test("BinaryTyping: value has leading/trailing spaces") {
    val col = BinaryColumn(id="1", name="name", description=Some("description"), nullable=true, nullReplacementValue=Some(""), trim=true, nullableValues=""::Nil, encoding=EncodingTypeBase64, metadata=None)
    Typing.typeValue("  YXJjdGVzdA==  ", col) match {
      case (Some(res), err) => {
        assert(res === Array[Byte](97, 114, 99, 116, 101, 115, 116))
        assert(err === None)
      }
      case (_,_) => assert(false)
    }
  }  

  test("BinaryTyping: invalid base64") {
    val col = BinaryColumn(id="1", name="name", description=Some("description"), nullable=true, nullReplacementValue=Some(""), trim=true, nullableValues=""::Nil, encoding=EncodingTypeBase64, metadata=None)
    val value = "ኃይሌ ገብረሥላሴ"
    Typing.typeValue(value, col) match {
      case (res, Some(err)) => {
        assert(res === None)
        assert(err === TypingError("name", s"""Unable to convert '${value}' to binary using 'base64' decoding."""))
      }
      case (_,_) => {
        println("Here")
        assert(false)
      }
    }
  }

  test("BinaryTyping: invalid hexadecimal") {
    val col = BinaryColumn(id="1", name="name", description=Some("description"), nullable=true, nullReplacementValue=Some(""), trim=true, nullableValues=""::Nil, encoding=EncodingTypeHexadecimal, metadata=None)
    val value = "6172637465737x"
    Typing.typeValue(value, col) match {
      case (res, Some(err)) => {
        assert(res === None)
        assert(err === TypingError("name", s"""Unable to convert '${value}' to binary using 'hexadecimal' decoding."""))
      }
      case (_,_) => {
        assert(false)
      }
    }
  }  

  // test("BinaryTyping: complex characters") {
  //   val col = BinaryColumn(id="1", name="name", description=Some("description"), nullable=false, nullReplacementValue=None, trim=true, nullableValues=""::Nil, trueValues=List("true","TRUE"), falseValues=List("false","FALSE"), metadata=None)
  //   Typing.typeValue("ኃይሌ ገብረሥላሴ", col) match {
  //     case (res, Some(err)) => {
  //       assert(res === None)
  //       assert(err === TypingError("name", s"""Unable to convert 'ኃይሌ ገብረሥላሴ' to boolean using provided true values: ['true', 'TRUE'] or false values: ['false', 'FALSE']"""))
  //     }
  //     case (_,_) => assert(false)
  //   }
  // }  

  // test("BinaryTyping: allowed complex characters") {
  //   val col = BinaryColumn(id="1", name="name", description=Some("description"), nullable=false, nullReplacementValue=None, trim=true, nullableValues=""::Nil, trueValues=List("true","TRUE","ኃይሌ"), falseValues=List("false","FALSE"), metadata=None)
  //   Typing.typeValue("ኃይሌ", col) match {
  //     case (Some(res), err) => {
  //       assert(res === true)
  //       assert(err === None)
  //     }
  //     case (_,_) => assert(false)
  //   }
  // }    
}
