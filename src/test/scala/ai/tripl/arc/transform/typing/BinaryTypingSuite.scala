package ai.tripl.arc.transform

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import ai.tripl.arc.api.API._
import ai.tripl.arc.util._

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
    val col = BinaryColumn(None, name="name", description=Some("description"), nullable=true, nullReplacementValue=Some(""), trim=true, nullableValues=""::Nil, encoding=EncodingTypeBase64, metadata=None)
    Typing.typeValue(" YXJjdGVzdA==", col) match {
      case (Some(res), err) => {
        assert(res === Array[Byte](97, 114, 99, 116, 101, 115, 116))
        assert(err === None)
      }
      case (_,_) => assert(false)
    }
  }

  test("BinaryTyping: value has trailing spaces") {
    val col = BinaryColumn(None, name="name", description=Some("description"), nullable=true, nullReplacementValue=Some(""), trim=true, nullableValues=""::Nil, encoding=EncodingTypeBase64, metadata=None)
    Typing.typeValue("YXJjdGVzdA==  ", col) match {
      case (Some(res), err) => {
        assert(res === Array[Byte](97, 114, 99, 116, 101, 115, 116))
        assert(err === None)
      }
      case (_,_) => assert(false)
    }
  }

  test("BinaryTyping: value has leading/trailing spaces") {
    val col = BinaryColumn(None, name="name", description=Some("description"), nullable=true, nullReplacementValue=Some(""), trim=true, nullableValues=""::Nil, encoding=EncodingTypeBase64, metadata=None)
    Typing.typeValue("  YXJjdGVzdA==  ", col) match {
      case (Some(res), err) => {
        assert(res === Array[Byte](97, 114, 99, 116, 101, 115, 116))
        assert(err === None)
      }
      case (_,_) => assert(false)
    }
  }

  test("BinaryTyping: invalid base64") {
    val col = BinaryColumn(None, name="name", description=Some("description"), nullable=true, nullReplacementValue=Some(""), trim=true, nullableValues=""::Nil, encoding=EncodingTypeBase64, metadata=None)
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
    val col = BinaryColumn(None, name="name", description=Some("description"), nullable=true, nullReplacementValue=Some(""), trim=true, nullableValues=""::Nil, encoding=EncodingTypeHexadecimal, metadata=None)
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
}
