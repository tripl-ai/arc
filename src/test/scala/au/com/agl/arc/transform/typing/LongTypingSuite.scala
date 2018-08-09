package au.com.agl.arc.util

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import au.com.agl.arc.api.API._
import au.com.agl.arc.util._

class LongTypingSuite extends FunSuite with BeforeAndAfter {

  before {
  }

  after {
  }

  test("Type Long Column") {

    // Test trimming
    {
      val col = LongColumn(id="1", name="name", description=Some("description"), nullable=true, nullReplacementValue=Some("9223372036854775806"), trim=true, nullableValues="" :: Nil, metadata=None)

      // value is null -> nullReplacementValue
      {
        Typing.typeValue(null, col) match {
          case (Some(res), err) => {
            assert(res === 9223372036854775806L)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      // value has leading spaces
      {
        Typing.typeValue("     9223372036854775806", col) match {
          case (Some(res), err) => {
            assert(res === 9223372036854775806L)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      // value has trailing spaces
      {
        Typing.typeValue("9223372036854775806     ", col) match {
          case (Some(res), err) => {
            assert(res === 9223372036854775806L)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      // value has leading/trailing spaces
      {
        Typing.typeValue("   9223372036854775806     ", col) match {
          case (Some(res), err) => {
            assert(res === 9223372036854775806L)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      // value.isAllowedNullValue after trim -> nullReplacementValue
      {
        Typing.typeValue(" ", col) match {
          case (Some(res), err) => {
            assert(res === 9223372036854775806L)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }
    }

    // Test not trimming
    {
      val col = LongColumn(id="1", name="name", description=Some("description"), nullable=true, nullReplacementValue=Some("9223372036854775806"), trim=false, nullableValues="" :: Nil, metadata=None)

      {
        val value = "   9223372036854775806"
        Typing.typeValue(value, col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError("name", s"Unable to convert '${value}' to long"))
          }
          case (_,_) => assert(false)
        }
      }
    }


    // Test null input WITH nullReplacementValue
    {
      val col = LongColumn(id="1", name="name", description=Some("description"), nullable=true, nullReplacementValue=Some("9223372036854775806"), trim=false, nullableValues="" :: Nil, metadata=None)

      // value.isNull
      {
        Typing.typeValue(null, col) match {
          case (Some(res), err) => {
            assert(res === 9223372036854775806L)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      // value.isAllowedNullValue
      {
        Typing.typeValue("", col) match {
          case (Some(res), err) => {
            assert(res === 9223372036854775806L)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      // value.isNotNull
      {
        Typing.typeValue("9223372036854775806", col) match {
          case (Some(res), err) => {
            assert(res === 9223372036854775806L)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }
    }

    // Test null input WITHOUT nullReplacementValue
    {
      val col = LongColumn(id="1", name="name", description=Some("description"), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil, metadata=None)

      // value.isNull
      {
        Typing.typeValue(null, col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError.nullReplacementValueNullErrorForCol(col))
          }
          case (_,_) => assert(false)
        }
      }

      // value.isAllowedNullValue
      {
        Typing.typeValue("", col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError.nullReplacementValueNullErrorForCol(col))
          }
          case (_,_) => assert(false)
        }
      }

      // value.isNotNull
      {
        Typing.typeValue("9223372036854775806", col) match {
          case (Some(res), err) => {
            assert(res === 9223372036854775806L)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

    }

    // Test other miscellaneous input types
    {
      val col = LongColumn(id = "1", name = "name", description = Some("description"), nullable = false, nullReplacementValue = None, trim = false, nullableValues = "" :: Nil, metadata=None)

      // value contains non number/s or characters
      {
        val value = "abc"
        Typing.typeValue(value, col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError("name", s"Unable to convert '${value}' to long"))
          }
          case (_, _) => assert(false)
        }
      }

      // value contains number beyond maximum long boundary
      {
        val nextVal = Long.MaxValue.toDouble + 1
        val value = nextVal.toString
        Typing.typeValue(value, col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError("name", s"Unable to convert '${value}' to long"))
          }
          case (_, _) => assert(false)
        }
      }

      // value contains number beyond minimum long boundary
      {
        val nextVal = Int.MinValue.toDouble - 1
        val value = nextVal.toString()
        Typing.typeValue(value, col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError("name", s"Unable to convert '${value}' to long"))
          }
          case (_, _) => assert(false)
        }
      }

      // value contains negative number
      {
        val value = "-92233720368547758"
        Typing.typeValue(value, col) match {
          case (Some(res), err) => {
            assert(res === -92233720368547758L)
            assert(err === None)
          }
          case (_, _) => assert(false)
        }
      }

      // value contains complex characters
      {
        val value = "ኃይሌ ገብረሥላሴ"
        Typing.typeValue(value, col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError("name", s"Unable to convert '${value}' to long"))
          }
          case (_, _) => assert(false)
        }
      }
    }
  }
}
