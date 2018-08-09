package au.com.agl.arc.util

import org.apache.spark.sql.types.Decimal

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import au.com.agl.arc.api.API._

class DecimalTypingSuite extends FunSuite with BeforeAndAfter {

  before {
  }

  after {
  }

  test("Type Decimal Column") {

    // Test trimming
    {
      val col = DecimalColumn(id = "1", name = "name", description = Some("description"), nullable = true, nullReplacementValue = Some("42.22"), trim = true, nullableValues = "" :: Nil, precision = 4, scale = 2, formatter = None, metadata=None)
      val decimalValue = Decimal(42.22);
      // value is null -> nullReplacementValue
      {

        Typing.typeValue(null, col) match {
          case (Some(res), err) => {
            assert(res === decimalValue)
            assert(err === None)
          }
          case (_, _) => assert(false)
        }
      }

      // value has leading spaces
      {
        Typing.typeValue("     42.22", col) match {
          case (Some(res), err) => {
            assert(res === decimalValue)
            assert(err === None)
          }
          case (_, _) => assert(false)
        }
      }

      // value has trailing spaces
      {
        Typing.typeValue("42.22     ", col) match {
          case (Some(res), err) => {
            assert(res === decimalValue)
            assert(err === None)
          }
          case (_, _) => assert(false)
        }
      }

      // value has leading/trailing spaces
      {
        Typing.typeValue("   42.22     ", col) match {
          case (Some(res), err) => {
            assert(res === decimalValue)
            assert(err === None)
          }
          case (_, _) => assert(false)
        }
      }

      // value.isAllowedNullValue after trim -> nullReplacementValue
      {
        Typing.typeValue(" ", col) match {
          case (Some(res), err) => {
            assert(res === decimalValue)
            assert(err === None)
          }
          case (_, _) => assert(false)
        }
      }

      // value contains non number/s or characters
      {
        val value = "abc"
        Typing.typeValue(value, col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError("name", s"Unable to convert '${value}' to decimal(4, 2)"))
          }
          case (_, _) => assert(false)
        }
      }
    }

    // Test not trimming
    {
      val col = DecimalColumn(id = "1", name = "name", description = Some("description"), nullable = true, nullReplacementValue = Some("42.22"), trim = false, nullableValues = "" :: Nil, precision = 4, scale = 2, formatter = None, metadata=None)

      {
        val value = "   42.22"
        Typing.typeValue(value, col) match {
          case (res, Some(err)) => {

            assert(res === None)
            assert(err === TypingError("name", s"Unable to convert '${value}' to decimal(4, 2)"))
          }
          case (_, _) => assert(false)
        }
      }
    }


    // Test null input WITH nullReplacementValue
    {
      val col = DecimalColumn(id = "1", name = "name", description = Some("description"), nullable = true, nullReplacementValue = Some("42.22"), trim = false, nullableValues = "" :: Nil, precision = 4, scale = 2, formatter = None, metadata=None)
      val decimalValue = Decimal(42.22);
      // value is null -> nullReplacementValue
      {
        Typing.typeValue(null, col) match {
          case (Some(res), err) => {
            assert(res === decimalValue)
            assert(err === None)
          }
          case (_, _) => assert(false)
        }
      }

      // value.isAllowedNullValue
      {
        Typing.typeValue("", col) match {
          case (Some(res), err) => {
            assert(res === decimalValue)
            assert(err === None)
          }
          case (_, _) => assert(false)
        }
      }

      // value.isNotNull
      {
        Typing.typeValue("42.22", col) match {
          case (Some(res), err) => {
            assert(res === decimalValue)
            assert(err === None)
          }
          case (_, _) => assert(false)
        }
      }
    }

    // Test null input WITHOUT nullReplacementValue
    {
      val col = DecimalColumn(id = "1", name = "name", description = Some("description"), nullable = false, nullReplacementValue = None, trim = false, nullableValues = "" :: Nil, precision = 4, scale = 2, formatter = None, metadata=None)

      // value.isNull
      {
        Typing.typeValue(null, col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError.nullReplacementValueNullErrorForCol(col))
          }
          case (_, _) => assert(false)
        }
      }

      // value.isAllowedNullValue
      {
        Typing.typeValue("", col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError.nullReplacementValueNullErrorForCol(col))
          }
          case (_, _) => assert(false)
        }
      }

      // value.isNotNull
      {
        val decimalValue = Decimal(42.22);
        Typing.typeValue("42.22", col) match {
          case (Some(res), err) => {
            assert(res === decimalValue)
            assert(err === None)
          }
          case (_, _) => assert(false)
        }
      }
    }

    // Test other miscellaneous input types

    {
      val col = DecimalColumn(id = "1", name = "name", description = Some("description"), nullable = false, nullReplacementValue = None, trim = false, nullableValues = "" :: Nil, precision = 10, scale = 0, formatter = None, metadata=None)

      // value contains non numbers or characters
      {
        val value = "abc"
        Typing.typeValue(value, col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError("name", s"Unable to convert '${value}' to decimal(10, 0)"))
          }
          case (_, _) => assert(false)
        }
      }
    }

    // test valid precision value for Long range
    {
      val col = DecimalColumn(id = "1", name = "name", description = Some("description"), nullable = false, nullReplacementValue = None, trim = false, nullableValues = "" :: Nil, precision = 10, scale = 0, formatter = None, metadata=None)

      // precision '10' is valid for integer value that is converted to Long
      {
        val nextVal = Int.MaxValue.toLong + 1
        val decimalValue = Decimal(nextVal);
        val value = nextVal.toString()

        Typing.typeValue(value, col) match {
          case (Some(res), err) => {
            assert(res === decimalValue)
            assert(err === None)
          }
          case (_, _) => assert(false)
        }
      }
    }

    // test invalid precision value
    {
      val col = DecimalColumn(id = "1", name = "name", description = Some("description"), nullable = false, nullReplacementValue = None, trim = false, nullableValues = "" :: Nil, precision = 9, scale = 0, formatter = None, metadata=None)

      // invalid precision(<10) for the value that is converted to Long
      {
        val nextVal = Int.MaxValue.toLong + 1

        val decimalValue = Decimal(nextVal);
        val value = nextVal.toString()

        Typing.typeValue(value, col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError("name", s"Unable to convert '${value}' to decimal(9, 0)"))
          }

          case (_, _) => assert(false)
        }
      }
    }

    //test negative decimal
    {
      val col = DecimalColumn(id = "1", name = "name", description = Some("description"), nullable = false, nullReplacementValue = None, trim = false, nullableValues = "" :: Nil, precision = 4, scale = 2, formatter = None, metadata=None)

      // value contains negative number
      {
        val decimalValue = Decimal(-42.22);
        Typing.typeValue("-42.22", col) match {
          case (Some(res), err) => {
            assert(res === decimalValue)
            assert(err === None)
          }
          case (_, _) => assert(false)
        }
      }
    }

    {
      val col = DecimalColumn(id = "1", name = "name", description = Some("description"), nullable = false, nullReplacementValue = None, trim = false, nullableValues = "" :: Nil, precision = 4, scale = 2, formatter = None, metadata=None)

      // value contains complex characters
      {
        val value = "ኃይሌ ገብረሥላሴ"
        Typing.typeValue(value, col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError("name", s"Unable to convert '${value}' to decimal(4, 2)"))
          }
          case (_, _) => assert(false)
        }
      }
    }
  }
}
