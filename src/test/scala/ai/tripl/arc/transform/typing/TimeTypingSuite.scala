package ai.tripl.arc.transform

import java.sql.Time

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import ai.tripl.arc.api.API._
import ai.tripl.arc.util._

class TimeTypingSuite extends FunSuite with BeforeAndAfter {

  before {
  }

  after {
  }

  test("Type Time Column") {

    // Test trimming
    {
      val timeValue = "12:34:56"
      val fmt = List("HH:mm:ss")
      val col = TimeColumn(None, name="name", description=Some("description"), nullable=true, nullReplacementValue=Some("12:34:56"), trim=true, nullableValues="" :: Nil, formatters=fmt, metadata=None)

      // value is null -> nullReplacementValue
      {
        Typing.typeValue(null, col) match {

          case (Some(res), err) => {
            assert(res === timeValue)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      // value has leading spaces
      {
        Typing.typeValue("     12:34:56", col) match {
          case (Some(res), err) => {
            assert(res === timeValue)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      // value has trailing spaces
      {
        Typing.typeValue("12:34:56     ", col) match {
          case (Some(res), err) => {
            assert(res === timeValue)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      // value has leading/trailing spaces
      {
        Typing.typeValue("   12:34:56     ", col) match {
          case (Some(res), err) => {
            assert(res === timeValue)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      // value.isAllowedNullValue after trim -> nullReplacementValue
      {
        Typing.typeValue(" ", col) match {
          case (Some(res), err) => {
            assert(res === timeValue)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }
    }

    // Test not trimming
    {
      val timeValue = "12:34:56"
      val fmt = List("HH:mm:ss")
      val col = TimeColumn(None, name="name", description=Some("description"), nullable=true, nullReplacementValue=Some("12:34:56"), trim=false, nullableValues="" :: Nil, formatters=fmt, metadata=None)

            // value has leading spaces
            {
              val value = "   12:34:56"
              Typing.typeValue(value, col) match {
                case (res, Some(err)) => {
                  assert(res === None)
                  assert(err === TypingError.forCol(col, s"""Unable to convert '$value' to time using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}]"""))
                }
                case (_, _) => assert(false)
              }
            }

            // value has trailing spaces
            {
              val value = "12:34:56     "
              Typing.typeValue(value, col) match {
                case (res, Some(err)) => {
                  assert(res === None)
                  assert(err === TypingError.forCol(col, s"""Unable to convert '$value' to time using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}]"""))
                }
                case (_, _) => assert(false)
              }
            }

            // value has leading and trailing spaces
            {
              val value = "   12:34:56     "
              Typing.typeValue(value, col) match {
                case (res, Some(err)) => {
                  assert(res === None)
                  assert(err === TypingError.forCol(col, s"""Unable to convert '$value' to time using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}]"""))
                }
                case (_, _) => assert(false)
              }
            }

      // value.isAllowedNullValue after trim

      {
        val value = " "
        Typing.typeValue(value, col) match {

          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError.forCol(col, s"""Unable to convert '$value' to time using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}]"""))
          }
          case (_, _) => assert(false)
        }
      }
    }

    // Test null input WITH nullReplacementValue
    {
      val timeValue = "12:34:56"
      val fmt = List("HH:mm:ss")
      val col = TimeColumn(None, name="name", description=Some("description"), nullable=true, nullReplacementValue=Some("12:34:56"), trim=false, nullableValues="" :: Nil, formatters=fmt, metadata=None)

      // value.isNull
      {
        Typing.typeValue(null, col) match {
          case (Some(res), err) => {
            assert(res === timeValue)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      // value.isAllowedNullValue
      {
        Typing.typeValue("", col) match {
          case (Some(res), err) => {
            assert(res === timeValue)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      // value.isNotNull
      {
        Typing.typeValue("12:34:56", col) match {
          case (Some(res), err) => {
            assert(res === timeValue)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }
    }

    // Test null input WITHOUT nullReplacementValue
    {
      val timeValue = Time.valueOf("12:34:56").toString
      val fmt = List("HH:mm:ss")
      val col = TimeColumn(None, name="name", description=Some("description"), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil, formatters=fmt, metadata=None)

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
        Typing.typeValue("12:34:56", col) match {
          case (Some(res), err) => {
            assert(res === timeValue)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }
    }

    // Test other miscellaneous input types
    {
      val fmt = List("HH:mm:ss")
      val col = TimeColumn(None, name="name", description=Some("description"), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil, formatters=fmt, metadata=None)

      // format is different (e.g. dd-mm-yyyy)
      {
        val value = "18-12-2016"
        Typing.typeValue(value, col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError.forCol(col, s"""Unable to convert '$value' to time using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}]"""))
          }
          case (_, _) => assert(false)
        }
      }

      //value contains invalid numbers
      {
        val value = "121-125-200016"
        Typing.typeValue(value, col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError.forCol(col, s"""Unable to convert '$value' to time using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}]"""))
          }
          case (_, _) => assert(false)
        }
      }

      // value contains invalid format
      {
        val value = "1215-2016"
        Typing.typeValue(value, col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError.forCol(col, s"""Unable to convert '$value' to time using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}]"""))
          }
          case (_, _) => assert(false)
        }
      }

      // value contains characters instead of legit numbers
      {
        val value = "ab-xy-2016"
        Typing.typeValue(value, col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError.forCol(col, s"""Unable to convert '$value' to time using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}]"""))
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
            assert(err === TypingError.forCol(col, s"""Unable to convert '$value' to time using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}]"""))
          }
          case (_, _) => assert(false)
        }
      }
    }

    // Test multiple formatters
    {
      val fmt = List("HH:mm:ss", "HH:mm:ss.nnnnnnnnn", "HHmmss")
      val col = TimeColumn(None, name="name", description=Some("description"), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil, formatters=fmt, metadata=None)

      {
        val timeValue = "12:34:56.987654321"
        val value = "12:34:56.987654321"
        Typing.typeValue(value, col) match {
          case (Some(res), err) => {
            assert(res === timeValue)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      {
        val timeValue = "12:34:56"
        val value = "12:34:56"
        Typing.typeValue(value, col) match {
          case (Some(res), err) => {
            assert(res === timeValue)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      {
        val timeValue = "12:34:56"
        val value = "123456"
        Typing.typeValue(value, col) match {
          case (Some(res), err) => {
            assert(res === timeValue)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      {
        val timeValue = "12:34:00"
        val value = "12:34:00"
        Typing.typeValue(value, col) match {
          case (Some(res), err) => {
            assert(res === timeValue)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }
    }

    // Test bad inputs
    {
      val fmt = List("HH:mm:ss")
      val col = TimeColumn(None, name="name", description=Some("description"), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil, formatters=fmt, metadata=None)

      {
        val value = "24:10:31"
        Typing.typeValue(value, col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError.forCol(col, s"""Unable to convert '$value' to time using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}]"""))
          }
          case (_,_) => assert(false)
        }
      }
    }
  }
}
