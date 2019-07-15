package ai.tripl.arc.transform

import java.sql.Date

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import ai.tripl.arc.api.API._
import ai.tripl.arc.util._

class DateTypingSuite extends FunSuite with BeforeAndAfter {

  before {
  }

  after {
  }

  test("Type Date Column") {

    // Test trimming
    {
      val dateValue = Date.valueOf("2016-12-18")
      val fmt = List("yyyy-MM-dd")
      val col = DateColumn(id="1", name="name", description=Some("description"), nullable=true, nullReplacementValue=Some("2016-12-18"), trim=true, nullableValues="" :: Nil, formatters=fmt, metadata=None, strict=false)

      // value is null -> nullReplacementValue
      {
        Typing.typeValue(null, col) match {

          case (Some(res), err) => {
            assert(res === dateValue)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      // value has leading spaces
      {
        Typing.typeValue("     2016-12-18", col) match {
          case (Some(res), err) => {
            assert(res === dateValue)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      // value has trailing spaces
      {
        Typing.typeValue("2016-12-18     ", col) match {
          case (Some(res), err) => {
            assert(res === dateValue)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      // value has leading/trailing spaces
      {
        Typing.typeValue("   2016-12-18     ", col) match {
          case (Some(res), err) => {
            assert(res === dateValue)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      // value.isAllowedNullValue after trim -> nullReplacementValue
      {
        Typing.typeValue(" ", col) match {
          case (Some(res), err) => {
            assert(res === dateValue)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }
    }

    // Test not trimming
    {
      val dateValue = Date.valueOf("2016-12-18")
      val fmt = List("yyyy-MM-dd")
      val col = DateColumn(id="1", name="name", description=Some("description"), nullable=true, nullReplacementValue=Some("2016-12-18"), trim=false, nullableValues="" :: Nil, formatters=fmt, metadata=None, strict=false)

            // value has leading spaces
            {
              val value = "   2016-12-18"
              Typing.typeValue(value, col) match {
                case (res, Some(err)) => {
                  assert(res === None)
                  assert(err === TypingError.forCol(col, s"""Unable to convert '$value' to date using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}]"""))
                }
                case (_, _) => assert(false)
              }
            }

            // value has trailing spaces
            {
              val value = "2016-12-18     "
              Typing.typeValue(value, col) match {
                case (res, Some(err)) => {
                  assert(res === None)
                  assert(err === TypingError.forCol(col, s"""Unable to convert '$value' to date using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}]"""))
                }
                case (_, _) => assert(false)
              }
            }

            // value has leading and trailing spaces
            {
              val value = "   2016-12-18     "
              Typing.typeValue(value, col) match {
                case (res, Some(err)) => {
                  assert(res === None)
                  assert(err === TypingError.forCol(col, s"""Unable to convert '$value' to date using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}]"""))
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
            assert(err === TypingError.forCol(col, s"""Unable to convert '$value' to date using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}]"""))
          }
          case (_, _) => assert(false)
        }
      }
    }

    // Test null input WITH nullReplacementValue
    {
      val dateValue = Date.valueOf("2016-12-18")
      val fmt = List("yyyy-MM-dd")
      val col = DateColumn(id="1", name="name", description=Some("description"), nullable=true, nullReplacementValue=Some("2016-12-18"), trim=false, nullableValues="" :: Nil, formatters=fmt, metadata=None, strict=false)

      // value.isNull
      {
        Typing.typeValue(null, col) match {
          case (Some(res), err) => {
            assert(res === dateValue)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      // value.isAllowedNullValue
      {
        Typing.typeValue("", col) match {
          case (Some(res), err) => {
            assert(res === dateValue)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      // value.isNotNull
      {
        Typing.typeValue("2016-12-18", col) match {
          case (Some(res), err) => {
            assert(res === dateValue)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }
    }

    // Test null input WITHOUT nullReplacementValue
    {
      val dateValue = Date.valueOf("2016-12-18")
      val fmt = List("yyyy-MM-dd")
      val col = DateColumn(id="1", name="name", description=Some("description"), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil, formatters=fmt, metadata=None, strict=false)

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
        Typing.typeValue("2016-12-18", col) match {
          case (Some(res), err) => {
            assert(res === dateValue)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }
    }

    // Test other miscellaneous input types
    {
      val fmt = List("yyyy-MM-dd")
      val col = DateColumn(id="1", name="name", description=Some("description"), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil, formatters=fmt, metadata=None, strict=false)

      // format is different (e.g. dd-mm-yyyy)
      {
        val value = "18-12-2016"
        Typing.typeValue(value, col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError.forCol(col, s"""Unable to convert '$value' to date using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}]"""))
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
            assert(err === TypingError.forCol(col, s"""Unable to convert '$value' to date using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}]"""))
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
            assert(err === TypingError.forCol(col, s"""Unable to convert '$value' to date using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}]"""))
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
            assert(err === TypingError.forCol(col, s"""Unable to convert '$value' to date using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}]"""))
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
            assert(err === TypingError.forCol(col, s"""Unable to convert '$value' to date using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}]"""))
          }
          case (_, _) => assert(false)
        }
      }
    }

    // Test multiple formatters
    {
      val dateValue = Date.valueOf("2016-12-18")
      val fmt = List("yyyy-MM-dd", "dd-MM-yyyy", "dd MMM yyyy")
      val col = DateColumn(id="1", name="date", description=Some("description"), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil, formatters=fmt, metadata=None, strict=false)

      {
        val value = "18-12-2016"
        Typing.typeValue(value, col) match {
          case (Some(res), err) => {
            assert(res === dateValue)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      {
        val value = "2016-12-18"
        Typing.typeValue(value, col) match {
          case (Some(res), err) => {
            assert(res === dateValue)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      {
        val value = "18 Dec 2016"
        Typing.typeValue(value, col) match {
          case (Some(res), err) => {
            assert(res === dateValue)
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      {
        val value = "18 December 16"
        Typing.typeValue(value, col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError.forCol(col, s"""Unable to convert '$value' to date using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}]"""))
          }
          case (_, _) => assert(false)
        }
      }
    }
  }

  test("Type Date Column: impossible date: strict") {
      val fmt = List("yyyy-MM-dd")
      val col = DateColumn(id="1", name="date", description=Some("description"), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil, formatters=fmt, metadata=None, strict=true)

    // 30 February is impossible
    val value = "2000-02-30"
      Typing.typeValue(value, col) match {
        case (None, Some(err)) => {
          assert(err === TypingError.forCol(col, s"""Unable to convert '$value' to date using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}]"""))
        }
        case (_,_) => assert(false)
      }
  }

  test("Type Date Column: impossible date: not strict") {
      val dateValue = Date.valueOf("2000-02-29")
      val fmt = List("yyyy-MM-dd")
      val col = DateColumn(id="1", name="date", description=Some("description"), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil, formatters=fmt, metadata=None, strict=false)

    // 30 February is impossible
    val value = "2000-02-30"
    Typing.typeValue(value, col) match {
      case (Some(res), err) => {
        assert(res === dateValue)
        assert(err === None)
      }
      case (_,_) => assert(false)
    }
  }
}
