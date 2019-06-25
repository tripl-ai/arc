package ai.tripl.arc.transform

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import ai.tripl.arc.api.API._
import ai.tripl.arc.util._

class IntegerTypingSuite extends FunSuite with BeforeAndAfter {

  before {
  }

  after {
  }

  test("Type Integer Column") {

    // Test trimming
    {
      val col = IntegerColumn(id = "1", name = "name", description = Some("description"), nullable = true, nullReplacementValue = Some("42"), trim = true, nullableValues = "" :: Nil, metadata=None, formatters = None)

      // value is null -> nullReplacementValue
      {
        Typing.typeValue(null, col) match {
          case (Some(res), err) => {
            assert(res === 42)
            assert(err === None)
          }
          case (_, _) => assert(false)
        }
      }

      // value has leading spaces
      {
        Typing.typeValue("     88", col) match {
          case (Some(res), err) => {
            assert(res === 88)
            assert(err === None)
          }
          case (_, _) => assert(false)
        }
      }

      // value has trailing spaces
      {
        Typing.typeValue("88     ", col) match {
          case (Some(res), err) => {
            assert(res === 88)
            assert(err === None)
          }
          case (_, _) => assert(false)
        }
      }

      // value has leading/trailing spaces
      {
        Typing.typeValue("   88     ", col) match {
          case (Some(res), err) => {
            assert(res === 88)
            assert(err === None)
          }
          case (_, _) => assert(false)
        }
      }

      // value.isAllowedNullValue after trim -> nullReplacementValue
      {
        Typing.typeValue(" ", col) match {
          case (Some(res), err) => {
            assert(res === 42)
            assert(err === None)
          }
          case (_, _) => assert(false)
        }
      }
    }

    // Test not trimming
    {
      val col = IntegerColumn(id = "1", name = "name", description = Some("description"), nullable = true, nullReplacementValue = Some("42"), trim = false, nullableValues = "" :: Nil, metadata=None, formatters = None)

      {
        val value = "   42"
        //println(Typing.typeValue(value,col))
        Typing.typeValue(value, col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError("name", s"Unable to convert '${value}' to integer using formatters ['#,##0;-#,##0']"))
          }
          case (_, _) => assert(false)
        }
      }

      {
        val value = "42   "
        Typing.typeValue(value, col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError("name", s"Unable to convert '${value}' to integer using formatters ['#,##0;-#,##0']"))
          }
          case (_, _) => assert(false)
        }
      }

      {
        val value = "    42   "
        Typing.typeValue(value, col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError("name", s"Unable to convert '${value}' to integer using formatters ['#,##0;-#,##0']"))
          }
          case (_, _) => assert(false)
        }
      }

    }


    // Test null input WITH nullReplacementValue
    {
      val col = IntegerColumn(id = "1", name = "name", description = Some("description"), nullable = true, nullReplacementValue = Some("42"), trim = false, nullableValues = "" :: Nil, metadata=None, formatters = None)

      // value.isNull
      {
        Typing.typeValue(null, col) match {
          case (Some(res), err) => {
            assert(res === 42)
            assert(err === None)
          }
          case (_, _) => assert(false)
        }
      }

      // value.isAllowedNullValue
      {
        Typing.typeValue("", col) match {
          case (Some(res), err) => {
            assert(res === 42)
            assert(err === None)
          }
          case (_, _) => assert(false)
        }
      }

      // value.isNotNull
      {
        Typing.typeValue("88", col) match {
          case (Some(res), err) => {
            assert(res === 88)
            assert(err === None)
          }
          case (_, _) => assert(false)
        }
      }
    }

    // Test null input WITHOUT nullReplacementValue
    {
      val col = IntegerColumn(id = "1", name = "name", description = Some("description"), nullable = false, nullReplacementValue = None, trim = false, nullableValues = "" :: Nil, metadata=None, formatters = None)

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
        Typing.typeValue("42", col) match {
          case (Some(res), err) => {
            assert(res === 42)
            assert(err === None)
          }
          case (_, _) => assert(false)
        }
      }
    }

    // Test other miscellaneous input types
    {
      val col = IntegerColumn(id = "1", name = "name", description = Some("description"), nullable = false, nullReplacementValue = None, trim = false, nullableValues = "" :: Nil, metadata=None, formatters = None)

      // value contains non numbers or characters
      {
        val value = "abc"
        Typing.typeValue(value, col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError("name", s"Unable to convert '${value}' to integer using formatters ['#,##0;-#,##0']"))
          }
          case (_, _) => assert(false)
        }
      }

      // value contains number beyond maximum integer boundary pow(2,31)-1
      {
        val nextVal = Int.MaxValue.toLong+1
        val value = nextVal.toString()
        Typing.typeValue(value, col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError("name", s"Unable to convert '${value}' to integer using formatters ['#,##0;-#,##0']"))
          }
          case (_, _) => assert(false)
        }
      }

      // value contains number beyond minimum integer boundary -pow(2,31)
      {
        val nextVal = Int.MinValue.toLong-1
        val value = nextVal.toString()
        Typing.typeValue(value, col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError("name", s"Unable to convert '${value}' to integer using formatters ['#,##0;-#,##0']"))
          }
          case (_, _) => assert(false)
        }
      }

      // value contains negative number
      {
        val value = "-2"
        Typing.typeValue(value, col) match {
          case (Some(res), err) => {
            assert(res === -2)
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
            assert(err === TypingError("name", s"Unable to convert '${value}' to integer using formatters ['#,##0;-#,##0']"))
          }
          case (_, _) => assert(false)
        }
      }
    }


    //test formatter change negative suffix
    {
      val col = IntegerColumn(id = "1", name = "name", description = Some("description"), nullable = false, nullReplacementValue = None, trim = false, nullableValues = "" :: Nil, metadata=None, formatters = Option(List("#,##0;#,##0-")))

      // value contains negative number
      {
        val value = "42-"
        Typing.typeValue(value, col) match {
          case (Some(res), err) => {
            assert(res === -42)
            assert(err === None)
          }
          case (_, _) => assert(false)
        }
      }
    }  

    //test multiple formatter
    {
      val col = IntegerColumn(id = "1", name = "name", description = Some("description"), nullable = false, nullReplacementValue = None, trim = false, nullableValues = "" :: Nil, metadata=None, formatters = Option(List("#,##0;#,##0-", "#,##0;(#,##0)")))

      // value contains negative number
      {
        val value = "(42)"
        Typing.typeValue(value, col) match {
          case (Some(res), err) => {
            assert(res === -42)
            assert(err === None)
          }
          case (_, _) => assert(false)
        }
      }
    }      

    //test formatter in error message
    {
      val col = IntegerColumn(id = "1", name = "name", description = Some("description"), nullable = false, nullReplacementValue = None, trim = false, nullableValues = "" :: Nil, metadata=None, formatters = Option(List("#,##0;#,##0-")))

      // value contains negative number
      {
        val value = "-42"
        Typing.typeValue(value, col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError("name", s"Unable to convert '${value}' to integer using formatters ['#,##0;#,##0-']"))
          }
          case (_, _) => assert(false)
        }
      }
    } 

  }

}
