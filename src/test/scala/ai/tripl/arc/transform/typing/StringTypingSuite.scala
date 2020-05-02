package ai.tripl.arc.transform

import scala.util.matching.Regex

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import ai.tripl.arc.api.API._
import ai.tripl.arc.util._

class StringTypingSuite extends FunSuite with BeforeAndAfter {

  before {
  }

  after {
  }

  test("Type String Column") {

   // Test trimming with nullReplacementValue
    {
      val col = StringColumn(None, name="name", description=Some("description"), nullable=true, nullReplacementValue=Some("Maurice"), trim=true, nullableValues="" :: Nil,  metadata=None, minLength=None, maxLength=None, regex=None)

      // value is null -> nullReplacementValue
      {
        Typing.typeValue(null, col) match {
          case (Some(res), err) => {
            assert(res === "Maurice")
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      // value has leading spaces
      {
        Typing.typeValue("     Wendy", col) match {
          case (Some(res), err) => {
            assert(res === "Wendy")
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      // value has trailing spaces
      {
        Typing.typeValue("Wendy     ", col) match {
          case (Some(res), err) => {
            assert(res === "Wendy")
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      // value has leading/trailing spaces
      {
        Typing.typeValue("   Wendy     ", col) match {
          case (Some(res), err) => {
            assert(res === "Wendy")
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      // value.isAllowedNullValue after trim -> nullReplacementValue
      {
        Typing.typeValue(" ", col) match {
          case (Some(res), err) => {
            assert(res === "Maurice")
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }
    }

   // Test trimming without nullReplacementValue
    {
      val col = StringColumn(None, name="name", description=Some("description"), nullable=true, nullReplacementValue=None, trim=true, nullableValues="" :: "null" :: Nil,  metadata=None, minLength=None, maxLength=None, regex=None)

      // value.isAllowedNullValue after trim -> null
      {
        Typing.typeValue(" null", col) match {
          case (res, err) => {
            assert(res === None)
            assert(err === None)
          }
        }
      }

      // value.isAllowedNullValue after trim -> null
      {
        Typing.typeValue(" ", col) match {
          case (res, err) => {
            assert(res === None)
            assert(err === None)
          }
        }
      }
    }

    // Test not trimming
    {
      val col = StringColumn(None, name="name", description=Some("description"), nullable=true, nullReplacementValue=Some("Maurice"), trim=false, nullableValues="" :: Nil,  metadata=None, minLength=None, maxLength=None, regex=None)

      // value has leading spaces
      {
        Typing.typeValue("     Wendy", col) match {
          case (Some(res), err) => {
            assert(res === "     Wendy")
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      // value has trailing spaces
      {
        Typing.typeValue("Wendy     ", col) match {
          case (Some(res), err) => {
            assert(res === "Wendy     ")
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      // value has trailing spaces
      {
        Typing.typeValue("   Wendy     ", col) match {
          case (Some(res), err) => {
            assert(res === "   Wendy     ")
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      // value.isAllowedNullValue after trim
      {
        Typing.typeValue(" ", col) match {
          case (Some(res), err) => {
            assert(res === " ")
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }
    }

    // Test null input WITH nullReplacementValue
    {
      val col = StringColumn(None, name="name", description=Some("description"), nullable=true, nullReplacementValue=Some("Maurice"), trim=false, nullableValues="" :: Nil,  metadata=None, minLength=None, maxLength=None, regex=None)

      // value.isNull
      {
        Typing.typeValue(null, col) match {
          case (Some(res), err) => {
            assert(res === "Maurice")
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      // value.isAllowedNullValue
      {
        Typing.typeValue("", col) match {
          case (Some(res), err) => {
            assert(res === "Maurice")
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      // value.isNotNull
      {
        Typing.typeValue("Wendy", col) match {
          case (Some(res), err) => {
            assert(res === "Wendy")
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }
    }

    // Test null input WITHOUT nullReplacementValue
    {
      val col = StringColumn(id=None, name="name", description=Some("description"), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: " " :: Nil, metadata=None, minLength=None, maxLength=None, regex=None)

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
        Typing.typeValue("Wendy", col) match {
          case (Some(res), err) => {
            assert(res === "Wendy")
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }
    }

    // Test complex nullableValues (unicode)
    {
      val col = StringColumn(None, name="name", description=Some("description"), nullable=true, nullReplacementValue=Some("ኃይሌ ገብረሥላሴ"), trim=false, nullableValues="español" :: "lamfo340jnf34" :: " a " :: Nil, metadata=None, minLength=None, maxLength=None, regex=None)

      // value.isAllowedNullValue
      {
        Typing.typeValue("español", col) match {
          case (Some(res), err) => {
            assert(res === "ኃይሌ ገብረሥላሴ")
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      // value.isAllowedNullValue
      {
        Typing.typeValue("lamfo340jnf34", col) match {
          case (Some(res), err) => {
            assert(res === "ኃይሌ ገብረሥላሴ")
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      // value.isAllowedNullValue
      {
        Typing.typeValue(" a ", col) match {
          case (Some(res), err) => {
            assert(res === "ኃይሌ ገብረሥላሴ")
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }

      // value
      {
        Typing.typeValue("Escócia", col) match {
          case (Some(res), err) => {
            assert(res === "Escócia")
            assert(err === None)
          }
          case (_,_) => assert(false)
        }
      }
    }
  }

  test("Test minLength") {
    {
      val col = StringColumn(None, name="name", description=Some("description"), nullable=true, nullReplacementValue=None, trim=true, nullableValues="" :: Nil,  metadata=None, minLength=Option(50), maxLength=None, regex=None)
      val value = "abcdefghijklmnopqrstuvwxyz1"

      {
        Typing.typeValue(value, col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError("name", s"String '${value}' (27 characters) is less than minLength (50)."))
          }
          case (_,_) => assert(false)
        }
      }
    }
  }

  test("Test maxLength") {
    {
      val col = StringColumn(None, name="name", description=Some("description"), nullable=true, nullReplacementValue=None, trim=true, nullableValues="" :: Nil,  metadata=None, minLength=None, maxLength=Option(10), regex=None)
      val value = "abcdefghijklmnopqrstuvwxyz1"

      {
        Typing.typeValue(value, col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError("name", s"String '${value}' (27 characters) is greater than maxLength (10)."))
          }
          case (_,_) => assert(false)
        }
      }
    }
  }

  test("Test minLength and maxLength") {
    {
      val col = StringColumn(None, name="name", description=Some("description"), nullable=true, nullReplacementValue=None, trim=true, nullableValues="" :: Nil,  metadata=None, minLength=Option(50), maxLength=Option(10), regex=None)
      val value = "abcdefghijklmnopqrstuvwxyz1"

      {
        Typing.typeValue(value, col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError("name", s"String '${value}' (27 characters) is less than minLength (50). String '${value}' (27 characters) is greater than maxLength (10)."))
          }
          case (_,_) => assert(false)
        }
      }
    }
  }

  test("Test regex") {
    {
      val col = StringColumn(None, name="name", description=Some("description"), nullable=true, nullReplacementValue=None, trim=true, nullableValues="" :: Nil,  metadata=None, minLength=None, maxLength=None, regex=Some(new Regex("[a-z]*")))
      val value = "abcdefghijklmnopqrstuvwxyz1"

      {
        Typing.typeValue(value, col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError("name", s"String '${value}' does not match regex '[a-z]*'."))
          }
          case (_,_) => assert(false)
        }
      }
    }
  }

  test("Test minLength and regex") {
    {
      val col = StringColumn(None, name="name", description=Some("description"), nullable=true, nullReplacementValue=None, trim=true, nullableValues="" :: Nil,  metadata=None, minLength=Some(50), maxLength=None, regex=Some(new Regex("[a-z]*")))
      val value = "abcdefghijklmnopqrstuvwxyz1"

      {
        Typing.typeValue(value, col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError("name", s"String '${value}' (27 characters) is less than minLength (50). String '${value}' does not match regex '[a-z]*'."))
          }
          case (_,_) => assert(false)
        }
      }
    }
  }  

  test("Test maxLength and regex") {
    {
      val col = StringColumn(None, name="name", description=Some("description"), nullable=true, nullReplacementValue=None, trim=true, nullableValues="" :: Nil,  metadata=None, minLength=None, maxLength=Some(10), regex=Some(new Regex("[a-z]*")))
      val value = "abcdefghijklmnopqrstuvwxyz1"

      {
        Typing.typeValue(value, col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError("name", s"String '${value}' (27 characters) is greater than maxLength (10). String '${value}' does not match regex '[a-z]*'."))
          }
          case (_,_) => assert(false)
        }
      }
    }
  }  

  test("Test minLength and maxLength and regex") {
    {
      val col = StringColumn(None, name="name", description=Some("description"), nullable=true, nullReplacementValue=None, trim=true, nullableValues="" :: Nil,  metadata=None, minLength=Option(50), maxLength=Option(10), regex=Some(new Regex("[a-z]*")))
      val value = "abcdefghijklmnopqrstuvwxyz1"

      {
        Typing.typeValue(value, col) match {
          case (res, Some(err)) => {
            assert(res === None)
            assert(err === TypingError("name", s"String '${value}' (27 characters) is less than minLength (50). String '${value}' (27 characters) is greater than maxLength (10). String '${value}' does not match regex '[a-z]*'."))
          }
          case (_,_) => assert(false)
        }
      }
    }
  }

}
