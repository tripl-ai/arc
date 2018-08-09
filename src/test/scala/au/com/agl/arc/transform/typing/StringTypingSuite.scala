package au.com.agl.arc.util

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import au.com.agl.arc.api.API._
import au.com.agl.arc.util._

class StringTypingSuite extends FunSuite with BeforeAndAfter {

  before {
  }

  after {
  }

  test("Type String Column") {

   // Test trimming
    {
      val col = StringColumn(id="1", name="name", description=Some("description"), nullable=true, nullReplacementValue=Some("Maurice"), trim=true, nullableValues="" :: Nil,  metadata=None)
      
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

    // Test not trimming
    {
      val col = StringColumn(id="1", name="name", description=Some("description"), nullable=true, nullReplacementValue=Some("Maurice"), trim=false, nullableValues="" :: Nil,  metadata=None)
      
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
      val col = StringColumn(id="1", name="name", description=Some("description"), nullable=true, nullReplacementValue=Some("Maurice"), trim=false, nullableValues="" :: Nil,  metadata=None)
 
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
      val col = StringColumn(id="2", name="name", description=Some("description"), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil, metadata=None)      

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
      val col = StringColumn(id="1", name="name", description=Some("description"), nullable=true, nullReplacementValue=Some("ኃይሌ ገብረሥላሴ"), trim=false, nullableValues="español" :: "lamfo340jnf34" :: " a " :: Nil, metadata=None)
 
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
}
