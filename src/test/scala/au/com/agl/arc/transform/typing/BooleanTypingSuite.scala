package au.com.agl.arc.util

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import au.com.agl.arc.api.API._
import au.com.agl.arc.util._

class BooleanTypingSuite extends FunSuite with BeforeAndAfter {

  before {
  }

  after {
  }

  test("BooleanTyping: value has leading spaces") {
    val col = BooleanColumn(id="1", name="name", description=Some("description"), primaryKey=Option(true), nullable=true, nullReplacementValue=Some("true"), trim=true, nullableValues=""::Nil, trueValues=List("true","true"), falseValues=List("false","false"))
    Typing.typeValue(null, col) match {
      case (Some(res), err) => {
        assert(res === true)
        assert(err === None)
      }
      case (_,_) => assert(false)
    }
  }

  test("BooleanTyping: value has trailing spaces") {
    val col = BooleanColumn(id="1", name="name", description=Some("description"), primaryKey=Option(true), nullable=true, nullReplacementValue=Some("true"), trim=true, nullableValues=""::Nil, trueValues=List("true","true"), falseValues=List("false","false"))
    Typing.typeValue("true     ", col) match {
      case (Some(res), err) => {
        assert(res === true)
        assert(err === None)
      }
      case (_,_) => assert(false)
    }
  }

  test("BooleanTyping: value has leading/trailing spaces") {
    val col = BooleanColumn(id="1", name="name", description=Some("description"), primaryKey=Option(true), nullable=true, nullReplacementValue=Some("true"), trim=true, nullableValues=""::Nil, trueValues=List("true","true"), falseValues=List("false","false"))
    Typing.typeValue("   true     ", col) match {
      case (Some(res), err) => {
        assert(res === true)
        assert(err === None)
      }
      case (_,_) => assert(false)
    }
  }  

  test("BooleanTyping: value.isAllowedNullValue after trim -> nullReplacementValue") {
    val col = BooleanColumn(id="1", name="name", description=Some("description"), primaryKey=Option(true), nullable=true, nullReplacementValue=Some("true"), trim=true, nullableValues=""::Nil, trueValues=List("true","true"), falseValues=List("false","false"))
    Typing.typeValue(" ", col) match {
      case (Some(res), err) => {
        assert(res === true)
        assert(err === None)
      }
      case (_,_) => assert(false)
    }
  }  

  test("BooleanTyping: value.isAllowedNullValue") {
    val col = BooleanColumn(id="1", name="name", description=Some("description"), primaryKey=Option(true), nullable=true, nullReplacementValue=Some("true"), trim=true, nullableValues=""::Nil, trueValues=List("true","true"), falseValues=List("false","false"))
    Typing.typeValue("", col) match {
      case (Some(res), err) => {
        assert(res === true)
        assert(err === None)
      }
      case (_,_) => assert(false)
    }
  } 

  test("BooleanTyping: null input WITH nullReplacementValue") {
    val col = BooleanColumn(id="1", name="name", description=Some("description"), primaryKey=Option(true), nullable=true, nullReplacementValue=Some("true"), trim=true, nullableValues=""::Nil, trueValues=List("true","true"), falseValues=List("false","false"))
    Typing.typeValue(null, col) match {
      case (Some(res), err) => {
        assert(res === true)
        assert(err === None)
      }
      case (_,_) => assert(false)
    }
  } 

  test("BooleanTyping: empty input WITHOUT nullReplacementValue") {
    val col = BooleanColumn(id="1", name="name", description=Some("description"), primaryKey=Option(true), nullable=false, nullReplacementValue=None, trim=true, nullableValues=""::Nil, trueValues=List("true","TRUE"), falseValues=List("false","FALSE"))
    Typing.typeValue("", col) match {
      case (res, Some(err)) => {
        assert(res === None)
        assert(err === TypingError.nullReplacementValueNullErrorForCol(col))
      }
      case (_,_) => assert(false)
    }
  } 

  test("BooleanTyping: null input WITHOUT nullReplacementValue") {
    val col = BooleanColumn(id="1", name="name", description=Some("description"), primaryKey=Option(true), nullable=false, nullReplacementValue=None, trim=true, nullableValues=""::Nil, trueValues=List("true","TRUE"), falseValues=List("false","FALSE"))
    Typing.typeValue(null, col) match {
      case (res, Some(err)) => {
        assert(res === None)
        assert(err === TypingError.nullReplacementValueNullErrorForCol(col))
      }
      case (_,_) => assert(false)
    }
  } 

  test("BooleanTyping: invalid characters") {
    val col = BooleanColumn(id="1", name="name", description=Some("description"), primaryKey=Option(true), nullable=false, nullReplacementValue=None, trim=true, nullableValues=""::Nil, trueValues=List("true","TRUE"), falseValues=List("false","FALSE"))
    Typing.typeValue("abc", col) match {
      case (res, Some(err)) => {
        assert(res === None)
        assert(err === TypingError("name", s"""Unable to convert 'abc' to boolean using provided true values: ['true', 'TRUE'] or false values: ['false', 'FALSE']"""))
      }
      case (_,_) => assert(false)
    }
  }

  test("BooleanTyping: complex characters") {
    val col = BooleanColumn(id="1", name="name", description=Some("description"), primaryKey=Option(true), nullable=false, nullReplacementValue=None, trim=true, nullableValues=""::Nil, trueValues=List("true","TRUE"), falseValues=List("false","FALSE"))
    Typing.typeValue("ኃይሌ ገብረሥላሴ", col) match {
      case (res, Some(err)) => {
        assert(res === None)
        assert(err === TypingError("name", s"""Unable to convert 'ኃይሌ ገብረሥላሴ' to boolean using provided true values: ['true', 'TRUE'] or false values: ['false', 'FALSE']"""))
      }
      case (_,_) => assert(false)
    }
  }  

  test("BooleanTyping: allowed complex characters") {
    val col = BooleanColumn(id="1", name="name", description=Some("description"), primaryKey=Option(true), nullable=false, nullReplacementValue=None, trim=true, nullableValues=""::Nil, trueValues=List("true","TRUE","ኃይሌ"), falseValues=List("false","FALSE"))
    Typing.typeValue("ኃይሌ", col) match {
      case (Some(res), err) => {
        assert(res === true)
        assert(err === None)
      }
      case (_,_) => assert(false)
    }
  }    
}
