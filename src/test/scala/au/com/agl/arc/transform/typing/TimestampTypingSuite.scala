package au.com.agl.arc

import java.sql.Timestamp
import java.time.{LocalTime, ZoneId, ZonedDateTime, Instant}

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import au.com.agl.arc.api.API._
import au.com.agl.arc.util._

class TimestampTypingSuite extends FunSuite with BeforeAndAfter {

  before {
  }

  after {
  }

  test("Type Timestamp Column: null") {
    val datetimeValue = ZonedDateTime.of(2017, 12, 20, 21, 46, 54, 0, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss")
    val col = TimestampColumn(id="1", name="timestamp", description=Some("description"), primaryKey=Option(true), nullable=false, nullReplacementValue=Some("20-12-2017 21:46:54"), trim=true, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None)


    Typing.typeValue(null, col) match {

      case (Some(res), err) => {
        assert(res === timestampValue)
        assert(err === None)
      }
      case (_,_) => assert(false)
    }
  }

  test("Type Timestamp Column: leading spaces trim") {
    val datetimeValue = ZonedDateTime.of(2017, 12, 20, 21, 46, 54, 0, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss")
    val col = TimestampColumn(id="1", name="timestamp", description=Some("description"), primaryKey=Option(true), nullable=false, nullReplacementValue=Some("20-12-2017 21:46:54"), trim=true, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None)


    Typing.typeValue("    20-12-2017 21:46:54", col) match {
      case (Some(res), err) => {
        assert(res === timestampValue)
        assert(err === None)
      }
      case (_,_) => assert(false)
    }
  }

  test("Type Timestamp Column: trailing spaces") {
    val datetimeValue = ZonedDateTime.of(2017, 12, 20, 21, 46, 54, 0, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss")
    val col = TimestampColumn(id="1", name="timestamp", description=Some("description"), primaryKey=Option(true), nullable=false, nullReplacementValue=Some("20-12-2017 21:46:54"), trim=true, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None)


    Typing.typeValue("20-12-2017 21:46:54    ", col) match {
      case (Some(res), err) => {
        assert(res === timestampValue)
        assert(err === None)
      }
      case (_,_) => assert(false)
    }
  }

  test("Type Timestamp Column: leading/trailing spaces") {
    val datetimeValue = ZonedDateTime.of(2017, 12, 20, 21, 46, 54, 0, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss")
    val col = TimestampColumn(id="1", name="timestamp", description=Some("description"), primaryKey=Option(true), nullable=false, nullReplacementValue=Some("20-12-2017 21:46:54"), trim=true, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None)


    Typing.typeValue("   20-12-2017 21:46:54    ", col) match {
      case (Some(res), err) => {
        assert(res === timestampValue)
        assert(err === None)
      }
      case (_,_) => assert(false)
    }
  }

  test("Type Timestamp Column: ' '") {
    val datetimeValue = ZonedDateTime.of(2017, 12, 20, 21, 46, 54, 0, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss")
    val col = TimestampColumn(id="1", name="timestamp", description=Some("description"), primaryKey=Option(true), nullable=false, nullReplacementValue=Some("20-12-2017 21:46:54"), trim=true, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None)

    Typing.typeValue(" ", col) match {
      case (Some(res), err) => {
        assert(res === timestampValue)
        assert(err === None)
      }
      case (_,_) => assert(false)
    }
  }

  test("Type Timestamp Column: leading spaces not-trim") {
    val datetimeValue = ZonedDateTime.of(2017, 12, 20, 19, 16, 55, 0, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss")
    val col = TimestampColumn(id="1", name="timestamp", description=Some("description"), primaryKey=Option(true), nullable=false, nullReplacementValue=Some("20-12-2017 21:46:54"), trim=false, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None)

    val value = "   20-12-2017 19:16:55"
    Typing.typeValue(value, col) match {
      case (res, Some(err)) => {
        assert(res === None)
        assert(err === TypingError.forCol(col, s"""Unable to convert '$value' to timestamp using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}] and timezone '${col.timezoneId}'"""))
      }
      case (_, _) => assert(false)
    }
  }

  test("Type Timestamp Column: trailing spaces not-trim") {
    val datetimeValue = ZonedDateTime.of(2017, 12, 20, 19, 16, 55, 0, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss")
    val col = TimestampColumn(id="1", name="timestamp", description=Some("description"), primaryKey=Option(true), nullable=false, nullReplacementValue=Some("20-12-2017 21:46:54"), trim=false, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None)

    val value = "20-12-2017 19:16:55     "
    Typing.typeValue(value, col) match {
      case (res, Some(err)) => {
        assert(res === None)
        assert(err === TypingError.forCol(col, s"""Unable to convert '$value' to timestamp using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}] and timezone '${col.timezoneId}'"""))
      }
      case (_, _) => assert(false)
    }
  }  

  test("Type Timestamp Column: leading/trailing spaces not-trim") {
    val datetimeValue = ZonedDateTime.of(2017, 12, 20, 19, 16, 55, 0, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss")
    val col = TimestampColumn(id="1", name="timestamp", description=Some("description"), primaryKey=Option(true), nullable=false, nullReplacementValue=Some("20-12-2017 21:46:54"), trim=false, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None)

    val value = "   20-12-2017 19:16:55     "
    Typing.typeValue(value, col) match {
      case (res, Some(err)) => {
        assert(res === None)
        assert(err === TypingError.forCol(col, s"""Unable to convert '$value' to timestamp using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}] and timezone '${col.timezoneId}'"""))
      }
      case (_, _) => assert(false)
    }
  } 

  test("Type Timestamp Column: ' ' not-trim") {
    val datetimeValue = ZonedDateTime.of(2017, 12, 20, 19, 16, 55, 0, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss")
    val col = TimestampColumn(id="1", name="timestamp", description=Some("description"), primaryKey=Option(true), nullable=false, nullReplacementValue=Some("20-12-2017 21:46:54"), trim=false, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None)

    val value = " "
    Typing.typeValue(value, col) match {

      case (res, Some(err)) => {
        assert(res === None)
        assert(err === TypingError.forCol(col, s"""Unable to convert '$value' to timestamp using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}] and timezone '${col.timezoneId}'"""))
      }
      case (_, _) => assert(false)
    }
  } 

  test("Type Timestamp Column: null input WITH nullReplacementValue") {
    val datetimeValue = ZonedDateTime.of(2017, 12, 20, 19, 16, 55, 0, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss")
    val col = TimestampColumn(id="1", name="timestamp", description=Some("description"), primaryKey=Option(true), nullable=true, nullReplacementValue=Some("20-12-2017 19:16:55"), trim=false, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None)

    Typing.typeValue(null, col) match {
      case (Some(res), err) => {
        assert(res === timestampValue)
        assert(err === None)
      }
      case (_,_) => assert(false)
    }
  } 

  test("Type Timestamp Column: ' ' value.isAllowedNullValue") {
    val datetimeValue = ZonedDateTime.of(2017, 12, 20, 19, 16, 55, 0, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss")
    val col = TimestampColumn(id="1", name="timestamp", description=Some("description"), primaryKey=Option(true), nullable=true, nullReplacementValue=Some("20-12-2017 19:16:55"), trim=false, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None)

    Typing.typeValue("", col) match {
      case (Some(res), err) => {
        assert(res === timestampValue)
        assert(err === None)
      }
      case (_,_) => assert(false)
    }
  } 

  test("Type Timestamp Column: value.isAllowedNullValue") {
    val datetimeValue = ZonedDateTime.of(2017, 12, 20, 19, 16, 55, 0, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss")
    val col = TimestampColumn(id="1", name="timestamp", description=Some("description"), primaryKey=Option(true), nullable=true, nullReplacementValue=Some("20-12-2017 19:16:55"), trim=false, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None)

    Typing.typeValue("20-12-2017 19:16:55", col) match {
      case (Some(res), err) => {
        assert(res ===timestampValue)
        assert(err === None)
      }
      case (_,_) => assert(false)
    }
  }   

  test("Type Timestamp Column: null input WITHOUT nullReplacementValue") {
    val datetimeValue = ZonedDateTime.of(2017, 12, 20, 19, 16, 55, 0, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss")
    val col = TimestampColumn(id="1", name="timestamp", description=Some("description"), primaryKey=Option(true), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None)

    Typing.typeValue(null, col) match {
      case (res, Some(err)) => {
        assert(res === None)
        assert(err === TypingError.nullReplacementValueNullErrorForCol(col))
      }
      case (_,_) => assert(false)
    }
  }  

  test("Type Timestamp Column: isAllowedNullValue") {
    val datetimeValue = ZonedDateTime.of(2017, 12, 20, 19, 16, 55, 0, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss")
    val col = TimestampColumn(id="1", name="timestamp", description=Some("description"), primaryKey=Option(true), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None)

    Typing.typeValue("", col) match {
      case (res, Some(err)) => {
        assert(res === None)
        assert(err === TypingError.nullReplacementValueNullErrorForCol(col))
      }
      case (_,_) => assert(false)
    }
  }  

  test("Type Timestamp Column: isNotNull") {
    val datetimeValue = ZonedDateTime.of(2017, 12, 20, 19, 16, 55, 0, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss")
    val col = TimestampColumn(id="1", name="timestamp", description=Some("description"), primaryKey=Option(true), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None)

    Typing.typeValue("20-12-2017 19:16:55", col) match {
      case (Some(res), err) => {
        assert(res === timestampValue)
        assert(err === None)
      }
      case (_,_) => assert(false)
    }
  }  

  test("Type Timestamp Column: multiple formatters - valid") {
    val datetimeValue = ZonedDateTime.of(2016, 12, 18, 17, 55, 21, 0, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss", "yyyy-MM-dd HH:mm:ss")
    val col = TimestampColumn(id="1", name="timestamp", description=Some("description"), primaryKey=Option(true), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None)

    {
      val value = "18-12-2016 17:55:21"
      Typing.typeValue(value, col) match {
        case (Some(res), err) => {
          assert(res === timestampValue)
          assert(err === None)
        }
        case (_,_) => assert(false)
      }
    } 

    {
      val value = "18/12/2016 17:55:21"
      Typing.typeValue(value, col) match {
        case (Some(res), err) => {
          assert(res === timestampValue)
          assert(err === None)
        }
        case (_,_) => assert(false)
      }
    }      
  }  

  test("Type Timestamp Column: multiple formatters - not-valid") {
    val datetimeValue = ZonedDateTime.of(2016, 12, 18, 17, 55, 21, 0, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss", "yyyy-MM-dd HH:mm:ss")
    val col = TimestampColumn(id="1", name="timestamp", description=Some("description"), primaryKey=Option(true), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None)

    {
      val value = "18 December 16 17:55:21"
      Typing.typeValue(value, col) match {        
        case (res, Some(err)) => {
          assert(res === None)
          assert(err === TypingError.forCol(col, s"""Unable to convert '$value' to timestamp using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}] and timezone '${col.timezoneId}'"""))
        }
        case (_, _) => assert(false)
      }
    }

    {
      val value = "1215-2016"
      Typing.typeValue(value, col) match {
        case (res, Some(err)) => {
          assert(res === None)
          assert(err === TypingError.forCol(col, s"""Unable to convert '$value' to timestamp using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}] and timezone '${col.timezoneId}'"""))
        }
        case (_, _) => assert(false)
      }
    }

    {
      val value = "ab-xy-2016"
      Typing.typeValue(value, col) match {
        case (res, Some(err)) => {
          assert(res === None)
          assert(err === TypingError.forCol(col, s"""Unable to convert '$value' to timestamp using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}] and timezone '${col.timezoneId}'"""))
        }
        case (_, _) => assert(false)
      }
    }    
  }  

  test("Type Timestamp Column: timezones not UTC") {
    val datetimeValue = ZonedDateTime.of(2017, 12, 20, 21, 46, 54, 0, ZoneId.of("+1000"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss")
    val col = TimestampColumn(id="1", name="timestamp", description=Some("description"), primaryKey=Option(true), nullable=false, nullReplacementValue=None, trim=true, nullableValues="" :: Nil, timezoneId="+1000", formatters=fmt, None)

    val value = "20-12-2017 21:46:54"
    Typing.typeValue(value, col) match {

      case (Some(res), err) => {
        assert(res === timestampValue)
        assert(err === None)
      }
      case (_,_) => assert(false)
    }   
  }   

  test("Type Timestamp Column: timezones UTC") {
    // Australia/Sydney has a +1100 offset at this time of year
    val datetimeValue = ZonedDateTime.of(2017, 12, 20, 10, 46, 54, 0, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss")
    val col = TimestampColumn(id="1", name="timestamp", description=None, primaryKey=Option(true), nullable=false, nullReplacementValue=None, trim=true, nullableValues="" :: Nil, timezoneId="Australia/Sydney", formatters=fmt, None)

    val value = "20-12-2017 21:46:54"
    Typing.typeValue(value, col) match {
      case (Some(res), err) => {
        assert(res === timestampValue)
        assert(err === None)
      }
      case (_,_) => assert(false)
    } 
  }   

  test("Type Timestamp Column: hard-coded time component") {
    // Australia/Sydney has a +1100 offset at this time of year
    val datetimeValue = ZonedDateTime.of(2017, 12, 20, 10, 45, 59, 0, ZoneId.of("Australia/Sydney"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("dd-MM-yyyy")
    val time = LocalTime.of(10, 45, 59, 0)
    val col = TimestampColumn(id="1", name="timestamp", description=None, primaryKey=Option(true), nullable=false, nullReplacementValue=None, trim=true, nullableValues="" :: Nil, timezoneId="Australia/Sydney", formatters=fmt, time=Option(time))

    val value = "20-12-2017"
    Typing.typeValue(value, col) match {
      case (Some(res), err) => {
        assert(res === timestampValue)
      }
      case (_,err) => {
        assert(err === None)
      }
    }
  }  

  test("Type Timestamp Column: Timezone included in string") {
      val datetimeValue = ZonedDateTime.of(2016, 12, 18, 17, 55, 21, 0, ZoneId.of("UTC"))
      val timestampValue = Timestamp.from(datetimeValue.toInstant())
      val fmt = List("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
      val col = TimestampColumn(id="1", name="timestamp", description=Some("description"), primaryKey=Option(true), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil, timezoneId="Australia/Sydney", formatters=fmt, None)

      val value = "2016-12-19T04:55:21.000+11:00"
      Typing.typeValue(value, col) match {
        case (Some(res), err) => {
          assert(res === timestampValue)
          assert(err === None)
        }
        case (_,_) => assert(false)
      }
  }  


  test("Type Timestamp Column: Timestamp from Epoch") {
      val timestampValue = Timestamp.from(Instant.ofEpochSecond(1527726973))
      val fmt = List("ssssssssss")
      val col = TimestampColumn(id="1", name="timestamp", description=Some("description"), primaryKey=Option(true), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil, timezoneId="UTC", formatters=fmt, None)

      val value = "1527726973"
      Typing.typeValue(value, col) match {
        case (Some(res), err) => {
          assert(res === timestampValue)
          assert(err === None)
        }
        case (_,_) => assert(false)
      }
  }    

  test("Type Timestamp Column: Timestamp from Invalid Epoch") {
      val timestampValue = Timestamp.from(Instant.ofEpochSecond(1527726973))
      val fmt = List("ssssssssss")
      val col = TimestampColumn(id="1", name="timestamp", description=Some("description"), primaryKey=Option(true), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil, timezoneId="UTC", formatters=fmt, None)

      val value = "-1"
      Typing.typeValue(value, col) match {
        case (None, Some(err)) => {
          assert(err === TypingError.forCol(col, s"""Unable to convert '$value' to timestamp using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}] and timezone '${col.timezoneId}'"""))
        }
        case (_,_) => assert(false)
      }
  }   

  test("Type Timestamp Column: Timestamp from EpochMillis") {
      val timestampValue = Timestamp.from(Instant.ofEpochMilli(1527726973423L))
      val fmt = List("sssssssssssss")
      val col = TimestampColumn(id="1", name="timestamp", description=Some("description"), primaryKey=Option(true), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil, timezoneId="UTC", formatters=fmt, None)

      val value = "1527726973423"
      Typing.typeValue(value, col) match {
        case (Some(res), err) => {
          assert(res === timestampValue)
          assert(err === None)
        }
        case (_,_) => assert(false)
      }
  }   

  test("Type Timestamp Column: Timestamp from Invalid EpochMillis") {
      val timestampValue = Timestamp.from(Instant.ofEpochSecond(1527726973423L))
      val fmt = List("ssssssssss")
      val col = TimestampColumn(id="1", name="timestamp", description=Some("description"), primaryKey=Option(true), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil, timezoneId="UTC", formatters=fmt, None)

      val value = "-1"
      Typing.typeValue(value, col) match {
        case (None, Some(err)) => {
          assert(err === TypingError.forCol(col, s"""Unable to convert '$value' to timestamp using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}] and timezone '${col.timezoneId}'"""))
        }
        case (_,_) => assert(false)
      }
  }   

  test("Type Timestamp Column: Timestamp from ISO8601") {
    val datetimeValue = ZonedDateTime.of(2017, 10, 1, 22, 30, 0, 0, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("yyyy-MM-dd'T'HH:mm:ss")
    val col = TimestampColumn(id="1", name="timestamp", description=Some("description"), primaryKey=Option(true), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil, timezoneId="UTC", formatters=fmt, None)

    val value = "2017-10-01T22:30:00"
    Typing.typeValue(value, col) match {
      case (Some(res), err) => {
        assert(res === timestampValue)
        assert(err === None)
      }
      case (_,_) => assert(false)
    }
  }      
}