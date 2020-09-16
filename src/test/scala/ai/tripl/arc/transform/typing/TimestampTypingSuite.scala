package ai.tripl.arc.transform

import java.sql.Timestamp
import java.time.{LocalTime, ZoneId, ZonedDateTime, Instant}
import java.time.format.DateTimeFormatter

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import ai.tripl.arc.api.API._
import ai.tripl.arc.util._

class TimestampTypingSuite extends FunSuite with BeforeAndAfter {

  before {
  }

  after {
  }

  test("Type Timestamp Column: null") {
    val datetimeValue = ZonedDateTime.of(2017, 12, 20, 21, 46, 54, 0, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss")
    val col = TimestampColumn(None, name="timestamp", description=Some("description"), nullable=false, nullReplacementValue=Some("20-12-2017 21:46:54"), trim=true, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None, metadata=None, strict=false, caseSensitive=false)

    Typing.typeValue(null, col) match {
      case (Some(res), err) => {
        assert(res === timestampValue)
        assert(err === None)
      }
      case (_,Some(err)) => fail(err.toString)
      case (_,_) => assert(false)
    }
  }

  case class Test(
    fmt: String,
    value: String,
    expected: Timestamp
  )

  test("Type Timestamp Column: leading spaces trim") {
    val datetimeValue = ZonedDateTime.of(2017, 12, 20, 21, 46, 54, 0, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss")
    val col = TimestampColumn(None, name="timestamp", description=Some("description"), nullable=false, nullReplacementValue=Some("20-12-2017 21:46:54"), trim=true, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None, metadata=None, strict=false, caseSensitive=false)

    Typing.typeValue("    20-12-2017 21:46:54", col) match {
      case (Some(res), err) => {
        assert(res === timestampValue)
        assert(err === None)
      }
      case (_,Some(err)) => fail(err.toString)
      case (_,_) => assert(false)
    }
  }

  test("Type Timestamp Column: trailing spaces") {
    val datetimeValue = ZonedDateTime.of(2017, 12, 20, 21, 46, 54, 0, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss")
    val col = TimestampColumn(None, name="timestamp", description=Some("description"), nullable=false, nullReplacementValue=Some("20-12-2017 21:46:54"), trim=true, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None, metadata=None, strict=false, caseSensitive=false)

    Typing.typeValue("20-12-2017 21:46:54    ", col) match {
      case (Some(res), err) => {
        assert(res === timestampValue)
        assert(err === None)
      }
      case (_,Some(err)) => fail(err.toString)
      case (_,_) => assert(false)
    }
  }

  test("Type Timestamp Column: leading/trailing spaces") {
    val datetimeValue = ZonedDateTime.of(2017, 12, 20, 21, 46, 54, 0, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss")
    val col = TimestampColumn(None, name="timestamp", description=Some("description"), nullable=false, nullReplacementValue=Some("20-12-2017 21:46:54"), trim=true, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None, metadata=None, strict=false, caseSensitive=false)

    Typing.typeValue("   20-12-2017 21:46:54    ", col) match {
      case (Some(res), err) => {
        assert(res === timestampValue)
        assert(err === None)
      }
      case (_,Some(err)) => fail(err.toString)
      case (_,_) => assert(false)
    }
  }

  test("Type Timestamp Column: ' '") {
    val datetimeValue = ZonedDateTime.of(2017, 12, 20, 21, 46, 54, 0, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss")
    val col = TimestampColumn(None, name="timestamp", description=Some("description"), nullable=false, nullReplacementValue=Some("20-12-2017 21:46:54"), trim=true, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None, metadata=None, strict=false, caseSensitive=false)

    Typing.typeValue(" ", col) match {
      case (Some(res), err) => {
        assert(res === timestampValue)
        assert(err === None)
      }
      case (_,Some(err)) => fail(err.toString)
      case (_,_) => assert(false)
    }
  }

  test("Type Timestamp Column: leading spaces not-trim") {
    val datetimeValue = ZonedDateTime.of(2017, 12, 20, 19, 16, 55, 0, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss")
    val col = TimestampColumn(None, name="timestamp", description=Some("description"), nullable=false, nullReplacementValue=Some("20-12-2017 21:46:54"), trim=false, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None, metadata=None, strict=false, caseSensitive=false)

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
    val col = TimestampColumn(None, name="timestamp", description=Some("description"), nullable=false, nullReplacementValue=Some("20-12-2017 21:46:54"), trim=false, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None, metadata=None, strict=false, caseSensitive=false)

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
    val col = TimestampColumn(None, name="timestamp", description=Some("description"), nullable=false, nullReplacementValue=Some("20-12-2017 21:46:54"), trim=false, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None, metadata=None, strict=false, caseSensitive=false)

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
    val col = TimestampColumn(None, name="timestamp", description=Some("description"), nullable=false, nullReplacementValue=Some("20-12-2017 21:46:54"), trim=false, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None, metadata=None, strict=false, caseSensitive=false)

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
    val col = TimestampColumn(None, name="timestamp", description=Some("description"), nullable=true, nullReplacementValue=Some("20-12-2017 19:16:55"), trim=false, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None, metadata=None, strict=false, caseSensitive=false)

    Typing.typeValue(null, col) match {
      case (Some(res), err) => {
        assert(res === timestampValue)
        assert(err === None)
      }
      case (_,Some(err)) => fail(err.toString)
      case (_,_) => assert(false)
    }
  }

  test("Type Timestamp Column: ' ' value.isAllowedNullValue") {
    val datetimeValue = ZonedDateTime.of(2017, 12, 20, 19, 16, 55, 0, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss")
    val col = TimestampColumn(None, name="timestamp", description=Some("description"), nullable=true, nullReplacementValue=Some("20-12-2017 19:16:55"), trim=false, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None, metadata=None, strict=false, caseSensitive=false)

    Typing.typeValue("", col) match {
      case (Some(res), err) => {
        assert(res === timestampValue)
        assert(err === None)
      }
      case (_,Some(err)) => fail(err.toString)
      case (_,_) => assert(false)
    }
  }

  test("Type Timestamp Column: value.isAllowedNullValue") {
    val datetimeValue = ZonedDateTime.of(2017, 12, 20, 19, 16, 55, 0, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss")
    val col = TimestampColumn(None, name="timestamp", description=Some("description"), nullable=true, nullReplacementValue=Some("20-12-2017 19:16:55"), trim=false, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None, metadata=None, strict=false, caseSensitive=false)

    Typing.typeValue("20-12-2017 19:16:55", col) match {
      case (Some(res), err) => {
        assert(res ===timestampValue)
        assert(err === None)
      }
      case (_,Some(err)) => fail(err.toString)
      case (_,_) => assert(false)
    }
  }

  test("Type Timestamp Column: null input WITHOUT nullReplacementValue") {
    val datetimeValue = ZonedDateTime.of(2017, 12, 20, 19, 16, 55, 0, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss")
    val col = TimestampColumn(None, name="timestamp", description=Some("description"), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None, metadata=None, strict=false, caseSensitive=false)

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
    val col = TimestampColumn(None, name="timestamp", description=Some("description"), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None, metadata=None, strict=false, caseSensitive=false)

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
    val col = TimestampColumn(None, name="timestamp", description=Some("description"), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None, metadata=None, strict=false, caseSensitive=false)

    Typing.typeValue("20-12-2017 19:16:55", col) match {
      case (Some(res), err) => {
        assert(res === timestampValue)
        assert(err === None)
      }
      case (_,Some(err)) => fail(err.toString)
      case (_,_) => assert(false)
    }
  }

  test("Type Timestamp Column: multiple formatters - valid") {
    val datetimeValue = ZonedDateTime.of(2016, 12, 18, 17, 55, 21, 0, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss", "yyyy-MM-dd HH:mm:ss")
    val col = TimestampColumn(None, name="timestamp", description=Some("description"), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None, metadata=None, strict=false, caseSensitive=false)

    {
      val value = "18-12-2016 17:55:21"
      Typing.typeValue(value, col) match {
        case (Some(res), err) => {
          assert(res === timestampValue)
          assert(err === None)
        }
        case (_,Some(err)) => fail(err.toString)
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
        case (_,Some(err)) => fail(err.toString)
        case (_,_) => assert(false)
      }
    }
  }

  test("Type Timestamp Column: multiple formatters - not-valid") {
    val datetimeValue = ZonedDateTime.of(2016, 12, 18, 17, 55, 21, 0, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss", "yyyy-MM-dd HH:mm:ss")
    val col = TimestampColumn(None, name="timestamp", description=Some("description"), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil, timezoneId="GMT", formatters=fmt, None, metadata=None, strict=false, caseSensitive=false)

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
    val col = TimestampColumn(None, name="timestamp", description=Some("description"), nullable=false, nullReplacementValue=None, trim=true, nullableValues="" :: Nil, timezoneId="+1000", formatters=fmt, None, metadata=None, strict=false, caseSensitive=false)

    val value = "20-12-2017 21:46:54"
    Typing.typeValue(value, col) match {

      case (Some(res), err) => {
        assert(res === timestampValue)
        assert(err === None)
      }
      case (_,Some(err)) => fail(err.toString)
      case (_,_) => assert(false)
    }
  }

  test("Type Timestamp Column: timezones UTC") {
    // Australia/Sydney has a +1100 offset at this time of year
    val datetimeValue = ZonedDateTime.of(2017, 12, 20, 10, 46, 54, 0, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss")
    val col = TimestampColumn(None, name="timestamp", description=None, nullable=false, nullReplacementValue=None, trim=true, nullableValues="" :: Nil, timezoneId="Australia/Sydney", formatters=fmt, None, metadata=None, strict=false, caseSensitive=false)

    val value = "20-12-2017 21:46:54"
    Typing.typeValue(value, col) match {
      case (Some(res), err) => {
        assert(res === timestampValue)
        assert(err === None)
      }
      case (_,Some(err)) => fail(err.toString)
      case (_,_) => assert(false)
    }
  }

  test("Type Timestamp Column: hard-coded time component") {
    // Australia/Sydney has a +1100 offset at this time of year
    val datetimeValue = ZonedDateTime.of(2017, 12, 20, 10, 45, 59, 0, ZoneId.of("Australia/Sydney"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("dd-MM-yyyy")
    val time = LocalTime.of(10, 45, 59, 0)
    val col = TimestampColumn(None, name="timestamp", description=None, nullable=false, nullReplacementValue=None, trim=true, nullableValues="" :: Nil, timezoneId="Australia/Sydney", formatters=fmt, time=Option(time), metadata=None, strict=false, caseSensitive=false)

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
      val col = TimestampColumn(None, name="timestamp", description=Some("description"), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil, timezoneId="Australia/Sydney", formatters=fmt, None, metadata=None, strict=false, caseSensitive=false)

      val value = "2016-12-19T04:55:21.000+11:00"
      Typing.typeValue(value, col) match {
        case (Some(res), err) => {
          assert(res === timestampValue)
          assert(err === None)
        }
        case (_,Some(err)) => fail(err.toString)
        case (_,_) => assert(false)
      }
  }


  test("Type Timestamp Column: Timestamp from Epoch") {
      val timestampValue = Timestamp.from(Instant.ofEpochSecond(1527726973))
      val fmt = List("ssssssssss")
      val col = TimestampColumn(None, name="timestamp", description=Some("description"), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil, timezoneId="UTC", formatters=fmt, None, metadata=None, strict=false, caseSensitive=false)

      val value = "1527726973"
      Typing.typeValue(value, col) match {
        case (Some(res), err) => {
          assert(res === timestampValue)
          assert(err === None)
        }
        case (_,Some(err)) => fail(err.toString)
        case (_,_) => assert(false)
      }
  }

  test("Type Timestamp Column: Timestamp from Invalid Epoch") {
      val timestampValue = Timestamp.from(Instant.ofEpochSecond(1527726973))
      val fmt = List("ssssssssss")
      val col = TimestampColumn(None, name="timestamp", description=Some("description"), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil, timezoneId="UTC", formatters=fmt, None, metadata=None, strict=false, caseSensitive=false)

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
      val col = TimestampColumn(None, name="timestamp", description=Some("description"), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil, timezoneId="UTC", formatters=fmt, None, metadata=None, strict=false, caseSensitive=false)

      val value = "1527726973423"
      Typing.typeValue(value, col) match {
        case (Some(res), err) => {
          assert(res === timestampValue)
          assert(err === None)
        }
        case (_,Some(err)) => fail(err.toString)
        case (_,_) => assert(false)
      }
  }

  test("Type Timestamp Column: Timestamp from Invalid EpochMillis") {
      val timestampValue = Timestamp.from(Instant.ofEpochSecond(1527726973423L))
      val fmt = List("ssssssssss")
      val col = TimestampColumn(None, name="timestamp", description=Some("description"), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil, timezoneId="UTC", formatters=fmt, None, metadata=None, strict=false, caseSensitive=false)

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
    val col = TimestampColumn(None, name="timestamp", description=Some("description"), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil, timezoneId="UTC", formatters=fmt, None, metadata=None, strict=false, caseSensitive=false)

    val value = "2017-10-01T22:30:00"
    Typing.typeValue(value, col) match {
      case (Some(res), err) => {
        assert(res === timestampValue)
        assert(err === None)
      }
      case (_,_) => assert(false)
    }
  }

  test("Type Timestamp Column: impossible date: strict") {
    val fmt = List("uuuu-MM-dd HH:mm:ss")
    val col = TimestampColumn(None, name="timestamp", description=None, nullable=false, nullReplacementValue=None, trim=true, nullableValues="" :: Nil, timezoneId="UTC", formatters=fmt, None, metadata=None, strict=true, caseSensitive=false)

    // 30 February is impossible
    val value = "2000-02-30 00:00:00"
      Typing.typeValue(value, col) match {
        case (None, Some(err)) => {
          assert(err === TypingError.forCol(col, s"""Unable to convert '$value' to timestamp using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}] and timezone '${col.timezoneId}'"""))
        }
        case (_,_) => assert(false)
      }
  }

  test("Type Timestamp Column: impossible date: not strict") {
    val datetimeValue = ZonedDateTime.of(2000, 2, 29, 0, 0, 0, 0, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("uuuu-MM-dd HH:mm:ss")
    val col = TimestampColumn(None, name="timestamp", description=None, nullable=false, nullReplacementValue=None, trim=true, nullableValues="" :: Nil, timezoneId="UTC", formatters=fmt, None, metadata=None, strict=false, caseSensitive=false)

    // 30 February is impossible
    val value = "2000-02-30 00:00:00"
    Typing.typeValue(value, col) match {
      case (Some(res), err) => {
        assert(res === timestampValue)
        assert(err === None)
      }
      case (_,Some(err)) => fail(err.toString)
      case (_,_) => assert(false)
    }
  }

  test("Type Timestamp Column: Formatter contains YearOfEra") {
    val pattern = "yyyy-MM-dd HH:mm:ss GG"
    val formatter = DateTimeFormatter.ofPattern(pattern).toString

    assert(formatter.contains("(YearOfEra,"))
    assert(formatter.contains("(Era,"))
    assert(!formatter.contains("(Year,"))
  }

  test("Type Timestamp Column: Formatter contains Year") {
    val pattern = "uuuu-MM-dd HH:mm:ss"
    val formatter = DateTimeFormatter.ofPattern(pattern).toString

    assert(!formatter.contains("(YearOfEra,"))
    assert(!formatter.contains("(Era,"))
    assert(formatter.contains("(Year,"))
  }

  test("Type Timestamp Column: Formatter supports timezones if specified") {
    val tests = List(
      Test("uuuu-MM-dd'T'HH:mm:ss '['VV']'", "2017-10-01T23:30:00 [Africa/Kinshasa]", Timestamp.from(ZonedDateTime.of(2017, 10, 1, 22, 30, 0, 0, ZoneId.of("UTC")).toInstant)),
      Test("uuuu-MM-dd'T'HH:mm:ss '['z']'", "2017-10-01T23:30:00 [WET]", Timestamp.from(ZonedDateTime.of(2017, 10, 1, 22, 30, 0, 0, ZoneId.of("UTC")).toInstant)),
      Test("uuuu-MM-dd'T'HH:mm:ss '['zz']'", "2017-10-01T23:30:00 [WET]", Timestamp.from(ZonedDateTime.of(2017, 10, 1, 22, 30, 0, 0, ZoneId.of("UTC")).toInstant)),
      Test("uuuu-MM-dd'T'HH:mm:ss '['zzz']'", "2017-10-01T23:30:00 [WET]", Timestamp.from(ZonedDateTime.of(2017, 10, 1, 22, 30, 0, 0, ZoneId.of("UTC")).toInstant)),
      Test("uuuu-MM-dd'T'HH:mm:ss '['zzzz']'", "2017-10-01T23:30:00 [Western European Time]", Timestamp.from(ZonedDateTime.of(2017, 10, 1, 22, 30, 0, 0, ZoneId.of("UTC")).toInstant)),
      Test("uuuu-MM-dd'T'HH:mm:ss '['O']'", "2017-10-01T23:30:00 [GMT+1]", Timestamp.from(ZonedDateTime.of(2017, 10, 1, 22, 30, 0, 0, ZoneId.of("UTC")).toInstant)),
      Test("uuuu-MM-dd'T'HH:mm:ss '['OOOO']'", "2017-10-01T23:30:00 [GMT+01:00]", Timestamp.from(ZonedDateTime.of(2017, 10, 1, 22, 30, 0, 0, ZoneId.of("UTC")).toInstant)),
      Test("uuuu-MM-dd'T'HH:mm:ss '['X']'", "2017-10-01T23:30:00 [+01]", Timestamp.from(ZonedDateTime.of(2017, 10, 1, 22, 30, 0, 0, ZoneId.of("UTC")).toInstant)),
      Test("uuuu-MM-dd'T'HH:mm:ss '['XX']'", "2017-10-01T23:30:00 [+0100]", Timestamp.from(ZonedDateTime.of(2017, 10, 1, 22, 30, 0, 0, ZoneId.of("UTC")).toInstant)),
      Test("uuuu-MM-dd'T'HH:mm:ss '['XXX']'", "2017-10-01T23:30:00 [+01:00]", Timestamp.from(ZonedDateTime.of(2017, 10, 1, 22, 30, 0, 0, ZoneId.of("UTC")).toInstant)),
      Test("uuuu-MM-dd'T'HH:mm:ss '['XXXX']'", "2017-10-01T23:30:00 [+010000]", Timestamp.from(ZonedDateTime.of(2017, 10, 1, 22, 30, 0, 0, ZoneId.of("UTC")).toInstant)),
      Test("uuuu-MM-dd'T'HH:mm:ss '['XXXXX']'", "2017-10-01T23:30:00 [+01:00:00]", Timestamp.from(ZonedDateTime.of(2017, 10, 1, 22, 30, 0, 0, ZoneId.of("UTC")).toInstant)),
      Test("uuuu-MM-dd'T'HH:mm:ss '['x']'", "2017-10-01T23:30:00 [+01]", Timestamp.from(ZonedDateTime.of(2017, 10, 1, 22, 30, 0, 0, ZoneId.of("UTC")).toInstant)),
      Test("uuuu-MM-dd'T'HH:mm:ss '['xx']'", "2017-10-01T23:30:00 [+0100]", Timestamp.from(ZonedDateTime.of(2017, 10, 1, 22, 30, 0, 0, ZoneId.of("UTC")).toInstant)),
      Test("uuuu-MM-dd'T'HH:mm:ss '['xxx']'", "2017-10-01T23:30:00 [+01:00]", Timestamp.from(ZonedDateTime.of(2017, 10, 1, 22, 30, 0, 0, ZoneId.of("UTC")).toInstant)),
      Test("uuuu-MM-dd'T'HH:mm:ss '['xxxx']'", "2017-10-01T23:30:00 [+010000]", Timestamp.from(ZonedDateTime.of(2017, 10, 1, 22, 30, 0, 0, ZoneId.of("UTC")).toInstant)),
      Test("uuuu-MM-dd'T'HH:mm:ss '['xxxxx']'", "2017-10-01T23:30:00 [+01:00:00]", Timestamp.from(ZonedDateTime.of(2017, 10, 1, 22, 30, 0, 0, ZoneId.of("UTC")).toInstant)),
      Test("uuuu-MM-dd'T'HH:mm:ss '['Z']'", "2017-10-01T23:30:00 [+0100]", Timestamp.from(ZonedDateTime.of(2017, 10, 1, 22, 30, 0, 0, ZoneId.of("UTC")).toInstant)),
      Test("uuuu-MM-dd'T'HH:mm:ss '['ZZ']'", "2017-10-01T23:30:00 [+0100]", Timestamp.from(ZonedDateTime.of(2017, 10, 1, 22, 30, 0, 0, ZoneId.of("UTC")).toInstant)),
      Test("uuuu-MM-dd'T'HH:mm:ss '['ZZZ']'", "2017-10-01T23:30:00 [+0100]", Timestamp.from(ZonedDateTime.of(2017, 10, 1, 22, 30, 0, 0, ZoneId.of("UTC")).toInstant)),
      Test("uuuu-MM-dd'T'HH:mm:ss '['ZZZZ']'", "2017-10-01T23:30:00 [GMT+01:00]", Timestamp.from(ZonedDateTime.of(2017, 10, 1, 22, 30, 0, 0, ZoneId.of("UTC")).toInstant)),
      Test("uuuu-MM-dd'T'HH:mm:ss '['ZZZZZ']'", "2017-10-01T23:30:00 [+01:00:00]", Timestamp.from(ZonedDateTime.of(2017, 10, 1, 22, 30, 0, 0, ZoneId.of("UTC")).toInstant))
    )

    for (test <- tests) {
      val col = TimestampColumn(None, name="timestamp", description=None, nullable=false, nullReplacementValue=None, trim=true, nullableValues="" :: Nil, timezoneId="Australia/Sydney", formatters=List(test.fmt), None, metadata=None, strict=true, caseSensitive=false)
      Typing.typeValue(test.value, col) match {
        case (Some(res), err) => {
          assert(res === test.expected)
          assert(err === None)
        }
        case (_,Some(err)) => fail(err.toString)
        case (_,_) => assert(false)
      }
    }
  }

  test("Type Timestamp Column: nanosecond") {
    val datetimeValue = ZonedDateTime.of(2019, 5, 22, 12, 4, 0, 949903001, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("yyyy-MM-dd HH:mm:ss.SSSSSSSSS")
    val col = TimestampColumn(None, name="timestamp", description=None, nullable=false, nullReplacementValue=None, trim=true, nullableValues="" :: Nil, timezoneId="UTC", formatters=fmt, None, metadata=None, strict=false, caseSensitive=false)

    val value = "2019-05-22 12:04:00.949903001"
    Typing.typeValue(value, col) match {
      case (Some(res), err) => {
        assert(res === timestampValue)
        assert(err === None)
      }
      case (_,Some(err)) => fail(err.toString)
      case (_,_) => assert(false)
    }
  }

  test("Type Timestamp Column: case insensitive") {
    val datetimeValue = ZonedDateTime.of(2019, 5, 22, 12, 4, 0, 949903001, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("yyyy-MMM-dd HH:mm:ss.SSSSSSSSS")
    val col = TimestampColumn(None, name="timestamp", description=None, nullable=false, nullReplacementValue=None, trim=true, nullableValues="" :: Nil, timezoneId="UTC", formatters=fmt, None, metadata=None, strict=false, caseSensitive=false)

    val value = "2019-MAY-22 12:04:00.949903001"
    Typing.typeValue(value, col) match {
      case (Some(res), err) => {
        assert(res === timestampValue)
        assert(err === None)
      }
      case (_,Some(err)) => fail(err.toString)
      case (_,_) => assert(false)
    }
  }

  test("Type Timestamp Column: case sensitive") {
    val datetimeValue = ZonedDateTime.of(2019, 5, 22, 12, 4, 0, 949903001, ZoneId.of("UTC"))
    val timestampValue = Timestamp.from(datetimeValue.toInstant())
    val fmt = List("yyyy-MMM-dd HH:mm:ss.SSSSSSSSS")
    val col = TimestampColumn(None, name="timestamp", description=None, nullable=false, nullReplacementValue=None, trim=true, nullableValues="" :: Nil, timezoneId="UTC", formatters=fmt, None, metadata=None, strict=false, caseSensitive=true)

    val value = "2019-MAY-22 12:04:00.949903001"
    Typing.typeValue(value, col) match {
      case (res, Some(err)) => {
        assert(res === None)
        assert(err === TypingError.forCol(col, s"""Unable to convert '$value' to timestamp using formatters [${col.formatters.map(c => s"'${c}'").mkString(", ")}] and timezone '${col.timezoneId}'"""))
      }
      case (_,_) => assert(false)
    }
  }

}