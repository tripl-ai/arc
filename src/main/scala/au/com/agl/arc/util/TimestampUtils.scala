package au.com.agl.arc.util

import java.sql.Timestamp

import java.time.{ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter

object TimestampUtils {

  private val ISO_FORMAT_WITH_ZONE = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZZZZ")

  // used to parse cli reference date into timstamp for Spark, throw exception if not valid input
  // only valid pattern is "yyyy-MM-dd'T'HH:mm:ssZZZZ" to enforce date with time and zone input
  // to be specified externally.
  //
  // Parsed date time will be converted to UTC time zone
  //
  // we choose to not return an Option as we want to throw exceptions for now in the Spark environment
  //
  def parseReferenceTimestamp(value: String): Timestamp = {
    val inputDateTime = ZonedDateTime.parse(value, ISO_FORMAT_WITH_ZONE)
    new Timestamp(inputDateTime.toInstant().toEpochMilli())
  }

}
