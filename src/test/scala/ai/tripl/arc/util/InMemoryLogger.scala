package ai.tripl.arc.util

import scala.collection.mutable.StringBuilder

import org.apache.log4j.AppenderSkeleton
import org.apache.log4j.spi.LoggingEvent

class InMemoryLoggerAppender extends AppenderSkeleton {

  val builder = new StringBuilder()

  def getResult(): String = {
    builder.toString
  }

  @Override
  def append(event: LoggingEvent) {
    builder.append(s"${event.getMessage.toString}\n")
  }

  @Override
  def close() {}

  @Override
  def requiresLayout(): Boolean = false

}