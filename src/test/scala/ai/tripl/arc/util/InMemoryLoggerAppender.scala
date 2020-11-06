package ai.tripl.arc.util

import org.apache.log4j.AppenderSkeleton
import org.apache.log4j.spi.LoggingEvent

class InMemoryLoggerAppender extends AppenderSkeleton {

  var log = new scala.collection.mutable.ListBuffer[String]()

  def getResult(): List[String] = {
    log.toList
  }

  @Override
  def append(event: LoggingEvent) {
    log += event.getMessage.toString
  }

  @Override
  def close() {}

  @Override
  def requiresLayout(): Boolean = false

}