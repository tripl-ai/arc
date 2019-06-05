package ai.tripl.arc.util

import java.net.URI
import java.time.Instant

import org.apache.spark.sql._

object LoadUtils {

  sealed trait LoadTableNameStrategy
  object LoadTableNameStrategy {
    case object Epoch extends LoadTableNameStrategy
  }

  // TODO move the date time to the context of the job eg time started
  def loadTableName(baseTableName: String, strategy: LoadTableNameStrategy = LoadTableNameStrategy.Epoch): String = {
    strategy match {
      case LoadTableNameStrategy.Epoch => {
        val epoch = Instant.now.getEpochSecond()
        s"${baseTableName}_${epoch}"
      }
    }
  }
}
