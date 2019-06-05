package ai.tripl.arc.util

import java.util.HashMap

trait DetailException { self: Throwable =>
  def detail: HashMap[String, Object]
}