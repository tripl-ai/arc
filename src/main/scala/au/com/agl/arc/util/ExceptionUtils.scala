package au.com.agl.arc.util

import java.util.HashMap

trait DetailException { self: Throwable =>
  def detail: HashMap[String, Object]
}