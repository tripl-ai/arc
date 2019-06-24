package ai.tripl.arc.util

trait DetailException { self: Throwable =>
  def detail: collection.mutable.Map[String, Object] 
}