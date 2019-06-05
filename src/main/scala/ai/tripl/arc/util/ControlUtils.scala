package ai.tripl.arc.util

object ControlUtils {

  def using[A <: AutoCloseable, B](param: A)(f: A => B): B =
    try {
      f(param)
    } finally {
      param.close()
    }

}
