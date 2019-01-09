package au.com.agl.arc.util

import au.com.agl.arc.BuildInfo

object Utils {

  // taken from Spark src code as in private package
  def getSparkClassLoader: ClassLoader = getClass.getClassLoader

  def getContextOrSparkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

  def getFrameworkVersion: String = BuildInfo.version
}
