package au.com.agl.arc.util

object Utils {

  // taken from Spark src code as in private package
  def getSparkClassLoader: ClassLoader = getClass.getClassLoader

  def getContextOrSparkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

  def getFrameworkVersion: String = au.com.agl.arc.ArcBuildInfo.BuildInfo.version
}
