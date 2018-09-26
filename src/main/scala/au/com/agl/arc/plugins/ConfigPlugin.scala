package au.com.agl.arc.plugins

import java.util.{ServiceLoader, Map => JMap, HashMap => JHashMap}

import scala.collection.JavaConverters._

import au.com.agl.arc.util.Utils

trait ConfigPlugin {

  def values()(implicit logger: au.com.agl.arc.util.log.logger.Logger): JMap[String, Object]

}

object ConfigPlugin {

  def pluginForName(name: String): Option[ConfigPlugin] = {

    val loader = Utils.getContextOrSparkClassLoader
    val serviceLoader = ServiceLoader.load(classOf[ConfigPlugin], loader)

    val plugins = for (p <- serviceLoader.iterator().asScala.toList if p.getClass.getName == name) yield p

    plugins.headOption
  }

}
