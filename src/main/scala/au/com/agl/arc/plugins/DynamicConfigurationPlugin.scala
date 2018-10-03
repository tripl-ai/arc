package au.com.agl.arc.plugins

import java.util.{ServiceLoader, Map => JMap, HashMap => JHashMap}

import scala.collection.JavaConverters._

import au.com.agl.arc.util.Utils

trait DynamicConfigurationPlugin {

  def values()(implicit logger: au.com.agl.arc.util.log.logger.Logger): JMap[String, Object]

}

object DynamicConfigurationPlugin {

  def pluginForName(name: String): Option[DynamicConfigurationPlugin] = {

    val loader = Utils.getContextOrSparkClassLoader
    val serviceLoader = ServiceLoader.load(classOf[DynamicConfigurationPlugin], loader)

    val plugins = for (p <- serviceLoader.iterator().asScala.toList if p.getClass.getName == name) yield p

    plugins.headOption
  }

}
