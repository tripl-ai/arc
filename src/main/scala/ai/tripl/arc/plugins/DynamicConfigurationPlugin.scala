package ai.tripl.arc.plugins

import java.util.{ServiceLoader, Map => JMap}

import scala.collection.JavaConverters._

import ai.tripl.arc.util.Utils

trait DynamicConfigurationPlugin {

  def values(params: Map[String, String])(implicit logger: ai.tripl.arc.util.log.logger.Logger): JMap[String, Object]

}

object DynamicConfigurationPlugin {

  def resolveAndExecutePlugin(plugin: String, params: Map[String, String])(implicit logger: ai.tripl.arc.util.log.logger.Logger): Option[JMap[String, Object]] = {

    val loader = Utils.getContextOrSparkClassLoader
    val serviceLoader = ServiceLoader.load(classOf[DynamicConfigurationPlugin], loader)

    val plugins = for (p <- serviceLoader.iterator().asScala.toList if p.getClass.getName == plugin) yield p

    plugins.headOption match {
      case Some(p) => {
        Option(p.values(params))
      }
      case None => None
    }
  }

}
