package au.com.agl.arc.plugins
import java.util

import au.com.agl.arc.util.log.logger.Logger

class DynamicConfigurationPluginTest extends DynamicConfigurationPlugin {

  override def values(params: Map[String, String])(implicit logger: Logger): util.Map[String, Object] = {

    logger.info()
      .field("event", "ensure logging object is accessible")
      .log()   

    val values = new java.util.HashMap[String, Object]()
    values.put("arc.foo", "baz")

    params.get("key") match {
      case Some(v) => values.put("arc.paramvalue", v)
      case None =>
    }
    
    values
  }

}
