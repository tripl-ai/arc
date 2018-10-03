package au.com.agl.arc.plugins
import java.util

import au.com.agl.arc.util.log.logger.Logger

class DynamicConfigurationPluginTest extends DynamicConfigurationPlugin {
  override def values()(implicit logger: Logger): util.Map[String, Object] = {
    val values = new java.util.HashMap[String, Object]()
    values.put("arc.foo", "baz")
    values
  }
}
