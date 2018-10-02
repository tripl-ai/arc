package au.com.agl.arc.plugins

import au.com.agl.arc.util.log.logger.Logger
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SQLContext

class UDFPluginTest extends UDFPlugin {

  override def register(sqlContext: SQLContext)(implicit logger: Logger): Seq[String] = {
    sqlContext.udf.register("str_reverse", (s: String) => UDFPluginTest.strReverse(s))

    "str_reverse" :: Nil
  }

}

object UDFPluginTest {

  def strReverse(str: String): String = {
    StringUtils.reverse(str)
  }


}

