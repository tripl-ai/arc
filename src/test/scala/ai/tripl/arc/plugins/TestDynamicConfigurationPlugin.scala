package ai.tripl.arc.plugins
import java.util

import com.typesafe.config._

import org.apache.spark.sql.SparkSession

import ai.tripl.arc.util.log.logger.Logger
import ai.tripl.arc.api.API.ARCContext
import ai.tripl.arc.config.Error._

class TestDynamicConfigurationPlugin extends DynamicConfigurationPlugin {

  val version = "0.0.1"

  def instantiate(index: Int, config: Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[StageError], Config] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "environments" :: "key" :: Nil
    val key = getValue[String]("key")
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (key, invalidKeys) match {
      case (Right(key), Right(invalidKeys)) =>

        val values = new java.util.HashMap[String, Object]()
        values.put("arc.foo", "baz")
        values.put("arc.paramvalue", key)

        Right(ConfigFactory.parseMap(values))
      case _ =>
        val allErrors: Errors = List(key, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val err = StageError(index, this.getClass.getName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}
