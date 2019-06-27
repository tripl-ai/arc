package ai.tripl.arc.plugins

import java.util.{ServiceLoader, Map => JMap}

import scala.collection.JavaConverters._

import com.typesafe.config._

import org.apache.spark.sql.SparkSession

import ai.tripl.arc.api.API.{ARCContext, ConfigPlugin}
import ai.tripl.arc.util.Utils

trait DynamicConfigurationPlugin extends ConfigPlugin[Config] {

}