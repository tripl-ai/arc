package ai.tripl.arc.config

import scala.collection.JavaConverters._

import com.typesafe.config._

import org.apache.spark.sql._

import ai.tripl.arc.api.API._
import ai.tripl.arc.config.Error._

object Plugins {

  // resolveConfigPlugins reads a list of objects at a specific path in the config and attempts to instantiate them
  def resolveConfigPlugins[T](c: Config, path: String, plugins: List[ConfigPlugin[T]])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[Error], List[T]] = {
    if (c.hasPath(path)) {

      // check valid type
      val objectList = try {
        c.getObjectList(path)
      } catch {
        case e: com.typesafe.config.ConfigException.WrongType => return Left(StageError(0, path, c.origin.lineNumber, ConfigError(path, Some(c.origin.lineNumber), s"Expected ${path} to be a List of Objects.") :: Nil) :: Nil)
        case e: Exception => return Left(StageError(0, path, c.origin.lineNumber, ConfigError(path, Some(c.origin.lineNumber), e.getMessage) :: Nil) :: Nil)
      }

      val (errors, instances) = objectList.asScala.zipWithIndex.foldLeft[(List[StageError], List[T])]( (Nil, Nil) ) { case ( (errors, instances), (plugin, index) ) =>
        import ConfigReader._
        val config = plugin.toConfig

        implicit val c = config

        val pluginType = getValue[String]("type")
        val environments = if (config.hasPath("environments")) config.getStringList("environments").asScala.toList else Nil

        // skip stage if not in environment
        if (!arcContext.ignoreEnvironments && !environments.contains(arcContext.environment.get)) {
          logger.trace()
            .field("event", "validateConfig")
            .field("type", pluginType.right.getOrElse("unknown"))
            .field("stageIndex", index)
            .field("environment", arcContext.environment.get)
            .list("environments", environments.asJava)
            .field("message", "skipping stage due to environment configuration")
            .field("skipPlugin", true)
            .log()

          (errors, instances)
        } else {
          val instanceOrError: Either[List[StageError], T] = pluginType match {
            case Left(errors) => Left(StageError(index, path, plugin.origin.lineNumber, errors) :: Nil)
            case Right(pluginType) => resolvePlugin(index, pluginType, config, plugins)
          }

          instanceOrError match {
            case Left(error) => (error ::: errors, instances)
            case Right(instance) => (errors, instance :: instances)
          }
        }
      }

      errors match {
        case Nil => Right(instances.reverse)
        case _ => Left(errors.reverse)
      }
    } else {
      Right(Nil)
    }
  }

  // resolvePlugin searches a provided list of plugins for a name/version combination
  // it then validates only a single plugin exists and if so calls the instantiate method
  def resolvePlugin[T](index: Int, name: String, config: Config, plugins: List[ConfigPlugin[T]])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[StageError], T] = {
    // match on either full class name or just the simple name AND version or not
    val splitPlugin = name.split(":", 2)
    val hasPackage = splitPlugin(0) contains "."
    val hasVersion = splitPlugin.length > 1

    val nameFilteredPlugins = if (hasPackage) {
      plugins.filter(plugin => plugin.getClass.getName == splitPlugin(0))
    } else {
      plugins.filter(plugin => plugin.getClass.getSimpleName == splitPlugin(0))
    }
    val filteredPlugins = if (hasVersion) {
      nameFilteredPlugins.filter(plugin => plugin.version == splitPlugin(1))
    } else {
      nameFilteredPlugins
    }

    val availablePluginsMessage = s"""Available plugins: ${plugins.map(c => s"${c.getClass.getName}:${c.version}").mkString("[",",","]")}."""
    val versionMessage = if (hasVersion) s"name:version" else "name"

    // return clean error messages if missing or duplicate
    if (filteredPlugins.length == 0) {
      Left(StageError(index, name, config.origin.lineNumber, ConfigError("stages", Some(config.origin.lineNumber), s"No plugins found with ${versionMessage} ${name}. ${availablePluginsMessage}") :: Nil) :: Nil)
    } else if (filteredPlugins.length > 1) {
      Left(StageError(index, name, config.origin.lineNumber, ConfigError("stages", Some(config.origin.lineNumber), s"Multiple plugins found with name ${splitPlugin(0)}. ${availablePluginsMessage}") :: Nil) :: Nil)
    } else {
      filteredPlugins.head.instantiate(index, config)
    }
  }


}