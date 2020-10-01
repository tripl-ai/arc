package ai.tripl.arc.config

import scala.collection.JavaConverters._
import scala.util.Try

import com.typesafe.config._
import com.typesafe.config.ConfigException

import org.apache.spark.sql._

import ai.tripl.arc.api.API._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.config.ConfigUtils.parseResolution
import ai.tripl.arc.util.EitherUtils._

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
        implicit var c = config

        // resolve the minimal keys
        c = config.withOnlyPath("type").resolveWith(arcContext.resolutionConfig).resolve()
        val pluginType = getValue[String]("type")
        c = config.withOnlyPath("environments").resolveWith(arcContext.resolutionConfig).resolve()
        val environments = if (c.hasPath("environments")) c.getStringList("environments").asScala.toList else Nil
        // lazy evaluate name
        val name = Try {
          c = config.withOnlyPath("name").resolveWith(arcContext.resolutionConfig).resolve()
          getValue[String]("name")
        }.getOrElse(Right(""))

        // read the resolution key
        c = config.withOnlyPath("resolution").resolveWith(arcContext.resolutionConfig).resolve()
        val resolution = getValue[String]("resolution", default = Some("strict"), validValues = "strict" :: "lazy" :: Nil) |> parseResolution("resolution") _
        c = config.withoutPath("resolution")

        // skip stage if not in environment
        if (!arcContext.ignoreEnvironments && !environments.contains(arcContext.environment.get)) {
          logger.info()
            .field("event", "validateConfig")
            .field("type", pluginType.right.getOrElse("unknown"))
            .field("pluginIndex", index)
            .field("environment", arcContext.environment.get)
            .list("environments", environments.asJava)
            .field("message", "skipping plugin due to environment configuration")
            .field("skipPlugin", true)
            .log()

          (errors, instances)
        } else {
          val instanceOrError: Either[List[StageError], T] = (resolution, pluginType, name) match {
            case (Right(resolution), Right(pluginType), Right(name)) => {
              if (resolution == Resolution.Lazy) {
                // defer resolution of the config
                resolvePlugin(false, index, "ai.tripl.arc.plugins.pipeline.LazyEvaluator", c, plugins) match {
                  case Left(err) => Left(err)
                  case Right(plugin) => {
                    plugin match {
                      case stage: PipelineStage => {
                        resolvePluginName(index, pluginType, c, plugins) match {
                          case Left(err) => Left(err)
                          case Right(p) => {
                            val pluginMap = new java.util.HashMap[String, Object]()
                            pluginMap.put("type", s"${p.getClass.getName}:${p.version}")
                            stage.stageDetail.put("plugin", pluginMap)
                            stage.stageDetail.put("name", name)
                          }
                        }

                      }
                      case _ =>
                    }
                    Right(plugin)
                  }
                }
              } else {
                // resolve the config immediately
                try {
                  c = config.resolveWith(arcContext.resolutionConfig).resolve()
                  resolvePlugin(path.contains("plugins."), index, pluginType, c, plugins)
                } catch {
                  case e: ConfigException.UnresolvedSubstitution => {
                    Left(StageError(index, name, config.origin.lineNumber, ConfigError("stages", Some(config.origin.lineNumber), e.getMessage()) :: Nil) :: Nil)
                  }
                }

              }
            }
            case _ =>
              val allErrors: Errors = List(resolution, pluginType, name).collect{ case Left(errs) => errs }.flatten
              val err = StageError(index, path, c.origin.lineNumber, allErrors)
              Left(err :: Nil)
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

  def resolvePluginName[T](index: Int, name: String, config: Config, plugins: List[ConfigPlugin[T]])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[StageError], ConfigPlugin[T]] = {
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

    // logging messages
    val availablePluginsMessage = s"""Available plugins: ${plugins.map(c => s"${c.getClass.getName}:${c.version}").mkString("[",",","]")}."""
    val versionMessage = if (hasVersion) s"name:version" else "name"

    // return clean error messages if missing or duplicate
    if (filteredPlugins.length == 0) {
      Left(StageError(index, name, config.origin.lineNumber, ConfigError("stages", Some(config.origin.lineNumber), s"No plugins found with ${versionMessage} ${name}. ${availablePluginsMessage}") :: Nil) :: Nil)
    } else if (filteredPlugins.length > 1) {
      Left(StageError(index, name, config.origin.lineNumber, ConfigError("stages", Some(config.origin.lineNumber), s"Multiple plugins found with name ${splitPlugin(0)}. ${availablePluginsMessage}") :: Nil) :: Nil)
    } else {
      Right(filteredPlugins.head)
    }
  }

  // resolvePlugin searches a provided list of plugins for a name/version combination
  // it then validates only a single plugin exists and if so calls the instantiate method
  def resolvePlugin[T](log: Boolean, index: Int, name: String, config: Config, plugins: List[ConfigPlugin[T]])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[StageError], T] = {
    resolvePluginName(index, name, config, plugins) match {
      case Left(err) => Left(err)
      case Right(plugin) => plugin.instantiate(index, config)
    }
  }

}