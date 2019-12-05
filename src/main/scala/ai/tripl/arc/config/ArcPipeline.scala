package ai.tripl.arc.config

import java.net.URI

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper

import com.typesafe.config._

import org.apache.commons.codec.digest.DigestUtils

import org.apache.spark.sql._

import ai.tripl.arc.api.API._
import ai.tripl.arc.config.ConfigUtils._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.config.Plugins._
import ai.tripl.arc.util.EitherUtils._

object ArcPipeline {

  def parsePipeline(configUri: Option[String], arcContext: ARCContext)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): Either[List[Error], (ETLPipeline, ARCContext)] = {
    arcContext.environment match {
      case Some(_) => {
        configUri match {
          case Some(uri) => parseConfig(Right(new URI(uri)), arcContext)
          case None => Left(ConfigError("file", None, s"No config defined as a command line argument --etl.config.uri or ETL_CONF_URI environment variable.") :: Nil)
        }
      }
      case None => Left(ConfigError("file", None, s"No environment defined as a command line argument --etl.config.environment or ETL_CONF_ENVIRONMENT environment variable.") :: Nil)
    }
  }

  def parseConfig(uri: Either[String, URI], arcContext: ARCContext)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): Either[List[Error], (ETLPipeline, ARCContext)] = {
    val base = ConfigFactory.load()

    val etlConfString = uri match {
      case Left(str) => Right(str)
      case Right(uri) => getConfigString(uri, arcContext)
    }

    etlConfString.rightFlatMap { etlConfRaw =>
      // convert from ipynb to config if required
      val (uriString, configString) = uri match {
        case Right(uri) => {
          if (uri.toString.endsWith(".ipynb")) {
            (uri.toString, readIPYNB(uri.toString, etlConfRaw))
          } else {
            (uri.toString, etlConfRaw)
          }
        }
        case _ => ("", etlConfRaw)
      }

      // calculate hash of raw string so that logs can be used to detect changes
      val etlConfStringHash = DigestUtils.md5Hex(etlConfRaw.getBytes)

      logger.info()
        .field("event", "validateConfig")
        .field("uri", uriString)
        .field("content-md5", etlConfStringHash)
        .log()

      val etlConf = ConfigFactory.parseString(configString, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))

      // convert to json string so that parameters can be correctly parsed
      val commandLineArgumentsJson = new ObjectMapper().writeValueAsString(arcContext.commandLineArguments.asJava).replace("\\", "")
      val commandLineArgumentsConf = ConfigFactory.parseString(commandLineArgumentsJson, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))

      // try to read objects in the plugins.config path. these must resolve before trying to read anything else
      val dynamicConfigsOrErrors = resolveConfigPlugins(etlConf, "plugins.config", arcContext.dynamicConfigurationPlugins)(spark, logger, arcContext)
      dynamicConfigsOrErrors match {
        case Left(errors) => Left(errors)
        case Right(dynamicConfigs) => {

          val resolvedConfig = dynamicConfigs match {
            case Nil =>
              etlConf.resolveWith(commandLineArgumentsConf.withFallback(etlConf).withFallback(base)).resolve()
            case _ =>
              val dynamicConfigsConf = dynamicConfigs.reduceRight[Config]{ case (c1, c2) => c1.withFallback(c2) }
              etlConf.resolveWith(commandLineArgumentsConf.withFallback(dynamicConfigsConf).withFallback(etlConf).withFallback(base)).resolve()
          }

          // use resolved config to parse other plugins
          val lifecyclePluginsOrErrors = resolveConfigPlugins(resolvedConfig, "plugins.lifecycle", arcContext.lifecyclePlugins)(spark, logger, arcContext)

          if (!resolvedConfig.hasPath("stages")) {
            throw new Exception(s"""Key 'stages' missing from job configuration. Have keys: [${resolvedConfig.entrySet().asScala.map(_.getKey).toList.mkString(",")}].""")
          }
          val pipelinePluginsOrErrors = resolveConfigPlugins(resolvedConfig, "stages", arcContext.pipelineStagePlugins)(spark, logger, arcContext)

          (lifecyclePluginsOrErrors, pipelinePluginsOrErrors) match {
            case (Left(lifecycleErrors), Left(pipelineErrors)) => Left(lifecycleErrors.reverse ::: pipelineErrors.reverse)
            case (Right(_), Left(pipelineErrors)) => Left(pipelineErrors.reverse)
            case (Left(lifecycleErrors), Right(_)) => Left(lifecycleErrors.reverse)
            case (Right(lifecycleInstances), Right(pipelineInstances)) => {

              // flatten any PipelineExecuteStage stages and their LifecylePlugins
              val stages: List[(List[PipelineStage], List[LifecyclePluginInstance])] = pipelineInstances.map {
                pipelineStage => {
                  pipelineStage match {
                    case ai.tripl.arc.execute.PipelineExecuteStage(_, _, _, _, pipeline, pipelineLifecycleInstances) => (pipeline.stages, pipelineLifecycleInstances)
                    case pipelineStage: PipelineStage => (List(pipelineStage), List.empty)
                  }
                }
              }

              // flatPipelineInstances is a merge of all the pipeline plugins
              val flatPipelineInstances = stages.flatMap{
                case (pipelineStage, _) => pipelineStage
              }

              // activeLifecyclePluginInstances is a merge of all the lifecycle plugins
              val activeLifecyclePluginInstances = stages.foldLeft(lifecycleInstances) { 
                case (lifecycleInstances, (_, pipelineLifecycleInstances)) => lifecycleInstances ::: pipelineLifecycleInstances
              }

              // used the resolved config to add registered lifecyclePlugins to context
              val ctx = ARCContext(
                jobId=arcContext.jobId,
                jobName=arcContext.jobName,
                environment=arcContext.environment,
                environmentId=arcContext.environmentId,
                configUri=arcContext.configUri,
                isStreaming=arcContext.isStreaming,
                ignoreEnvironments=arcContext.ignoreEnvironments,
                storageLevel=arcContext.storageLevel,
                immutableViews=arcContext.immutableViews,
                commandLineArguments=arcContext.commandLineArguments,
                dynamicConfigurationPlugins=arcContext.dynamicConfigurationPlugins,
                lifecyclePlugins=arcContext.lifecyclePlugins,
                activeLifecyclePlugins=activeLifecyclePluginInstances,
                pipelineStagePlugins=arcContext.pipelineStagePlugins,
                udfPlugins=arcContext.udfPlugins,
                userData=arcContext.userData
              )

              Right((ETLPipeline(flatPipelineInstances), ctx))
            }
          }
        }
      }
    }
  }

}
