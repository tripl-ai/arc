package ai.tripl.arc.config

import java.net.URI

import scala.collection.JavaConverters._
import scala.util.Properties._

import com.fasterxml.jackson.databind.ObjectMapper

import com.typesafe.config._

import org.apache.commons.codec.digest.DigestUtils

import org.apache.spark.sql._

import ai.tripl.arc.api.API._
import ai.tripl.arc.config.ConfigUtils._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.config.Plugins._
import ai.tripl.arc.execute.PipelineExecuteStage
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.SerializableConfiguration

object ArcPipeline {

  def parsePipeline(configUri: Option[String], arcContext: ARCContext)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): Either[List[Error], (ETLPipeline, ARCContext)] = {
    configUri match {
      case Some(uri) =>
        arcContext.environment match {
          case Some(_) => parseConfig(Right(new URI(uri)), arcContext)
          case None => Left(ConfigError("file", None, s"No environment defined as a command line argument --etl.config.environment or ETL_CONF_ENV environment variable.") :: Nil)
        }
      case None => Left(ConfigError("file", None, s"No config defined as a command line argument --etl.config.uri or ETL_CONF_URI environment variable.") :: Nil)
    }
  }

  def parseConfig(uri: Either[String, URI], arcContext: ARCContext)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): Either[List[Error], (ETLPipeline, ARCContext)] = {
    val etlConfString = uri match {
      case Left(str) => Right(str)
      case Right(uri) => getConfigString(uri)(spark, logger, arcContext)
    }

    etlConfString.flatMap { etlConfRaw =>
      // convert from ipynb to config if required
      val (uriString, configString) = uri match {
        case Right(uri) => {
          if (uri.toString.endsWith(".ipynb")) {
            if (!arcContext.ipynb) {
              throw new Exception(s"Support for IPython Notebook Configuration Files (.ipynb) for configuration '${uri.toString}' has been disabled by policy.")
            }
            (uri.toString, readIPYNB(Some(uri.toString), etlConfRaw))
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
          // calculate the variables used for resolution
          arcContext.resolutionConfig = dynamicConfigs match {
            case Nil => commandLineArgumentsConf.withFallback(etlConf).withFallback(arcContext.resolutionConfig)
            case _ =>
              val dynamicConfigsConf = dynamicConfigs.reduceRight[Config]{ case (c1, c2) => c1.withFallback(c2) }
              commandLineArgumentsConf.withFallback(dynamicConfigsConf).withFallback(etlConf).withFallback(arcContext.resolutionConfig)
          }

          // use resolved config to parse other plugins
          val lifecyclePluginsOrErrors = resolveConfigPlugins(etlConf, "plugins.lifecycle", arcContext.lifecyclePlugins)(spark, logger, arcContext)

          if (!etlConf.hasPath("stages")) {
            throw new Exception(s"""Key 'stages' missing from job configuration. Have keys: [${etlConf.entrySet().asScala.map(_.getKey).toList.mkString(",")}].""")
          }
          val pipelinePluginsOrErrors = resolveConfigPlugins(etlConf, "stages", arcContext.pipelineStagePlugins)(spark, logger, arcContext)

          (lifecyclePluginsOrErrors, pipelinePluginsOrErrors) match {
            case (Left(lifecycleErrors), Left(pipelineErrors)) => Left(lifecycleErrors.reverse ::: pipelineErrors.reverse)
            case (Right(_), Left(pipelineErrors)) => Left(pipelineErrors.reverse)
            case (Left(lifecycleErrors), Right(_)) => Left(lifecycleErrors.reverse)
            case (Right(lifecycleInstances), Right(pipelineInstances)) => {

              // flatten any PipelineExecuteStage stages and their LifecylePlugins
              // flatPipelineInstances is a merge of all the pipeline plugins
              // activeLifecyclePluginInstances is a merge of all the lifecycle plugins
              val (flatPipelineInstances, activeLifecyclePluginInstances) = pipelineInstances.map {
                case PipelineExecuteStage(_, _, _, _, _, pipeline, pipelineLifecycleInstances) => (pipeline.stages, pipelineLifecycleInstances)
                case pipelineStage: PipelineStage => (List(pipelineStage), Nil)
              }.unzip match {
                case (stages, plugins) => (stages.flatten, lifecycleInstances ::: plugins.flatten)
              }

              // used the resolved config to add registered lifecyclePlugins to context
              val ctx = ARCContext(
                jobId=arcContext.jobId,
                jobName=arcContext.jobName,
                environment=arcContext.environment,
                configUri=arcContext.configUri,
                isStreaming=arcContext.isStreaming,
                ignoreEnvironments=arcContext.ignoreEnvironments,
                storageLevel=arcContext.storageLevel,
                immutableViews=arcContext.immutableViews,
                ipynb=arcContext.ipynb,
                inlineSQL=arcContext.inlineSQL,
                inlineSchema=arcContext.inlineSchema,
                dropUnsupported=arcContext.dropUnsupported,
                commandLineArguments=arcContext.commandLineArguments,
                dynamicConfigurationPlugins=arcContext.dynamicConfigurationPlugins,
                lifecyclePlugins=arcContext.lifecyclePlugins,
                activeLifecyclePlugins=activeLifecyclePluginInstances,
                pipelineStagePlugins=arcContext.pipelineStagePlugins,
                udfPlugins=arcContext.udfPlugins,
                serializableConfiguration=new SerializableConfiguration(spark.sparkContext.hadoopConfiguration),
                userData=arcContext.userData,
                resolutionConfig=arcContext.resolutionConfig,
              )

              Right((ETLPipeline(flatPipelineInstances), ctx))
            }
          }
        }
      }
    }
  }

}
