package ai.tripl.arc

import ai.tripl.arc.udf.UDF
import ai.tripl.arc.plugins.{DynamicConfigurationPlugin, LifecyclePlugin, PipelineStagePlugin, UDFPlugin}

object ARC {

  import java.util.UUID
  import java.util.ServiceLoader
  import org.apache.commons.lang3.exception.ExceptionUtils
  import scala.collection.JavaConverters._

  import org.slf4j.MDC

  import scala.annotation.tailrec
  import scala.util.Properties._

  import org.apache.spark.sql._
  import org.apache.spark.storage.StorageLevel

  import ai.tripl.arc.api.API._
  import ai.tripl.arc.util.{DetailException, Utils, ListenerUtils}
  import ai.tripl.arc.util.log.LoggerFactory

  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis()

    // read command line arguments into a map
    // must be in --key=value format
    val clArgs = collection.mutable.Map[String, String]()
    val (opts, vals) = args.partition {
      _.startsWith("--")
    }
    opts.map { x =>
      // regex split on only single = signs not at start or end of line
      val pair = x.split("=(?!=)(?!$)", 2)
      if (pair.length == 2) {
        clArgs += (pair(0).split("-{1,2}")(1) -> pair(1))
      }
    }
    val commandLineArguments = clArgs.toMap

    val jobName: Option[String] = commandLineArguments.get("etl.config.job.name").orElse(envOrNone("ETL_CONF_JOB_NAME"))
    for (j <- jobName) {
        MDC.put("jobName", j)
    }

    val jobId: Option[String] = commandLineArguments.get("etl.config.job.id").orElse(envOrNone("ETL_CONF_JOB_ID"))
    for (j <- jobId) {
        MDC.put("jobId", j)
    }

    val isStreaming = commandLineArguments.get("etl.config.streaming").orElse(envOrNone("ETL_CONF_STREAMING")) match {
      case Some(v) if v.trim.toLowerCase == "true" => true
      case Some(v) if v.trim.toLowerCase == "false" => false
      case _ => false
    }
    MDC.put("streaming", isStreaming.toString)

    val ignoreEnvironments = commandLineArguments.get("etl.config.ignoreEnvironments").orElse(envOrNone("ETL_CONF_IGNORE_ENVIRONMENTS")) match {
      case Some(v) if v.trim.toLowerCase == "true" => true
      case Some(v) if v.trim.toLowerCase == "false" => false
      case _ => false
    }
    MDC.put("ignoreEnvironments", ignoreEnvironments.toString)

    val configUri: Option[String] = commandLineArguments.get("etl.config.uri").orElse(envOrNone("ETL_CONF_URI"))

    val frameworkVersion = Utils.getFrameworkVersion

    val spark: SparkSession = try {
      SparkSession
        .builder()
        .appName(jobId.getOrElse(s"arc:${frameworkVersion}-${UUID.randomUUID.toString}"))
        .config("spark.debug.maxToStringFields", "8192")
        .config("spark.sql.orc.impl", "native") // needed to overcome structured streaming write issue
        .getOrCreate()
    } catch {
      case e: Exception =>
        val exceptionThrowables = ExceptionUtils.getThrowableList(e).asScala
        val exceptionThrowablesMessages = exceptionThrowables.map(e => e.getMessage).asJava
        val exceptionThrowablesStackTraces = exceptionThrowables.map(e => e.getStackTrace).asJava

        val detail = new java.util.HashMap[String, Object]()
        detail.put("event", "exception")
        detail.put("messages", exceptionThrowablesMessages)
        detail.put("stackTrace", exceptionThrowablesStackTraces)

        val logger = LoggerFactory.getLogger(jobId.getOrElse(s"arc:${frameworkVersion}-${UUID.randomUUID.toString}"))
        logger.error()
          .field("event", "exit")
          .field("status", "failure")
          .field("success", java.lang.Boolean.valueOf(false))
          .field("duration", System.currentTimeMillis() - startTime)
          .field("reason", detail)
          .log()

        // silently try to shut down log so that all messages are sent before stopping the spark session
        try {
          org.apache.log4j.LogManager.shutdown
        } catch {
          case e: Exception =>
        }

        // stop spark session and return fail error code
        sys.exit(1)
    }


    // add spark config to log
    val sparkConf = new java.util.HashMap[String, String]()
    spark.sparkContext.hadoopConfiguration.set("io.compression.codecs", classOf[ai.tripl.arc.util.ZipCodec].getName)

    spark.sparkContext.getConf.getAll.foreach{ case (k, v) => sparkConf.put(k, v) }

    implicit val logger = LoggerFactory.getLogger(jobId.getOrElse(spark.sparkContext.applicationId))

    // add tags
    val tags: Option[String] = commandLineArguments.get("etl.config.tags").orElse(envOrNone("ETL_CONF_TAGS"))
    for (tgs <- tags) {
      val t = tgs.split(" ")
      t.foreach { x =>
        // regex split on only single = signs not at start or end of line
        val pair = x.split("=(?!=)(?!$)", 2)
        if (pair.length == 2) {
          MDC.put(pair(0), pair(1))
        }
      }
    }

    val environment: Option[String] = commandLineArguments.get("etl.config.environment").orElse(envOrNone("ETL_CONF_ENV"))
    for (environment <- environment) {
      MDC.put("environment", environment)
    }

    val environmentId: Option[String] = commandLineArguments.get("etl.config.environment.id").orElse(envOrNone("ETL_CONF_ENV_ID"))
    for (e <- environmentId) {
        MDC.put("environmentId", e)
    }

    MDC.put("applicationId", spark.sparkContext.applicationId)

    // read storagelevel
    val (storageLevel, storageLevelName) = commandLineArguments.get("etl.config.storageLevel").orElse(envOrNone("ETL_CONF_STORAGE_LEVEL")) match {
      case Some(v) if v.trim.toUpperCase == "DISK_ONLY" => (StorageLevel.DISK_ONLY, "DISK_ONLY")
      case Some(v) if v.trim.toUpperCase == "DISK_ONLY_2" => (StorageLevel.DISK_ONLY_2, "DISK_ONLY_2")
      case Some(v) if v.trim.toUpperCase == "MEMORY_AND_DISK" => (StorageLevel.MEMORY_AND_DISK, "MEMORY_AND_DISK")
      case Some(v) if v.trim.toUpperCase == "MEMORY_AND_DISK_2" => (StorageLevel.MEMORY_AND_DISK_2, "MEMORY_AND_DISK_2")
      case Some(v) if v.trim.toUpperCase == "MEMORY_AND_DISK_SER" => (StorageLevel.MEMORY_AND_DISK_SER, "MEMORY_AND_DISK_SER")
      case Some(v) if v.trim.toUpperCase == "MEMORY_AND_DISK_SER_2" => (StorageLevel.MEMORY_AND_DISK_SER_2, "MEMORY_AND_DISK_SER_2")
      case Some(v) if v.trim.toUpperCase == "MEMORY_ONLY" => (StorageLevel.MEMORY_ONLY, "MEMORY_ONLY")
      case Some(v) if v.trim.toUpperCase == "MEMORY_ONLY_SER" => (StorageLevel.MEMORY_ONLY_SER, "MEMORY_ONLY_SER")
      case Some(v) if v.trim.toUpperCase == "MEMORY_ONLY_SER_2" => (StorageLevel.MEMORY_ONLY_SER_2, "MEMORY_ONLY_SER_2")
      case _ => (StorageLevel.MEMORY_AND_DISK_SER, "MEMORY_AND_DISK_SER")
    }

    // read immutableViews
    val immutableViews = commandLineArguments.get("etl.config.immutableViews").orElse(envOrNone("ETL_CONF_IMMUTABLE_VIEWS")) match {
      case Some(v) if v.trim.toLowerCase == "true" => true
      case Some(v) if v.trim.toLowerCase == "false" => false
      case _ => false
    }

    // log available plugins
    val loader = Utils.getContextOrSparkClassLoader

    val arcContext = ARCContext(
      jobId=jobId,
      jobName=jobName,
      environment=environment,
      environmentId=environmentId,
      configUri=configUri,
      isStreaming=isStreaming,
      ignoreEnvironments=ignoreEnvironments,
      storageLevel=storageLevel,
      immutableViews=immutableViews,
      commandLineArguments=commandLineArguments,
      dynamicConfigurationPlugins=ServiceLoader.load(classOf[DynamicConfigurationPlugin], loader).iterator().asScala.toList,
      lifecyclePlugins=ServiceLoader.load(classOf[LifecyclePlugin], loader).iterator().asScala.toList,
      activeLifecyclePlugins=Nil,
      pipelineStagePlugins=ServiceLoader.load(classOf[PipelineStagePlugin], loader).iterator().asScala.toList,
      udfPlugins=ServiceLoader.load(classOf[UDFPlugin], loader).iterator().asScala.toList,
      userData=collection.mutable.Map.empty
    )

    // add spark listeners and register udfs
    try {
      if (logger.isTraceEnabled) {
        ListenerUtils.addExecutorListener()(spark, logger)
      }
      val registeredUDFs = UDF.registerUDFs()(spark, logger, arcContext)
      logger.info()
        .field("event", "enter")
        .field("config", sparkConf)
        .field("sparkVersion", spark.version)
        .field("frameworkVersion", frameworkVersion)
        .field("scalaVersion", scala.util.Properties.versionNumberString)
        .field("javaVersion", System.getProperty("java.runtime.version"))
        .field("environment", environment.getOrElse(""))
        .field("storageLevel", storageLevelName)
        .field("immutableViews", java.lang.Boolean.valueOf(immutableViews))
        .field("dynamicConfigurationPlugins", arcContext.dynamicConfigurationPlugins.map(c => s"${c.getClass.getName}:${c.version}").asJava)
        .field("lifecyclePlugins",  arcContext.lifecyclePlugins.map(c => s"${c.getClass.getName}:${c.version}").asJava)
        .field("pipelineStagePlugins", arcContext.pipelineStagePlugins.map(c => s"${c.getClass.getName}:${c.version}").asJava)
        .field("udfPlugins", registeredUDFs)
        .log()
    } catch {
      case e: Exception =>
        val exceptionThrowables = ExceptionUtils.getThrowableList(e).asScala
        val exceptionThrowablesMessages = exceptionThrowables.map(e => e.getMessage).asJava
        val exceptionThrowablesStackTraces = exceptionThrowables.map(e => e.getStackTrace).asJava

        val detail = new java.util.HashMap[String, Object]()
        detail.put("event", "exception")
        detail.put("messages", exceptionThrowablesMessages)
        detail.put("stackTrace", exceptionThrowablesStackTraces)

        logger.error()
          .field("event", "exit")
          .field("sparkVersion", spark.version)
          .field("frameworkVersion", frameworkVersion)
          .field("scalaVersion", scala.util.Properties.versionNumberString)
          .field("javaVersion", System.getProperty("java.runtime.version"))
          .field("environment", environment.getOrElse(""))
          .field("storageLevel", storageLevelName)
          .field("immutableViews", java.lang.Boolean.valueOf(immutableViews))
          .field("dynamicConfigurationPlugins", arcContext.dynamicConfigurationPlugins.map(c => s"${c.getClass.getName}:${c.version}").asJava)
          .field("lifecyclePlugins",  arcContext.lifecyclePlugins.map(c => s"${c.getClass.getName}:${c.version}").asJava)
          .field("pipelineStagePlugins", arcContext.pipelineStagePlugins.map(c => s"${c.getClass.getName}:${c.version}").asJava)
          .field("status", "failure")
          .field("success", java.lang.Boolean.valueOf(false))
          .field("duration", System.currentTimeMillis() - startTime)
          .field("reason", detail)
          .log()

        // silently try to shut down log so that all messages are sent before stopping the spark session
        try {
          org.apache.log4j.LogManager.shutdown
        } catch {
          case e: Exception =>
        }

        // stop spark session and return fail error code
        spark.stop()
        sys.exit(1)
    }

    // try to parse config
    val pipelineConfig = try {
      ai.tripl.arc.config.ArcPipeline.parsePipeline(configUri, arcContext)(spark, logger)
    } catch {
      case e: Exception =>
        val exceptionThrowables = ExceptionUtils.getThrowableList(e).asScala
        val exceptionThrowablesMessages = exceptionThrowables.map(e => e.getMessage).asJava
        val exceptionThrowablesStackTraces = exceptionThrowables.map(e => e.getStackTrace).asJava

        val detail = new java.util.HashMap[String, Object]()
        detail.put("event", "exception")
        detail.put("messages", exceptionThrowablesMessages)
        detail.put("stackTrace", exceptionThrowablesStackTraces)

        logger.error()
          .field("event", "exit")
          .field("status", "failure")
          .field("success", java.lang.Boolean.valueOf(false))
          .field("duration", System.currentTimeMillis() - startTime)
          .field("reason", detail)
          .log()

        // silently try to shut down log so that all messages are sent before stopping the spark session
        try {
          org.apache.log4j.LogManager.shutdown
        } catch {
          case e: Exception =>
        }

        // stop spark session and return fail error code
        spark.stop()
        sys.exit(1)
    }

    val error: Boolean = pipelineConfig match {
      case Right((pipeline, ctx)) =>
        try {
          ARC.run(pipeline)(spark, logger, ctx)
          false
        } catch {
          case e: Exception with DetailException =>
            val exceptionThrowables = ExceptionUtils.getThrowableList(e).asScala
            val exceptionThrowablesMessages = exceptionThrowables.map(e => e.getMessage).asJava
            val exceptionThrowablesStackTraces = exceptionThrowables.map(e => e.getStackTrace).asJava

            e.detail.put("event", "exception")
            e.detail.put("messages", exceptionThrowablesMessages)
            e.detail.put("stackTrace", exceptionThrowablesStackTraces)

            logger.error()
              .field("event", "exit")
              .field("status", "failure")
              .field("success", java.lang.Boolean.valueOf(false))
              .field("duration", System.currentTimeMillis() - startTime)
              .map("stage", e.detail.asJava)
              .log()

            true

          case e: Exception =>
            val exceptionThrowables = ExceptionUtils.getThrowableList(e).asScala
            val exceptionThrowablesMessages = exceptionThrowables.map(e => e.getMessage).asJava
            val exceptionThrowablesStackTraces = exceptionThrowables.map(e => e.getStackTrace).asJava

            val detail = new java.util.HashMap[String, Object]()
            detail.put("event", "exception")
            detail.put("messages", exceptionThrowablesMessages)
            detail.put("stackTrace", exceptionThrowablesStackTraces)

            logger.error()
              .field("event", "exit")
              .field("status", "failure")
              .field("success", java.lang.Boolean.valueOf(false))
              .field("duration", System.currentTimeMillis() - startTime)
              .field("reason", detail)
              .log()

            true
        }

      case Left(errors) => {
        logger.error()
          .field("event", "exit")
          .field("status", "failure")
          .field("success", java.lang.Boolean.valueOf(false))
          .field("duration", System.currentTimeMillis() - startTime)
          .list("reason", ai.tripl.arc.config.Error.pipelineErrorJSON(errors))
          .log()

        println(s"ETL Config contains errors:\n${ai.tripl.arc.config.Error.pipelineErrorMsg(errors)}\n")

        true
      }
    }

    if (!arcContext.isStreaming) {
      if (!error) {
        logger.info()
          .field("event", "exit")
          .field("status", "success")
          .field("success", java.lang.Boolean.valueOf(true))
          .field("duration", System.currentTimeMillis() - startTime)
          .log()
      }

      if (error) {
        // silently try to shut down log so that all messages are sent before stopping the spark session
        try {
          org.apache.log4j.LogManager.shutdown
        } catch {
          case e: Exception =>
        }

        // stop spark session and return error code to indicate sucess/failure
        spark.stop()
        sys.exit(1)
      } else {

        // if running on a databricks cluster do not try to shutdown so that status does not always equal failure
        // databricks adds custom keys to SparkConf so if any key contains databricks then assume running on their cluster
        // if there is a better way to detect when running on databricks then please submit PR
        // https://docs.databricks.com/user-guide/jobs.html#jar-job-tips
        if (spark.sparkContext.getConf.getAll.filter { case (k,v) => k.contains("databricks") }.length == 0) {

          // silently try to shut down log so that all messages are sent before stopping the spark session
          try {
            org.apache.log4j.LogManager.shutdown
          } catch {
            case e: Exception =>
          }

          // stop spark session and return error code to indicate sucess/failure
          spark.stop()
          sys.exit(0)
        }
      }
    } else {
      try {
        spark.streams.active(0).awaitTermination
      } catch {
        case e: Exception =>
      }
    }

  }

  /** An ETL Pipeline submits each of its stages in order to Spark.
    * The submission order will match the order declared in the pipeline
    * configuration. Spark may alter the order of evaulation once it has
    * analyzed the DAG. The run method is designed to mimic a basic interpreter,
    * we new stage types are created they need to be added here in order for
    * them to be executed.
    *
    * It would be possible to extend this process to support other compute
    * engines as the submitted stages are not specific to Spark.
    */
  def run(pipeline: ETLPipeline)
  (implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    def before(currentValue: PipelineStage, index: Int, stages: List[PipelineStage]): Boolean = {
      // if any lifecyclePlugin returns false do not run remaining lifecyclePlugins and do not run the stage
      arcContext.activeLifecyclePlugins.foldLeft(true) { (state, lifeCyclePlugin) =>
        if (state) {
          logger.trace().message(s"Executing before() on LifecyclePlugin: ${lifeCyclePlugin.getClass.getName}")
          lifeCyclePlugin.before(currentValue, index, stages)
        } else {
          false
        }
      }
    }

    def after(result: Option[DataFrame], currentValue: PipelineStage, index: Int, stages: List[PipelineStage]): Unit = {
      for (p <- arcContext.activeLifecyclePlugins) {
        logger.trace().message(s"Executing after on LifecyclePlugin: ${stages(index).getClass.getName}")
        p.after(result, currentValue, index, stages)
      }
    }

    @tailrec
    def runStages(stages: List[(PipelineStage, Int)]): Option[DataFrame] = {
      stages match {
        case Nil => None // end
        case (stage, index) :: Nil =>
          val runStage = before(stage, index, pipeline.stages)
          if (runStage) {
            val result = processStage(stage)
            after(result, stage, index, pipeline.stages)
            result
          } else {
            None
          }
        case (stage, index) :: tail =>
          val runStage = before(stage, index, pipeline.stages)
          if (runStage) {
            val result = processStage(stage)
            after(result, stage, index, pipeline.stages)
            runStages(tail)
          } else {
            None
          }            
      }
    }

    runStages(pipeline.stages.zipWithIndex)
  }

  def processStage(stage: PipelineStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    val startTime = System.currentTimeMillis()

    logger.info()
      .field("event", "enter")
      .map("stage", stage.stageDetail.asJava)
      .log()

    val df = stage.execute()

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stage.stageDetail.asJava)
      .log()

    df
  }

}
