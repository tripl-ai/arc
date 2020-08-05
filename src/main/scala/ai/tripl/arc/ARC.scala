package ai.tripl.arc

import ai.tripl.arc.udf.UDF
import ai.tripl.arc.plugins.{DynamicConfigurationPlugin, LifecyclePlugin, PipelineStagePlugin, UDFPlugin}
import ai.tripl.arc.util.CloudUtils
import ai.tripl.arc.util.SerializableConfiguration

object ARC {

  import java.util.UUID
  import java.util.ServiceLoader
  import scala.collection.JavaConverters._
  import scala.util.Try

  import org.apache.commons.lang3.exception.ExceptionUtils

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
    val (opts, _) = args.partition {
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

    // configurations
    val isStreaming = Try(commandLineArguments.get("etl.config.streaming").orElse(envOrNone("ETL_CONF_STREAMING")).get.toBoolean).getOrElse(false)
    val ignoreEnvironments = Try(commandLineArguments.get("etl.config.ignoreEnvironments").orElse(envOrNone("ETL_CONF_IGNORE_ENVIRONMENTS")).get.toBoolean).getOrElse(false)
    val enableStackTrace = Try(commandLineArguments.get("etl.config.enableStackTrace").orElse(envOrNone("ETL_CONF_ENABLE_STACKTRACE")).get.toBoolean).getOrElse(false)
    val immutableViews = Try(commandLineArguments.get("etl.config.immutableViews").orElse(envOrNone("ETL_CONF_IMMUTABLE_VIEWS")).get.toBoolean).getOrElse(false)
    val configUri: Option[String] = commandLineArguments.get("etl.config.uri").orElse(envOrNone("ETL_CONF_URI"))
    val jobId: Option[String] = commandLineArguments.get("etl.config.job.id").orElse(envOrNone("ETL_CONF_JOB_ID"))
    val jobName: Option[String] = commandLineArguments.get("etl.config.job.name").orElse(envOrNone("ETL_CONF_JOB_NAME"))
    val environment: Option[String] = commandLineArguments.get("etl.config.environment").orElse(envOrNone("ETL_CONF_ENV"))
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

    // policies
    val policyInlineSchema = Try(commandLineArguments.get("etl.policy.inline.schema").orElse(envOrNone("ETL_POLICY_INLINE_SCHEMA")).get.toBoolean).getOrElse(true)
    val policyInlineSQL = Try(commandLineArguments.get("etl.policy.inline.sql").orElse(envOrNone("ETL_POLICY_INLINE_SQL")).get.toBoolean).getOrElse(true)
    val policyIPYNB = Try(commandLineArguments.get("etl.policy.ipynb").orElse(envOrNone("ETL_POLICY_IPYNB")).get.toBoolean).getOrElse(true)

    // set global logging
    MDC.put("streaming", isStreaming.toString)
    MDC.put("ignoreEnvironments", ignoreEnvironments.toString)
    MDC.put("enableStackTrace", enableStackTrace.toString)
    jobId.foreach { j => MDC.put("jobId", j) }
    jobName.foreach { jn => MDC.put("jobName", jn) }
    environment.foreach { e => MDC.put("environment", e) }

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

        val detail = new java.util.HashMap[String, Object]()
        detail.put("event", "exception")
        detail.put("messages", exceptionThrowablesMessages)

        if (enableStackTrace) {
          detail.put("stackTrace", exceptionThrowables.map(e => e.getStackTrace).asJava)
        }

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

    // only set default aws provider override if not provided
    if (Option(spark.sparkContext.hadoopConfiguration.get("fs.s3a.aws.credentials.provider")).isEmpty) {
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", CloudUtils.defaultAWSProvidersOverride)
    }

    // add spark config to log
    val sparkConfLog = new java.util.HashMap[String, String]()
    spark.sparkContext.getConf.getAll
    .filter { case (k, _) => !("spark.authenticate.secret").contains(k) }
    .foreach { case (k, v) => sparkConfLog.put(k, v) }

    implicit val logger = LoggerFactory.getLogger(jobId.getOrElse(spark.sparkContext.applicationId))
    MDC.put("applicationId", spark.sparkContext.applicationId)

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

    // log available plugins
    val loader = Utils.getContextOrSparkClassLoader

    val arcContext = ARCContext(
      jobId=jobId,
      jobName=jobName,
      environment=environment,
      configUri=configUri,
      isStreaming=isStreaming,
      ignoreEnvironments=ignoreEnvironments,
      storageLevel=storageLevel,
      immutableViews=immutableViews,
      ipynb=policyIPYNB,
      inlineSQL=policyInlineSQL,
      inlineSchema=policyInlineSchema,
      commandLineArguments=commandLineArguments,
      dynamicConfigurationPlugins=ServiceLoader.load(classOf[DynamicConfigurationPlugin], loader).iterator().asScala.toList,
      lifecyclePlugins=ServiceLoader.load(classOf[LifecyclePlugin], loader).iterator().asScala.toList,
      activeLifecyclePlugins=Nil,
      pipelineStagePlugins=ServiceLoader.load(classOf[PipelineStagePlugin], loader).iterator().asScala.toList,
      udfPlugins=ServiceLoader.load(classOf[UDFPlugin], loader).iterator().asScala.toList,
      serializableConfiguration=new SerializableConfiguration(spark.sparkContext.hadoopConfiguration),
      userData=collection.mutable.Map[String, Object]()
    )

    // add spark listeners and register udfs
    try {
      if (logger.isTraceEnabled) {
        ListenerUtils.addExecutorListener()(spark, logger)
      }
      val registeredUDFs = UDF.registerUDFs()(spark, logger, arcContext)
      logger.info()
        .field("event", "enter")
        .field("config", sparkConfLog)
        .field("sparkVersion", spark.version)
        .field("arcVersion", frameworkVersion)
        .field("hadoopVersion", org.apache.hadoop.util.VersionInfo.getVersion)
        .field("scalaVersion", scala.util.Properties.versionNumberString)
        .field("javaVersion", System.getProperty("java.runtime.version"))
        .field("environment", environment.getOrElse(""))
        .field("storageLevel", storageLevelName)
        .field("immutableViews", java.lang.Boolean.valueOf(arcContext.immutableViews))
        .field("policyIPYNB", java.lang.Boolean.valueOf(arcContext.ipynb))
        .field("policyInlineSQL", java.lang.Boolean.valueOf(arcContext.inlineSQL))
        .field("policyInlineSchema", java.lang.Boolean.valueOf(arcContext.inlineSchema))
        .field("dynamicConfigurationPlugins", arcContext.dynamicConfigurationPlugins.map(c => s"${c.getClass.getName}:${c.version}").asJava)
        .field("lifecyclePlugins",  arcContext.lifecyclePlugins.map(c => s"${c.getClass.getName}:${c.version}").asJava)
        .field("pipelineStagePlugins", arcContext.pipelineStagePlugins.map(c => s"${c.getClass.getName}:${c.version}").asJava)
        .field("udfPlugins", registeredUDFs)
        .log()
    } catch {
      case e: Exception =>
        val exceptionThrowables = ExceptionUtils.getThrowableList(e).asScala
        val exceptionThrowablesMessages = exceptionThrowables.map(e => e.getMessage).asJava

        val detail = new java.util.HashMap[String, Object]()
        detail.put("event", "exception")
        detail.put("messages", exceptionThrowablesMessages)

        if (enableStackTrace) {
          detail.put("stackTrace", exceptionThrowables.map(e => e.getStackTrace).asJava)
        }

        logger.error()
          .field("event", "exit")
          .field("sparkVersion", spark.version)
          .field("arcVersion", frameworkVersion)
          .field("hadoopVersion", org.apache.hadoop.util.VersionInfo.getVersion)
          .field("scalaVersion", scala.util.Properties.versionNumberString)
          .field("javaVersion", System.getProperty("java.runtime.version"))
          .field("environment", environment.getOrElse(""))
          .field("storageLevel", storageLevelName)
          .field("immutableViews", java.lang.Boolean.valueOf(arcContext.immutableViews))
          .field("ipynb", java.lang.Boolean.valueOf(arcContext.ipynb))
          .field("inlineSQL", java.lang.Boolean.valueOf(arcContext.inlineSQL))
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

        val detail = new java.util.HashMap[String, Object]()
        detail.put("event", "exception")
        detail.put("messages", exceptionThrowablesMessages)

        if (enableStackTrace) {
          detail.put("stackTrace", exceptionThrowables.map(e => e.getStackTrace).asJava)
        }

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

            e.detail.put("event", "exception")
            e.detail.put("messages", exceptionThrowablesMessages)

            if (enableStackTrace) {
              e.detail.put("stackTrace", exceptionThrowables.map(e => e.getStackTrace).asJava)
            }

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

            val detail = new java.util.HashMap[String, Object]()
            detail.put("event", "exception")
            detail.put("messages", exceptionThrowablesMessages)

            if (enableStackTrace) {
              detail.put("stackTrace", exceptionThrowables.map(e => e.getStackTrace).asJava)
            }

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

        // if running on local master (not databricks cluster or yarn) try to shutdown so that status is returned to the console correctly
        // databricks will fail if spark.stop is called: see https://docs.databricks.com/user-guide/jobs.html#jar-job-tips
        val isLocalMaster = spark.sparkContext.master.toLowerCase.startsWith("local")
        if (isLocalMaster) {
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

    def runStage(currentValue: PipelineStage, index: Int, stages: List[PipelineStage]): Boolean = {
      // if any lifecyclePlugin returns false do not run remaining lifecyclePlugins and do not run the stage
      arcContext.activeLifecyclePlugins.foldLeft(true) { (state, lifeCyclePlugin) =>
        if (state) {
          logger.trace().message(s"Executing runStage() on LifecyclePlugin: ${lifeCyclePlugin.getClass.getName}")
          lifeCyclePlugin.runStage(currentValue, index, stages)
        } else {
          false
        }
      }
    }

    def before(currentValue: PipelineStage, index: Int, stages: List[PipelineStage]): Unit = {
      for (lifeCyclePlugin <- arcContext.activeLifecyclePlugins) {
        logger.trace().message(s"Executing after on LifecyclePlugin: ${stages(index).getClass.getName}")
        lifeCyclePlugin.before(currentValue, index, stages)
      }
    }

    def after(result: Option[DataFrame], currentValue: PipelineStage, index: Int, stages: List[PipelineStage]): Option[DataFrame] = {
      // if any lifecyclePlugin returns a modified dataframe use that for the remaining lifecyclePlugins
      // unfortuately this means that lifecyclePlugin order is important but is required for this operation
      arcContext.activeLifecyclePlugins.foldLeft(result) { (mutatedResult, lifeCyclePlugin) =>
        logger.trace().message(s"Executing after on LifecyclePlugin: ${stages(index).getClass.getName}")
        lifeCyclePlugin.after(mutatedResult, currentValue, index, stages)
      }
    }

    // runStage will not execute the stage nor before/after if ANY of the LifecyclePlugins.runStage methods return false
    // this is to simplify the job of workflow designers trying to ensure plugins are ordered correctly
    @tailrec
    def runStages(stages: List[(PipelineStage, Int)])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
      stages match {
        case Nil => None // end
        case (stage, index) :: Nil =>
          if (runStage(stage, index, pipeline.stages)) processStage(stage, index) else None
        case (stage, index) :: tail =>
          if (runStage(stage, index, pipeline.stages)) {
            processStage(stage, index)
            runStages(tail)
          } else {
            None
          }
      }
    }

    def processStage(stage: PipelineStage, index: Int)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
      // allow enriching the stageDetail by before
      before(stage, index, pipeline.stages)

      logger.info()
        .field("event", "enter")
        .map("stage", stage.stageDetail.asJava)
        .log()

      val startTime = System.currentTimeMillis()
      val result = stage.execute()
      val endTime = System.currentTimeMillis()

      // allow enriching the stageDetail by after
      val afterResult = after(result, stage, index, pipeline.stages)

      logger.info()
        .field("event", "exit")
        .field("duration", endTime - startTime)
        .map("stage", stage.stageDetail.asJava)
        .log()

      afterResult
    }

    runStages(pipeline.stages.zipWithIndex)
  }

}
