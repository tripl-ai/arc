package ai.tripl.arc

import ai.tripl.arc.udf.UDF
import ai.tripl.arc.plugins.{DynamicConfigurationPlugin, LifecyclePlugin, PipelineStagePlugin, UDFPlugin}

object ARC {

  import java.lang._
  import java.util.UUID
  import java.util.ServiceLoader
  import org.apache.commons.lang3.exception.ExceptionUtils
  import scala.collection.JavaConverters._

  import org.slf4j.MDC

  import scala.annotation.tailrec
  import scala.util.Properties._

  import org.apache.spark.sql._
  import org.apache.spark.sql.types._

  import ai.tripl.arc.api.API._
  import ai.tripl.arc.util._
  import ai.tripl.arc.util.log.LoggerFactory 

  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis()

    // read command line arguments into a map
    // must be in --key=value format
    val argsMap = collection.mutable.Map[String, String]()
    val (opts, vals) = args.partition {
      _.startsWith("--")
    }
    opts.map { x =>
      // regex split on only single = signs not at start or end of line
      val pair = x.split("=(?!=)(?!$)", 2)
      if (pair.length == 2) {
        argsMap += (pair(0).split("-{1,2}")(1) -> pair(1))
      }
    }

    val jobName: Option[String] = argsMap.get("etl.config.job.name").orElse(envOrNone("ETL_CONF_JOB_NAME"))
    for (j <- jobName) {
        MDC.put("jobName", j) 
    }    

    val jobId: Option[String] = argsMap.get("etl.config.job.id").orElse(envOrNone("ETL_CONF_JOB_ID"))
    for (j <- jobId) {
        MDC.put("jobId", j) 
    }    

    val streaming: Option[String] = argsMap.get("etl.config.streaming").orElse(envOrNone("ETL_CONF_STREAMING"))
    val isStreaming = streaming match {
      case Some(v) if v.trim.toLowerCase == "true" => true
      case Some(v) if v.trim.toLowerCase == "false" => false
      case _ => false
    }
    MDC.put("streaming", isStreaming.toString) 

    val ignoreEnv: Option[String] = argsMap.get("etl.config.ignoreEnvironments").orElse(envOrNone("ETL_CONF_IGNORE_ENVIRONMENTS"))
    val ignoreEnvironments = ignoreEnv match {
      case Some(v) if v.trim.toLowerCase == "true" => true
      case Some(v) if v.trim.toLowerCase == "false" => false
      case _ => false
    }
    MDC.put("ignoreEnvironments", ignoreEnvironments.toString)     

    val disableDepVal: Option[String] = argsMap.get("etl.config.disableDependencyValidation").orElse(envOrNone("ETL_CONF_DISABLE_DEPENDENCY_VALIDATION"))
    val disableDependencyValidation = disableDepVal match {
      case Some(v) if v.trim.toLowerCase == "true" => true
      case Some(v) if v.trim.toLowerCase == "false" => false
      case _ => false
    }
    MDC.put("ignoreEnvironments", ignoreEnvironments.toString)         

    val configUri: Option[String] = argsMap.get("etl.config.uri").orElse(envOrNone("ETL_CONF_URI"))    

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

        var detail = new java.util.HashMap[String, Object]()
        detail.put("event", "exception")
        detail.put("messages", exceptionThrowablesMessages)
        detail.put("stackTrace", exceptionThrowablesStackTraces)

        val logger = LoggerFactory.getLogger(jobId.getOrElse(s"arc:${frameworkVersion}-${UUID.randomUUID.toString}"))
        logger.error()
          .field("event", "exit")
          .field("status", "failure")
          .field("success", Boolean.valueOf(false))
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


    import spark.implicits._

    // add spark config to log
    val sparkConf = new java.util.HashMap[String, String]()
    spark.sparkContext.hadoopConfiguration.set("io.compression.codecs", classOf[ai.tripl.arc.util.ZipCodec].getName)

    spark.sparkContext.getConf.getAll.foreach{ case (k, v) => sparkConf.put(k, v) }

    implicit val logger = LoggerFactory.getLogger(jobId.getOrElse(spark.sparkContext.applicationId))

    // add tags 
    val tags: Option[String] = argsMap.get("etl.config.tags").orElse(envOrNone("ETL_CONF_TAGS"))
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

    val environment: Option[String] = argsMap.get("etl.config.environment").orElse(envOrNone("ETL_CONF_ENV"))
    val env = environment match {
      case Some(value) => value
      case None => {
        logger.warn()
          .field("event", "deprecation")
          .field("message", "command line argument 'etl.config.environment' or 'ETL_CONF_ENV' will be required in next release. defaulting to 'prd'")        
          .log()

        "prd"      
      }
    }  
    MDC.put("environment", env) 
    
    val environmentId: Option[String] = argsMap.get("etl.config.environment.id").orElse(envOrNone("ETL_CONF_ENV_ID"))
    for (e <- environmentId) {
        MDC.put("environmentId", e) 
    }

    MDC.put("applicationId", spark.sparkContext.applicationId) 

    val arcContext = ARCContext(jobId=jobId, jobName=jobName, environment=env, environmentId=environmentId, configUri=configUri, isStreaming=isStreaming, ignoreEnvironments=ignoreEnvironments, lifecyclePlugins=Nil, disableDependencyValidation=disableDependencyValidation)
    
    // log available plugins
    val loader = Utils.getContextOrSparkClassLoader
    val dynamicConfigurationPlugins = ServiceLoader.load(classOf[DynamicConfigurationPlugin], loader).iterator().asScala.toList.map(c => c.getClass.getName).asJava   
    val lifecyclePlugins = ServiceLoader.load(classOf[LifecyclePlugin], loader).iterator().asScala.toList.map(c => c.getClass.getName).asJava   
    val pipelineStagePlugins = ServiceLoader.load(classOf[PipelineStagePlugin], loader).iterator().asScala.toList.map(c => c.getClass.getName).asJava   
    val udfPlugins = ServiceLoader.load(classOf[UDFPlugin], loader).iterator().asScala.toList.map(c => c.getClass.getName).asJava   

    logger.info()
      .field("event", "enter")
      .field("config", sparkConf)
      .field("sparkVersion", spark.version)
      .field("frameworkVersion", frameworkVersion)
      .field("scalaVersion", scala.util.Properties.versionNumberString)
      .field("javaVersion", System.getProperty("java.runtime.version"))
      .field("environment", env)
      .field("dynamicConfigurationPlugins", dynamicConfigurationPlugins)
      .field("lifecyclePlugins", lifecyclePlugins)
      .field("pipelineStagePlugins", pipelineStagePlugins)
      .field("udfPlugins", udfPlugins)
      .log()   

    // add spark listeners
    try {
      if (logger.isTraceEnabled) {
        ListenerUtils.addExecutorListener()(spark, logger)
      }
    } catch {
      case e: Exception => 
        val exceptionThrowables = ExceptionUtils.getThrowableList(e).asScala
        val exceptionThrowablesMessages = exceptionThrowables.map(e => e.getMessage).asJava
        val exceptionThrowablesStackTraces = exceptionThrowables.map(e => e.getStackTrace).asJava

        var detail = new java.util.HashMap[String, Object]()
        detail.put("event", "exception")
        detail.put("messages", exceptionThrowablesMessages)
        detail.put("stackTrace", exceptionThrowablesStackTraces)

        logger.error()
          .field("event", "exit")
          .field("status", "failure")
          .field("success", Boolean.valueOf(false))
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
      val dependencyGraph = ConfigUtils.Graph(Nil, Nil, false)
      ConfigUtils.parsePipeline(configUri, argsMap, dependencyGraph, arcContext)(spark, logger)
    } catch {
      case e: Exception => 
        val exceptionThrowables = ExceptionUtils.getThrowableList(e).asScala
        val exceptionThrowablesMessages = exceptionThrowables.map(e => e.getMessage).asJava
        val exceptionThrowablesStackTraces = exceptionThrowables.map(e => e.getStackTrace).asJava

        var detail = new java.util.HashMap[String, Object]()
        detail.put("event", "exception")
        detail.put("messages", exceptionThrowablesMessages)
        detail.put("stackTrace", exceptionThrowablesStackTraces)

        logger.error()
          .field("event", "exit")
          .field("status", "failure")
          .field("success", Boolean.valueOf(false))
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
      case Right( (pipeline, _, arcCtx) ) =>
        try {
          UDF.registerUDFs(spark.sqlContext)
          ARC.run(pipeline)(spark, logger, arcCtx)
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
              .field("success", Boolean.valueOf(false))
              .field("duration", System.currentTimeMillis() - startTime)
              .map("stage", e.detail)
              .log()       
            
            true

          case e: Exception => 
            val exceptionThrowables = ExceptionUtils.getThrowableList(e).asScala
            val exceptionThrowablesMessages = exceptionThrowables.map(e => e.getMessage).asJava
            val exceptionThrowablesStackTraces = exceptionThrowables.map(e => e.getStackTrace).asJava

            var detail = new java.util.HashMap[String, Object]()
            detail.put("event", "exception")
            detail.put("messages", exceptionThrowablesMessages)
            detail.put("stackTrace", exceptionThrowablesStackTraces)

            logger.error()
              .field("event", "exit")
              .field("status", "failure")
              .field("success", Boolean.valueOf(false))
              .field("duration", System.currentTimeMillis() - startTime)
              .field("reason", detail)
              .log()   
              
            true
        }
      case Left(errors) => {
        logger.error()
          .field("event", "exit")
          .field("status", "failure")
          .field("success", Boolean.valueOf(false))
          .field("duration", System.currentTimeMillis() - startTime)        
          .list("reason", ConfigUtils.Error.pipelineErrorJSON(errors))
          .log()   

        println(s"ETL Config contains errors:\n${ConfigUtils.Error.pipelineErrorMsg(errors)}\n")

        true
      }
    }

    if (!arcContext.isStreaming) {
      if (!error) {
        logger.info()
          .field("event", "exit")
          .field("status", "success")
          .field("success", Boolean.valueOf(true))
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

    def before(stage: PipelineStage): Unit = {
      for (p <- arcContext.lifecyclePlugins) {
        logger.trace().message(s"Executing before() on LifecyclePlugin: ${p.getClass.getName}")
        p.before(stage)
      }
    }

    def after(stage: PipelineStage, result: Option[DataFrame], isLast: Boolean): Unit = {
      for (p <- arcContext.lifecyclePlugins) {
        logger.trace().message(s"Executing after(last = $isLast) on LifecyclePlugin: ${p.getClass.getName}")
        p.after(stage, result, isLast)
      }
    }

    @tailrec
    def runStages(stages: List[PipelineStage]): Option[DataFrame] = {
      stages match {
        case Nil => None // end
        case head :: Nil =>
          before(head)
          val result = processStage(head)
          after(head, result, true)
          result
        case head :: tail =>
          before(head)
          val result = processStage(head)
          after(head, result, false)
          runStages(tail)
      }
    }

    runStages(pipeline.stages)
  }

  def processStage(stage: PipelineStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    stage.execute()
  }  

}
