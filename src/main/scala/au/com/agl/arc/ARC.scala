package au.com.agl.arc

object ARC {

  import java.util.UUID
  import org.apache.commons.lang3.exception.ExceptionUtils
  import scala.collection.JavaConverters._

  import org.slf4j.MDC

  import scala.annotation.tailrec
  import scala.util.Properties._
  import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
  import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator

  import org.apache.spark.sql._
  import org.apache.spark.sql.types._

  import au.com.agl.arc.api.API._
  import au.com.agl.arc.util._
  import au.com.agl.arc.util.log.LoggerFactory 

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
      val pair = x.split("=(?!=)(?!$)")
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

    val spark: SparkSession = try {
      SparkSession
        .builder()
        .appName(jobId.getOrElse(s"arc:${BuildInfo.version}-${UUID.randomUUID.toString}"))
        .config("spark.debug.maxToStringFields", "8192")
        .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
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

        val logger = LoggerFactory.getLogger(jobId.getOrElse(s"arc:${BuildInfo.version}-${UUID.randomUUID.toString}"))
        logger.error()
          .field("event", "exit")
          .field("status", "failure")
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
    spark.sparkContext.hadoopConfiguration.set("io.compression.codecs", classOf[au.com.agl.arc.util.ZipCodec].getName)

    spark.sparkContext.getConf.getAll.foreach{ case (k, v) => sparkConf.put(k, v) }

    implicit val logger = LoggerFactory.getLogger(jobId.getOrElse(spark.sparkContext.applicationId))
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
    
    val envId: Option[String] = argsMap.get("etl.config.environment.id").orElse(envOrNone("ETL_CONF_ENV_ID"))
    for (e <- envId) {
        MDC.put("environmentId", e) 
    }         

    logger.info()
      .field("event", "enter")
      .field("config", sparkConf)
      .field("sparkVersion", spark.version)
      .field("frameworkVersion", BuildInfo.version)
      .field("applicationId", spark.sparkContext.applicationId)
      .field("environment", env)
      .log()   

    // add spark listeners
    try {
      GeoSparkSQLRegistrator.registerAll(spark)
      ListenerUtils.addListeners()(spark, logger)
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
      ConfigUtils.parsePipeline()(spark, logger, argsMap, env)
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
      case Right(pipeline) =>
        try {
          ARC.run(pipeline)(spark, logger)
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
              .field("duration", System.currentTimeMillis() - startTime)
              .map("stage", e.detail)
              .log()       
            
            true
        }
      case Left(errors) => {
        val errorMsg = ConfigUtils.Error.pipelineErrorMsg(errors)
        println(errorMsg)

        // TODO add errors to json as data
        logger.error()
          .field("event", "config_errors")
          .field("message", errorMsg)
          .log()   

        true
      }
    }

    if (!error) {
      logger.info()
        .field("event", "exit")
        .field("status", "success")
        .field("duration", System.currentTimeMillis() - startTime)
        .log()   
    }

    // silently try to shut down log so that all messages are sent before stopping the spark session
    try {
      org.apache.log4j.LogManager.shutdown
    } catch {
      case e: Exception => 
    }

    // stop spark session and return error code to indicate sucess/failure
    spark.stop()
    sys.exit(if (error) 1 else 0)
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
  (implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger) = {

    def processStage(stage: PipelineStage) {
      stage match {
        case e : AvroExtract =>
          extract.AvroExtract.extract(e)          
        case e : DelimitedExtract =>
          extract.DelimitedExtract.extract(e)
        case e : HTTPExtract =>
          extract.HTTPExtract.extract(e)              
        case e : JDBCExtract =>
          extract.JDBCExtract.extract(e)             
        case e : JSONExtract =>
          extract.JSONExtract.extract(e)
        case e : KafkaExtract =>
          extract.KafkaExtract.extract(e)                             
        case e : ORCExtract =>
          extract.ORCExtract.extract(e)              
        case e : ParquetExtract =>
          extract.ParquetExtract.extract(e)
        case e : XMLExtract =>
          extract.XMLExtract.extract(e)
          
        case t : DiffTransform =>
          transform.DiffTransform.transform(t)
        case t : JSONTransform =>
          transform.JSONTransform.transform(t)
        case t : MetadataFilterTransform =>
          transform.MetadataFilterTransform.transform(t)                 
        case t : MLTransform =>
          transform.MLTransform.transform(t)          
        case t : SQLTransform =>
          transform.SQLTransform.transform(t)      
        // case t : TensorFlowServingTransform =>
          // transform.TensorFlowServingTransform.transform(t)              
        case t : TypingTransform =>
          transform.TypingTransform.transform(t)

        case l : AvroLoad =>
          load.AvroLoad.load(l)
        case l : AzureEventHubsLoad =>
          load.AzureEventHubsLoad.load(l)          
        case l : DelimitedLoad =>
          load.DelimitedLoad.load(l)
        case l : HTTPLoad =>
          load.HTTPLoad.load(l)             
        case l : JDBCLoad =>
          load.JDBCLoad.load(l)          
        case l : JSONLoad =>
          load.JSONLoad.load(l)
        case l : KafkaLoad =>
          load.KafkaLoad.load(l)                              
        case l : ORCLoad =>
          load.ORCLoad.load(l)             
        case l : ParquetLoad =>
          load.ParquetLoad.load(l)   
        case l : XMLLoad =>
          load.XMLLoad.load(l)            

        case x : HTTPExecute =>
          execute.HTTPExecute.execute(x)  
        case x : JDBCExecute =>
          execute.JDBCExecute.execute(x)
        case x : KafkaCommitExecute =>
          execute.KafkaCommitExecute.execute(x)          

        case v : EqualityValidate =>
          validate.EqualityValidate.validate(v)           
        case v : SQLValidate =>
          validate.SQLValidate.validate(v)                     
      }
    }

    @tailrec
    def runStages(stages: List[PipelineStage]) {
      stages match {
        case Nil => // finished
        case head :: tail =>
          processStage(head)
          runStages(tail)
      }
    }

    runStages(pipeline.stages)
  }
}
