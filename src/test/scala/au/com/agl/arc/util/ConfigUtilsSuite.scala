package au.com.agl.arc.util

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import com.typesafe.config._

import au.com.agl.arc.api.API._
import au.com.agl.arc.util.ConfigUtils._
import au.com.agl.arc.util.log.LoggerFactory 

import org.apache.spark.sql._

class ConfigUtilsSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _

  before {
    val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    session = spark
  }

  after {
    session.stop()
  }

  test("Read ParquetExtract with missing value errors") {
    implicit val spark = session
    implicit val env = "tst"
    import spark.implicits._

    val etlConfString = """|stages = [
                           |  {
                           |    "environments": ["tst"]  
                           |    "type": "ParquetExtract"     
                           |  }
                           |]
                           |""".stripMargin

    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    val base = ConfigFactory.load() 

    val etlConf = ConfigFactory.parseString(etlConfString, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))
    val config = etlConf.withFallback(base).resolve()

    val pipeline = ConfigUtils.readPipeline(config, uri=new URI("http://test.test"))

    val errors = ConfigError.err("name", "No value found for name") :::
                 ConfigError.err("inputURI", "No value found for inputURI") :::
                 ConfigError.err("outputView", "No value found for outputView") :::
                 ConfigError.err("persist", "No value found for persist") ::: Nil

    assert(pipeline === Left(List(StageError("unnamed stage", errors))))
  }

//   test("Read invalid Authentication no values") {
//     implicit val spark = session
//     implicit val env = "test"
//     import spark.implicits._

//     val etlConfString = """|stages = [
//                            |  {
//                            |    "type": "ParquetExtract"
//                            |    "authentication": {
//                            |      "method": "AzureSharedKey"
//                            |    }
//                            |  }
//                            |]
//                            |""".stripMargin

//     implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

//     val base = ConfigFactory.load() 

//     val etlConf = ConfigFactory.parseString(etlConfString, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))
//     val config = etlConf.withFallback(base).resolve()

//     val pipeline = ConfigUtils.readPipeline(config, uri=new URI("http://test.test"))

//     val errors = ConfigError.err("name", "No value found for name") :::
//                  ConfigError.err("inputURI", "No value found for inputURI") :::
//                  ConfigError.err("outputView", "No value found for outputView") :::
//                  ConfigError.err("persist", "No value found for persist") :::
//                  ConfigError.err("authentication", "Unable to read config value: Authentication method 'AzureSharedKey' requires 'accountName' parameter.") ::: Nil                 

//     assert(pipeline === Left(List(StageError("unnamed stage", errors))))
//   }
  
//   test("Read invalid Authentication partial value") {
//     implicit val spark = session
//     import spark.implicits._

//     val etlConfString = """|stages = [
//                            |  {
//                            |    "type": "ParquetExtract"
//                            |    "authentication": {
//                            |      "method": "AzureSharedKey",
//                            |      "accountName": "a",
//                            |    }
//                            |  }
//                            |]
//                            |""".stripMargin

//     implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

//     val base = ConfigFactory.load() 

//     val etlConf = ConfigFactory.parseString(etlConfString, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))
//     val c = etlConf.withFallback(base).resolve()

//     val pipeline = ConfigUtils.readPipeline(c)

//     val errors = ConfigError.err("name", "No value found for name") :::
//                  ConfigError.err("inputURI", "No value found for inputURI") :::
//                  ConfigError.err("outputView", "No value found for outputView") :::
//                  ConfigError.err("persist", "No value found for persist") :::
//                  ConfigError.err("authentication", "Unable to read config value: Authentication method 'AzureSharedKey' requires 'signature' parameter.") ::: Nil                 

//     assert(pipeline === Left(List(StageError("unnamed stage", errors))))
//   }

//   test("Read valid pipeline") {
//     implicit val spark = session
//     import spark.implicits._

//     val etlConfString = """|stages = [
//                            |  {
//                            |    "type": "ParquetExtract"
//                            |    "name": "test",
//                            |    "inputURI": "wasbs://test",
//                            |    "outputView": "test",
//                            |    "persist": false,
//                            |    "authentication": {
//                            |      "method": "AzureSharedKey",
//                            |      "accountName": "a",
//                            |      "signature": "b"
//                            |    }
//                            |  }
//                            |]
//                            |""".stripMargin

//     implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

//     val base = ConfigFactory.load() 

//     val etlConf = ConfigFactory.parseString(etlConfString, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))
//     val c = etlConf.withFallback(base).resolve()

//     val pipeline = ConfigUtils.readPipeline(c)

//     val stage = Right(ETLPipeline(List(ParquetExtract("test",Nil,"test",new URI("wasbs://test"),Some(Authentication.AzureSharedKey("a","b")),Map.empty,false))))

//     assert(pipeline === stage)
//   }  

//   test("Read Unknown Stage") {
//     implicit val spark = session
//     import spark.implicits._

//     val etlConfString = """|stages = [
//                            |  {
//                            |  }
//                            |]
//                            |""".stripMargin

//     implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

//     val base = ConfigFactory.load() 

//     val etlConf = ConfigFactory.parseString(etlConfString, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))
//     val c = etlConf.withFallback(base).resolve()

//     val pipeline = ConfigUtils.readPipeline(c)
//   }
}
