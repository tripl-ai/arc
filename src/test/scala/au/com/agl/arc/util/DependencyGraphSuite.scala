package au.com.agl.arc.util

import java.net.URI

import scala.io.Source
import scala.collection.JavaConverters._

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import au.com.agl.arc.api.API._
import au.com.agl.arc.api.{Delimited, Delimiter, QuoteCharacter}
import au.com.agl.arc.util.log.LoggerFactory
import au.com.agl.arc.util.ConfigUtils._

import com.typesafe.config._

class DependencyGraphSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _

  before {
    val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    session = spark
  }

  after {
    session.stop()
  }


  // test("Test addVertex") { 
  //   implicit val spark = session
  //   implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
  //   implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false)

  //   // make empty graph
  //   val emptyVertices: RDD[(VertexId, (Int, String))] = spark.sparkContext.emptyRDD
  //   val emptyEdges: RDD[Edge[String]] = spark.sparkContext.emptyRDD
  //   val emptyGraph: Graph[(Int, String), String] = Graph(emptyVertices, emptyEdges)

  //   var outputGraph = emptyGraph
    
  //   outputGraph = ConfigUtils.addVertex(outputGraph, 0, "testVertex0")
  //   outputGraph = ConfigUtils.addVertex(outputGraph, 1, "testVertex0")

  //   assert(outputGraph.vertices.collect.length == 1)

  //   outputGraph = ConfigUtils.addVertex(outputGraph, 2, "testVertex1")

  //   assert(outputGraph.vertices.collect.length == 2)
  // } 


  test("Test vertexExists: false") { 
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false)

    val conf = """{
      "stages": [          
        {
          "type": "ParquetLoad",
          "name": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputView": "testView",
          "outputURI": "/tmp/out.parquet"
        },      
      ]
    }
    """

    val base = ConfigFactory.load()
    val etlConf = ConfigFactory.parseString(conf, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))
    val config = etlConf.withFallback(base)
    var argsMap = collection.mutable.Map[String, String]()
    val pipeline = ConfigUtils.readPipeline(config.resolve(), new URI(""), argsMap, arcContext)    

    pipeline match {
      case Left(stageError) => {
        assert(stageError == StageError(0, "test",3,List(ConfigError("inputView",Some(10), "view 'testView' does not exist by this stage of the job."))) :: Nil)
      }
      case Right(_) => assert(false)
    }
  }  

  test("Test vertexExists: true") { 
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false)

    val conf = """{
      "stages": [     
        {
          "type": "DelimitedExtract",
          "name": "file extract",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "/tmp/in.csv",
          "outputView": "testView"
        },
        {
          "type": "ParquetLoad",
          "name": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputView": "testView",
          "outputURI": "/tmp/out.parquet"
        }
      ]
    }
    """

    val base = ConfigFactory.load()
    val etlConf = ConfigFactory.parseString(conf, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))
    val config = etlConf.withFallback(base)
    var argsMap = collection.mutable.Map[String, String]()
    val pipeline = ConfigUtils.readPipeline(config.resolve(), new URI(""), argsMap, arcContext)    

    pipeline match {
      case Left(_) => assert(false)
      case Right( (pipeline, graph) ) => {
        // println(s"""nodes: ${graph.vertices.collect.map { case (id, (stageId, name)) => s"$stageId: $name" }.mkString("['","', '","']")}""")
        // println(s"""edges: ${graph.edges.collect.map { case Edge(src, dst, _) => s"$src -> $dst" }.mkString("['","', '","']")}""")
        assert(true)
      }
    }
  }  

  test("Test vertexExists: false - SQLTransform") { 
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false)

    val conf = s"""{
      "stages": [          
        {
          "type": "SQLTransform",
          "name": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "${getClass.getResource("/conf/sql/").toString}/customer.sql",
          "outputView": "outputView"          
        },      
      ]
    }
    """

    val base = ConfigFactory.load()
    val etlConf = ConfigFactory.parseString(conf, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))
    val config = etlConf.withFallback(base)
    var argsMap = collection.mutable.Map[String, String]()
    val pipeline = ConfigUtils.readPipeline(config.resolve(), new URI(""), argsMap, arcContext)    

    pipeline match {
      case Left(stageError) => {
        assert(stageError == StageError(0, "test",3,List(ConfigError("inputURI",Some(10),"view 'customer' does not exist by this stage of the job."))) :: Nil)
      }
      case Right( (pipeline, graph) ) => assert(false)
    }
  } 

  test("Test vertexExists: true - SQLTransform") { 
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false)

    val conf = s"""{
      "stages": [   
        {
          "type": "DelimitedExtract",
          "name": "file extract",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "/tmp/in.csv",
          "outputView": "customer"
        },               
        {
          "type": "SQLTransform",
          "name": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "${getClass.getResource("/conf/sql/").toString}/customer.sql",
          "outputView": "outputView"          
        }    
      ]
    }
    """

    val base = ConfigFactory.load()
    val etlConf = ConfigFactory.parseString(conf, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))
    val config = etlConf.withFallback(base)
    var argsMap = collection.mutable.Map[String, String]()
    val pipeline = ConfigUtils.readPipeline(config.resolve(), new URI(""), argsMap, arcContext)    

    pipeline match {
      case Left(_) => assert(false)
      case Right( (pipeline, graph) ) => assert(true)
    }
  } 

  test("Test vertexExists: multiple") { 
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false)

    val conf = s"""{
      "stages": [   
        {
          "type": "DelimitedExtract",
          "name": "file extract",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "/tmp/in.csv",
          "outputView": "customer"
        },               
        {
          "type": "SQLTransform",
          "name": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "${getClass.getResource("/conf/sql/").toString}/customer.sql",
          "outputView": "outputView0"          
        },
        {
          "type": "SQLTransform",
          "name": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "${getClass.getResource("/conf/sql/").toString}/customer.sql",
          "outputView": "outputView1"          
        }         
      ]
    }
    """

    val base = ConfigFactory.load()
    val etlConf = ConfigFactory.parseString(conf, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))
    val config = etlConf.withFallback(base)
    var argsMap = collection.mutable.Map[String, String]()
    val pipeline = ConfigUtils.readPipeline(config.resolve(), new URI(""), argsMap, arcContext)    

    pipeline match {
      case Left(_) => assert(false)
      case Right( (pipeline, graph) ) => {
        println(s"""nodes: ${graph.vertices.collect.map { case (id, (stageId, name)) => s"$stageId: $name" }.mkString("['","', '","']")}""")
        println(s"""edges: ${graph.edges.collect.map { case Edge(src, dst, _) => s"$src -> $dst" }.mkString("['","', '","']")}""")
        assert(true)
      }
    }
  } 


  // test("Test basic ") { 
  //   implicit val spark = session
  //   implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
  //   implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false)

  //   val conf = """{
  //     "stages": [       
  //       {
  //         "type": "DelimitedExtract",
  //         "name": "extract",
  //         "environments": [
  //           "production",
  //           "test"
  //         ],
  //         "inputURI": "/tmp/fake.csv",
  //         "outputView": "output"
  //       },
  //       {
  //         "type": "DelimitedLoad",
  //         "name": "extract",
  //         "environments": [
  //           "production",
  //           "test"
  //         ],
  //         "inputView": "output0",
  //         "outputURI": "/tmp/fake.csv",
  //       }        
  //     ]
  //   }"""

  //   val base = ConfigFactory.load()
  //   val etlConf = ConfigFactory.parseString(conf, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))
  //   val config = etlConf.withFallback(base)
  //   var argsMap = collection.mutable.Map[String, String]()
  //   val pipeline = ConfigUtils.readPipeline(config.resolve(), new URI(""), argsMap, arcContext)    

  //   pipeline match {
  //     case Left(stageError) => {
  //       assert(stageError == StageError("file extract",3,List(ConfigError("customDelimiter", None, "Missing required attribute 'customDelimiter'."))) :: Nil)
  //     }
  //     case Right(_) => assert(false)
  //   }
  // }    
}
