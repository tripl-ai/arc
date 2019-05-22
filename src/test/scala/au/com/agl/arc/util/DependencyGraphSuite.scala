package au.com.agl.arc.util

import java.net.URI

import scala.io.Source
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql._

import au.com.agl.arc.api.API._
import au.com.agl.arc.plugins.LifecyclePlugin
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

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")   

    session = spark
  }

  after {
    session.stop()
  }

  // to help debug
  // println(s"""nodes: ${graph.vertices.map { vertex => s"${vertex.stageId}: ${vertex.name}" }.mkString("['","', '","']")}""")
  // println(s"""edges: ${graph.edges.map { case Edge(src, dst) => s"${src.stageId}: ${src.name} -> ${dst.stageId}: ${dst.name}" }.mkString("['","', '","']")}""")


  test("Test addVertex") { 
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false, lifecyclePlugins=new ListBuffer[LifecyclePlugin]())

    // make empty graph
    var outputGraph = Graph(Nil,Nil,false)
    
    outputGraph = outputGraph.addVertex(Vertex(0, "testVertex0"))
    outputGraph = outputGraph.addVertex(Vertex(0, "testVertex0"))

    assert(outputGraph.vertices.length == 1)

    outputGraph = outputGraph.addVertex(Vertex(2, "testVertex1"))

    assert(outputGraph.vertices.length == 2)
  } 

  test("Test addEdge") { 
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false, lifecyclePlugins=new ListBuffer[LifecyclePlugin]())

    // make empty graph
    var outputGraph = Graph(Nil,Nil,false)
    
    outputGraph = outputGraph.addVertex(Vertex(0, "testVertex0"))

    outputGraph = outputGraph.addEdge("testVertex0", "testVertex1")

    assert(outputGraph.edges.length == 0)

    outputGraph = outputGraph.addVertex(Vertex(1, "testVertex1"))
    outputGraph = outputGraph.addEdge("testVertex0", "testVertex1")

    assert(outputGraph.edges.length == 1)
  }   

  test("Test vertexExists: false") { 
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false, lifecyclePlugins=new ListBuffer[LifecyclePlugin]())

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

    val argsMap = collection.mutable.Map[String, String]()
    val graph = ConfigUtils.Graph(Nil, Nil, false)
    val pipelineEither = ConfigUtils.parseConfig(Left(conf), argsMap, graph, arcContext)

    pipelineEither match {
      case Left(stageError) => {
        assert(stageError == StageError(0, "test",3,List(ConfigError("inputView",Some(10), "view 'testView' does not exist by this stage of the job."))) :: Nil)
      }
      case Right(_) => assert(false)
    }
  }  

  test("Test vertexExists: true") { 
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false, lifecyclePlugins=new ListBuffer[LifecyclePlugin]())

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

    val argsMap = collection.mutable.Map[String, String]()
    val graph = ConfigUtils.Graph(Nil, Nil, false)
    val pipelineEither = ConfigUtils.parseConfig(Left(conf), argsMap, graph, arcContext)

    pipelineEither match {
      case Left(_) => assert(false)
      case Right( (_, _) ) => {
        assert(true)
      }
    }
  }  

  test("Test vertexExists: false - SQLTransform") { 
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false, lifecyclePlugins=new ListBuffer[LifecyclePlugin]())

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

    val argsMap = collection.mutable.Map[String, String]()
    val graph = ConfigUtils.Graph(Nil, Nil, false)
    val pipelineEither = ConfigUtils.parseConfig(Left(conf), argsMap, graph, arcContext)

    pipelineEither match {
      case Left(stageError) => {
        assert(stageError == StageError(0, "test",3,List(ConfigError("inputURI",Some(10),"view 'customer' does not exist by this stage of the job."))) :: Nil)
      }
      case Right( (_, _) ) => assert(false)
    }
  } 

  test("Test vertexExists: true - SQLTransform") { 
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false, lifecyclePlugins=new ListBuffer[LifecyclePlugin]())

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

    val argsMap = collection.mutable.Map[String, String]()
    val graph = ConfigUtils.Graph(Nil, Nil, false)
    val pipelineEither = ConfigUtils.parseConfig(Left(conf), argsMap, graph, arcContext)

    pipelineEither match {
      case Left(_) => assert(false)
      case Right( (_, graph) ) => {
        assert(graph.vertices == Vertex(0, "customer") :: Vertex(1, "outputView") :: Nil)
        assert(graph.edges == Edge(Vertex(0, "customer"), Vertex(1, "outputView")) :: Nil)
      }
    }
  } 

  test("Test vertexExists: multiple") { 
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false, lifecyclePlugins=new ListBuffer[LifecyclePlugin]())

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

    val argsMap = collection.mutable.Map[String, String]()
    val graph = ConfigUtils.Graph(Nil, Nil, false)
    val pipelineEither = ConfigUtils.parseConfig(Left(conf), argsMap, graph, arcContext)

    pipelineEither match {
      case Left(_) => assert(false)
      case Right( (_, graph) ) => {
        assert(graph.vertices == Vertex(0, "customer") :: Vertex(1, "outputView0") :: Vertex(2, "customer") :: Vertex(3, "outputView0") :: Vertex(4, "outputView1") :: Nil)
        assert(graph.edges == Edge(Vertex(0, "customer"),Vertex(1, "outputView0")) :: Edge(Vertex(2,"customer"),Vertex(3,"outputView0")) :: Edge(Vertex(2,"customer"),Vertex(4,"outputView1")) :: Nil)
      }
    }
  } 

  test("Test vertexExists: PipelineStagePlugin") { 
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false, lifecyclePlugins=new ListBuffer[LifecyclePlugin]())

    val conf = s"""{
      "stages": [   
        {
          "type": "au.com.agl.arc.plugins.ArcCustomPipelineStage",
          "name": "custom plugin",
          "environments": [
            "production",
            "test"
          ],
          "params": {
            "foo": "bar"
          }
        },
        {
          "type": "ParquetLoad",
          "name": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputView": "thisviewwillnotbechecked",
          "outputURI": "/tmp/out.parquet"
        }                 
      ]
    }
    """

    val argsMap = collection.mutable.Map[String, String]()
    val graph = ConfigUtils.Graph(Nil, Nil, false)
    val pipelineEither = ConfigUtils.parseConfig(Left(conf), argsMap, graph, arcContext) 

    pipelineEither match {
      case Left(_) => assert(false)
      case Right( (_, graph) ) => {
        assert(graph.vertices == Vertex(1, "1:ParquetLoad"):: Nil)
        assert(graph.edges == Nil)
      }
    }
  } 

}
