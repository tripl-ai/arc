package ai.tripl.arc.plugins.lifecycle

import java.util.UUID

import scala.reflect.runtime.universe._
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.node._
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.trees._
import org.apache.spark.sql.{DataFrame, SparkSession}

import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.plugins.LifecyclePlugin
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.Utils
import ai.tripl.arc.config.Error._

class DataLineage extends LifecyclePlugin {

  val version = Utils.getFrameworkVersion

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], LifecyclePluginInstance] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "environments" :: "output" :: Nil
    val stageFilter = getValue[StringList]("afterPlugins", default = Some("AvroLoad" :: "DelimitedLoad" :: "HTTPLoad" :: "JDBCLoad" :: "OrcLoad" :: "TextLoad" :: "XMLLoad" :: "ParquetLoad" :: Nil))
    val output = getValue[String]("output", default = Some("log"), validValues = "log" :: Nil) |> parseOutput("output") _
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (output, stageFilter, invalidKeys) match {
      case (Right(output), Right(stageFilter), Right(invalidKeys)) =>
        val runtimeMirror = scala.reflect.runtime.universe.runtimeMirror(Option(Thread.currentThread().getContextClassLoader).getOrElse(spark.getClass.getClassLoader))

        Right(DataLineageInstance(
          plugin=this,
          stageFilter=stageFilter,
          output=output,
          runtimeMirror=runtimeMirror
        ))
      case _ =>
        val allErrors: Errors = List(output, stageFilter, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val err = StageError(index, this.getClass.getName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

  def parseOutput(path: String)(strategy: String)(implicit c: com.typesafe.config.Config): Either[Errors, DataLineageOutput] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._

    strategy.toLowerCase.trim match {
      case "log" => Right(DataLineageOutputLog)
      case _ => Left(ConfigError(path, None, s"Invalid state. Please raise issue.") :: Nil)
    }
  }
}

sealed trait DataLineageOutput {
  def sparkString(): String
}
case object DataLineageOutputLog extends DataLineageOutput { val sparkString = "log" }

case class DataLineageInstance(
    plugin: DataLineage,
    stageFilter: List[String],
    output: DataLineageOutput,
    runtimeMirror: reflect.runtime.universe.Mirror
  ) extends LifecyclePluginInstance {

  override def after(result: Option[DataFrame], stage: PipelineStage, index: Int, stages: List[PipelineStage])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    logger.trace()
      .field("event", "after")
      .field("stage", stage.name)
      .log()

    if (stageFilter.contains(stage.plugin.getClass.getSimpleName)) {
      result match {
        case Some(df) => {
            output match {
              case DataLineageOutputLog => {
                implicit val formats = DefaultFormats
                val objectMapper = new ObjectMapper()
                val queryExecution = objectMapper.readValue(compact(render(jsonFields(df.queryExecution.analyzed, runtimeMirror))), classOf[java.util.HashMap[String, Object]])
                stage.stageDetail.put("queryExecution", queryExecution)
              }
              case _ =>
            }
        }
        case None => 
      }
    }

    result
  }


  // exclude members which lead to infinite recursion or other problems
  val excludeMembers = Set(
    "sparkSession", // infinite recursion
    "hadoopConf", // infinite recursion
    "canonicalized", // infinite recursion
    "containsChild", // contains the scala class
    "fileFormat", // infinite recursion
    "origin", // creates a lot of useless objects
    "hash", // not very useful
    "jvmId" // not very useful
  )

  val includeMembers = Set(
    "validConstraints",
    "simpleString",
    "toSQL"
  )

  // get constructor members of the input type
  def constructorMembers(tpe: reflect.runtime.universe.Type): Seq[Symbol] = {
    val constructorSymbol = tpe.dealias.member(termNames.CONSTRUCTOR)
    val params = if (constructorSymbol.isMethod) {
      constructorSymbol.asMethod.paramLists
    } else {
      // Find the primary constructor, and use its parameter ordering.
      val primaryConstructorSymbol: Option[Symbol] = constructorSymbol.asTerm.alternatives.find(
        s => s.isMethod && s.asMethod.isPrimaryConstructor)
      if (primaryConstructorSymbol.isEmpty) {
        throw new Exception("primaryConstructorSymbol.isEmpty")
      } else {
        primaryConstructorSymbol.get.asMethod.paramLists
      } 
    }
    params.flatten
  }

  // get value members of the input type
  def valueMembers(tpe: reflect.runtime.universe.Type): Iterable[Symbol] = {
    tpe.members collect { 
      case m: MethodSymbol if m.isAccessor => m.accessed
    }
  }

  // find and invoke members of the input class returning (key, value)
  def classMembers(node: Any, runtimeMirror: Mirror, memoizedClassMembers: collection.mutable.Map[String, List[String]]) : Seq[(String, Any)] = {
    val nodeClass = node.getClass
    if (nodeClass.toString.contains("$")) {
      Seq[(String, Any)]()
    } else {
      try {
        // reflection is super expensive so try to store in memory if possible
        val memberNames = memoizedClassMembers.get(nodeClass.getName).getOrElse {
          val classSymbol = runtimeMirror.staticClass(nodeClass.getName)
          val classType = classSymbol.selfType
          val cMembers = constructorMembers(classType).map(_.name.decodedName.toString.trim)
          val vMembers = valueMembers(classType).map(_.name.decodedName.toString.trim)
          val memberNames = (cMembers ++ vMembers ++ includeMembers).distinct.filterNot(excludeMembers.contains(_)).toList

          memoizedClassMembers.put(nodeClass.getName, memberNames)
          memberNames
        }
        val memberValues = memberNames.flatMap { name =>
          try {
            Option(nodeClass.getMethod(name).invoke(node))
          } catch {
            case e: Exception => None
          }
        }
        memberNames.zip(memberValues)
      } catch {
        case e: Exception => Seq[(String, Any)]()
      }
    }
  }

  def jsonFields(node: Any, runtimeMirror: Mirror, memoizedClassMembers: collection.mutable.Map[String, List[String]] = collection.mutable.Map[String, List[String]]()): List[JField] = {

    // get the members of the input node
    val members = classMembers(node, runtimeMirror, memoizedClassMembers)


    val childs = node match {
      case node: TreeNode[_] => List("numChildren" -> JInt(node.children.length))
      case _ => List.empty
    }

    List("class" -> JString(node.getClass.getName)) ++
    childs ++ 
    members.map { 
      case (name, null) => {
        Option(name -> JNull)
      }
      case (name, value: Map[_, _]) if value.isEmpty => {
        Option(name -> JObject())
      }        
      case (name, value: Map[_, _]) if !value.isEmpty => {
        Option(name -> JArray(value.map(v => JObject(jsonFields(v, runtimeMirror, memoizedClassMembers))).toList))
      }           
      case (name, value: Seq[_]) if value.length > 0 =>
        Option(name -> JArray(
          value.map(v => {
            v match {
              // java.lang.AssertionError: assertion failed: no symbol could be loaded from interface org.apache.hadoop.classification.InterfaceAudience$Public 
              case v: org.apache.hadoop.fs.Path => JString(v.toString)
              case _ => JObject(jsonFields(v, runtimeMirror, memoizedClassMembers))
            }
          }).toList
        ))     
      case (name, value: Seq[_]) if value.length == 0 =>
        Option(name -> JArray(List.empty))
      case (name, value: Set[_]) if value.size > 0 =>
        Option(name -> JArray(
          value.toList.map(v => {
            v match {
              // java.lang.AssertionError: assertion failed: no symbol could be loaded from interface org.apache.hadoop.classification.InterfaceAudience$Public 
              case v: org.apache.hadoop.fs.Path => JString(v.toString)
              case _ => JObject(jsonFields(v, runtimeMirror, memoizedClassMembers))
            }
          }).toList
        ))
      case (name, value: Set[_]) if value.size == 0 =>
        Option(name -> JArray(List.empty))
      case (name, value: Option[_]) =>
        value match {
          case Some(v) => {
            Option(name -> JObject(jsonFields(v, runtimeMirror, memoizedClassMembers)))
          }
          case None => Option(name -> JNull)
        }
      case (name, value: Boolean) => {
        Option(name -> JBool(value))
      }
      case (name, value: Byte) => {
        Option(name -> JInt(value.toInt))
      }    
      case (name, value: Short) => {
        Option(name -> JInt(value.toInt))
      }     
      case (name, value: Int) => {
        Option(name -> JInt(value))
      }  
      case (name, value: Long) => {
        Option(name -> JInt(value))
      }     
      case (name, value: BigInt) => {
        Option(name -> JInt(value))
      }              
      case (name, value: Float) => {
        Option(name -> JDouble(value))
      }           
      case (name, value: Double) => {
        Option(name -> JDouble(value))
      }       
      case (name, value: String) => {
        Option(name -> JString(value))
      }    
      case (name, value: org.apache.spark.unsafe.types.UTF8String) => {
        Option(name -> JString(value.toString))
      }    
      case (name, value: UUID) => {
        Option(name -> JString(value.toString))
      }                 
      case (name, value: DataType) => {
        Option(name -> JString(value.simpleString))
      }          
      case (name, value) => {
        Option(name -> JObject(jsonFields(value, runtimeMirror, memoizedClassMembers)))
      }
    }.flatten.toList  
  }

}