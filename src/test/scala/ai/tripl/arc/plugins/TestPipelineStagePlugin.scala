package ai.tripl.arc.plugins

import org.apache.spark.sql.{DataFrame, SparkSession}

import com.typesafe.config._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.config.ConfigUtils._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.util.CloudUtils
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.ExtractUtils
import ai.tripl.arc.util.MetadataUtils
import ai.tripl.arc.util.Utils

class TestPipelineStagePlugin extends PipelineStagePlugin {
  
  val version = "1.0.1"

  def instantiate(index: Int, config: Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "environments" :: "name" :: "description" :: "params" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")    
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, description, invalidKeys) match {
      case (Right(name), Right(description), Right(invalidKeys)) => 
        val instance = TestPipelineStageInstance(
          plugin=this,
          name=name,
          description=description,
          params=params
        )

        Right(instance)
      case _ =>
        val allErrors: Errors = List(name, description, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val err = StageError(index, this.getClass.getName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class TestPipelineStageInstance(
    plugin: PipelineStagePlugin, 
    name: String, 
    description: Option[String],     
    params: Map[String, String]
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    TestPipelineStageInstance.extract(this)
  }
  
}

object TestPipelineStageInstance {
  def extract(extract: TestPipelineStageInstance)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    None
  }
}
