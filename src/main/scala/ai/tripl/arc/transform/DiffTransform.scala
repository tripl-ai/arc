package ai.tripl.arc.transform

import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import com.typesafe.config._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.CloudUtils
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.ExtractUtils
import ai.tripl.arc.util.MetadataUtils
import ai.tripl.arc.util.ListenerUtils
import ai.tripl.arc.util.Utils

class DiffTransform extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputLeftView" :: "inputRightView" :: "outputIntersectionView" :: "outputLeftView" :: "outputRightView" :: "persist" :: "params" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputLeftView = getValue[String]("inputLeftView")
    val inputRightView = getValue[String]("inputRightView")
    val outputIntersectionView = getOptionalValue[String]("outputIntersectionView")
    val outputLeftView = getOptionalValue[String]("outputLeftView")
    val outputRightView = getOptionalValue[String]("outputRightView")
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    (name, description, inputLeftView, inputRightView, outputIntersectionView, outputLeftView, outputRightView, persist, invalidKeys) match {
      case (Right(name), Right(description), Right(inputLeftView), Right(inputRightView), Right(outputIntersectionView), Right(outputLeftView), Right(outputRightView), Right(persist), Right(invalidKeys)) => 

        val stage = DiffTransformStage(
          plugin=this,
          name=name,
          description=description,
          inputLeftView=inputLeftView,
          inputRightView=inputRightView,
          outputIntersectionView=outputIntersectionView,
          outputLeftView=outputLeftView,
          outputRightView=outputRightView,
          params=params,
          persist=persist
        )

        stage.stageDetail.put("inputLeftView", inputLeftView)  
        stage.stageDetail.put("inputRightView", inputRightView)   
        stage.stageDetail.put("persist", java.lang.Boolean.valueOf(persist))      
        for (outputIntersectionView <- outputIntersectionView) {
          stage.stageDetail.put("outputIntersectionView", outputIntersectionView)  
        }  
        for (outputLeftView <- outputLeftView) {
          stage.stageDetail.put("outputLeftView", outputLeftView)  
        } 
        for (outputRightView <- outputRightView) {
          stage.stageDetail.put("outputRightView", outputRightView)  
        }                 

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, inputLeftView, inputRightView, outputIntersectionView, outputLeftView, outputRightView, persist, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}


case class DiffTransformStage(
    plugin: DiffTransform,
    name: String, 
    description: Option[String], 
    inputLeftView: String, 
    inputRightView: String, 
    outputIntersectionView: Option[String], 
    outputLeftView: Option[String], 
    outputRightView: Option[String], 
    params: Map[String, String], 
    persist: Boolean
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    DiffTransformStage.execute(this)
  }
}

object DiffTransformStage {

  def execute(stage: DiffTransformStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    val inputLeftDF = spark.table(stage.inputLeftView)   
    val inputRightDF = spark.table(stage.inputRightView)   

    // do a full join on a calculated hash of all values in row on each dataset
    // trying to calculate the hash value inside the joinWith method produced an inconsistent result
    val leftHashDF = inputLeftDF.withColumn("_hash", sha2(to_json(struct(inputLeftDF.columns.map(col):_*)),512))
    val rightHashDF = inputRightDF.withColumn("_hash", sha2(to_json(struct(inputRightDF.columns.map(col):_*)),512))
    val transformedDF = leftHashDF.joinWith(rightHashDF, leftHashDF("_hash") === rightHashDF("_hash"), "full")

    if (stage.persist && !transformedDF.isStreaming) {
      transformedDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
    }   

    val outputIntersectionDF = transformedDF.filter(col("_1").isNotNull).filter(col("_2").isNotNull).select(col("_1.*")).drop("_hash")
    val outputLeftDF = transformedDF.filter(col("_2").isNull).select(col("_1.*")).drop("_hash")
    val outputRightDF = transformedDF.filter(col("_1").isNull).select(col("_2.*")).drop("_hash")

    // register views
    stage.outputIntersectionView match {
      case Some(oiv) => outputIntersectionDF.createOrReplaceTempView(oiv)
      case None => 
    }
    stage.outputLeftView match {
      case Some(olv) => outputLeftDF.createOrReplaceTempView(olv)
      case None => 
    }
    stage.outputRightView match {
      case Some(orv) => outputRightDF.createOrReplaceTempView(orv)
      case None => 
    }    

    Option(outputIntersectionDF)
  }

}
