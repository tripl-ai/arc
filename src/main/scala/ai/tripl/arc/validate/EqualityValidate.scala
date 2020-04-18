package ai.tripl.arc.validate

import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ai.tripl.arc.api.API._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.Utils

class EqualityValidate extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "leftView" :: "rightView" :: "params" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val leftView = getValue[String]("leftView")
    val rightView = getValue[String]("rightView")
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, description, leftView, rightView, invalidKeys) match {
      case (Right(name), Right(description), Right(leftView), Right(rightView), Right(invalidKeys)) =>

        val stage = EqualityValidateStage(
          plugin=this,
          name=name,
          description=description,
          leftView=leftView,
          rightView=rightView,
          params=params
        )

        stage.stageDetail.put("leftView", leftView)
        stage.stageDetail.put("params", params.asJava)
        stage.stageDetail.put("rightView", rightView)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, leftView, rightView, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class EqualityValidateStage(
    plugin: EqualityValidate,
    name: String,
    description: Option[String],
    leftView: String,
    rightView: String,
    params: Map[String, String]
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    EqualityValidateStage.execute(this)
  }
}

object EqualityValidateStage {

  def execute(stage: EqualityValidateStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    val rawLeftDF = spark.table(stage.leftView)
    val rawRightDF = spark.table(stage.rightView)

    // remove any internal fields as some will be added by things like the ParquetExtract which includes filename
    val leftInternalFields = rawLeftDF.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)
    val rightInternalFields = rawRightDF.schema.filter(field => { field.metadata.contains("internal") && field.metadata.getBoolean("internal") == true }).map(_.name)
    val leftDF = rawLeftDF.drop(leftInternalFields:_*)
    val rightDF = rawRightDF.drop(rightInternalFields:_*)

    // test column count equality
    val leftExceptRightColumns = leftDF.columns diff rightDF.columns
    val rightExceptLeftColumns = rightDF.columns diff leftDF.columns
    if (leftExceptRightColumns.length != 0 || rightExceptLeftColumns.length != 0) {
      stage.stageDetail.put("leftExceptRightColumns", leftExceptRightColumns)
      stage.stageDetail.put("rightExceptLeftColumns", rightExceptLeftColumns)

      throw new Exception(s"""EqualityValidate ensures the two input datasets are the same (including column order), but '${stage.leftView}' (${leftDF.columns.length} columns) contains columns: [${leftExceptRightColumns.map(fieldName => s"'${fieldName}'").mkString(", ")}] that are not in '${stage.rightView}' and '${stage.rightView}' (${rightDF.columns.length} columns) contains columns: [${rightExceptLeftColumns.map(fieldName => s"'${fieldName}'").mkString(", ")}] that are not in '${stage.leftView}'. Columns are not equal so cannot the data be compared.""") with DetailException {
        override val detail = stage.stageDetail
      }
    }

    // test column order equality
    if (leftDF.schema.map(_.name).toArray.deep != rightDF.schema.map(_.name).toArray.deep) {
      stage.stageDetail.put("leftColumns", leftDF.schema.map(_.name).toArray)
      stage.stageDetail.put("rightColumns", rightDF.schema.map(_.name).toArray)

      throw new Exception(s"""EqualityValidate ensures the two input datasets are the same (including column order), but '${stage.leftView}' contains columns (ordered): [${leftDF.columns.map(fieldName => s"'${fieldName}'").mkString(", ")}] and '${stage.rightView}' contains columns (ordered): [${rightDF.columns.map(fieldName => s"'${fieldName}'").mkString(", ")}]. Columns are not equal so cannot the data be compared.""") with DetailException {
        override val detail = stage.stageDetail
      }
    }

    // test column type equality
    if (leftDF.schema.map(_.dataType).toArray.deep != rightDF.schema.map(_.dataType).toArray.deep) {
      stage.stageDetail.put("leftColumnsTypes", leftDF.schema.map(_.dataType.typeName).toArray)
      stage.stageDetail.put("rightColumnsTypes", rightDF.schema.map(_.dataType.typeName).toArray)

      throw new Exception(s"""EqualityValidate ensures the two input datasets are the same (including column order), but '${stage.leftView}' contains column types (ordered): [${leftDF.schema.map(_.dataType.typeName).toArray.map(fieldType => s"'${fieldType}'").mkString(", ")}] and '${stage.rightView}' contains column types (ordered): [${rightDF.schema.map(_.dataType.typeName).toArray.map(fieldType => s"'${fieldType}'").mkString(", ")}]. Columns are not equal so cannot the data be compared.""") with DetailException {
        override val detail = stage.stageDetail
      }
    }

    // do not test column nullable equality

    // do a full join on a calculated hash of all values in row on each dataset
    // trying to calculate the hash value inside the joinWith method produced an inconsistent result
    val leftHashDF = leftDF.withColumn("_hash", sha2(to_json(struct(leftDF.columns.map(col):_*)),512))
    val rightHashDF = rightDF.withColumn("_hash", sha2(to_json(struct(rightDF.columns.map(col):_*)),512))
    val transformedDF = leftHashDF.joinWith(rightHashDF, leftHashDF("_hash") === rightHashDF("_hash"), "full")

    val leftExceptRight = transformedDF.filter(col("_2").isNull)
    val rightExceptLeft = transformedDF.filter(col("_1").isNull)
    val leftExceptRightCount = leftExceptRight.count
    val rightExceptLeftCount = rightExceptLeft.count

    if (leftExceptRightCount != 0 || rightExceptLeftCount != 0) {
      stage.stageDetail.put("leftExceptRightCount", java.lang.Long.valueOf(leftExceptRightCount))
      stage.stageDetail.put("rightExceptLeftCount", java.lang.Long.valueOf(rightExceptLeftCount))

      throw new Exception(s"EqualityValidate ensures the two input datasets are the same (including column order), but '${stage.leftView}' (${leftDF.count} rows) contains ${leftExceptRightCount} rows that are not in '${stage.rightView}' and '${stage.rightView}' (${rightDF.count} rows) contains ${rightExceptLeftCount} rows which are not in '${stage.leftView}'.") with DetailException {
        override val detail = stage.stageDetail
      }
    }

    None
  }
}