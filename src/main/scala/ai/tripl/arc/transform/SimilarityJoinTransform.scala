package ai.tripl.arc.transform

import java.util.UUID
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{CountVectorizer, MinHashLSH, MinHashLSHModel, NGram, RegexTokenizer}

import com.typesafe.config._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.Utils
import ai.tripl.arc.util.DetailException

class SimilarityJoinTransform extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "leftView" :: "leftFields" :: "rightView" :: "rightFields" :: "outputView" :: "persist" :: "shingleLength" :: "numHashTables" :: "threshold" :: "caseSensitive" :: "numPartitions" :: "partitionBy" :: "params" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val leftView = getValue[String]("leftView")
    val leftFields = getValue[StringList]("leftFields")
    val rightView = getValue[String]("rightView")
    val rightFields = getValue[StringList]("rightFields")
    val outputView = getValue[String]("outputView")
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val shingleLength = getValue[Int]("shingleLength", default = Some(3))
    val numHashTables = getValue[Int]("numHashTables", default = Some(5))
    val threshold = getValue[java.lang.Double]("threshold", default = Some(0.8))
    val caseSensitive = getValue[java.lang.Boolean]("caseSensitive", default = Some(false))
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    (name, description, leftView, leftFields, rightView, rightFields, outputView, persist, shingleLength, numHashTables, threshold, caseSensitive, partitionBy, numPartitions, invalidKeys) match {
      case (Right(name), Right(description), Right(leftView), Right(leftFields), Right(rightView), Right(rightFields), Right(outputView), Right(persist), Right(shingleLength), Right(numHashTables), Right(threshold), Right(caseSensitive), Right(partitionBy), Right(numPartitions), Right(invalidKeys)) => 
      
        val stage = SimilarityJoinTransformStage(
          plugin=this,
          name=name,
          description=description,
          leftView=leftView,
          leftFields=leftFields,
          rightView=rightView,
          rightFields=rightFields,
          outputView=outputView,
          persist=persist,
          shingleLength=shingleLength,
          numHashTables=numHashTables,
          threshold=threshold,
          caseSensitive=caseSensitive,
          partitionBy=partitionBy,
          numPartitions=numPartitions,
          params=params
        )

        stage.stageDetail.put("leftView", leftView)  
        stage.stageDetail.put("leftFields", leftFields.asJava)  
        stage.stageDetail.put("rightView", rightView)  
        stage.stageDetail.put("rightFields", rightFields.asJava)  
        stage.stageDetail.put("outputView", outputView)  
        stage.stageDetail.put("persist", java.lang.Boolean.valueOf(persist))
        stage.stageDetail.put("shingleLength", java.lang.Integer.valueOf(shingleLength))
        stage.stageDetail.put("numHashTables", java.lang.Integer.valueOf(numHashTables)) 
        stage.stageDetail.put("threshold", java.lang.Double.valueOf(threshold))
        stage.stageDetail.put("caseSensitive", java.lang.Boolean.valueOf(caseSensitive))
        stage.stageDetail.put("partitionBy", partitionBy.asJava)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, leftView, leftFields, rightView, rightFields, outputView, persist, shingleLength, numHashTables, threshold, caseSensitive, partitionBy, numPartitions, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class SimilarityJoinTransformStage(
    plugin: SimilarityJoinTransform,
    name: String, 
    description: Option[String], 
    leftView: String, 
    leftFields: List[String], 
    rightView: String, 
    rightFields: List[String],
    outputView: String,
    persist: Boolean,
    shingleLength: Int,
    numHashTables: Int,
    threshold: Double,
    caseSensitive: Boolean,
    partitionBy: List[String], 
    numPartitions: Option[Int], 
    params: Map[String, String]
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    SimilarityJoinTransformStage.execute(this)
  }
}

object SimilarityJoinTransformStage {

  def execute(stage: SimilarityJoinTransformStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    // create a guid to name the two derived columns (leftView and rightView) to avoid collisions with existing column names
    val uuid = UUID.randomUUID.toString

    // split input string into individual characters
    val regexTokenizer = new RegexTokenizer()
      .setInputCol(uuid)
      .setPattern("")
      .setMinTokenLength(1)      
      .setToLowercase(!stage.caseSensitive)

    // produce ngrams to group the characters
    val nGram = new NGram()
      .setInputCol(regexTokenizer.getOutputCol)
      .setN(stage.shingleLength)

    // convert to vector
    val countVectorizer = new CountVectorizer()
      .setInputCol(nGram.getOutputCol)

    // build locality-sensitive hashing model
    val minHashLSH = new MinHashLSH()
      .setInputCol(countVectorizer.getOutputCol)
      .setNumHashTables(stage.numHashTables)
      .setOutputCol("lsh")

    val pipeline = new Pipeline()
      .setStages(Array(regexTokenizer, nGram, countVectorizer, minHashLSH))

    val transformedDF = try {

      val leftView = spark.table(stage.leftView)   
      val rightView = spark.table(stage.rightView)   

      // create a string space concatenated field
      val inputLeftView = leftView.select(
        col("*"), trim(concat(stage.leftFields.map{ field => when(col(field).isNotNull, concat(col(field).cast(StringType), lit(" "))).otherwise("") }:_*)).alias(uuid)
      )
      val inputRightView = rightView.select(
        col("*"), trim(concat(stage.rightFields.map{ field => when(col(field).isNotNull, concat(col(field).cast(StringType), lit(" "))).otherwise("") }:_*)).alias(uuid)
      )      

      val pipelineModel = pipeline.fit(inputLeftView)

      val datasetA = pipelineModel.transform(inputLeftView)
      val datasetB = pipelineModel.transform(inputRightView)

      val leftOutputColumns = leftView.columns.map{columnName => col(s"datasetA.${columnName}")}
      val rightOutputColumns = rightView.columns.map{columnName => col(s"datasetB.${columnName}")}

      pipelineModel.stages.collectFirst{ case minHashLSHModel: MinHashLSHModel => minHashLSHModel } match {
        case Some(minHashLSHModel) => { 
          minHashLSHModel
            .approxSimilarityJoin(datasetA, datasetB, (1.0-stage.threshold))
            .select((leftOutputColumns ++ rightOutputColumns ++ Seq((lit(1.0)-col("distCol")).alias("similarity"))):_*)
        }
        case None => throw new Exception("could not find MinHashLSHModel in trained model")
      }

    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    // repartition to distribute rows evenly
    val repartitionedDF = stage.partitionBy match {
      case Nil => {
        stage.numPartitions match {
          case Some(numPartitions) => transformedDF.repartition(numPartitions)
          case None => transformedDF
        }
      }
      case partitionBy => {
        // create a column array for repartitioning
        val partitionCols = partitionBy.map(col => transformedDF(col))
        stage.numPartitions match {
          case Some(numPartitions) => transformedDF.repartition(numPartitions, partitionCols:_*)
          case None => transformedDF.repartition(partitionCols:_*)
        }
      }
    }
    if (arcContext.immutableViews) repartitionedDF.createTempView(stage.outputView) else repartitionedDF.createOrReplaceTempView(stage.outputView)

    if (!repartitionedDF.isStreaming) {
      // add partition and predicate pushdown detail to logs
      stage.stageDetail.put("outputColumns", java.lang.Integer.valueOf(repartitionedDF.schema.length))
      stage.stageDetail.put("numPartitions", java.lang.Integer.valueOf(repartitionedDF.rdd.partitions.length))

      if (stage.persist) {
        repartitionedDF.persist(arcContext.storageLevel)
        stage.stageDetail.put("records", java.lang.Long.valueOf(repartitionedDF.count))
      }
    }

    Option(repartitionedDF)
  }
}