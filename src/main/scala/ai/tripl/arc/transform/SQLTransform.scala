package ai.tripl.arc.transform

import java.net.URI
import scala.collection.JavaConverters._

import org.apache.spark.sql._

import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.SQLUtils
import ai.tripl.arc.util.QueryExecutionUtils
import ai.tripl.arc.util.Utils

class SQLTransform extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "sql" :: "outputView" :: "authentication" :: "persist" :: "sqlParams" :: "params" :: "numPartitions" :: "partitionBy" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val authentication = readAuthentication("authentication")

    // requires 'inputURI' or 'sql'
    val isInputURI = c.hasPath("inputURI")
    val source = if (isInputURI) "inputURI" else "sql"

    val parsedURI = if (isInputURI) getValue[String]("inputURI") |> parseURI("inputURI") _ else Right(new URI(""))
    val inputSQL = if (isInputURI) parsedURI |> textContentForURI("inputURI", authentication) _ else Right("")
    val inlineSQL = if (!isInputURI) getValue[String]("sql") |> verifyInlineSQLPolicy("sql") _ else Right("")
    val sqlParams = readMap("sqlParams", c)
    val sql = if (isInputURI) inputSQL else inlineSQL
    val validSQL = sql |> injectSQLParams(source, sqlParams, false) _ |> validateSQL(source) _
    val outputView = getValue[String]("outputView")
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, description, parsedURI, inlineSQL, sql, validSQL, outputView, persist, numPartitions, partitionBy, invalidKeys) match {
      case (Right(name), Right(description), Right(parsedURI), Right(inlineSQL), Right(sql), Right(validSQL), Right(outputView), Right(persist), Right(numPartitions), Right(partitionBy), Right(invalidKeys)) =>

        val lowerValidSQL = validSQL.toLowerCase

        Seq("now()", "current_date()", "current_timestamp()").foreach { nonDeterministic => 
          if (lowerValidSQL contains nonDeterministic) {
            logger.warn()
              .field("event", "validateConfig")
              .field("name", name)
              .field("type", "SQLTransform")
              .field("message", s"sql contains ${nonDeterministic.toUpperCase} function which may produce non-deterministic result")
              .log()
          }
        }

        // warn for deprecated UDFs
        arcContext.udfPlugins.foreach { plugin =>
          plugin.deprecations.foreach { deprecation =>
            if (lowerValidSQL contains s"${deprecation.function}(") {
              logger.warn()
                .field("event", "validateConfig")
                .field("name", name)
                .field("type", "SQLTransform")
                .field("message", s"sql contains deprecated function '${deprecation.function}' which will be removed in future Arc version. Use `${deprecation.replaceFunction}` instead.")
                .log()
            }            
          }
        }

        val uri = if (isInputURI) Option(parsedURI) else None

        val stage = SQLTransformStage(
          plugin=this,
          name=name,
          description=description,
          inputURI=uri,
          sql=sql,
          outputView=outputView,
          params=params,
          sqlParams=sqlParams,
          persist=persist,
          numPartitions=numPartitions,
          partitionBy=partitionBy
        )

        if (uri.isDefined) stage.stageDetail.put("inputURI", parsedURI.toString)
        stage.stageDetail.put("outputView", outputView)
        stage.stageDetail.put("persist", java.lang.Boolean.valueOf(persist))
        stage.stageDetail.put("sql", sql)
        stage.stageDetail.put("sqlParams", sqlParams.asJava)
        stage.stageDetail.put("params", params.asJava)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, parsedURI, inlineSQL, sql, validSQL, outputView, persist, numPartitions, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class SQLTransformStage(
    plugin: SQLTransform,
    name: String,
    description: Option[String],
    inputURI: Option[URI],
    sql: String,
    outputView: String,
    params: Map[String, String],
    sqlParams: Map[String, String],
    persist: Boolean,
    numPartitions: Option[Int],
    partitionBy: List[String]
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    SQLTransformStage.execute(this)
  }
}

object SQLTransformStage {

  def execute(stage: SQLTransformStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    val stmt = SQLUtils.injectParameters(stage.sql, stage.sqlParams, false)
    stage.stageDetail.put("sql", stmt)

    val transformedDF = try {
      spark.sql(stmt)
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
      stage.stageDetail.put("partitionFilters", QueryExecutionUtils.getPartitionFilters(repartitionedDF.queryExecution.executedPlan).toArray)
      stage.stageDetail.put("dataFilters", QueryExecutionUtils.getDataFilters(repartitionedDF.queryExecution.executedPlan).toArray)
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
