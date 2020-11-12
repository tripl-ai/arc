package ai.tripl.arc.plugins.lifecycle

import java.security.SecureRandom

import org.apache.spark.sql.{DataFrame, SparkSession}

import ai.tripl.arc.api.API._
import ai.tripl.arc.plugins.LifecyclePlugin
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.Utils
import ai.tripl.arc.config.Error._

class ChaosMonkey extends LifecyclePlugin with JupyterCompleter {

  val version = Utils.getFrameworkVersion

  val snippet = """%lifecycleplugin
    |{
    |  "type": "ChaosMonkey",
    |  "environments": [
    |    "test"
    |  ],
    |  "strategy": "exception",
    |  "probability": 0.05
    |}""".stripMargin

  val documentationURI = new java.net.URI(s"${baseURI}/plugins/#chaosmonkey")

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], LifecyclePluginInstance] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "environments" :: "probability" :: "strategy" :: Nil
    val probability = getValue[java.lang.Double]("probability") |> doubleMinMax("probability", Some(0), Some(1)) _
    val strategy = getValue[String]("strategy", default = Some("exception"), validValues = "exception" :: Nil) |> parseChaosMonkeyStrategy("strategy") _
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (probability, strategy, invalidKeys) match {
      case (Right(probability), Right(strategy), Right(invalidKeys)) =>
        Right(ChaosMonkeyInstance(
          plugin=this,
          probability=probability,
          strategy=strategy
        ))
      case _ =>
        val allErrors: Errors = List(probability, strategy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val err = StageError(index, this.getClass.getName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

  def parseChaosMonkeyStrategy(path: String)(strategy: String)(implicit c: com.typesafe.config.Config): Either[Errors, ChaosMonkeyStrategy] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._

    strategy.toLowerCase.trim match {
      case "exception" => Right(ChaosMonkeyStrategy.StrategyException)
      case _ => Left(ConfigError(path, None, s"Invalid state. Please raise issue.") :: Nil)
    }
  }
}

sealed trait ChaosMonkeyStrategy {
  def sparkString(): String
}

object ChaosMonkeyStrategy {
  case object StrategyException extends ChaosMonkeyStrategy { val sparkString = "exception" }
  case class StrategyDropRecords(percent: Double) extends ChaosMonkeyStrategy { val sparkString = "dropRecords" }
}

case class ChaosMonkeyInstance(
    plugin: ChaosMonkey,
    probability: Double,
    strategy: ChaosMonkeyStrategy
  ) extends LifecyclePluginInstance {

  override def after(result: Option[DataFrame], stage: PipelineStage, index: Int, stages: List[PipelineStage])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    logger.trace()
      .field("event", "after")
      .field("stage", stage.name)
      .log()

    val secureRandom = new SecureRandom
    if (secureRandom.nextDouble < probability) {
      strategy match {
        case ChaosMonkeyStrategy.StrategyException => throw new Exception("ChaosMonkey triggered and exception thrown.")
        case _ =>
      }
    }
    result
  }
}
