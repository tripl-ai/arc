package ai.tripl.arc.config

import scala.collection.JavaConverters._

object Error {

    sealed trait Error

    type Errors =  List[ConfigError]

    case class ConfigError(path: String, lineNumber: Option[Int], message: String) extends Error

    case class StageError(idx: Int, stage: String, lineNumber: Int, errors: Errors) extends Error

    object ConfigError {

        def err(path: String, lineNumber: Option[Int], message: String): Errors = ConfigError(path, lineNumber, message) :: Nil

    }

    type StringConfigValue = Either[Errors, String]

    def stringOrDefault(sv: StringConfigValue, default: String): String = {
      sv match {
        case Right(v) => v
        case Left(err) => default
      }
    }

    def errToString(err: Error): String = {
        err match {
            case StageError(idx, stage, lineNumber, configErrors) => {
                s"""Stage: $idx '${stage}' (starting on line ${lineNumber}):\n${configErrors.map(e => "  - " + errToString(e)).mkString("\n")}"""
            }

            case ConfigError(attribute, lineNumber, message) => {
                lineNumber match {
                    case Some(ln) => s"""${attribute} (Line ${ln}): $message"""
                    case None => s"""${attribute}: $message"""
                }
            }
        }
    }

    def errToSimpleString(err: Error): String = {
        err match {
            case StageError(_, stage, lineNumber, configErrors) => {
                s"""${configErrors.map(e => "- " + errToSimpleString(e)).mkString("\n")}"""
            }

            case ConfigError(attribute, lineNumber, message) => {
                lineNumber match {
                    case Some(ln) => s"""${attribute} (Line ${ln}): $message"""
                    case None => s"""${attribute}: $message"""
                }
            }
        }
    }


    def errorsToJSON(err: Error): java.util.HashMap[String, Object] = {
      err match {
        case StageError(idx, stage, lineNumber, configErrors) => {
          val stageErrorMap = new java.util.HashMap[String, Object]()
          stageErrorMap.put("stageIndex", Integer.valueOf(idx))
          stageErrorMap.put("stage", stage)
          stageErrorMap.put("lineNumber", Integer.valueOf(lineNumber))
          stageErrorMap.put("errors", configErrors.map(configError => errorsToJSON(configError)).asJava)
          stageErrorMap
        }
        case ConfigError(attribute, lineNumber, message) => {
          val configErrorMap = new java.util.HashMap[String, Object]()
          lineNumber match {
            case Some(ln) => {
              configErrorMap.put("attribute", attribute)
              configErrorMap.put("lineNumber", Integer.valueOf(ln))
              configErrorMap.put("message", message)
            }
            case None => {
              configErrorMap.put("attribute", attribute)
              configErrorMap.put("message", message)
            }
          }
          configErrorMap
        }
      }
    }

    def pipelineErrorMsg(errors: List[Error]): String = {
        errors.map(e => s"${errToString(e)}").mkString("\n")
    }

    def pipelineSimpleErrorMsg(errors: List[Error]): String = {
        errors.map(e => s"${errToSimpleString(e)}").mkString("\n")
    }

    def pipelineErrorJSON(errors: List[Error]): java.util.List[java.util.HashMap[String, Object]] = {
        errors.map(e => errorsToJSON(e)).asJava
    }

    }