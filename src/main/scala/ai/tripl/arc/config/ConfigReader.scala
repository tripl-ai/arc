package ai.tripl.arc.config

import java.lang._

import scala.collection.JavaConverters._

import com.typesafe.config._

import Error._

trait ConfigReader[A] {

    def getValue(path: String, c: Config, default: Option[A] = None, validValues: Seq[A] = Seq.empty): Either[Errors, A] =
        ConfigReader.getConfigValue[A](path, c, expectedType, default, validValues){ read(path, c) }

    def getOptionalValue(path: String, c: Config, default: Option[A], validValues: Seq[A] = Seq.empty): Either[Errors, Option[A]] =
        ConfigReader.getOptionalConfigValue(path, c, expectedType, default, validValues){ read(path, c) }

    def expectedType: String

    def read(path: String, c: Config): A

}

object ConfigReader {

    def getConfigValue[A](path: String, c: Config, expectedType: String,
                            default: Option[A] = None, validValues: Seq[A] = Seq.empty)(read: => A): Either[Errors, A] = {

       def err(lineNumber: Option[Int], msg: String): Either[Errors, A] = Left(ConfigError(path, lineNumber, msg) :: Nil)

       try {
        if (c.hasPath(path)) {
            val value = read
            if (!validValues.isEmpty) {
            if (validValues.contains(value)) {
                Right(value)
            } else {
                err(Some(c.getValue(path).origin.lineNumber()), s"""Invalid value. Valid values are ${validValues.map(value => s"'${value.toString}'").mkString("[",",","]")}.""")
            }
            } else {
            Right(read)
            }
        } else {
            default match {
            case Some(value) => {
                if (!validValues.isEmpty) {
                if (validValues.contains(value)) {
                    Right(value)
                } else {
                    err(None, s"""Invalid default value '$value'. Valid values are ${validValues.map(value => s"'${value.toString}'").mkString("[",",","]")}.""")
                }
                } else {
                Right(value)
                }
            }
            case None => err(None, s"""Missing required attribute '$path'.""")
            }
        }
        } catch {
        case wt: ConfigException.WrongType => err(Some(c.getValue(path).origin.lineNumber()), s"Wrong type, expected: '$expectedType'.")
        case e: Exception => err(Some(c.getValue(path).origin.lineNumber()), s"Unable to read value: ${e.getMessage}")
        }

    }

    def getOptionalConfigValue[A](path: String, c: Config, expectedType: String,
                                        default: Option[A] = None, validValues: Seq[A] = Seq.empty)(read: => A): Either[Errors, Option[A]] = {
        if (c.hasPath(path)) {
        val value = getConfigValue(path, c, expectedType, None, validValues)(read)
        value match {
            case Right(cv) => Right(Option(cv))
            case Left(l) => Left(l) // matching works around typing error
        }
        } else {
        Right(default)
        }
    }

    implicit object StringConfigReader extends ConfigReader[String] {

        val expectedType = "string"

        def read(path: String, c: Config): String = c.getString(path)

    }

    implicit object StringListConfigReader extends ConfigReader[StringList] {

        val expectedType = "string array"

        def read(path: String, c: Config): StringList = c.getStringList(path).asScala.toList

    }

    implicit object IntConfigReader extends ConfigReader[Int] {

        val expectedType = "int"

        def read(path: String, c: Config): Int = c.getInt(path)

    }

    implicit object IntListConfigReader extends ConfigReader[IntList] {

        val expectedType = "integer array"

        def read(path: String, c: Config): IntList = c.getIntList(path).asScala.map(f => f.toInt).toList

    }

    implicit object BooleanConfigReader extends ConfigReader[Boolean] {

        val expectedType = "boolean"

        def read(path: String, c: Config): Boolean = c.getBoolean(path)

    }

    implicit object BooleanListConfigReader extends ConfigReader[BooleanList] {

        val expectedType = "boolean array"

        def read(path: String, c: Config): BooleanList = c.getBooleanList(path).asScala.map(f => f.booleanValue).toList

    }

    implicit object LongConfigReader extends ConfigReader[Long] {

        val expectedType = "long"

        def read(path: String, c: Config): Long = c.getLong(path)

    }

    implicit object LongListConfigReader extends ConfigReader[LongList] {

        val expectedType = "long array"

        def read(path: String, c: Config): LongList = c.getLongList(path).asScala.map(f => f.toLong).toList

    }

    implicit object DoubleConfigReader extends ConfigReader[Double] {

        val expectedType = "double"

        def read(path: String, c: Config): Double = c.getDouble(path)

    }

    implicit object DoubleListConfigReader extends ConfigReader[DoubleList] {

        val expectedType = "double array"

        def read(path: String, c: Config): DoubleList = c.getDoubleList(path).asScala.map(f => f.toDouble).toList

    }

    def getValue[A](path: String, default: Option[A] = None, validValues: Seq[A] = Seq.empty)(implicit c: Config, reader: ConfigReader[A]): Either[Errors, A] = {
        reader.getValue(path, c, default, validValues)
    }

    def getOptionalValue[A](path: String, default: Option[A] = None, validValues: Seq[A] = Seq.empty)(implicit c: Config, reader: ConfigReader[A]): Either[Errors, Option[A]] = {
        reader.getOptionalValue(path, c, default, validValues)
    }

}