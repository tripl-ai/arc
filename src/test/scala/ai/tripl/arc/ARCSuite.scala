package ai.tripl.arc

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.commons.lang3.exception.ExceptionUtils
import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ai.tripl.arc.util._
import ai.tripl.arc.api._
import ai.tripl.arc.api.API._

import ai.tripl.arc.util.TestUtils

class ARCSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  
  val targetFile = FileUtils.getTempDirectoryPath()
  val inputView = "inputView"
  val outputView = "outputView"

  before {
    implicit val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.ui.port", "9999")
      .appName("Spark ETL Test")
      .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")       

    session = spark
  }

  after {
    session.stop()
  }

}
