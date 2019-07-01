package ai.tripl.arc

import java.net.URI

import scala.io.Source
import scala.collection.JavaConverters._
import com.fasterxml.jackson.databind.ObjectMapper

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql._

import ai.tripl.arc.api.API._
import ai.tripl.arc.api.{Delimited, Delimiter, QuoteCharacter}
import ai.tripl.arc.util.log.LoggerFactory
import ai.tripl.arc.util.ConfigUtils
import ai.tripl.arc.util.TestUtils
import ai.tripl.arc.util.HTTPUtils
import ai.tripl.arc.util.ConfigUtils._
import ai.tripl.arc.config.Error
import ai.tripl.arc.config.Error._

import com.typesafe.config._


class HTTPUtilsSuite extends FunSuite {

  test("Test maskHeaders") { 
    val actHeaders = Map("Authorization" -> "Basic YWxhZGRpbjpvcGVuc2VzYW1l", "ignore" -> "true")
    val expHeaders = Map("Authorization" -> "******************************", "ignore" -> "true")
    assert(HTTPUtils.maskHeaders("Authorization" :: Nil)(actHeaders) == expHeaders)
  }  

}
