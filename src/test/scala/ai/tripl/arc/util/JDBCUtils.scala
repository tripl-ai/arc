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
import ai.tripl.arc.util.JDBCUtils
import ai.tripl.arc.util.ConfigUtils._
import ai.tripl.arc.config.Error
import ai.tripl.arc.config.Error._

import com.typesafe.config._


class JDBCUtilsSuite extends FunSuite {

  test("Test maskPassword") { 
    assert(JDBCUtils.maskPassword("jdbc:postgresql://localhost/test?user=fred&password=secret&ssl=true") == "jdbc:postgresql://localhost/test?user=fred&password=******&ssl=true")
    assert(JDBCUtils.maskPassword("jdbc:postgresql://localhost/test?user=fred&password=secret") == "jdbc:postgresql://localhost/test?user=fred&password=******")
    assert(JDBCUtils.maskPassword("jdbc:postgresql://localhost/test?user=fred&password=secret&password=secret") == "jdbc:postgresql://localhost/test?user=fred&password=******&password=******")
    assert(JDBCUtils.maskPassword("jdbc:postgresql://localhost/test?user=fred&password=secret&password=sec&password=secret") == "jdbc:postgresql://localhost/test?user=fred&password=******&password=******&password=******")
    assert(JDBCUtils.maskPassword("jdbc:postgresql://localhost/test?user=fred&ssl=true") == "jdbc:postgresql://localhost/test?user=fred&ssl=true")
  }  

}
