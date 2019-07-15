package ai.tripl.arc.util

import org.scalatest.FunSuite

class JDBCUtilsSuite extends FunSuite {

  test("Test maskPassword") { 
    assert(JDBCUtils.maskPassword("jdbc:postgresql://localhost/test?user=fred&password=secret&ssl=true") == "jdbc:postgresql://localhost/test?user=fred&password=******&ssl=true")
    assert(JDBCUtils.maskPassword("jdbc:postgresql://localhost/test?user=fred&password=secret") == "jdbc:postgresql://localhost/test?user=fred&password=******")
    assert(JDBCUtils.maskPassword("jdbc:postgresql://localhost/test?user=fred&password=secret&password=secret") == "jdbc:postgresql://localhost/test?user=fred&password=******&password=******")
    assert(JDBCUtils.maskPassword("jdbc:postgresql://localhost/test?user=fred&password=secret&password=sec&password=secret") == "jdbc:postgresql://localhost/test?user=fred&password=******&password=******&password=******")
    assert(JDBCUtils.maskPassword("jdbc:postgresql://localhost/test?user=fred&ssl=true") == "jdbc:postgresql://localhost/test?user=fred&ssl=true")
  }

}