package ai.tripl.arc.util

import org.scalatest.FunSuite

class HTTPUtilsSuite extends FunSuite {

  test("Test maskHeaders") {
    val actHeaders = Map("Authorization" -> "Basic YWxhZGRpbjpvcGVuc2VzYW1l", "ignore" -> "true")
    val expHeaders = Map("Authorization" -> "******************************", "ignore" -> "true")
    assert(HTTPUtils.maskHeaders("Authorization" :: Nil)(actHeaders) == expHeaders)
  }

}
