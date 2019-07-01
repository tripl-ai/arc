package ai.tripl.arc.util

import java.sql.DriverManager

object JDBCUtils {

  def checkDriverExists(url: String): Boolean = {
    try {
      DriverManager.getDriver(url)
      true
    } catch {
      case _: Throwable => false
    }
  }

  def escapeName(element: String): String = {
    element match {
      // if element has hypen within [ ] then return it
      case x if x matches "\\[.*-.*\\]" => element
      // if element has no within [ ] then remove [ and ] 
      case x if x matches "\\[[aA-zZ0-9]*\\]" => element.replace("[", "").replace("]", "")
      case _ => element
    }
  }

  // mask password=abc to password=xxx
  def maskPassword(jdbcURL: String): String = {
    val passwordRegex = "password=([^&]*)"
    val matches = passwordRegex.r.findAllIn(jdbcURL).toList
    if (matches.length == 0) {
      jdbcURL
    } else {
      passwordRegex.r.replaceAllIn(jdbcURL, s"password=${"*" * (matches(0).length - 9)}")
    }
  }  
}
