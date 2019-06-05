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
}



