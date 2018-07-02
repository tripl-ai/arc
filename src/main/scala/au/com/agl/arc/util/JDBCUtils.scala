package au.com.agl.arc.util

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

}
