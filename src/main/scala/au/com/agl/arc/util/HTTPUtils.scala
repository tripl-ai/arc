package au.com.agl.arc.util

import java.net.URI
import java.time.Instant

import org.apache.spark.sql._

object HTTPUtils {

  // mask headers to 20% length in this list to prevent logs from printing keys
  // todo: this could be passed in as a list of headers to mask
  def maskHeaders(headers: Map[String, String]): Map[String, String] = {
    val maskHeaders =  "Authorization" :: Nil
    headers.map({case (key, value) => {
        val maskedValue = if (maskHeaders.contains(key)) {
            s"${value.substring(0,(value.length*0.2).toInt)}..."
        } else {
            value
        }
        (key, maskedValue) 
    }})  
  }
}
