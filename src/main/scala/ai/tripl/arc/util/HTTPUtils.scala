package ai.tripl.arc.util

object HTTPUtils {

  // mask headers to 20% length in this list to prevent logs from printing keys
  // todo: this could be passed in as a list of headers to mask
  def maskHeaders(maskHeaders: List[String])(headers: Map[String, String]): Map[String, String] = {
    headers.map({case (key, value) => {
        val maskedValue = if (maskHeaders.contains(key)) {
            "*" * value.length
        } else {
            value
        }
        (key, maskedValue)
    }})
  }
}
