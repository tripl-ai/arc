package ai.tripl.arc.util


object MaskUtils {

  def maskParams(mask: List[String])(params: Map[String, String]): Map[String, String] = {
    params.map({case (key, value) => {
        val maskedValue = if (mask.contains(key)) {
            "*" * value.length
        } else {
            value
        }
        (key, maskedValue)
    }})
  }

}

