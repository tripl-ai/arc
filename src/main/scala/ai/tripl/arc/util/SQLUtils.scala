package ai.tripl.arc.util

object SQLUtils {

  def injectParameters(sql: String, params: Map[String, String], allowMissing: Boolean)(implicit logger: ai.tripl.arc.util.log.logger.Logger): String = {
    // replace sqlParams parameters
    var stmt = params.foldLeft(sql) {
      case (stmt, (k,v)) => {
          val matchPlaceholderRegex = "[$][{]\\s*" + k + "\\s*(?:=[^}]+)?[}]"

          // throw error if no match found
          if (!allowMissing && matchPlaceholderRegex.r.findAllIn(stmt).length == 0) {
            throw new Exception(s"No placeholder found for parameter: '${k}'.")
          }

          matchPlaceholderRegex.r.replaceAllIn(stmt, v)
      }
    }

    // find missing replacements
    val placeholdersRegex = "[$][{](\\w*)(?:=[^}]+)?[}]"

    // // replace any environment variable parameters
    // val remainingValues = placeholdersRegex.r.findAllMatchIn(stmt)
    // stmt = remainingValues.foldLeft(stmt) {
    //   case (stmt, m) => {
    //     val k = m.group(1)
    //     envOrNone(k) match {
    //       case Some(v) => {
    //         val matchPlaceholderRegex = "[$][{]\\s*" + k + "\\s*(?:=[^}]+)?[}]"
    //         matchPlaceholderRegex.r.replaceAllIn(stmt, v)
    //       }
    //       case None => stmt
    //     }
    //   }
    // }

    // find any values not replaced
    val missingValues = placeholdersRegex.r.findAllIn(stmt).toList
    if (missingValues.length != 0) {
      throw new Exception(s"""No replacement value found in parameters: ${params.keys.toList.mkString("[", ", ", "]")} for placeholders: ${missingValues.mkString("[", ", ", "]")}.""")
    }

    stmt
  }
}
