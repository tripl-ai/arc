package ai.tripl.arc.util

object SQLUtils {

  def injectParameters(sql: String, params: Map[String, String], allowMissing: Boolean)(implicit logger: ai.tripl.arc.util.log.logger.Logger): String = {
    // replace sql parameters
    // using regex from the apache zeppelin project
    val stmt = params.foldLeft(sql) {
      case (sql, (k,v)) => {
          val placeholderRegex = "[$][{]\\s*" + k + "\\s*(?:=[^}]+)?[}]"

          // throw error if no match found
          if (!allowMissing && placeholderRegex.r.findAllIn(sql).length == 0) {
            throw new Exception(s"No placeholder found for parameter: '${k}'.")
          }

          placeholderRegex.r.replaceAllIn(sql, v)
      }
    }

    // find missing replacements
    val missingRegex = "[$][{](\\w*)(?:=[^}]+)?[}]"
    val missingValues = missingRegex.r.findAllIn(stmt).toList
    if (missingValues.length != 0) {
      throw new Exception(s"""No replacement value found in parameters: ${params.keys.toList.mkString("[", ", ", "]")} for placeholders: ${missingValues.mkString("[", ", ", "]")}.""")
    }

    stmt
  }
}
