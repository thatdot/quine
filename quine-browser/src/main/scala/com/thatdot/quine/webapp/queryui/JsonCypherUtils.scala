package com.thatdot.quine.webapp.queryui

import io.circe.Json

/** Utilities for turning JSON data into Cypher queries. */
object JsonCypherUtils {

  /** Converts an arbitrary string into a valid Cypher property name by
    * surrounding the string in ticks and replacing internal ticks with
    * double ticks.
    */
  def stringToCypherIdent(s: String): String = "`" ++ s.replace("`", "``") ++ "`"

  /** Converts a circe JSON value into an inline Cypher literal string. e.g.,
    * {{{
    *   {"one": 1, "two": [2, false]}   ~>   {`one`: 1, `two`: [2, false]}
    * }}}
    *
    * Note: this function guards against Cypher injection. e.g.,
    * {{{
    *   {"one: 1} // INJECTED!!!": 1}    ~>   {`one: 1} // INJECTED!!!`: 1}
    *   {"one`: 1} // INJECTED!!!": 2}   ~>   {`one``: 1} // INJECTED!!!`: 2}
    * }}}
    *
    * Cypher query parameters should generally be used instead of this function
    * due to the risk of injection.
    */
  def jsonToCypher(json: Json): String = json.fold(
    jsonNull = "null",
    jsonBoolean = b => if (b) "true" else "false",
    // The Cypher syntax for numbers is a _superset_ of the JSON syntax, so toString is safe.
    jsonNumber = _.toString,
    jsonString = s => "\"" ++ s.replace("\\", "\\\\").replace("\"", "\\\"") ++ "\"",
    jsonArray = arr => "[" ++ arr.map(jsonToCypher).mkString(", ") ++ "]",
    jsonObject = obj => {
      val kvPairs = obj.toList.map { case (k, v) => stringToCypherIdent(k) ++ ": " ++ jsonToCypher(v) }
      "{" ++ kvPairs.mkString(", ") ++ "}"
    },
  )
}
