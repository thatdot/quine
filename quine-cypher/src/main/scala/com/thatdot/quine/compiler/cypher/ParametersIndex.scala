package com.thatdot.quine.compiler.cypher

/** Store to track which parameter name lives at what index in the runtime
  * parameter array
  *
  * @param index mapping from name to index
  */
final case class ParametersIndex(index: Map[String, Int])
object ParametersIndex {
  val empty: ParametersIndex = ParametersIndex(Map.empty)
}
