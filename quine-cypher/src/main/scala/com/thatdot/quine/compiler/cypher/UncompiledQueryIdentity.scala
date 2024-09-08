package com.thatdot.quine.compiler.cypher

import org.opencypher.v9_0.util.symbols

/** The parts of an uncompiled query that are fundamental to that query's identity (for the sake of caching)
  * note that this ignores certain (obvious) equivalences, like queryText that varies only in whitespace
  *
  * @param queryText
  * @param unfixedParameters
  * @param initialColumns
  */
final private[cypher] case class UncompiledQueryIdentity(
  queryText: String,
  unfixedParameters: Seq[String] = Seq.empty,
  initialColumns: Seq[(String, symbols.CypherType)] = Seq.empty,
)
