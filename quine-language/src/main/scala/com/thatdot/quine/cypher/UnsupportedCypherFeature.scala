package com.thatdot.quine.cypher

/** A Cypher construct that parses under the openCypher grammar but Quine does not support (an
  * undirected relationship being the motivating case).
  *
  * The AST visitors have a return type fixed by the generated `CypherBaseVisitor`, so they have no
  * channel to report a diagnostic. This exception is that channel: a visitor throws it, and
  * [[com.thatdot.quine.cypher.phases.ParserPhase]] catches it at the phase boundary and turns it
  * into a [[com.thatdot.quine.language.diagnostic.Diagnostic]] — so the construct surfaces as an
  * error instead of crashing analysis, and never enters the AST.
  */
final case class UnsupportedCypherFeature(featureMessage: String) extends Exception(featureMessage)
