package com.thatdot.quine.app.routes

/** Whether the experimental QuinePattern engine is enabled via the `qp.enabled` system property.
  *
  * The Quine language server (the Cypher LSP behind the query editor) is the QuinePattern frontend:
  * its diagnostics, semantic tokens, completion, hover, and query-kind routing all assert
  * QuinePattern's view of a query. By default Quine executes queries with the openCypher engine, so
  * surfacing those would diverge from what actually runs. When QuinePattern is disabled (the
  * default) the LSP routes are therefore not served, and the browser editor — told so through the
  * injected bootstrap config — runs in basic mode (client-side Monarch highlighting only). Reading
  * the flag here keeps the route gating and the browser config in agreement on one source of truth.
  */
object QuinePatternSettings {
  def isEnabled: Boolean = sys.props.get("qp.enabled").flatMap(_.toBooleanOption).getOrElse(false)
}
