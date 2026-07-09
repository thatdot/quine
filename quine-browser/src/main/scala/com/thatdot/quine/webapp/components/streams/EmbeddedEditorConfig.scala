package com.thatdot.quine.webapp.components.streams

/** Connection config for the embedded Cypher editors in the Streams forms, threaded from
  * [[StreamsPage]] through the panels and [[SchemaFormRenderer]] to each [[EmbeddedQueryEditor]].
  *
  * @param qpEnabled whether the QuinePattern language server is enabled (gates the LSP connection)
  * @param serverUrl REST server base URL (empty/none ⇒ same origin), used to derive the LSP
  *                  WebSocket host so a remote-server embedding dials the right backend
  */
final case class EmbeddedEditorConfig(qpEnabled: Boolean, serverUrl: Option[String])
