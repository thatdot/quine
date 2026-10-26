package com.thatdot.quine.webapp.components.streams

import com.raquo.laminar.api.L.HtmlElement

/** Config threaded from [[StreamsPage]] through the panels and [[SchemaFormRenderer]]: the
  * connection config for each [[EmbeddedQueryEditor]], and any extra controls a form adds beside
  * particular fields.
  *
  * @param qpEnabled   whether the QuinePattern language server is enabled (gates the LSP connection)
  * @param serverUrl   REST server base URL (empty/none ⇒ same origin), used to derive the LSP
  *                    WebSocket host so a remote-server embedding dials the right backend
  * @param fieldAddons controls rendered beneath particular fields; a form sets these with `copy`
  *                    before rendering
  */
final case class EmbeddedEditorConfig(
  qpEnabled: Boolean,
  serverUrl: Option[String],
  fieldAddons: FieldAddons = FieldAddons.none,
)

/** Extra controls for particular fields of a schema-rendered form, such as an assist that fills a
  * field in.
  *
  * @param forField given the schema an object is rendered from, one of its field names, and the
  *                 object's path in the form state, the control to render beneath that field, if any
  */
final case class FieldAddons(forField: (String, String, List[String]) => Option[HtmlElement])

object FieldAddons {
  val none: FieldAddons = FieldAddons((_, _, _) => None)
}
