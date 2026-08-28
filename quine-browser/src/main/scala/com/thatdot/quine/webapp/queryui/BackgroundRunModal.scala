package com.thatdot.quine.webapp.queryui

import com.raquo.laminar.api.L._
import io.circe.Json

import com.thatdot.quine.openapi.{OpenApiParser, ParsedSpec}
import com.thatdot.quine.webapp.components.streams.{EmbeddedEditorConfig, SchemaFormRenderer}
import com.thatdot.quine.webapp.resultspanel.tapmodal.TapModalStyles
import com.thatdot.quine.webapp.util.{BackgroundQueryRequest, Pot, SignalSample}

/** "Run in background" — the query bar's dialog for dispatching the current buffer as an
  * out-of-band run.
  *
  * Reuses the tap modal's shell classes (overlay, dialog, header, body) and its shell
  * semantics — host-owned open state, outside-click, Escape, and a body built only while open —
  * exactly as `ExplorerSettingsModal` does.
  *
  * The form itself is the same schema-driven renderer the Streams page's create forms use, run
  * over the `BackgroundQueryDef` schema from the live OpenAPI document. That is what gives this
  * dialog the full destination union — Kafka, Kinesis, SNS, HTTP endpoints, Cypher, files —
  * with the same widgets and validation as everywhere else, and keeps it correct as the server's
  * schema changes. The host loads the spec lazily (see `ApiSpecCache`), so the Explorer pays for
  * it only once someone opens this dialog.
  *
  * `query` is not part of the form: it comes from the editor buffer and is shown read-only, then
  * merged into the body at submit. Everything else the schema declares — destinations, name,
  * parameters, status retention — is rendered from the spec.
  *
  * Pure form: it assembles the request body and hands it to `onSubmit`; the host owns the call,
  * the error, and what happens next.
  */
object BackgroundRunModal {

  /** The request-body schema, by name. Looked up directly rather than through an
    * `ApiOperationRegistry` (which is the Streams page's endpoint-discovery concern) — this
    * dialog targets exactly one known body type.
    */
  private val SchemaName = "BackgroundQueryDef"

  /** @param openSignal whether the dialog is currently shown
    * @param setOpen close (`false`) or (re)open (`true`); Escape and outside-click call it with
    *                `false`
    * @param query the buffer that will be run, shown read-only for confirmation
    * @param spec the parsed OpenAPI document, which the host loads on first open
    * @param editorConfig threading for the Cypher editors the renderer embeds in query fields
    * @param onSubmit receives the assembled `BackgroundQueryDef` body
    * @param submitting true while the host's request is in flight
    * @param error the host's failure message, if the last attempt failed
    */
  def apply(
    openSignal: Signal[Boolean],
    setOpen: Observer[Boolean],
    query: Signal[String],
    spec: Signal[Pot[ParsedSpec]],
    editorConfig: EmbeddedEditorConfig,
    onSubmit: Json => Unit,
    submitting: Signal[Boolean],
    error: Signal[Option[String]],
  ): HtmlElement = {

    def close(): Unit = setOpen.onNext(false)

    div(
      cls := TapModalStyles.overlay,
      display <-- openSignal.map(if (_) "flex" else "none"),
      onClick.filter(e => e.target == e.currentTarget) --> (_ => close()),
      // Gated on `openSignal`: the overlay stays mounted (only `display` toggles), so without
      // the gate every app-wide Escape would close a dialog that isn't showing.
      documentEvents(_.onKeyDown)
        .filter(_.key == "Escape")
        .withCurrentValueOf(openSignal) --> { case (_, open) => if (open) close() },
      div(
        cls := s"${TapModalStyles.dialog} ${BackgroundRunModalStyles.dialog}",
        onClick.stopPropagation --> (_ => ()),
        div(
          cls := TapModalStyles.header,
          span(cls := TapModalStyles.title, "Run in background"),
          button(
            tpe := "button",
            cls := TapModalStyles.closeButton,
            title := "Close",
            onClick --> (_ => close()),
            "×",
          ),
        ),
        div(
          cls := TapModalStyles.body,
          // Keyed on the open flag *and* the spec's load phase, so the form is built once the
          // spec lands and torn down on close — the latter both frees the renderer's binders
          // and gives each open a clean set of fields rather than the last attempt's.
          child <-- openSignal.combineWith(spec).map {
            case (false, _) => emptyNode
            case (true, Pot.Empty) | (true, Pot.Pending) => loading()
            case (true, Pot.Failed(message)) => specUnavailable(message)
            case (true, ready) =>
              ready.toOption match {
                case Some(parsed) => form(query, parsed, editorConfig, onSubmit, submitting, error, () => close())
                case None => specUnavailable("The API specification could not be loaded.")
              }
          },
        ),
      ),
    )
  }

  /** Assemble the `BackgroundQueryDef` request body from the form state and the editor buffer.
    *
    * `query` is merged in from the buffer; it is stripped from the rendered schema so the user
    * isn't asked for it twice. Destinations default to `Drop` — see
    * [[BackgroundQueryRequest.withDefaultDestination]], shared with the Streams page's form.
    */
  private[queryui] def buildBody(formState: Json, query: String): Json =
    BackgroundQueryRequest.withDefaultDestination(
      formState.deepMerge(Json.obj("query" -> Json.fromString(query))),
    )

  private def loading(): HtmlElement =
    div(
      cls := "text-center py-4",
      div(cls := "spinner-border text-primary", role := "status"),
      p(cls := "mt-3 text-body-secondary mb-0", "Loading API specification…"),
    )

  private def specUnavailable(message: String): HtmlElement =
    div(cls := "alert alert-danger mb-0", s"Could not load the form: $message")

  private def form(
    query: Signal[String],
    spec: ParsedSpec,
    editorConfig: EmbeddedEditorConfig,
    onSubmit: Json => Unit,
    submitting: Signal[Boolean],
    error: Signal[Option[String]],
    close: () => Unit,
  ): HtmlElement = {
    val formState = Var(Json.obj())

    // Strip `query`: it comes from the editor buffer, shown read-only above, and is merged back
    // at submit. Leaving it in would ask for the query twice and let the two disagree.
    val renderedSchema: Option[com.thatdot.quine.openapi.SchemaNode] = spec.schemas
      .get(SchemaName)
      .map(OpenApiParser.resolveNode(_, spec.schemas))
      .map(node => node.copy(properties = node.properties.map(_.removed("query"))))

    // Whether the user picked a destination. Not a gate on submitting — [[buildBody]] defaults
    // one in — only what the note below the form says will happen.
    val hasDestination: Signal[Boolean] =
      formState.signal.map(BackgroundQueryRequest.destinationsOf(_).nonEmpty)

    def submit(): Unit =
      // One-shot read: the buffer can change under an open dialog, so the query is taken at
      // submit time rather than when the dialog was built.
      onSubmit(buildBody(formState.now(), SignalSample.now(query)))

    div(
      cls := BackgroundRunModalStyles.form,
      div(
        cls := BackgroundRunModalStyles.field,
        label(cls := BackgroundRunModalStyles.fieldLabel, "Query"),
        pre(cls := BackgroundRunModalStyles.queryPreview, child.text <-- query),
      ),
      renderedSchema match {
        case Some(schema) =>
          SchemaFormRenderer.render(schema, spec, Nil, formState, editorConfig, isRequired = true)
        case None =>
          div(cls := "alert alert-warning", s"$SchemaName not found in the API specification.")
      },
      child <-- error.map {
        case Some(message) => div(cls := "alert alert-danger mb-0", message)
        case None => emptyNode
      },
      div(
        cls := BackgroundRunModalStyles.footer,
        // States the default rather than blocking on it: running a query just to watch the
        // results here is the common case from a query bar, and making that the path of least
        // resistance beats making the user configure a destination to reach it.
        child <-- hasDestination.map {
          case true => emptyNode
          case false =>
            span(
              cls := BackgroundRunModalStyles.footerHint,
              "No destination set — results will stream to this browser only.",
            )
        },
        button(
          tpe := "button",
          cls := "btn btn-secondary",
          onClick --> (_ => close()),
          "Cancel",
        ),
        button(
          tpe := "button",
          cls := "btn btn-primary",
          disabled <-- submitting,
          child <-- submitting.map { busy =>
            if (busy) span(span(cls := "spinner-border spinner-border-sm me-1"), "Starting…")
            else span("Run")
          },
          onClick --> (_ => submit()),
        ),
      ),
    )
  }
}

/** CSS class names for [[BackgroundRunModal]]; the rules live in `common.css`. The modal shell
  * reuses `TapModalStyles`, and the fields themselves are rendered by `SchemaFormRenderer` in
  * its own (Bootstrap) classes, so only the width modifier, the query preview, and the footer
  * are declared here.
  */
object BackgroundRunModalStyles {
  val dialog = "background-run-dialog" // width modifier composed with `.tap-modal-dialog`
  val form = "background-run-form"
  val field = "background-run-field"
  val fieldLabel = "background-run-field-label"
  val queryPreview = "background-run-query-preview"
  val footer = "background-run-footer"
  val footerHint = "background-run-footer-hint"
}
