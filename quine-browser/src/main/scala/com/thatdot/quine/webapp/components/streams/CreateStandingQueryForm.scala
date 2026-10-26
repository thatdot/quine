package com.thatdot.quine.webapp.components.streams

import scala.concurrent.Future

import com.raquo.laminar.api.L._
import io.circe.Json
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

import com.thatdot.quine.openapi._

/** Schema-driven, single-surface form for creating a new standing query.
  *
  * Pure form: receives a schema to render, emits assembled JSON via `onSubmit`.
  * The parent handles the API call and reports the result.
  *
  * The whole form is rendered from the `StandingQueryDefinition` schema via
  * [[SchemaFormRenderer]]. `name` is handled as a dedicated input at the top
  * (stripped from the rendered schema to avoid duplication) and merged back in
  * at submit time. Everything else — pattern, outputs, includeCancellations,
  * inputBufferSize — comes from the spec, so new schema fields appear
  * automatically.
  */
object CreateStandingQueryForm {

  def apply(
    spec: ParsedSpec,
    createSchema: Option[SchemaNode],
    queryParams: List[QueryParam],
    onSubmit: (Json, Map[String, String]) => Future[Either[String, Json]],
    onComplete: () => Unit,
    onCancel: () => Unit,
    editorConfig: EmbeddedEditorConfig,
  ): HtmlElement = {
    val nameVar = Var("")
    val formState = Var(Json.obj())
    val submit = new SubmitState

    // Strip `name` from the schema — the form collects the name via a dedicated
    // input so users don't see the field twice. Merged back on submit. The
    // endpoint's HTTP query parameters (e.g. propagateTo, wakeUpParallelism) are
    // folded in as optional properties so they render inside "More Options"; they
    // are split back out of the request body and sent as query params on submit.
    val renderedSchema = createSchema
      .map(s => s.copy(properties = s.properties.map(_.removed("name"))))
      .map(s => QueryParamFields.mergeInto(s, queryParams))

    div(
      cls := "p-2",
      // Dedicated name input
      div(
        cls := "mb-3",
        label(
          cls := "form-label small fw-semibold mb-1",
          "Name",
          span(cls := "text-danger ms-1", "*"),
        ),
        input(
          cls := "form-control form-control-sm",
          typ := "text",
          placeholder := "my-standing-query",
          controlled(value <-- nameVar.signal, onInput.mapToValue --> nameVar),
        ),
      ),
      // Pattern / outputs / options / ... — all schema-driven.
      renderedSchema match {
        case Some(schema) =>
          // Novelty destinations get their assists beneath the fields they fill in; the standing query
          // doesn't exist yet, so there are no live results to sample.
          val assists = NoveltyAssists(None, formState, editorConfig.serverUrl.getOrElse(""))
          SchemaFormRenderer.render(
            schema,
            spec,
            Nil,
            formState,
            editorConfig.copy(fieldAddons = assists),
            isRequired = true,
          )
        case None =>
          div(
            cls := "alert alert-warning",
            "Create standing query schema not found in API spec.",
          )
      },
      ErrorAlert(submit.error.signal),
      // Actions
      div(
        cls := "d-flex justify-content-end mt-3",
        button(
          cls := "btn btn-secondary me-2",
          "Cancel",
          onClick --> { _ => onCancel() },
        ),
        FormSubmit.submitButton(
          idleLabel = "Create Standing Query",
          busyLabel = "Creating...",
          state = submit,
          canSubmit = nameVar.signal.map(_.trim.nonEmpty),
        ) { () =>
          val assembled = formState.now()
          val queryParamValues = QueryParamFields.extract(assembled, queryParams)
          val body = QueryParamFields
            .stripFrom(assembled, queryParams)
            .deepMerge(Json.obj("name" -> Json.fromString(nameVar.now())))
          submit.run(onSubmit(body, queryParamValues))(_ => onComplete())
        },
      ),
    )
  }
}
