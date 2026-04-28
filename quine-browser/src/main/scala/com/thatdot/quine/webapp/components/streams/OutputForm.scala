package com.thatdot.quine.webapp.components.streams

import scala.concurrent.Future

import com.raquo.laminar.api.L._
import io.circe.Json
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

import com.thatdot.quine.openapi._

/** Inline form for adding a new output to a standing query. Rendered within the
  * expanded row of the Standing Queries table so the add-output experience stays
  * visually attached to its SQ rather than flying in as a modal panel.
  *
  * Pure form: receives a schema to render, emits assembled JSON via `onSubmit`.
  * The parent handles the API call and reports the result.
  *
  * State is passed in as [[OutputForm.State]] so the panel-level callers can keep
  * the user's in-progress input alive across the 5-second polling rebuild of the
  * table — the form itself is a pure projection of those Vars.
  */
object OutputForm {

  final case class State(
    outputName: Var[String],
    formState: Var[Json],
    submit: SubmitState,
  ) {
    def reset(): Unit = {
      outputName.set("")
      formState.set(Json.obj())
      submit.reset()
    }
  }

  def freshState(): State =
    State(Var(""), Var(Json.obj()), new SubmitState)

  def apply(
    sqName: String,
    spec: ParsedSpec,
    outputSchema: Option[SchemaNode],
    state: State,
    onSubmit: Json => Future[Either[String, Json]],
    onComplete: () => Unit,
    onCancel: () => Unit,
  ): HtmlElement = {
    // Strip the schema's `name` property — the form collects the output name via its
    // dedicated input so users don't see the field twice. Injected at submit time.
    val renderedSchema = outputSchema
      .map(s => s.copy(properties = s.properties.map(_.removed("name"))))

    div(
      cls := "border rounded p-3 mt-2 bg-body",
      div(
        cls := "d-flex justify-content-between align-items-center mb-2",
        strong(cls := "small", s"Add output to '$sqName'"),
      ),
      // Output name
      div(
        cls := "mb-2",
        label(
          cls := "form-label small fw-semibold mb-1",
          "Name",
          span(cls := "text-danger ms-1", "*"),
        ),
        input(
          cls := "form-control form-control-sm",
          typ := "text",
          placeholder := "my-output",
          controlled(value <-- state.outputName.signal, onInput.mapToValue --> state.outputName),
        ),
      ),
      // Schema-driven fields. isRequired=true so the top-level workflow schema renders
      // directly instead of being wrapped in the optional-object "Include ..." toggle.
      renderedSchema match {
        case Some(schema) =>
          SchemaFormRenderer.render(schema, spec, Nil, state.formState, isRequired = true)
        case None =>
          div(cls := "alert alert-warning", "Output schema not found in API spec.")
      },
      ErrorAlert(state.submit.error.signal),
      // Actions
      div(
        cls := "d-flex justify-content-end mt-2",
        button(
          cls := "btn btn-sm btn-secondary me-2",
          "Cancel",
          onClick --> { _ => onCancel() },
        ),
        FormSubmit.submitButton(
          idleLabel = "Add Output",
          busyLabel = "Adding...",
          state = state.submit,
          canSubmit = state.outputName.signal.map(_.trim.nonEmpty),
          sizeSmall = true,
        ) { () =>
          val body = state.formState.now().deepMerge(Json.obj("name" -> Json.fromString(state.outputName.now())))
          state.submit.run(onSubmit(body))(_ => onComplete())
        },
      ),
    )
  }
}
