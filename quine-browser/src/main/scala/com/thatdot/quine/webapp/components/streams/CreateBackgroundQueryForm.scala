package com.thatdot.quine.webapp.components.streams

import scala.concurrent.Future

import com.raquo.laminar.api.L._
import io.circe.Json
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

import com.thatdot.quine.openapi._
import com.thatdot.quine.webapp.util.BackgroundQueryRequest

/** Schema-driven form for starting a one-off background query from the Streams page.
  *
  * Pure form: receives a schema to render, emits assembled JSON via `onSubmit`. The parent
  * handles the API call and reports the result.
  *
  * The whole form is the `BackgroundQueryDef` schema via [[SchemaFormRenderer]] — including
  * `query`, which picks up an embedded Cypher editor from the renderer's query-field detection.
  * That is the one difference from the query bar's dialog, which strips `query` because it
  * already has the editor buffer to run.
  *
  * The graph comes from the page's selector, not the form: the endpoint is graph-scoped, so it
  * is the client's URL that decides, exactly as for ingests and standing queries.
  */
object CreateBackgroundQueryForm {

  /** @param namespace the graph the query will run in, shown so the target isn't implicit
    */
  def apply(
    spec: ParsedSpec,
    createSchema: Option[SchemaNode],
    namespace: Signal[String],
    onSubmit: Json => Future[Either[String, Json]],
    onComplete: () => Unit,
    onCancel: () => Unit,
    editorConfig: EmbeddedEditorConfig,
  ): HtmlElement = {
    val formState = Var(Json.obj())
    val submit = new SubmitState

    // Whether the user picked a destination. Not a gate on submitting — the body defaults one
    // in — only what the note above the buttons says will happen.
    val hasDestination: Signal[Boolean] =
      formState.signal.map(BackgroundQueryRequest.destinationsOf(_).nonEmpty)

    val hasQuery: Signal[Boolean] =
      formState.signal.map(_.hcursor.downField("query").as[String].toOption.exists(_.trim.nonEmpty))

    div(
      cls := "p-2",
      div(
        cls := "alert alert-light border py-2 px-3 mb-3 small",
        "Runs once against the ",
        strong(child.text <-- namespace),
        " graph, selected at the top of this page. To schedule a query to run repeatedly, " +
        "create a job instead.",
      ),
      createSchema match {
        case Some(schema) =>
          SchemaFormRenderer.render(schema, spec, Nil, formState, editorConfig, isRequired = true)
        case None =>
          div(cls := "alert alert-warning", "Background query schema not found in API spec.")
      },
      ErrorAlert(submit.error.signal),
      div(
        cls := "d-flex align-items-center justify-content-end mt-3",
        child <-- hasDestination.map {
          case true => emptyNode
          case false =>
            span(
              cls := "small text-body-secondary me-auto",
              "No destination set — the query will run and report its row count, but its results " +
              "won't be written anywhere.",
            )
        },
        button(
          cls := "btn btn-secondary me-2",
          "Cancel",
          onClick --> { _ => onCancel() },
        ),
        FormSubmit.submitButton(
          idleLabel = "Run Query",
          busyLabel = "Starting...",
          state = submit,
          canSubmit = hasQuery,
        ) { () =>
          submit.run(onSubmit(BackgroundQueryRequest.withDefaultDestination(formState.now())))(_ => onComplete())
        },
      ),
    )
  }
}
