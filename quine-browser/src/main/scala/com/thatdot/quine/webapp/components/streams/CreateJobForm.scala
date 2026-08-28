package com.thatdot.quine.webapp.components.streams

import scala.concurrent.Future

import com.raquo.laminar.api.L._
import io.circe.Json
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

import com.thatdot.quine.openapi._
import com.thatdot.quine.webapp.util.SignalSample

/** Schema-driven, single-surface form for creating a scheduled job.
  *
  * Pure form: receives a schema to render, emits assembled JSON via `onSubmit`. The parent
  * handles the API call and reports the result.
  *
  * Modelled on [[CreateStandingQueryForm]]: `name` is a dedicated input at the top (stripped
  * from the rendered schema so it isn't asked for twice) and merged back at submit time.
  * Everything else comes from the `CreateJobRequest` schema via [[SchemaFormRenderer]] —
  * including the five-variant `schedule` union and the `action`'s full destination union, so
  * this is where a job gets the Kafka/Kinesis/SNS/webhook destinations that the query bar's
  * quick run-in-background dialog deliberately doesn't offer.
  *
  * `updateIfExists` is left to the schema renderer as an ordinary field. It is what makes a
  * create against an existing name replace it in place, which — since the API exposes no way to
  * read a job's action back — is the only editing path there is.
  */
object CreateJobForm {

  /** @param namespace the graph the job will target, from the Streams page's selector. Read at
    *                  submit time, so switching graphs with the form open retargets it rather
    *                  than pinning whichever graph happened to be selected when it opened.
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
    val nameVar = Var("")
    val formState = Var(Json.obj())
    val submit = new SubmitState

    val renderedSchema = createSchema
      .map(s => s.copy(properties = s.properties.map(_.removed("name"))))

    div(
      cls := "p-2",
      // The action's `namespace` is hidden from the rendered form (see `FormUiHints`) and
      // taken from the page's graph selector instead. Stated plainly, because it is otherwise
      // invisible: the field isn't on the form, and the jobs table can't show it either — the
      // API doesn't report a job's action.
      div(
        cls := "alert alert-light border py-2 px-3 mb-3 small",
        "Runs against the ",
        strong(child.text <-- namespace),
        " graph, selected at the top of this page.",
      ),
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
          placeholder := "nightly-rollup",
          controlled(value <-- nameVar.signal, onInput.mapToValue --> nameVar),
        ),
        div(
          cls := "form-text small",
          "Jobs are identified by name. Creating a job with an existing name replaces it only if " +
          "\"update if exists\" is set below.",
        ),
      ),
      renderedSchema match {
        case Some(schema) =>
          SchemaFormRenderer.render(schema, spec, Nil, formState, editorConfig, isRequired = true)
        case None =>
          div(
            cls := "alert alert-warning",
            "Create job schema not found in API spec.",
          )
      },
      ErrorAlert(submit.error.signal),
      div(
        cls := "d-flex justify-content-end mt-3",
        button(
          cls := "btn btn-secondary me-2",
          "Cancel",
          onClick --> { _ => onCancel() },
        ),
        FormSubmit.submitButton(
          idleLabel = "Create Job",
          busyLabel = "Creating...",
          state = submit,
          canSubmit = nameVar.signal.map(_.trim.nonEmpty),
        ) { () =>
          // One-shot read of the selector at submit time, so switching graphs with the form
          // open retargets the job rather than pinning whichever graph was selected on open.
          val selectedGraph = SignalSample.now(namespace)
          submit.run(onSubmit(buildBody(formState.now(), nameVar.now(), selectedGraph)))(_ => onComplete())
        },
      ),
    )
  }

  /** Assemble the `CreateJobRequest` body from the form state, the name input, and the page's
    * selected graph.
    *
    * `name` and `action.namespace` are both merged in rather than rendered: the first because
    * the form asks for it in a dedicated input at the top, the second because it comes from the
    * page's graph selector.
    *
    * `deepMerge` recurses into objects, so nesting `namespace` under `action` adds it to the
    * action the schema renderer built instead of replacing it — the query, destinations, and
    * discriminator all survive.
    */
  private[streams] def buildBody(formState: Json, name: String, namespace: String): Json =
    formState.deepMerge(
      Json.obj(
        "name" -> Json.fromString(name.trim),
        "action" -> Json.obj("namespace" -> Json.fromString(namespace)),
      ),
    )
}
