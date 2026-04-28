package com.thatdot.quine.webapp.components.streams

import com.raquo.laminar.api.L._
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

import com.thatdot.quine.openapi.OpenApiParser

/** Standing Queries panel — shows a list of standing queries with expandable
  * output rows, delete actions, and a form for creating new standing queries.
  *
  * The list/form scaffolding lives in [[StreamCollectionPanel]]; this object
  * only supplies the SQ-specific labels, operations, and child renderers, plus
  * the panel-scoped Vars that keep row expansion and the inline output form
  * alive across polling rebuilds.
  *
  * It wires [[StreamsApiClient]] methods into the Signal/Observer props that
  * [[StandingQueryTable]], [[CreateStandingQueryForm]], and [[OutputForm]] expect.
  */
object StandingQueryPanel {

  def apply(client: StreamsApiClient): HtmlElement = {
    // Panel-scoped state — outlives the table's polling-driven content updates
    // so expanded rows stay open and the inline add-output form keeps focus
    // through the 5-second list refresh.
    val expandedVar = Var(Set.empty[String])
    val addingOutputFor = Var(Option.empty[String])
    val outputFormState = OutputForm.freshState()

    // Resolve the output schema once for the inline output form
    val outputSchema = client.outputCreateSchema
      .map(s => OpenApiParser.resolveNode(s, client.spec.schemas))

    StreamCollectionPanel(
      title = "Standing Queries",
      newLabel = "New Standing Query",
      emptyMessage = "No standing queries configured.",
      emptyCta = "Create your first standing query",
      listFn = () => client.listStandingQueries(),
      renderCreateForm = (onComplete, onCancel) =>
        CreateStandingQueryForm(
          spec = client.spec,
          createSchema = client.sqCreateSchema,
          onSubmit = body => client.createStandingQuery(body),
          onComplete = onComplete,
          onCancel = onCancel,
        ),
      renderTable = { (entriesSignal, onAction) =>
        val refresh = onAction
        StandingQueryTable(
          entriesSignal = entriesSignal,
          onDeleteSq = Observer[String](name => client.deleteStandingQuery(name).foreach(_ => refresh())),
          onRemoveOutput = Observer[(String, String)] { case (sqName, outputName) =>
            client.removeOutput(sqName, outputName).foreach(_ => refresh())
          },
          expandedVar = expandedVar,
          addingOutputFor = addingOutputFor,
          outputFormState = outputFormState,
          outputSchema = outputSchema,
          spec = client.spec,
          onAddOutput = (sqName, body) => client.addOutput(sqName, body),
        )
      },
    )
  }
}
