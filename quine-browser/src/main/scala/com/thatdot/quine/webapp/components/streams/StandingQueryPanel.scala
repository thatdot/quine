package com.thatdot.quine.webapp.components.streams

import com.raquo.laminar.api.L._
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

import com.thatdot.quine.openapi.OpenApiParser
import com.thatdot.quine.webapp.dataservice.WiretapService
import com.thatdot.quine.webapp.util.Pot
import com.thatdot.quine.webapp.v2api.V2ApiTypes.V2StandingQueryInfo

/** Standing Queries panel - shows a list of standing queries with expandable
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

  def apply(
    client: StreamsApiClient,
    standingQueries: Signal[Pot[Seq[V2StandingQueryInfo]]],
    onRefresh: () => Unit,
    wiretap: WiretapService,
    editorConfig: EmbeddedEditorConfig,
    capabilities: StreamsCapabilities,
  ): HtmlElement = {
    // Panel-scoped state - outlives the table’s polling-driven content updates
    // so expanded rows stay open and the inline add-output form keeps focus
    // through the 5-second list refresh.
    val expandedVar = Var(Set.empty[String])
    val addingOutputFor = Var(Option.empty[String])
    val outputFormState = OutputForm.freshState()
    // Shared across delete-SQ and remove-output so a failure from either surfaces in the
    // same banner instead of being silently dropped. Separate from outputFormState’s own
    // SubmitState, which tracks the inline add-output form.
    val actionState = new SubmitState

    // Resolve the output schema once for the inline output form
    val outputSchema = client.outputCreateSchema
      .map(s => OpenApiParser.resolveNode(s, client.spec.schemas))

    StreamCollectionPanel(
      title = "Standing Queries",
      newLabel = "New Standing Query",
      emptyMessage = "No standing queries configured.",
      emptyCta = "Create your first standing query",
      canCreate = capabilities.canCreateStandingQuery,
      entries = standingQueries.map(_.map(_.toList.map(sq => sq.name -> sq))),
      onRefresh = onRefresh,
      renderCreateForm = (onComplete, onCancel) =>
        CreateStandingQueryForm(
          spec = client.spec,
          createSchema = client.sqCreateSchema,
          onSubmit = body => client.createStandingQuery(body),
          onComplete = onComplete,
          onCancel = onCancel,
          editorConfig = editorConfig,
        ),
      renderTable = { (entriesSignal: Signal[List[(String, V2StandingQueryInfo)]], onAction: () => Unit) =>
        val refresh = onAction
        div(
          ErrorAlert(actionState.error.signal),
          StandingQueryTable(
            entriesSignal = entriesSignal,
            canWriteOutputs = capabilities.canWriteStandingQueryOutputs,
            canDelete = capabilities.canDeleteStandingQuery,
            onDeleteSq = Observer[String](name => actionState.run(client.deleteStandingQuery(name))(_ => refresh())),
            onRemoveOutput = Observer[(String, String)] { case (sqName, outputName) =>
              actionState.run(client.removeOutput(sqName, outputName))(_ => refresh())
            },
            expandedVar = expandedVar,
            addingOutputFor = addingOutputFor,
            outputFormState = outputFormState,
            outputSchema = outputSchema,
            spec = client.spec,
            onAddOutput = (sqName, body) => {
              val f = client.addOutput(sqName, body)
              f.foreach(_ => refresh())
              f
            },
            wiretap = wiretap,
            editorConfig = editorConfig,
          ),
        )
      },
    )
  }
}
