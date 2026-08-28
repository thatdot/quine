package com.thatdot.quine.webapp.components.streams

import com.raquo.laminar.api.L._
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

import com.thatdot.quine.webapp.dataservice.WiretapService
import com.thatdot.quine.webapp.util.{BackgroundQueryDisplay, Pot}
import com.thatdot.quine.webapp.v2api.V2ApiTypes.V2BackgroundQueryStatus

/** Background Queries panel — the graph's ad-hoc out-of-band runs, in-flight ones first.
  *
  * Scoped to the selected graph, like the Ingest and Standing Query panels above it (and unlike
  * the cluster-wide Scheduled Jobs panel).
  *
  * Shows only runs '''not''' dispatched by a job: a job's executions belong with the job, and on
  * a graph with a frequent schedule they would otherwise bury the handful of runs a person
  * actually started. See [[BackgroundQueryDisplay.adhoc]].
  *
  * A run can be started here as well as from the query bar. The two build the same request from
  * the same schema; the difference is only where the query text comes from — typed into this
  * form, or taken from the editor buffer there.
  */
object BackgroundQueryPanel {

  def apply(
    client: StreamsApiClient,
    runs: Signal[Pot[Seq[V2BackgroundQueryStatus]]],
    namespace: Signal[String],
    onRefresh: () => Unit,
    onCancel: String => Unit,
    onDelete: String => Unit,
    wiretap: WiretapService,
    editorConfig: EmbeddedEditorConfig,
    capabilities: StreamsCapabilities,
  ): HtmlElement =
    StreamCollectionPanel[V2BackgroundQueryStatus](
      title = "Background Queries",
      newLabel = "New Background Query",
      emptyMessage = "No background queries have been run against this graph.",
      emptyCta = "Run your first background query",
      canCreate = capabilities.canRunBackgroundQuery,
      entries = runs.map(
        _.map(all => BackgroundQueryDisplay.displayOrder(BackgroundQueryDisplay.adhoc(all)).toList.map(r => r.id -> r)),
      ),
      onRefresh = onRefresh,
      renderCreateForm = (onComplete, onCancel) =>
        CreateBackgroundQueryForm(
          spec = client.spec,
          createSchema = client.backgroundQueryCreateSchema,
          namespace = namespace,
          onSubmit = body => client.runBackgroundQuery(body),
          onComplete = onComplete,
          onCancel = onCancel,
          editorConfig = editorConfig,
        ),
      renderTable = { (entriesSignal: Signal[List[(String, V2BackgroundQueryStatus)]], onAction: () => Unit) =>
        div(
          div(
            cls := "small text-body-secondary mb-2",
            "One-off runs — started here or from the query bar. A scheduled job's runs are " +
            "listed under the job. Expand a run to inspect its results as they arrive.",
          ),
          BackgroundQueryTable(
            entriesSignal = entriesSignal,
            canCancel = capabilities.canCancelBackgroundQuery,
            wiretap = wiretap,
            onCancel = Observer[String] { id =>
              onCancel(id)
              // The cancel lands asynchronously and the record only flips to "cancelled" once
              // the executing host unwinds, so this refresh just shortens the wait for the
              // first poll — the transition itself may still take another tick.
              onAction()
            },
            canDelete = capabilities.canDeleteBackgroundQuery,
            onDelete = Observer[String] { id =>
              onDelete(id)
              onAction()
            },
          ),
        )
      },
    )
}
