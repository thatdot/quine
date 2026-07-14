package com.thatdot.quine.webapp.components.streams

import com.raquo.laminar.api.L._
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

import com.thatdot.quine.webapp.util.Pot
import com.thatdot.quine.webapp.v2api.V2ApiTypes.V2IngestInfo

/** Ingest Streams panel — shows a list of ingest streams with play/pause/delete
  * actions, and a form for creating new ingest streams.
  *
  * The list/form scaffolding lives in [[StreamCollectionPanel]]; this object
  * only supplies the ingest-specific labels, operations, and child renderers.
  * The list comes from the shared DataService feed; mutations go through
  * [[StreamsApiClient]] and dispatch a refresh via `onRefresh`.
  */
object IngestStreamPanel {

  /** Stable row identity: clustered ingests can repeat the same name across member
    * positions, so the position is folded into the key.
    */
  private def rowKey(info: V2IngestInfo): String =
    info.memberIdx.fold(info.name)(idx => s"${info.name}#$idx")

  def apply(
    client: StreamsApiClient,
    ingests: Signal[Pot[Seq[V2IngestInfo]]],
    onRefresh: () => Unit,
    memberIndices: Signal[Seq[Int]],
    editorConfig: EmbeddedEditorConfig,
    capabilities: StreamsCapabilities,
  ): HtmlElement = {
    // Shared across the three row actions so a failure from any one of them (e.g. a
    // permission-denied delete) surfaces in the same banner instead of being silently dropped.
    val actionState = new SubmitState

    StreamCollectionPanel(
      title = "Ingest Streams",
      newLabel = "New Ingest",
      emptyMessage = "No ingest streams configured.",
      emptyCta = "Create your first ingest stream",
      canCreate = capabilities.canCreateIngest,
      entries = ingests.map(_.map(_.toList.map(info => rowKey(info) -> info))),
      onRefresh = onRefresh,
      renderCreateForm = (onComplete, onCancel) =>
        CreateIngestForm(
          spec = client.spec,
          createSchema = client.ingestCreateSchema,
          memberIndices = memberIndices,
          onSubmit = (body, memberIdx) => client.createIngest(body, memberIdx),
          onComplete = onComplete,
          onCancel = onCancel,
          editorConfig = editorConfig,
        ),
      renderTable = { (entriesSignal: Signal[List[(String, V2IngestInfo)]], onAction: () => Unit) =>
        val refresh = onAction
        div(
          ErrorAlert(actionState.error.signal),
          IngestStreamTable(
            entriesSignal = entriesSignal,
            memberIndices = memberIndices,
            canControl = capabilities.canControlIngest,
            canDelete = capabilities.canDeleteIngest,
            onDelete = Observer[(String, Option[Int])] { case (name, idx) =>
              actionState.run(client.deleteIngest(name, idx))(_ => refresh())
            },
            onPause = Observer[(String, Option[Int])] { case (name, idx) =>
              actionState.run(client.pauseIngest(name, idx))(_ => refresh())
            },
            onResume = Observer[(String, Option[Int])] { case (name, idx) =>
              actionState.run(client.resumeIngest(name, idx))(_ => refresh())
            },
          ),
        )
      },
    )
  }
}
