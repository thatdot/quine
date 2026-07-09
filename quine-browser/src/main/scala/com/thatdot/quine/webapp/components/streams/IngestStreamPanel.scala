package com.thatdot.quine.webapp.components.streams

import com.raquo.laminar.api.L._
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

/** Ingest Streams panel — shows a list of ingest streams with play/pause/delete
  * actions, and a form for creating new ingest streams.
  *
  * The list/form scaffolding lives in [[StreamCollectionPanel]]; this object
  * only supplies the ingest-specific labels, operations, and child renderers.
  * It wires [[StreamsApiClient]] methods into the Signal/Observer props that
  * [[IngestStreamTable]] and [[CreateIngestForm]] expect.
  */
object IngestStreamPanel {

  def apply(
    client: StreamsApiClient,
    memberIndices: Signal[Seq[Int]],
    editorConfig: EmbeddedEditorConfig,
  ): HtmlElement =
    StreamCollectionPanel(
      title = "Ingest Streams",
      newLabel = "New Ingest",
      emptyMessage = "No ingest streams configured.",
      emptyCta = "Create your first ingest stream",
      listFn = () => client.listIngests(),
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
      renderTable = { (entriesSignal, onAction) =>
        val refresh = onAction
        IngestStreamTable(
          entriesSignal = entriesSignal,
          memberIndices = memberIndices,
          onDelete = Observer[(String, Option[Int])] { case (name, idx) =>
            client.deleteIngest(name, idx).foreach(_ => refresh())
          },
          onPause = Observer[(String, Option[Int])] { case (name, idx) =>
            client.pauseIngest(name, idx).foreach(_ => refresh())
          },
          onResume = Observer[(String, Option[Int])] { case (name, idx) =>
            client.resumeIngest(name, idx).foreach(_ => refresh())
          },
        )
      },
    )
}
