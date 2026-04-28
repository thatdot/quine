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

  def apply(client: StreamsApiClient): HtmlElement =
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
          onSubmit = body => client.createIngest(body),
          onComplete = onComplete,
          onCancel = onCancel,
        ),
      renderTable = { (entriesSignal, onAction) =>
        val refresh = onAction
        IngestStreamTable(
          entriesSignal = entriesSignal,
          onDelete = Observer[String](name => client.deleteIngest(name).foreach(_ => refresh())),
          onPause = Observer[String](name => client.pauseIngest(name).foreach(_ => refresh())),
          onResume = Observer[String](name => client.resumeIngest(name).foreach(_ => refresh())),
        )
      },
    )
}
