package com.thatdot.quine.webapp.components.streams

import com.raquo.laminar.api.L._
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

import com.thatdot.quine.webapp.dataservice.WiretapService
import com.thatdot.quine.webapp.util.Pot
import com.thatdot.quine.webapp.v2api.V2ApiTypes.{V2BackgroundQueryStatus, V2JobStatus}

/** Scheduled Jobs panel — lists the cluster's jobs with a delete action, and a form for
  * creating new ones.
  *
  * The list/form scaffolding lives in [[StreamCollectionPanel]]; this object supplies only the
  * job-specific labels, operations, and child renderers.
  *
  * Unlike the two panels above it on the page, this one is '''not scoped to the selected
  * graph''': jobs are cluster-wide, and the API reports no graph per job. The note rendered
  * above the table says so, because a Streams page that otherwise scopes everything to one
  * graph would make a reader assume this table does too.
  */
object JobsPanel {

  def apply(
    client: StreamsApiClient,
    jobs: Signal[Pot[Seq[V2JobStatus]]],
    /** The page's selected graph. The panel itself is graph-independent (jobs are cluster-wide),
      * but a job's *action* targets one graph, and this is where the create form gets it.
      */
    namespace: Signal[String],
    /** Execution records for the current graph; each job row picks out its own to list them. */
    runs: Signal[Pot[Seq[V2BackgroundQueryStatus]]],
    onRefresh: () => Unit,
    onCancelRun: String => Unit,
    onDeleteRun: String => Unit,
    wiretap: WiretapService,
    editorConfig: EmbeddedEditorConfig,
    capabilities: StreamsCapabilities,
  ): HtmlElement = {
    val actionState = new SubmitState

    StreamCollectionPanel(
      title = "Scheduled Jobs",
      newLabel = "New Job",
      emptyMessage = "No scheduled jobs configured.",
      emptyCta = "Create your first scheduled job",
      canCreate = capabilities.canCreateJob,
      entries = jobs.map(_.map(_.toList.map(job => job.name -> job))),
      onRefresh = onRefresh,
      renderCreateForm = (onComplete, onCancel) =>
        CreateJobForm(
          spec = client.spec,
          createSchema = client.jobCreateSchema,
          namespace = namespace,
          onSubmit = body => client.createJob(body),
          onComplete = onComplete,
          onCancel = onCancel,
          editorConfig = editorConfig,
        ),
      renderTable = { (entriesSignal: Signal[List[(String, V2JobStatus)]], onAction: () => Unit) =>
        div(
          ErrorAlert(actionState.error.signal),
          div(
            cls := "small text-body-secondary mb-2",
            "Jobs run across the whole cluster. A job's target graph is chosen when it is created " +
            "and isn't reported by the API, so these aren't filtered by the graph selected above.",
          ),
          JobTable(
            entriesSignal = entriesSignal,
            runs = runs.map(_.toOption.getOrElse(Seq.empty)),
            canDelete = capabilities.canDeleteJob,
            onDelete = Observer[String] { name =>
              actionState.run(client.deleteJob(name))(_ => onAction())
            },
            canCancelRun = capabilities.canCancelBackgroundQuery,
            onCancelRun = Observer[String](onCancelRun(_)),
            canDeleteRun = capabilities.canDeleteBackgroundQuery,
            onDeleteRun = Observer[String](onDeleteRun(_)),
            wiretap = wiretap,
          ),
        )
      },
    )
  }
}
