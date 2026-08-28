package com.thatdot.quine.webapp.components.streams

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.dataservice.WiretapService
import com.thatdot.quine.webapp.util.{BackgroundQueryDisplay, InstantDisplay}
import com.thatdot.quine.webapp.v2api.V2ApiTypes.{V2BackgroundQueryStatus, V2JobStatus}

/** Renders the scheduled-jobs table.
  *
  * Pure renderer: Signals in, Observers out. No API knowledge — the parent wires the observers.
  *
  * Two absences are deliberate, and both come from the same place: `GET /api/v2/system/jobs`
  * reports a job's `schedule` but not its `action`.
  *
  *   - '''No graph column.''' A job's target graph lives in its action, so it simply isn't in
  *     the response.
  *   - '''No edit action.''' Editing needs the current query and destinations to prefill, and
  *     they aren't in the response either. Delete and re-create is the honest workflow until the
  *     API returns the action.
  */
object JobTable {

  /** @param runs every execution record for the current graph; each row picks out its own by
    *             job name. Note this is graph-scoped while jobs are not, so a job whose action
    *             targets a different graph shows no runs — [[runsNote]] says as much.
    */
  def apply(
    entriesSignal: Signal[List[(String, V2JobStatus)]],
    runs: Signal[Seq[V2BackgroundQueryStatus]],
    canDelete: Boolean,
    onDelete: Observer[String],
    canCancelRun: Boolean,
    onCancelRun: Observer[String],
    canDeleteRun: Boolean,
    onDeleteRun: Observer[String],
    wiretap: WiretapService,
  ): HtmlElement = {
    val expandedVar: Var[Set[String]] = Var(Set.empty)
    table(
      cls := "table table-hover mb-0",
      thead(
        tr(
          th(styleAttr := "width: 40px"),
          th("Name"),
          th("Type"),
          th("Schedule"),
          th("Next fire"),
          th("Last fire"),
          th("Status"),
          th("Actions"),
        ),
      ),
      children <-- entriesSignal.splitSeq(_._1) { strictSignal =>
        val jobName = strictSignal.key
        val jobSignal = strictSignal.map(_._2)
        val isExpanded = expandedVar.signal.map(_.contains(jobName)).distinct
        // A job's own runs, newest activity first — the same ordering the panels use.
        val jobRuns: Signal[List[(String, V2BackgroundQueryStatus)]] =
          runs.map(all =>
            BackgroundQueryDisplay
              .displayOrder(all.filter(_.jobName.contains(jobName)))
              .toList
              .map(run => run.id -> run),
          )
        tbody(
          renderRow(jobName, jobSignal, isExpanded, expandedVar, canDelete, onDelete),
          renderRunsRow(isExpanded, jobRuns, canCancelRun, onCancelRun, canDeleteRun, onDeleteRun, wiretap),
        )
      },
    )
  }

  private def renderRow(
    jobName: String,
    jobSignal: Signal[V2JobStatus],
    isExpanded: Signal[Boolean],
    expandedVar: Var[Set[String]],
    canDelete: Boolean,
    onDelete: Observer[String],
  ): HtmlElement =
    tr(
      td(
        cls := "text-center",
        styleAttr := "cursor: pointer",
        title := "Show this job's runs",
        onClick --> { _ =>
          expandedVar.update(open => if (open.contains(jobName)) open - jobName else open + jobName)
        },
        i(cls <-- isExpanded.map(e => if (e) "cil-chevron-bottom" else "cil-chevron-right")),
      ),
      td(cls := "fw-semibold", child.text <-- jobSignal.map(_.name)),
      td(child.text <-- jobSignal.map(_.jobType)),
      td(child.text <-- jobSignal.map(_.scheduleSummary)),
      td(cls := "text-nowrap", child.text <-- jobSignal.map(job => InstantDisplay.localTime(job.nextFireAt))),
      td(cls := "text-nowrap", child.text <-- jobSignal.map(job => InstantDisplay.localTime(job.lastFireAt))),
      td(
        child <-- jobSignal.map(_.running).map {
          case true => span(cls := "badge bg-success", "Running")
          case false => span(cls := "badge bg-secondary", "Idle")
        },
      ),
      td(
        cls := "text-nowrap",
        child <-- jobSignal.map(_.name).map { name =>
          if (canDelete)
            button(
              cls := "btn btn-sm btn-ghost-danger",
              // Says what else goes with it: deleting a job also cancels whatever it currently
              // has in flight, which is not obvious from the word "delete" alone.
              title := "Delete this job and cancel any run it has in flight",
              i(cls := "cil-trash"),
              onClick --> { _ => onDelete.onNext(name) },
            )
          else emptyNode
        },
      ),
    )

  /** The expanded row: this job's executions, tappable and cancellable like any other run. */
  private def renderRunsRow(
    isExpanded: Signal[Boolean],
    jobRuns: Signal[List[(String, V2BackgroundQueryStatus)]],
    canCancelRun: Boolean,
    onCancelRun: Observer[String],
    canDeleteRun: Boolean,
    onDeleteRun: Observer[String],
    wiretap: WiretapService,
  ): HtmlElement =
    tr(
      cls := "bg-body-tertiary",
      display <-- isExpanded.map(if (_) "table-row" else "none"),
      td(
        colSpan := 8,
        cls := "p-3",
        child <-- jobRuns.map {
          case Nil => div(cls := "small text-body-secondary", runsNote)
          case _ =>
            div(
              div(cls := "small text-body-secondary mb-2", "Runs dispatched by this job."),
              BackgroundQueryTable(
                entriesSignal = jobRuns,
                canCancel = canCancelRun,
                onCancel = onCancelRun,
                canDelete = canDeleteRun,
                onDelete = onDeleteRun,
                wiretap = wiretap,
              ),
            )
        },
      ),
    )

  /** Why an expanded job can be empty. Two quite different causes, and the UI can't tell them
    * apart: the API reports neither a job's target graph nor its past runs.
    */
  private val runsNote: String =
    "No runs in the graph selected above. A job's runs are listed under the graph its query " +
    "targets, and records are kept only until they expire."
}
