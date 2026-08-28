package com.thatdot.quine.webapp.components.streams

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.dataservice.WiretapService
import com.thatdot.quine.webapp.util.InstantDisplay
import com.thatdot.quine.webapp.v2api.V2ApiTypes.V2BackgroundQueryStatus

/** Renders a background-query executions table — the ad-hoc list, and a job's own runs.
  *
  * Pure renderer apart from inspections, which it opens through the wiretap capability directly
  * (as the standing-query table does) rather than routing through the parent: an inspection is
  * per-run browser state, not a mutation the page needs to sequence.
  *
  * Each row expands to its [[BackgroundQueryInspection]], mirroring how the standing-query panel
  * above puts SQ inspections inside the expanded row.
  */
object BackgroundQueryTable {

  def apply(
    entriesSignal: Signal[List[(String, V2BackgroundQueryStatus)]],
    canCancel: Boolean,
    onCancel: Observer[String],
    canDelete: Boolean,
    onDelete: Observer[String],
    wiretap: WiretapService,
  ): HtmlElement = {
    val expandedVar: Var[Set[String]] = Var(Set.empty)
    table(
      cls := "table table-hover mb-0",
      thead(
        tr(
          th(styleAttr := "width: 40px"),
          th("Query"),
          th("Status"),
          th("Rows"),
          th("Host"),
          th("Expires"),
          th("Actions"),
        ),
      ),
      children <-- entriesSignal.splitSeq(_._1) { strictSignal =>
        val runId = strictSignal.key
        val runSignal = strictSignal.map(_._2)
        val isExpanded = expandedVar.signal.map(_.contains(runId)).distinct
        tbody(
          renderRow(runId, runSignal, isExpanded, expandedVar, canCancel, onCancel, canDelete, onDelete),
          renderInspectionRow(runSignal, isExpanded, wiretap),
        )
      },
    )
  }

  /** The expanded row: this run's inspection, in the same widget the standing-query panel uses. */
  private def renderInspectionRow(
    runSignal: Signal[V2BackgroundQueryStatus],
    isExpanded: Signal[Boolean],
    wiretap: WiretapService,
  ): HtmlElement =
    tr(
      cls := "bg-body-tertiary",
      display <-- isExpanded.map(if (_) "table-row" else "none"),
      td(
        colSpan := 7,
        cls := "p-3",
        div(
          cls := "d-flex justify-content-between align-items-center mb-2",
          strong("Background Query Inspection"),
        ),
        // `distinctBy(_.id)`: the run record is re-emitted on every poll tick (status, row
        // count), and rebuilding this subtree each time would tear down a running inspection's
        // log and scroll position. Only a genuinely different run rebuilds it.
        child <-- runSignal.distinctBy(_.id).map(BackgroundQueryInspection(_, wiretap)),
      ),
    )

  private def renderRow(
    runId: String,
    runSignal: Signal[V2BackgroundQueryStatus],
    isExpanded: Signal[Boolean],
    expandedVar: Var[Set[String]],
    canCancel: Boolean,
    onCancel: Observer[String],
    canDelete: Boolean,
    onDelete: Observer[String],
  ): HtmlElement =
    tr(
      td(
        cls := "text-center",
        styleAttr := "cursor: pointer",
        title := "Inspect this run's results",
        onClick --> { _ =>
          expandedVar.update(open => if (open.contains(runId)) open - runId else open + runId)
        },
        i(cls <-- isExpanded.map(e => if (e) "cil-chevron-bottom" else "cil-chevron-right")),
      ),
      td(
        // The name when the run has one, else the query text; the query is always in the
        // tooltip, since a named run's text is otherwise nowhere on this page.
        cls := "text-truncate",
        styleAttr := "max-width: 28rem",
        title <-- runSignal.map(_.query),
        child.text <-- runSignal.map(_.displayName),
      ),
      td(
        child <-- runSignal.map { run =>
          run.status match {
            case V2BackgroundQueryStatus.StatusStarted => span(cls := "badge bg-success", "Running")
            case V2BackgroundQueryStatus.StatusCompleted => span(cls := "badge bg-secondary", "Completed")
            case V2BackgroundQueryStatus.StatusCancelled => span(cls := "badge bg-secondary", "Cancelled")
            case V2BackgroundQueryStatus.StatusFailed =>
              // The error is the whole point of a failed row, so it goes in the tooltip rather
              // than being dropped — there is no expand-row affordance on this table.
              span(cls := "badge bg-danger", title := run.error.getOrElse("Failed"), "Failed")
            // A status added server-side later reads as itself rather than vanishing.
            case other => span(cls := "badge bg-secondary", other)
          }
        },
      ),
      td(child.text <-- runSignal.map(_.totalRowCount.fold("—")(_.toString))),
      td(child.text <-- runSignal.map(_.hostId)),
      td(cls := "text-nowrap", child.text <-- runSignal.map(run => InstantDisplay.localTime(run.expiresAt))),
      td(
        cls := "text-nowrap",
        // Inspecting lives in the expanded row (the chevron), matching the standing-query
        // panel; the row-level actions are the ones that change something: stop a running run,
        // and delete its record (which also stops it first, so it is offered whatever the state).
        child <-- runSignal.map { run =>
          div(
            cls := "d-flex gap-1",
            if (canCancel && run.isRunning)
              button(
                cls := "btn btn-sm btn-ghost-danger",
                title := "Stop this run",
                i(cls := "cil-media-stop"),
                onClick --> { _ => onCancel.onNext(run.id) },
              )
            else emptyNode,
            if (canDelete)
              button(
                cls := "btn btn-sm btn-ghost-danger",
                // A running run is cancelled first, then its record removed — say so, since the
                // word "delete" alone doesn't imply the stop.
                title := "Delete this run's record (stops it first if still running)",
                i(cls := "cil-trash"),
                onClick --> { _ => onDelete.onNext(run.id) },
              )
            else emptyNode,
          )
        },
      ),
    )

}
