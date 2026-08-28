package com.thatdot.quine.webapp.components.streams

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.dataservice.{BackgroundQueryTapHandler, BqTapStatus, WiretapService}
import com.thatdot.quine.webapp.v2api.V2ApiTypes.V2BackgroundQueryStatus

/** Background query inspections — the live view of a run's results, on the Streams page.
  *
  * Deliberately the same widget as the standing-query inspections rendered a panel above, down to
  * the layout, the status badge, and the capped message log: they are the same idea (watch what a
  * server-side stream is producing, without leaving this page) and differ only in what they are
  * watching. See `StandingQueryTable.renderWiretapsSection`, which this mirrors.
  *
  * Starting one is a read, so it needs no write permission — unlike cancelling the run.
  */
object BackgroundQueryInspection {

  /** The start control for one run, plus its inspection card once running. */
  def apply(run: V2BackgroundQueryStatus, wiretap: WiretapService): HtmlElement = {
    // This run's inspection, if one is open. Keyed by execution id, so an inspection opened from
    // the Background Queries panel and one opened under the run's job are the same inspection.
    // `.distinct`: Airstream 17 signals do not drop duplicate values, so without it any tap
    // opening or closing on *another* run re-emits this map and rebuilds our card — which resets
    // the log scroll position. We only care when this run's handler appears or disappears.
    val handlerSignal: Signal[Option[BackgroundQueryTapHandler]] =
      wiretap.backgroundQueryTapsSignal.map(_.get(run.id)).distinct

    div(
      cls := "mt-2",
      child <-- handlerSignal.map {
        case None => startButton(run, wiretap)
        case Some(handler) => card(handler, wiretap)
      },
    )
  }

  private def startButton(run: V2BackgroundQueryStatus, wiretap: WiretapService): HtmlElement =
    button(
      cls := "btn btn-sm btn-primary",
      i(cls := "cil-media-play me-1"),
      "Start background query inspection",
      // Offered for a finished run too: the server holds a terminated run's output briefly, so
      // an inspection opened just after one completes can still show it.
      onClick --> { _ =>
        wiretap.wiretapDispatch.onNext(
          WiretapService.OpenBackgroundQueryTap(WiretapService.StreamsPageSubscriber, run.id, run.displayName),
        )
      },
    )

  private def statusBadge(status: BqTapStatus): HtmlElement = status match {
    case BqTapStatus.Connecting => span(cls := "badge bg-warning text-dark", "Connecting")
    case BqTapStatus.Live => span(cls := "badge bg-success", "Live")
    case BqTapStatus.Error(msg) => span(cls := "badge bg-danger", title := msg, "Error")
    // A finished run is the normal end state here, unlike a standing query's tap — say
    // "Finished" rather than "Closed", which would read as something the user did.
    case BqTapStatus.Ended(_) => span(cls := "badge bg-secondary", "Finished")
  }

  private def card(handler: BackgroundQueryTapHandler, wiretap: WiretapService): HtmlElement =
    div(
      cls := "border rounded p-2 bg-body",
      div(
        cls := "d-flex justify-content-between align-items-center mb-2",
        div(
          cls := "d-flex align-items-center gap-2 flex-grow-1 me-2",
          styleAttr := "min-width: 0",
          span(cls := "fw-semibold text-truncate", handler.displayName),
          child <-- handler.status.signal.map(statusBadge),
          span(
            cls := "small text-body-secondary",
            child.text <-- handler.rowCount.signal.map(n => s"$n ${if (n == 1) "row" else "rows"}"),
          ),
          // The count above is what this inspection saw, which is not the run's total if it was
          // already under way — worth saying, since the two are easy to conflate.
          child <-- handler.status.signal.map {
            case BqTapStatus.Ended(Some(completion)) =>
              completion.totalRowCount match {
                case Some(total) => span(cls := "small text-body-secondary", s"· $total produced")
                case None => emptyNode
              }
            case _ => emptyNode
          },
        ),
        button(
          cls := "btn btn-sm btn-outline-danger py-0 px-2 flex-shrink-0",
          title := "Stop background query inspection",
          "✕",
          onClick --> { _ =>
            wiretap.wiretapDispatch.onNext(
              WiretapService.CloseBackgroundQueryTap(WiretapService.StreamsPageSubscriber, handler.executionId),
            )
          },
        ),
      ),
      // Result log — newest at the bottom, capped by the handler, exactly as the standing-query
      // inspection's is.
      child <-- handler.messages.signal.map(_.isEmpty).distinct.map {
        case true =>
          div(cls := "text-body-secondary small", "No rows yet.")
        case false =>
          pre(
            cls := "mb-0 p-2 bg-body-tertiary rounded border small",
            styleAttr := "max-height: 16em; overflow: auto;",
            children <-- handler.messages.signal.map(_.toList.map(m => div(cls := "text-break", m))),
          )
      },
      child <-- handler.status.signal.map {
        case BqTapStatus.Ended(Some(completion)) if completion.droppedBufferedRows > 0 =>
          div(
            cls := "small text-warning mt-2",
            s"${completion.droppedBufferedRows} rows were dropped — inspections are best-effort " +
            "and skip rows when they can't keep up.",
          )
        case BqTapStatus.Ended(Some(completion)) if completion.error.nonEmpty =>
          div(cls := "small text-danger mt-2", completion.error.getOrElse(""): String)
        case _ => emptyNode
      },
    )
}
