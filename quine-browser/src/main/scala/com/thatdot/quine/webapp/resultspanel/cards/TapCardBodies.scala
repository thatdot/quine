package com.thatdot.quine.webapp.resultspanel.cards

import com.raquo.laminar.api.L._
import io.circe.{Json, Printer}

import com.thatdot.quine.webapp.Styles
import com.thatdot.quine.webapp.resultspanel.streaming.StreamingView
import com.thatdot.quine.webapp.resultspanel.{ResultsData, ResultsView, TapEntry, ViewerCommand, ViewerReads}

/** Card body for the tap-table kind. Kept thin: tap-table reuses the existing
  * [[StreamingView]] wholesale (design doc §3: "StreamingView for live tap tables").
  */
object TapCardBodies {

  /** Tap-table card body: the existing [[StreamingView.tapBody]] — the same live growing
    * table used for a tap in today's drawer, now hosted inside a card. The cap is
    * cap-aware: a live card renders the full buffer (tail-follow tracks new rows), a
    * capped one stops at the card's sampling budget. Lifting the cap rebuilds the popup
    * frame (`hasSampleLimit` is part of `CardsStore.cardRenderKey`), so branching on the
    * snapshot here is safe. Content only — every stream control (Stop / Get-more / batch
    * size / Go-live) is a fixture in the popup header's stream cluster, enabled or grayed
    * by state rather than mounted and unmounted (see `CardPopup.streamActions`). Sampling
    * is tap-only: adhoc queries return a complete result set, so [[AdhocCardBody]] renders
    * it in full.
    */
  def tapTable(
    reads: ViewerReads,
    entry: TapEntry,
    hasSampleLimit: Boolean,
    vd: Observer[ViewerCommand],
  ): HtmlElement = {
    val cap: Signal[Option[Int]] =
      if (hasSampleLimit) reads.sampleSize.map(Some(_)) else Signal.fromValue(None)
    // The header's filter (`ViewerState.search`, written by `ViewerControls`' magnifier)
    // applies to tap cards just as it does to adhoc ones — a display-level narrowing of
    // the capped buffer, in both Table and JSON views.
    val needle: Signal[String] = reads.search
    div(
      display := "contents",
      // The header's Table·JSON toggle applies here just as it does to adhoc cards —
      // per-card state (`ViewerState.view`), so flipping one card leaves the others alone.
      child <-- reads.view.map {
        case ResultsView.Table =>
          StreamingView.tapBody(
            entry,
            maxRows = cap,
            filterNeedle = needle,
            colWidths = reads.colWidths,
            selectedRow = reads.selectedRow,
            vd = vd,
          )
        case ResultsView.Json => jsonBody(entry, cap, needle)
      },
    )
  }

  /** JSON rendering of the (capped) buffer: each frame exactly as it arrived on the wire
    * ([[com.thatdot.quine.webapp.resultspanel.streaming.StreamRow]]`.raw`), re-printed as
    * batches land. Unlike the Table view, which unwraps a raw tap's `{"meta", "data"}`
    * envelope into columns, this view preserves it, so a raw tap and a transformed tap
    * (whose frames carry no envelope) are visibly different here — this is the card's one
    * wire-truth view, matching what an external WebSocket consumer of the tap endpoint
    * would receive.
    */
  private def jsonBody(entry: TapEntry, cap: Signal[Option[Int]], needle: Signal[String]): HtmlElement =
    div(
      cls := Styles.resultsContentArea,
      div(
        cls := Styles.resultsBody,
        pre(
          cls := Styles.resultsJson,
          child.text <-- entry.stream.rows.signal
            .combineWith(cap, needle)
            .map { case (rows, capNow, needleNow) =>
              val capped = capNow.fold(rows)(rows.take)
              val shown = capped.map(_.raw).filter(frame => ResultsData.matches(Seq(frame), needleNow))
              Printer.spaces2.print(Json.fromValues(shown))
            },
        ),
      ),
    )

}
