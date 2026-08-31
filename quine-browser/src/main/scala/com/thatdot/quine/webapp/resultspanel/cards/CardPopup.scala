package com.thatdot.quine.webapp.resultspanel.cards

import scala.scalajs.js

import com.raquo.laminar.api.L._
import org.scalajs.dom

import com.thatdot.quine.routes.CypherQueryResult
import com.thatdot.quine.webapp.Styles
import com.thatdot.quine.webapp.resultspanel.{
  DragGesture,
  ResultOutcome,
  ResultsData,
  ResultsIcons,
  SourceStatus,
  TapPoint,
  TapTarget,
  ViewerCommand,
  ViewerControls,
  ViewerReads,
}

/** The one expanded card — a floating popup results viewer (design doc §3 "Expanded card —
  * popup results viewer"). Geometry: margins on every side, rounded corners, shadow — a
  * popup, *not* a bottom-docked drawer — anchored low and spanning most of the width, and
  * vertically resizable via the existing grab-rail [[DragGesture]] (same gesture
  * `ResultsPanel.grabRail` uses, re-targeted at the popup's own height instead of the old
  * surface's).
  *
  * Host-agnostic: takes the card to render plus an [[Observer]] of [[CardCommand]] — no
  * dependency on [[CardsStore]] itself, so any host can mount it once it has a
  * `Signal[Option[Card]]` to feed in (see the integration notes).
  */
object CardPopup {

  /** Height the popup opens at, in px — mirrors `ResultsLayout.initialHeightPx`'s role but
    * kept local to this component (popup geometry is this component's own concern, not the
    * store's — the store carries no layout state, same separation `ResultsStore` keeps from
    * `ResultsLayout`).
    */
  private val initialHeightPx = 420.0
  private val minHeightPx = 220.0

  /** How long the popup's grow/shrink transition runs. Must match the transition duration on
    * `.card-popup` in common.css: it is how long the minimized card's content stays mounted
    * after it stops being the expanded card, so the popup shrinks away *with* its content
    * rather than blanking first and then shrinking as an empty box.
    */
  private val TransitionMs = 200

  def apply(
    expanded: Signal[Option[Card]],
    dispatch: Observer[CardCommand],
  ): HtmlElement = {
    val heightVar = Var(initialHeightPx)
    // Which card's query strip is open, by id — held here (not inside the frame) because
    // the frame rebuilds on every Card emit (entry swap, stop, mode flip), which would
    // reset frame-local state. Keyed by id so switching to another card starts collapsed.
    val queryOpenForVar: Var[Option[CardId]] = Var(None)
    // What the popup actually renders: `expanded`, but lagging it by `TransitionMs` on the
    // way out (see above). Expanding is immediate — the new card must be in place before the
    // popup grows.
    val shownVar: Var[Option[Card]] = Var(None)
    var clearHandle: Option[js.timers.SetTimeoutHandle] = None

    div(
      cls := CardStyles.popup,
      // Driven by `expanded`, not `shownVar`: the shrink has to start the moment the card is
      // minimized, while the content it shrinks with is still mounted.
      cls(CardStyles.popupCollapsed) <-- expanded.map(_.isEmpty),
      height <-- heightVar.signal.map(h => s"${h.toInt}px"),
      expanded --> { card =>
        // A card expanded during another's shrink-out cancels that pending clear, which would
        // otherwise unmount the newly-expanded card mid-animation.
        clearHandle.foreach(js.timers.clearTimeout)
        clearHandle = None
        card match {
          case some @ Some(_) => shownVar.set(some)
          case None =>
            clearHandle = Some(js.timers.setTimeout(TransitionMs.toDouble) {
              clearHandle = None
              shownVar.set(None)
            })
        }
      },
      child <-- shownVar.signal.map {
        case Some(card) => frame(card, heightVar, queryOpenForVar, dispatch)
        case None => emptyNode
      },
    )
  }

  private def frame(
    card: Card,
    heightVar: Var[Double],
    queryOpenForVar: Var[Option[CardId]],
    dispatch: Observer[CardCommand],
  ): HtmlElement = {
    val vd = dispatch.contramap[ViewerCommand](cmd => CardCommand.OnViewer(card.id, cmd))
    val reads = ViewerReads.of(card.viewer)

    div(
      cls := Styles.resultsContentArea,
      display := "flex",
      flexDirection := "column",
      width := "100%",
      height := "100%",
      grabRail(heightVar),
      header(card, reads, vd, dispatch),
      queryStrip(card, queryOpenForVar),
      errorStrip(card),
      div(
        cls := CardStyles.popupBody,
        flex := "1",
        overflow := "hidden",
        body(card, reads, vd),
      ),
    )
  }

  /** How this card's rows differ in shape from the results of the query the strip shows, or
    * `None` when they don't (an Enriched card's rows are exactly the enrichment query's
    * returned columns). The wording is card-local on purpose: it points the reader at this
    * popup's own table and JSON views, which no other surface showing a
    * [[com.thatdot.quine.webapp.resultspanel.TapPointQuery]] has — the graph-feed picker writes its own note about the same reshaping.
    */
  private def shapeNote(tapPoint: TapPoint, transformationType: Option[String]): Option[String] =
    tapPoint match {
      case TapPoint.Raw =>
        Some(
          "The table shows each match's data fields as columns. Switch to the JSON view for the " +
          "full result, including the {\"meta\", \"data\"} envelope around it.",
        )
      case _: TapPoint.PreEnrichment =>
        // Every known transformation gets its specific effect spelled out; an unknown (future)
        // one still gets the honest generic statement rather than nothing.
        Some(transformationType match {
          case Some("InlineData") =>
            "These rows are what this output's InlineData transformation made of those matches: " +
              "each match's data fields lifted to the top level, with the {\"meta\", \"data\"} " +
              "envelope dropped."
          case Some(other) =>
            s"These rows are what this output's $other transformation made of those matches, so " +
              "their shape differs from the matches the query returns."
          case None =>
            "These rows are what this output's transformation made of those matches, so their " +
              "shape differs from the matches the query returns."
        })
      case _: TapPoint.PostEnrichment => None
    }

  /** A collapsed-by-default strip showing the query behind a tap card's data (see
    * [[com.thatdot.quine.webapp.resultspanel.TapPointQuery]]) — the standing query's match pattern for a Raw/Transformed card, or
    * the enrichment query for an Enriched one. When the card's rows differ in shape from
    * the query's results (Raw and Transformed cards), the strip leads with [[shapeNote]]
    * saying how. Absent for adhoc cards and for tap cards whose query text the host couldn't
    * resolve. Open/closed state lives in `openFor` (popup-level, keyed by card id) so it
    * survives the frame rebuilds every Card change causes.
    */
  private def queryStrip(card: Card, openFor: Var[Option[CardId]]): Node = card match {
    case TapTableCard(_, target, _, Some(info), _, _, _) =>
      // A background query's rows are its own results, so it has no pipeline stage to name and
      // nothing reshapes them: one plain heading, no shape note.
      val (queryLabel, note) = target match {
        case TapTarget.StandingQuery(_, tapPoint) =>
          (TapPoint.queryLabel(tapPoint), shapeNote(tapPoint, info.transformation))
        case _: TapTarget.BackgroundQuery => ("Background query", None)
      }
      val isOpen: Signal[Boolean] = openFor.signal.map(_.contains(card.id))
      div(
        cls := CardStyles.popupQueryStrip,
        button(
          tpe := "button",
          cls := CardStyles.popupQueryToggle,
          onClick --> (_ => openFor.update(cur => if (cur.contains(card.id)) None else Some(card.id))),
          span(child.text <-- isOpen.map(e => if (e) "▾" else "▸")),
          span(queryLabel),
        ),
        children <-- isOpen.map {
          case true =>
            note.toList.map(n => div(cls := CardStyles.popupQueryNote, n)) :+
              pre(cls := CardStyles.popupQueryPre, info.query)
          case false => Nil
        },
      )
    case _ => emptyNode
  }

  /** A red banner under the header spelling out why a tap card's stream errored (the
    * status segment only wears the state; the message itself would be unreadable at that
    * size). Mounted for every tap card and display-toggled by the entry's status, so an
    * error arriving mid-stream surfaces without a frame rebuild; adhoc cards never error
    * this way (a failed run surfaces as a toast, not a card).
    */
  private def errorStrip(card: Card): Node = card match {
    case TapTableCard(_, target, entry, _, _, _, _) =>
      div(
        cls := CardStyles.popupErrorStrip,
        display <-- entry.status.map {
          case SourceStatus.Error(_) => "block"
          case _ => "none"
        },
        // State-neutral prefix: the message itself says whether the connection failed to
        // open or dropped mid-stream (see WiretapStore's closeDetail / liveCloseDetail).
        child.text <-- entry.status.map {
          case SourceStatus.Error(message) => s"Tap on ${target.label} failed: $message"
          case _ => ""
        },
      )
    case _: AdhocCard => emptyNode
  }

  // ── header row — the card's single control surface, left → right: identity (title +
  // kind pill), stream status, stream/run actions, view controls, window controls ──────
  private def header(
    card: Card,
    reads: ViewerReads,
    vd: Observer[ViewerCommand],
    cd: Observer[CardCommand],
  ): HtmlElement =
    div(
      cls := CardStyles.popupHeader,
      titleBlock(card.title),
      kindPill(card),
      statusSegment(card),
      div(
        cls := CardStyles.popupActions,
        card match {
          case _: AdhocCard =>
            Seq[Modifier[HtmlElement]](editButton(card, cd), reRunButton(card, cd))
          case tt: TapTableCard =>
            Seq[Modifier[HtmlElement]](streamActions(tt, cd))
        },
        exportMenu(card, reads, vd),
        button(
          tpe := "button",
          cls := CardStyles.popupMinimizeButton,
          title := "Minimize",
          "—",
          onClick --> (_ => cd.onNext(CardCommand.Minimize(card.id))),
        ),
        button(
          tpe := "button",
          cls := CardStyles.popupCloseButton,
          title := "Close",
          ResultsIcons.close,
          onClick --> (_ => cd.onNext(CardCommand.Close(card.id))),
        ),
      ),
    )

  private def titleBlock(raw: String): HtmlElement = {
    val (head, tail) = ResultsData.middleSplit(raw)
    div(
      cls := CardStyles.popupHeaderTitle,
      title := raw,
      span(cls := CardStyles.popupHeaderTitleHead, head),
      span(cls := CardStyles.popupHeaderTitleTail, tail),
    )
  }

  /** Identity only — no glyphs. The `●` dot is reserved for exactly one meaning across the
    * card ([[liveDot]]): "subscription open, rows can arrive". Tap cards carry no pill at
    * all — the status dot beside the count already marks them as streams, so a "tap" label
    * added a word without adding information.
    */
  private def kindPill(card: Card): Node = card match {
    case _: AdhocCard =>
      span(cls := CardStyles.popupKindPill, cls := Styles.kindQuery, "table")
    case _: TapTableCard => emptyNode
  }

  // ── stream status — one segment merging row count and stream state ─────────────────

  /** Adhoc: just the count. Tap: the stream state worn by the dot — `● n of N`,
    * `● n rows`, or a red `● error · n rows` — reactive to the (growing) buffer
    * and the (editable) budget, neither of which is in `cardRenderKey`, so a `.now()` read
    * would freeze the label at the last frame rebuild (same pattern as
    * `MinimizedDrawer.statusLine`). `mode` is safe to read statically: it *is* in the
    * render key, so a mode flip rebuilds this frame.
    */
  private def statusSegment(card: Card): HtmlElement = card match {
    case AdhocCard(_, _, _, outcomeOpt, _, _) =>
      span(
        cls := CardStyles.popupMeta,
        outcomeOpt.map(_.outcome) match {
          case Some(ResultOutcome.Tabular(r)) => s"${r.results.size} rows"
          case Some(ResultOutcome.TextResults(vs)) => s"${vs.size} results"
          case Some(_: ResultOutcome.EmptyResult) => "no rows"
          case _ => "…"
        },
      )
    case TapTableCard(_, _, entry, _, _, hasSampleLimit, viewer) =>
      span(
        cls := CardStyles.popupMeta,
        child <-- entry.status
          .combineWith(entry.stream.rows.signal, viewer.sampleSize.signal)
          .map { case (status, rows, budget) => tapStatus(status, rows.size, budget, hasSampleLimit) },
      )
  }

  /** One fixed shape for every state — `● <count>` — so the eye never re-scans for the
    * count between state flips. The `●` alone carries the stream state: pulsing
    * green means "subscription open, rows can arrive", static amber (the streams page's
    * paused badge color) means paused, static red means errored — the one state that adds
    * a word (`error`), with the full message in the segment's tooltip and spelled out in
    * the [[errorStrip]] banner.
    */
  private def tapStatus(status: SourceStatus, rows: Int, budget: Int, hasSampleLimit: Boolean): HtmlElement =
    status match {
      case SourceStatus.Error(message) =>
        span(
          cls := CardStyles.popupErrorAccent,
          title := message,
          errorDot,
          s" error · $rows rows",
        )
      // `TapEntry.status` folds the entry's `ended` flag into `Ended` — filled budget,
      // user Stop, and a departed source all land here: one paused state, one dot.
      case SourceStatus.Ended => span(pausedDot, s" $rows rows")
      case _ =>
        if (hasSampleLimit) span(liveDot, s" $rows of $budget") else span(liveDot, s" $rows rows")
    }

  private def liveDot: HtmlElement = span(cls := CardStyles.popupStatusDot, "●")

  private def pausedDot: HtmlElement =
    span(cls := CardStyles.popupStatusDot, cls := CardStyles.popupPausedAccent, "●")

  private def errorDot: HtmlElement =
    span(cls := CardStyles.popupStatusDot, cls := CardStyles.popupErrorAccent, "●")

  private def editButton(card: Card, cd: Observer[CardCommand]): HtmlElement =
    button(
      tpe := "button",
      cls := CardStyles.popupEditButton,
      title := "Copy this card's query into the query bar, your next run updates this card instead of opening a new one",
      "Edit ↑",
      onClick --> (_ => cd.onNext(CardCommand.EditQuery(card.id))),
    )

  private def reRunButton(card: Card, cd: Observer[CardCommand]): HtmlElement =
    button(
      tpe := "button",
      cls := CardStyles.popupRunButton,
      title := "Run this card's query again as-is and refresh its results in place, never opens a new card",
      "▶ Re-run",
      onClick --> (_ => cd.onNext(CardCommand.ReRun(card.id))),
    )

  /** Tap stream lifecycle — a fixed cluster, always mounted: `Get N more` + batch field
    * (the sampled exit) and one play/stop toggle (`Go live` while paused, `■ Stop` while
    * streaming). State enables or grays each control; nothing pops in or out, so positions
    * never shift. Streaming (sampled fill or live follow) → toggle reads Stop; paused
    * (filled budget, user Stop, departed source — all fold to `Ended` via
    * `TapEntry.status`) → Get-more / batch active, toggle reads Go live; errored →
    * Get-more / batch gray and the toggle reads Reconnect, the error state's one action.
    * The streaming/paused state itself is worn by the status dot ([[tapStatus]]),
    * not the buttons.
    */
  private def streamActions(card: TapTableCard, cd: Observer[CardCommand]): Modifier[HtmlElement] = {
    val paused: Signal[Boolean] = card.entry.status.map {
      case SourceStatus.Ended => true
      case _ => false
    }
    val errored: Signal[Boolean] = card.entry.status.map {
      case SourceStatus.Error(_) => true
      case _ => false
    }
    // A background query cannot be reopened once ended (its server-side relay is gone), so its
    // card gets a reduced control set rather than permanently grey buttons: the sampled-batch
    // controls never render (fetch-more only ever enables from a paused, reopenable stream),
    // and the Stop toggle renders while the run is live, then hides when it finishes or errors
    // — the status dot already wears the ended state. Standing-query taps (`resumable`) keep
    // the full disabled-while-paused treatment, since their controls do re-enable.
    // `CardsStore` is what actually enforces non-reopenability; this only keeps the controls
    // honest.
    val reopenable: Boolean = card.target.resumable
    val finished: Signal[Boolean] =
      paused.combineWith(errored).map { case (p, e) => (p || e) && !reopenable }
    val canFetch: Signal[Boolean] =
      paused.combineWith(errored).map { case (p, e) => p && !e && reopenable }
    Seq[Modifier[HtmlElement]](
      // The sampled-batch controls exist only for a resumable tap: fetch-more enables while
      // paused and reopens the stream, which a background query can never do — so for one they
      // would be born grey and die grey. Not rendered at all rather than hidden reactively,
      // since `resumable` is fixed for the card's lifetime.
      if (reopenable)
        button(
          tpe := "button",
          cls := CardStyles.popupMoreButton,
          disabled <-- canFetch.map(!_),
          child.text <-- card.viewer.sampleBatch.signal.map(b => s"Get $b more"),
          onClick --> (_ => cd.onNext(CardCommand.FetchMoreSamples(card.id))),
        )
      else emptyNode,
      if (reopenable)
        input(
          tpe := "number",
          cls := CardStyles.popupBatchInput,
          minAttr := "1",
          title := "Batch size for the next fetch",
          disabled <-- canFetch.map(!_),
          value <-- card.viewer.sampleBatch.signal.map(_.toString),
          onChange.mapToValue --> { text =>
            text.toIntOption.foreach(n => cd.onNext(CardCommand.SetSampleSize(card.id, n)))
          },
        )
      else emptyNode,
      button(
        tpe := "button",
        cls := CardStyles.popupLiveButton,
        display <-- finished.map(if (_) "none" else ""),
        // One toggle for the stream lifecycle: paused → resume unbounded, streaming
        // (sampled fill or live follow) → stop, errored → reconnect (the error state's one
        // action, riding the host's fresh-open path into a revive — see
        // CardCommand.Reconnect). All labels stay mounted in the same grid cell (the
        // inactive ones visibility-hidden) so the button's width is always that of the
        // widest label — no size jump on any state flip.
        title <-- errored.map(e => if (e) "Reopen this tap, replaces the errored stream" else ""),
        span(
          cls(CardStyles.popupToggleHidden) <-- paused.combineWith(errored).map { case (p, e) => !p || e },
          "▶ Go live",
        ),
        span(cls(CardStyles.popupToggleHidden) <-- paused.combineWith(errored).map { case (p, e) => p || e }, "■ Stop"),
        span(cls(CardStyles.popupToggleHidden) <-- errored.map(!_), "↻ Reconnect"),
        onClick.compose(_.sample(paused.combineWith(errored))) --> { case (p, e) =>
          cd.onNext(
            if (e) CardCommand.Reconnect(card.id)
            else if (p) CardCommand.GoLive(card.id)
            else CardCommand.Stop(card.id),
          )
        },
      ),
    )
  }

  /** JSON/CSV export — the existing [[ViewerControls]] export-menu behavior, reused as-is;
    * only shown for cards with tabular content.
    * [[ViewerControls]] only branches on whether the outcome is `Tabular` (to decide whether to
    * show the export affordance at all) — the actual export payload is re-derived by
    * `CardsStore.runExport` from the live entry/outcome at click time via `CardsStore.outcomeOf`,
    * not from the empty placeholder passed here for tap-table cards.
    */
  private def exportMenu(
    card: Card,
    reads: ViewerReads,
    vd: Observer[ViewerCommand],
  ): HtmlElement = {
    val emptyTabular = ResultOutcome.Tabular(CypherQueryResult(Seq.empty, Seq.empty))
    card match {
      case AdhocCard(_, _, _, Some(content), _, _) =>
        div(cls := CardStyles.popupExportWrap, ViewerControls(content.outcome, reads, vd))
      case _: TapTableCard =>
        div(cls := CardStyles.popupExportWrap, ViewerControls(emptyTabular, reads, vd))
      case _ => span(display := "none")
    }
  }

  // ── body ─────────────────────────────────────────────────────────────────────────
  private def body(
    card: Card,
    reads: ViewerReads,
    vd: Observer[ViewerCommand],
  ): HtmlElement = card match {
    case AdhocCard(_, _, _, outcomeOpt, _, _) => AdhocCardBody(outcomeOpt, reads, vd)
    case TapTableCard(_, _, entry, _, _, hasSampleLimit, _) =>
      TapCardBodies.tapTable(reads, entry, hasSampleLimit, vd)
  }

  // ── vertical resize (grab rail, reusing DragGesture like ResultsPanel.grabRail) ────
  private def grabRail(heightVar: Var[Double]): HtmlElement = {
    var railRef: Option[dom.html.Element] = None
    var start: Option[(Double, Double)] = None // (startPageY, startHeight)

    def parentOf(el: Option[dom.html.Element]): Option[dom.html.Element] =
      el.flatMap(e => Option(e.parentNode)).collect { case parent: dom.html.Element => parent }

    // rail → frame (the 100%-height flex column) → .card-popup (the element `heightVar`
    // drives) → .canvas-region (the space the popup can grow into). The growth cap MUST come
    // from the region, not the popup: the popup's own height is exactly what's being resized,
    // so capping against it clamps every grow-drag to the current height (resize could only
    // ever shrink).
    def frameEl: Option[dom.html.Element] = parentOf(railRef)
    def popupEl: Option[dom.html.Element] = parentOf(frameEl)
    def regionEl: Option[dom.html.Element] = parentOf(popupEl)

    // Region height minus the popup's fixed 24px bottom inset (see `.card-popup` in
    // common.css) and a matching 24px top margin, so a full grow stops just short of the
    // TopBar instead of running underneath it.
    def maxHeight(): Double =
      regionEl.map(_.clientHeight.toDouble - 48.0).getOrElse(Double.MaxValue)

    div(
      cls := CardStyles.popupGrabRail,
      title := "Drag to resize",
      onMountCallback(ctx => railRef = Some(ctx.thisNode.ref)),
      DragGesture.handle(
        onStart = e => {
          start = frameEl.map(p => (e.pageY, p.clientHeight.toDouble))
          start.isDefined
        },
        onMove = e =>
          start.foreach { case (startY, startHeight) =>
            heightVar.set((startHeight + (startY - e.pageY)).max(minHeightPx).min(maxHeight()))
          },
      ),
    )
  }
}
