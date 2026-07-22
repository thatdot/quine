package com.thatdot.quine.webapp.resultspanel.cards

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
  TapEntry,
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
  * `Signal[Option[CardState]]` to feed in (see the integration notes).
  */
object CardPopup {

  /** Height the popup opens at, in px — mirrors `ResultsLayout.initialHeightPx`'s role but
    * kept local to this component (popup geometry is this component's own concern, not the
    * store's — the store carries no layout state, same separation `ResultsStore` keeps from
    * `ResultsLayout`).
    */
  private val initialHeightPx = 420.0
  private val minHeightPx = 220.0

  def apply(
    expanded: Signal[Option[CardState]],
    dispatch: Observer[CardCommand],
  ): HtmlElement = {
    val heightVar = Var(initialHeightPx)
    // Which card's query strip is open, by id — held here (not inside the frame) because
    // the frame rebuilds on every CardState emit (entry swap, stop, mode flip), which would
    // reset frame-local state. Keyed by id so switching to another card starts collapsed.
    val queryOpenForVar: Var[Option[CardId]] = Var(None)

    div(
      cls := CardStyles.popup,
      display <-- expanded.map(c => if (c.isDefined) "flex" else "none"),
      height <-- heightVar.signal.map(h => s"${h.toInt}px"),
      child <-- expanded.map {
        case Some(card) => frame(card, heightVar, queryOpenForVar, dispatch)
        case None => emptyNode
      },
    )
  }

  private def frame(
    card: CardState,
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
      div(
        cls := CardStyles.popupBody,
        flex := "1",
        overflow := "hidden",
        body(card, reads, vd),
      ),
    )
  }

  /** A collapsed-by-default strip showing the query behind a tap card's data (see
    * [[TapCardQuery]]) — the standing query's match pattern for a Raw/Transformed card, or
    * the enrichment query for an Enriched one. When the card's rows differ in shape from
    * the query's results (Raw and Transformed cards), the strip leads with the note saying
    * how. Absent for adhoc cards and for tap cards whose query text the host couldn't
    * resolve. Open/closed state lives in `openFor` (popup-level, keyed by card id) so it
    * survives the frame rebuilds every CardState change causes.
    */
  private def queryStrip(card: CardState, openFor: Var[Option[CardId]]): Node = card.kind match {
    case CardKind.TapTableCard(_, _, Some(info)) =>
      val isOpen: Signal[Boolean] = openFor.signal.map(_.contains(card.id))
      div(
        cls := CardStyles.popupQueryStrip,
        button(
          tpe := "button",
          cls := CardStyles.popupQueryToggle,
          onClick --> (_ => openFor.update(cur => if (cur.contains(card.id)) None else Some(card.id))),
          span(child.text <-- isOpen.map(e => if (e) "▾" else "▸")),
          span(info.label),
        ),
        children <-- isOpen.map {
          case true =>
            info.note.toList.map(n => div(cls := CardStyles.popupQueryNote, n)) :+
              pre(cls := CardStyles.popupQueryPre, info.query)
          case false => Nil
        },
      )
    case _ => emptyNode
  }

  // ── header row — the card's single control surface, left → right: identity (title +
  // kind pill), stream status, stream/run actions, view controls, window controls ──────
  private def header(
    card: CardState,
    reads: ViewerReads,
    vd: Observer[ViewerCommand],
    cd: Observer[CardCommand],
  ): HtmlElement =
    div(
      cls := CardStyles.popupHeader,
      titleBlock(card.title),
      kindPill(card.kind),
      statusSegment(card),
      div(
        cls := CardStyles.popupActions,
        card.kind match {
          case _: CardKind.AdhocCard =>
            Seq[Modifier[HtmlElement]](editButton(card, cd), reRunButton(card, cd))
          case CardKind.TapTableCard(_, entry, _) =>
            Seq[Modifier[HtmlElement]](streamActions(card, entry, cd))
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
  private def kindPill(kind: CardKind): Node = kind match {
    case _: CardKind.AdhocCard =>
      span(cls := CardStyles.popupKindPill, cls := Styles.kindQuery, "table")
    case _: CardKind.TapTableCard => emptyNode
  }

  // ── stream status — one segment merging row count and stream state ─────────────────

  /** Adhoc: just the count. Tap: the stream state worn by the dot — `● n of N`,
    * `● n rows`, or `⚠ error` — reactive to the (growing) buffer
    * and the (editable) budget, neither of which is in `cardRenderKey`, so a `.now()` read
    * would freeze the label at the last frame rebuild (same pattern as
    * `MinimizedDrawer.statusLine`). `mode` is safe to read statically: it *is* in the
    * render key, so a mode flip rebuilds this frame.
    */
  private def statusSegment(card: CardState): HtmlElement = card.kind match {
    case CardKind.AdhocCard(_, _, outcomeOpt) =>
      span(
        cls := CardStyles.popupMeta,
        outcomeOpt.map(_.outcome) match {
          case Some(ResultOutcome.Tabular(r)) => s"${r.results.size} rows"
          case Some(ResultOutcome.TextResults(vs)) => s"${vs.size} results"
          case Some(_: ResultOutcome.EmptyResult) => "no rows"
          case _ => "…"
        },
      )
    case CardKind.TapTableCard(_, entry, _) =>
      span(
        cls := CardStyles.popupMeta,
        child <-- entry.status
          .combineWith(entry.stream.rows.signal, card.viewer.sampleSize.signal)
          .map { case (status, rows, budget) => tapStatus(status, rows.size, budget, card.mode) },
      )
  }

  /** One fixed shape for every state — `● <count>` — so the eye never re-scans for the
    * count between state flips. The `●` alone carries the stream state, no words: pulsing
    * green means "subscription open, rows can arrive", static amber (the streams page's
    * paused badge color) means paused.
    */
  private def tapStatus(status: SourceStatus, rows: Int, budget: Int, mode: SampleMode): HtmlElement =
    status match {
      case SourceStatus.Error(_) => span(s"⚠ error · $rows rows")
      // `TapEntry.status` folds the entry's `ended` flag into `Ended` — filled budget,
      // user Stop, and a departed source all land here: one paused state, one dot.
      case SourceStatus.Ended => span(pausedDot, s" $rows rows")
      case _ =>
        mode match {
          case SampleMode.Live => span(liveDot, s" $rows rows")
          case SampleMode.Sampled => span(liveDot, s" $rows of $budget")
        }
    }

  private def liveDot: HtmlElement = span(cls := CardStyles.popupStatusDot, "●")

  private def pausedDot: HtmlElement =
    span(cls := CardStyles.popupStatusDot, cls := CardStyles.popupPausedAccent, "●")

  private def editButton(card: CardState, cd: Observer[CardCommand]): HtmlElement =
    button(
      tpe := "button",
      cls := CardStyles.popupEditButton,
      title := "Load into the query bar for editing",
      "Edit ↑",
      onClick --> (_ => cd.onNext(CardCommand.EditQuery(card.id))),
    )

  private def reRunButton(card: CardState, cd: Observer[CardCommand]): HtmlElement =
    button(
      tpe := "button",
      cls := CardStyles.popupRunButton,
      "▶ Re-run",
      onClick --> (_ => cd.onNext(CardCommand.ReRun(card.id))),
    )

  /** Tap stream lifecycle — a fixed cluster, always mounted: `Get N more` + batch field
    * (the sampled exit) and one play/stop toggle (`Go live` while paused, `■ Stop` while
    * streaming). State enables or grays each control; nothing pops in or out, so positions
    * never shift. Streaming (sampled fill or live follow) → toggle reads Stop; paused
    * (filled budget, user Stop, departed source — all fold to `Ended` via
    * `TapEntry.status`) → Get-more / batch active, toggle reads Go live; errored → all
    * gray. The streaming/paused state itself is worn by the status dot ([[tapStatus]]),
    * not the buttons.
    */
  private def streamActions(card: CardState, entry: TapEntry, cd: Observer[CardCommand]): Modifier[HtmlElement] = {
    val paused: Signal[Boolean] = entry.status.map {
      case SourceStatus.Ended => true
      case _ => false
    }
    val errored: Signal[Boolean] = entry.status.map {
      case SourceStatus.Error(_) => true
      case _ => false
    }
    val canFetch: Signal[Boolean] = paused.combineWith(errored).map { case (p, e) => p && !e }
    Seq[Modifier[HtmlElement]](
      button(
        tpe := "button",
        cls := CardStyles.popupMoreButton,
        disabled <-- canFetch.map(!_),
        title := "Fetch another sampled batch (available while paused)",
        child.text <-- card.viewer.sampleBatch.signal.map(b => s"Get $b more"),
        onClick --> (_ => cd.onNext(CardCommand.FetchMoreSamples(card.id))),
      ),
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
      ),
      button(
        tpe := "button",
        cls := CardStyles.popupLiveButton,
        disabled <-- errored,
        // One toggle for the stream lifecycle: paused → resume unbounded, streaming
        // (sampled fill or live follow) → stop. Both labels stay mounted in the same
        // grid cell (the inactive one visibility-hidden) so the button's width is always
        // that of the widest label — no size jump on toggle.
        span(cls(CardStyles.popupToggleHidden) <-- paused.map(!_), "▶ Go live"),
        span(cls(CardStyles.popupToggleHidden) <-- paused, "■ Stop"),
        onClick.compose(_.sample(paused)) --> { p =>
          cd.onNext(if (p) CardCommand.GoLive(card.id) else CardCommand.Stop(card.id))
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
    card: CardState,
    reads: ViewerReads,
    vd: Observer[ViewerCommand],
  ): HtmlElement = {
    val emptyTabular = ResultOutcome.Tabular(CypherQueryResult(Seq.empty, Seq.empty))
    card.kind match {
      case CardKind.AdhocCard(_, _, Some(content)) =>
        div(cls := CardStyles.popupExportWrap, ViewerControls(content.outcome, reads, vd))
      case _: CardKind.TapTableCard =>
        div(cls := CardStyles.popupExportWrap, ViewerControls(emptyTabular, reads, vd))
      case _ => span(display := "none")
    }
  }

  // ── body ─────────────────────────────────────────────────────────────────────────
  private def body(
    card: CardState,
    reads: ViewerReads,
    vd: Observer[ViewerCommand],
  ): HtmlElement = card.kind match {
    case CardKind.AdhocCard(_, _, outcomeOpt) => AdhocCardBody(outcomeOpt, reads, vd)
    case CardKind.TapTableCard(_, entry, _) => TapCardBodies.tapTable(card, entry)
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
