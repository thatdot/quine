package com.thatdot.quine.webapp.resultspanel

import scala.scalajs.js

import com.raquo.laminar.api.L._
import org.scalajs.dom

import com.thatdot.quine.routes.CypherQueryResult
import com.thatdot.quine.webapp.util.Pot

/** The results surface's state and command/event processing, factored out of the view.
  *
  * Owns the state slices (all private), interprets [[ResultsCommand]]s (via [[dispatch]]), and
  * exposes the signals views render from plus that one command sink. State stays read-only to views
  * without a wrapper trait: the mutable Vars are `private`, so the only writer path is `dispatch`.
  * The root [[ResultsPanel.surface]] takes this store directly; leaf components get purpose-built
  * projections instead (`readsPrimary`, `sessionReads`, `tapsReads`, [[doorReads]]).
  *
  * Reactive *event* handling is surfaced as [[lifecycleMods]] — binders the root view applies, so
  * they activate on mount and tear down on unmount (element-scoped ownership; the store keeps no
  * `Owner` of its own). Constructed once by the host, which injects the surface's inputs.
  */
final class ResultsStore(
  liveContent: Signal[Option[ResultsContent]],
  subscriptions: TapSubscriptions = TapSubscriptions.empty,
  catalog: Signal[Pot[Vector[TapCatalogEntry]]] = Signal.fromValue(Pot.Empty),
  // History and collapse are injected so a host can persist and restore them (e.g. session
  // storage); the store owns the rest of its state. Default to fresh ones when unmanaged.
  history: HistoryState = HistoryState.empty,
  collapsedVar: Var[Boolean] = Var(false),
  rightInset: Signal[Int] = Signal.fromValue(0),
  onUseQuery: Observer[String] = Observer.empty,
) {

  // ── state slices (private — views read only the exposed signals / projections) ────
  private val viewerPrimary: ViewerState = ViewerState.initial // the single view's result-value state
  private val pickerOpenVar: Var[Boolean] = Var(false)
  private val pickerAddingVar: Var[Boolean] = Var(false) // picker showing the add-tap chooser vs. browsing
  private val navHistory: NavHistory = new NavHistory // browser-style visited stack (Back/Forward)
  private val switcherSearchVar: Var[String] = Var("") // the switcher's search text (hosted in the top bar)
  private val heightVar: Var[Double] = Var(ResultsLayout.initialHeightPx)
  private val heightAnimatingVar: Var[Boolean] = Var(false) // ease snap/grow height changes, but not drags

  // Dev affordance: `?stub-taps` swaps in a synthetic capability + sample catalog so the tap
  // picker can be exercised end to end before a host mounts a real store.
  private val stubbed: Boolean = dom.window.location.search.contains("stub-taps")
  private val effectiveSubscriptions: TapSubscriptions = if (stubbed) new StubTapSubscriptions else subscriptions
  private val taps: TapsState = new TapsState(effectiveSubscriptions)

  private var prevLive: Option[ResultsContent] = None
  private var heightBeforeSnap: Option[Double] = None // height to restore on a second grab-rail double-click

  // ── reads exposed to components ─────────────────────────────────────────────
  val effectiveCatalog: Signal[Pot[Vector[TapCatalogEntry]]] =
    if (stubbed) Signal.fromValue(Pot.Ready(StubTapSubscriptions.catalog)) else catalog
  val readsPrimary: ViewerReads = ViewerReads.of(viewerPrimary)
  val tapsReads: TapsReads = TapsReads.of(taps)
  val sessionReads: SessionReads = SessionReads.of(history)
  val navReads: NavReads = NavReads.of(navHistory)

  private val effectiveSources: Signal[Vector[LiveSource]] = taps.subscriptions.sources
  val displayed: Signal[Option[ResultsContent]] = HistoryState.displayed(history)
  val collapsed: Signal[Boolean] = collapsedVar.signal
  val sourcesOpen: Signal[Boolean] = pickerOpenVar.signal
  val addingTap: Signal[Boolean] = pickerAddingVar.signal
  val switcherSearch: Signal[String] = switcherSearchVar.signal
  val height: Signal[Double] = heightVar.signal
  val heightAnimating: Signal[Boolean] = heightAnimatingVar.signal
  val historyEntries: Signal[Vector[HistoryEntry]] = history.entries.signal

  // Host-driven right inset: the surface yields its right edge to whatever the host reserves
  // (e.g. an Explore management panel). `?right-inset=N` simulates it for dev.
  val effectiveInset: Signal[Int] = ResultsStore.devRightInset.map(Signal.fromValue(_)).getOrElse(rightInset)

  // ── derived selectors ───────────────────────────────────────────────────────
  // The currently-selected tap (if any); when None, a query run is showing.
  private val activeTap: Signal[Option[TapEntry]] =
    taps.selectedId.signal.combineWith(taps.entries.signal).map { case (sel, es) =>
      sel.flatMap(id => es.find(_.id == id))
    }

  // The active source (Main pane): a selected tap, else the current query run. A source is
  // `Left(query)` or `Right(tap)` — the viewer renders either uniformly. `.distinct`: pinning
  // rewrites the history Vector without changing the shown content, so without it the body and
  // headers would rebuild on every Keep toggle.
  val mainContent: Signal[Option[Either[ResultsContent, TapEntry]]] =
    activeTap
      .combineWith(displayed)
      .map { case (tapOpt, queryOpt) =>
        tapOpt
          .map(t => Right(t): Either[ResultsContent, TapEntry])
          .orElse(queryOpt.map(c => Left(c): Either[ResultsContent, TapEntry]))
      }
      .distinct

  /** Whether a result or tap is showing — read by the corner control (which can't see the taps
    * slice itself). Replaces the former output `contentShown` Var written by the view.
    */
  val contentShown: Signal[Boolean] = mainContent.map(_.isDefined)

  /** Imperative read of [[contentShown]] for the interpreter (computed from the slices, since a
    * derived `Signal` has no `now`).
    */
  private def isContentShown: Boolean =
    taps.selectedEntry.isDefined || HistoryState.displayedNow(history).isDefined

  // ── command interpreter ─────────────────────────────────────────────────────
  // Synchronous export of the current (derived) result; closes the menu.
  private def runExport(st: ViewerState, content: Option[ResultsContent])(f: CypherQueryResult => Unit): Unit =
    content.collect { case ResultsContent(ResultOutcome.Tabular(r), _) => r }.foreach { r =>
      f(ResultsData.derive(r, st.search.now(), st.sortCol.now(), st.sortDir.now()))
      st.exportOpen.set(false)
    }

  private def interpretViewer(cmd: ViewerCommand): Unit = {
    val st = viewerPrimary
    cmd match {
      case ViewerCommand.SetView(v) => st.view.set(v)
      case ViewerCommand.OpenFilter => st.filterOpen.set(true)
      case ViewerCommand.CloseFilter => st.filterOpen.set(false)
      case ViewerCommand.SetSearch(text) => st.search.set(text)
      case ViewerCommand.ClearSearch => st.search.set("")
      case ViewerCommand.ToggleSort(col) => ViewerState.toggleSort(st, col)
      case ViewerCommand.SetColWidths(ws) => st.colWidths.set(ws)
      case ViewerCommand.SelectRow(values) => st.selectedRow.set(Some(values))
      case ViewerCommand.CloseRow => st.selectedRow.set(None)
      case ViewerCommand.ToggleExport => st.exportOpen.update(!_)
      case ViewerCommand.CloseExport => st.exportOpen.set(false)
      case ViewerCommand.ToggleCsvFlatten => st.csvFlat.update(!_)
      case ViewerCommand.CopyJson =>
        runExport(st, HistoryState.displayedNow(history))(d => ResultsExport.copyToClipboard(ResultsExport.toJson(d)))
      case ViewerCommand.CopyCsv =>
        runExport(st, HistoryState.displayedNow(history))(d =>
          ResultsExport.copyToClipboard(ResultsExport.toCsv(d, st.csvFlat.now())),
        )
      case ViewerCommand.DownloadJson =>
        runExport(st, HistoryState.displayedNow(history))(d =>
          ResultsExport.download(ResultsExport.toJson(d), "json", "application/json"),
        )
      case ViewerCommand.DownloadCsv =>
        runExport(st, HistoryState.displayedNow(history))(d =>
          ResultsExport.download(ResultsExport.toCsv(d, st.csvFlat.now()), "csv", "text/csv"),
        )
    }
  }

  // ── visited-stack navigation (browser-style back/forward) ──────────────────
  /** The source currently in focus, as a visited-stack reference. */
  private def currentFocus: Option[VisitRef] =
    taps.selectedId
      .now()
      .map(VisitRef.Tap(_): VisitRef)
      .orElse(Option.when(HistoryState.displayedNow(history).isDefined)(VisitRef.Query(history.currentIdx.now())))

  /** Record the departed focus before a focus change (unless we're just re-focusing it). */
  private def pushVisit(unlessAt: VisitRef): Unit =
    currentFocus.filter(_ != unlessAt).foreach(navHistory.push)

  /** A reference is navigable if what it points at still exists (GC prunes runs, taps get removed). */
  private def isNavigable(ref: VisitRef): Boolean = ref match {
    case VisitRef.Query(idx) => history.entries.now().indices.contains(idx)
    case VisitRef.Tap(id) => taps.entryFor(id).isDefined
  }

  /** Focus a visited reference directly (no push — this *is* the navigation). */
  private def goTo(ref: VisitRef): Unit = {
    ref match {
      case VisitRef.Query(idx) => taps.clearSelection(); HistoryState.open(history, idx)
      case VisitRef.Tap(id) => taps.select(id)
    }
    pickerOpenVar.set(false)
  }

  /** Pop the newest still-navigable reference off `from`, moving the current focus onto `onto`
    * — one Back (or, swapped, Forward) step. Every stale reference is discarded, not just the
    * ones above the popped one — otherwise stale refs deeper in the stack keep the arrow
    * enabled and a later click lands on nothing.
    */
  private def navigate(from: Var[Vector[VisitRef]], onto: Var[Vector[VisitRef]]): Unit = {
    val valid = from.now().filter(isNavigable)
    from.set(valid.dropRight(1))
    valid.lastOption.foreach { ref =>
      currentFocus.foreach(cur => onto.update(_ :+ cur))
      goTo(ref)
    }
  }

  val dispatch: Observer[ResultsCommand] = Observer {
    case ResultsCommand.OnViewer(cmd) => interpretViewer(cmd)
    case ResultsCommand.Back => navigate(from = navHistory.back, onto = navHistory.forward)
    case ResultsCommand.Forward => navigate(from = navHistory.forward, onto = navHistory.back)
    case ResultsCommand.ToggleCurrentPin => HistoryState.toggleCurrentPin(history)
    case ResultsCommand.ToggleHistory =>
      pickerOpenVar.update(!_)
      switcherSearchVar.set("") // each open starts with a clear search
      if (pickerOpenVar.now()) pickerAddingVar.set(false) // the chip opens to browse, not the chooser
    case ResultsCommand.CloseHistory => pickerOpenVar.set(false)
    case ResultsCommand.SetSwitcherSearch(text) => switcherSearchVar.set(text)
    case ResultsCommand.ToggleTapChooser => pickerAddingVar.update(!_)
    case ResultsCommand.OpenEntry(idx) =>
      pushVisit(unlessAt = VisitRef.Query(idx))
      goTo(VisitRef.Query(idx))
    case ResultsCommand.TogglePinAt(idx) => HistoryState.togglePinAt(history, idx)
    case ResultsCommand.SelectTap(id) =>
      pushVisit(unlessAt = VisitRef.Tap(id))
      goTo(VisitRef.Tap(id))
    case ResultsCommand.OpenTap(target) =>
      // Start the tap, select it once its source arrives (deferred to sync for a fresh tap), and
      // close the picker to reveal it. The departed focus is pushed now — the eventual selection
      // is part of this same navigation, not a second step.
      taps.entries.now().find(e => e.target.contains(target) && !e.ended.now()) match {
        case Some(e) => pushVisit(unlessAt = VisitRef.Tap(e.id))
        case None => currentFocus.foreach(navHistory.push)
      }
      taps.openAndSelect(target)
      pickerOpenVar.set(false)
    case ResultsCommand.CloseTap(id) => taps.closeTap(id)
    case ResultsCommand.StopTap(id) => taps.stop(id)
    case ResultsCommand.RestartTap(id) =>
      taps.restart(id)
    case ResultsCommand.RemoveTap(id) => taps.remove(id)
    case ResultsCommand.SetHeight(px) =>
      heightAnimatingVar.set(false) // a drag should track the cursor, not ease toward it
      heightVar.set(px)
    case ResultsCommand.EnsureHeight(px) =>
      if (heightVar.now() < px) {
        heightAnimatingVar.set(true)
        heightVar.set(px)
      }
    case ResultsCommand.ToggleHeight(comfortable) =>
      heightAnimatingVar.set(true)
      val cur = heightVar.now()
      if (cur < comfortable - ResultsLayout.snapEpsilonPx) {
        heightBeforeSnap = Some(cur) // grow, remembering where we were
        heightVar.set(comfortable)
      } else {
        heightVar.set(heightBeforeSnap.getOrElse(ResultsLayout.initialHeightPx)) // already comfortable: restore
        heightBeforeSnap = None
      }
    case ResultsCommand.DoorToggle =>
      // Canvas door: hide the panel when it's on screen; otherwise restore whatever was showing
      // (content, or the picker if that's what was up), opening the switcher only when there's
      // nothing to show — the true first-run case.
      val shown = !collapsedVar.now() && (isContentShown || pickerOpenVar.now())
      if (shown) collapsedVar.set(true)
      else {
        collapsedVar.set(false)
        if (!isContentShown && !pickerOpenVar.now()) {
          pickerAddingVar.set(false)
          pickerOpenVar.set(true)
        }
      }
    case ResultsCommand.UseQuery(text) => onUseQuery.onNext(text)
  }

  val primaryDispatch: Observer[ViewerCommand] =
    dispatch.contramap(c => ResultsCommand.OnViewer(c))

  /** The slice the canvas door reads (it lives on the canvas layer, outside the surface). */
  val doorReads: DoorReads =
    DoorReads(collapsed, contentShown, sourcesOpen, historyEntries, effectiveInset, dispatch)

  // ── event bindings (facts in) — applied by the view, so they are element-owned ──
  val lifecycleMods: Seq[Modifier[HtmlElement]] = Seq(
    // Reconcile the host's tap set into sessions (open/freeze); auto-select the first arriving
    // tap only when no query run is showing (an otherwise-empty page).
    effectiveSources --> (srcs => TapsState.sync(taps, srcs, autoSelectIfIdle = history.entries.now().isEmpty)),
    // Auto-capture: a new run appends a history entry; a refine updates the head. A new run
    // (None -> Some) also takes focus — clear any selected tap and dismiss the picker.
    liveContent --> { live =>
      val isNewRun = prevLive.isEmpty && live.isDefined
      // A new run takes focus, so it's a navigation: record where we were (pre-prune indices)…
      if (isNewRun) currentFocus.foreach(navHistory.push)
      val dropped = HistoryState.recordLive(history, prevLive, live, ResultsStore.nowLabel())
      // …then re-point the stacks at the same runs after any pruning.
      navHistory.shiftBy(dropped)
      if (isNewRun) {
        taps.clearSelection()
        pickerOpenVar.set(false)
      }
      prevLive = live
    },
    // Reset the viewer when the shown entry changes (new run or history step) — but not when the
    // head merely refines (currentIdx unchanged).
    history.currentIdx.signal --> (_ => ViewerState.resetForNewContent(viewerPrimary)),
  )
}

object ResultsStore {

  private def nowLabel(): String = {
    val d = new js.Date()
    f"${d.getHours().toInt}%02d:${d.getMinutes().toInt}%02d"
  }

  /** Dev override for the host right-inset (`?right-inset=N` in the URL); None when absent. */
  private def devRightInset: Option[Int] = {
    val q = dom.window.location.search
    val key = "right-inset="
    val i = q.indexOf(key)
    if (i < 0) None
    else {
      val digits = q.substring(i + key.length).takeWhile(_.isDigit)
      if (digits.isEmpty) None else Some(digits.toInt)
    }
  }
}
