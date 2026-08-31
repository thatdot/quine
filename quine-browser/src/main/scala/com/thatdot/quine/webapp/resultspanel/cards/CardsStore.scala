package com.thatdot.quine.webapp.resultspanel.cards

import com.raquo.laminar.api.L._

import com.thatdot.quine.routes.{CypherQueryResult, QueryLanguage}
import com.thatdot.quine.webapp.resultspanel.{
  ResultOutcome,
  ResultsContent,
  ResultsData,
  ResultsExport,
  SourceStatus,
  TapEntry,
  TapPointQuery,
  TapTarget,
  ViewerCommand,
  ViewerState,
}

/** The card system's state and command/event processing, factored out of the view —
  * same shape as [[com.thatdot.quine.webapp.resultspanel.ResultsStore]]: private `Var`
  * slices, read-only `Signal` projections, and a single [[dispatch]] Observer that is
  * the only writer path.
  *
  * Constructed once by the host, which injects the store's inputs:
  *
  *   - `liveContent`: the query-run signal (same source `ResultsStore` reads). A new
  *     run (`None -> Some`, or `Some -> Some` carrying a *different* `runId` than the
  *     last seen) routes into the adhoc card already holding the same query (by
  *     identical trimmed text — one card per query), else into the edit-associated card
  *     (see [[CardCommand.EditQuery]] / [[AdhocCard.editAssociated]] — the association
  *     design doc §3's `Edit ↑` promises), else creates a fresh adhoc card. See
  *     [[onLiveEmission]] for the full precedence contract.
  *   - `onEditQuery`: sends query text up to the host's Monaco query bar (the
  *     `Edit ↑` action's destination).
  *   - `onReRun`: callback invoked with `(query, language)` to actually execute an
  *     adhoc re-run, returning whether the host accepted the run (false on its
  *     early-outs, e.g. a pending text query) — the card only claims the edit
  *     association for accepted runs. The store has no query-execution capability of
  *     its own — running a query is a host concern (same reason `ResultsStore` takes
  *     `liveContent` rather than owning the WS client).
  *
  * Host integration note: this store does not itself open/close taps —
  * [[CardCommand.Close]] on a tap card and the tap-table stop/restart/go-live
  * commands need a subscriptions capability analogous to `TapSubscriptions`. Since Lane A
  * only owns card shell + adhoc routing (checklist A1-A6, A8, A10; UI side of A9), those
  * tap-side effects are exposed as injected callbacks (`onCloseTap`, `onStopTap`, ...)
  * that default to no-ops — Lane C wires the real ones when it adds tap card kinds
  * (checklist C8/C9, which "need A5"). See the integration notes for exact wiring.
  */
final class CardsStore(
  liveContent: Signal[Option[ResultsContent]],
  onEditQuery: Observer[String] = Observer.empty,
  onReRun: (String, QueryLanguage) => Boolean = (_, _) => false,
  onCloseTap: TapTarget => Unit = _ => (),
  onStopTap: TapTarget => Unit = _ => (),
  onRestartTap: TapTarget => Unit = _ => (),
  onReopenTap: TapTarget => Unit = _ => (),
) {

  // ── state slices (private — views read only the exposed signals) ──────────────────
  private val cardsVar: Var[Vector[Card]] = Var(Vector.empty)
  private val expandedIdVar: Var[Option[CardId]] = Var(None)
  private val drawerSearchVar: Var[String] = Var("")

  private var prevLive: Option[ResultsContent] = None

  /** [[CardId]]s are minted here and nowhere else, from one monotonic counter, so they are
    * unique within a store's lifetime by construction. Ids are never persisted — a restore
    * mints fresh ones off this same counter (see [[restore]]) — so the counter never has to
    * be reconciled against a previous session's.
    */
  private var idCounter: Int = 0

  private def freshId(): CardId = {
    idCounter += 1
    CardId(idCounter)
  }

  /** Tap targets whose in-flight reopen is a sampling *continuation* (fetch-more / go-live
    * from a frozen stream) rather than a session-restore reconnect — consumed by
    * [[replaceTapTableEntry]] to decide whether to seed the swapped-in stream.
    */
  private var continuationKeys: Set[String] = Set.empty

  /** Tap targets whose in-flight reopen is a session-restore reconnect for a card that was
    * live at save time — consumed by [[replaceTapTableEntry]] to decide whether the swap
    * should also put the card back in `Live` mode (see [[restoreLive]] and
    * `CardSnapshot.toCard`, which always restores the card itself as `Sampled`: a reopen
    * that never resolves must not leave a stuck card claiming `Live`).
    */
  private var restoreLiveKeys: Set[String] = Set.empty

  /** The card receiving the current run's emissions. Resolved once at the run boundary
    * (the edit-associated card if any, else a freshly appended card) and used for every
    * subsequent emission of the same run — WS text queries emit once per batch, so
    * routing must be per-run, not per-emission, or batches 2..n of a re-run land on
    * whichever adhoc card happens to be newest.
    */
  private var currentRunTarget: Option[CardId] = None

  // ── reads exposed to components ─────────────────────────────────────────────────
  val cards: Signal[Vector[Card]] = cardsVar.signal
  val expandedId: Signal[Option[CardId]] = expandedIdVar.signal
  val drawerSearch: Signal[String] = drawerSearchVar.signal

  /** The minimized cards, most-recent-first — the order the drawer stacks them in. */
  val minimizedCards: Signal[Vector[Card]] =
    cardsVar.signal.combineWith(expandedIdVar.signal).map { case (cs, expanded) =>
      cs.filterNot(c => expanded.contains(c.id)).reverse
    }

  /** The one expanded card, if any. `.distinctBy` keeps [[CardPopup]]'s `child <--` from
    * tearing down and remounting the expanded card's whole subtree on every unrelated
    * `cardsVar` mutation (e.g. another card pausing, a background tap tick). Keyed on
    * [[cardRenderKey]] rather than structural `.distinct`: an adhoc card's outcome holds
    * the whole (growing) result payload, so structural equality would deep-compare every
    * row per emission — O(rows) per batch, quadratic over a streamed run. The key compares
    * the content's `(runId, revision)` instead (see [[com.thatdot.quine.webapp.resultspanel.ResultsContent]]).
    */
  val expandedCard: Signal[Option[Card]] =
    cardsVar.signal
      .combineWith(expandedIdVar.signal)
      .map { case (cs, expanded) =>
        expanded.flatMap(id => cs.find(_.id == id))
      }
      .distinctBy(_.map(cardRenderKey))

  /** Cheap change key for [[expandedCard]]: everything the popup frame renders from,
    * with an adhoc outcome reduced to its `(runId, revision)` identity and a tap-table
    * entry to its reference ([[TapEntry]] is a plain class — a swapped entry, e.g. a
    * restart, is a new reference). `viewer` is deliberately absent: its `Var`s mutate in
    * place and the popup binds them reactively, so they never require a frame rebuild.
    */
  private def cardRenderKey(c: Card): Any = {
    val kindKey: Any = c match {
      case AdhocCard(_, query, language, outcome, editAssociated, _) =>
        (query, language, outcome.map(o => (o.runId, o.revision)), editAssociated)
      case TapTableCard(_, target, entry, _, stopped, hasSampleLimit, _) => (target, entry, stopped, hasSampleLimit)
    }
    (c.id, kindKey)
  }

  private def findNow(id: CardId): Option[Card] = cardsVar.now().find(_.id == id)

  /** The card (if any) currently tapping `target` — one card per target is the invariant
    * (enforced at [[addTapTableCard]]), so first-match is unambiguous for any card list
    * that invariant actually held for; a stray pre-invariant duplicate (e.g. a session
    * restored from before this fix) just yields the first one, same as [[focusTarget]]
    * always did.
    */
  private def findByTarget(target: TapTarget): Option[Card] =
    cardsVar.now().find {
      case tt: TapTableCard => tt.target == target
      case _: AdhocCard => false
    }

  private def updateCard(id: CardId)(f: Card => Card): Unit =
    cardsVar.update(_.map(c => if (c.id == id) f(c) else c))

  /** [[updateCard]] for state that only a tap-table card has (its entry, resolved query,
    * and stopped flag). An adhoc card has none of those, so it is left untouched.
    */
  private def updateTapCard(id: CardId)(f: TapTableCard => TapTableCard): Unit =
    updateCard(id) {
      case tt: TapTableCard => f(tt)
      case adhoc: AdhocCard => adhoc
    }

  // ── command interpreter ─────────────────────────────────────────────────────────
  private def runExport(vs: ViewerState, outcome: Option[ResultOutcome])(f: CypherQueryResult => Unit): Unit =
    outcome.collect { case ResultOutcome.Tabular(r) => r }.foreach { r =>
      f(ResultsData.derive(r, vs.search.now(), vs.sortCol.now(), vs.sortDir.now()))
      vs.exportOpen.set(false)
    }

  private def outcomeOf(card: Card): Option[ResultOutcome] = card match {
    case adhoc: AdhocCard => adhoc.outcome.map(_.outcome)
    case tt: TapTableCard => Some(ResultOutcome.Tabular(tt.entry.stream.toCypherResult))
  }

  private def interpretViewer(id: CardId, cmd: ViewerCommand): Unit =
    findNow(id).foreach { card =>
      val vs = card.viewer
      cmd match {
        case ViewerCommand.SetView(v) => vs.view.set(v)
        case ViewerCommand.OpenFilter => vs.filterOpen.set(true)
        case ViewerCommand.CloseFilter => vs.filterOpen.set(false)
        case ViewerCommand.SetSearch(text) => vs.search.set(text)
        case ViewerCommand.ClearSearch => vs.search.set("")
        case ViewerCommand.ToggleSort(col) => ViewerState.toggleSort(vs, col)
        case ViewerCommand.SetColWidths(ws) => vs.colWidths.set(ws)
        case ViewerCommand.SelectRow(values) => vs.selectedRow.set(Some(values))
        case ViewerCommand.CloseRow => vs.selectedRow.set(None)
        case ViewerCommand.ToggleExport => vs.exportOpen.update(!_)
        case ViewerCommand.CloseExport => vs.exportOpen.set(false)
        case ViewerCommand.ToggleCsvFlatten => vs.csvFlat.update(!_)
        case ViewerCommand.CopyJson =>
          runExport(vs, outcomeOf(card))(d => ResultsExport.copyToClipboard(ResultsExport.toJson(d)))
        case ViewerCommand.CopyCsv =>
          runExport(vs, outcomeOf(card))(d => ResultsExport.copyToClipboard(ResultsExport.toCsv(d, vs.csvFlat.now())))
        case ViewerCommand.DownloadJson =>
          runExport(vs, outcomeOf(card))(d =>
            ResultsExport.download(ResultsExport.toJson(d), "json", "application/json"),
          )
        case ViewerCommand.DownloadCsv =>
          runExport(vs, outcomeOf(card))(d =>
            ResultsExport.download(ResultsExport.toCsv(d, vs.csvFlat.now()), "csv", "text/csv"),
          )
      }
    }

  /** Expanding a card minimizes whatever was expanded before it — the one-expanded-at-
    * a-time invariant (design doc §3 "One expanded card at a time"). Tap-table cards that
    * get minimized this way auto-stop, same as an explicit [[CardCommand.Minimize]].
    */
  private def expand(id: CardId): Unit = {
    val previouslyExpanded = expandedIdVar.now()
    previouslyExpanded.filterNot(_ == id).foreach(autoStopIfTapTable)
    expandedIdVar.set(Some(id))
  }

  private def minimize(id: CardId): Unit = {
    if (expandedIdVar.now().contains(id)) expandedIdVar.set(None)
    autoStopIfTapTable(id)
  }

  /** Auto-stop is only meaningful for tap-table cards — the one kind with a live stream
    * to free. Adhoc cards have nothing running (and no Restart control to clear the flag).
    *
    * An errored card is exempt: it has no stream left to free, and since [[stopCard]] sets
    * the entry's `ended` flag — which `TapEntry.status` folds into `Ended` — stopping it
    * would clobber the `Error` that the popup's error banner, its ↻ Reconnect button,
    * [[reconnect]]'s guard, [[addTapTableCard]]'s revive branch, and the tap modal's ⊕
    * badge all read. Minimizing a failed card would quietly turn its failure into a pause.
    *
    * A non-resumable tap is exempt too: auto-stop trades "stop consuming a stream you aren't
    * looking at" against "you can always restart it", and for a background query the second
    * half is false. Stopping one on minimize would end the run's capture for good, so a
    * minimized background-query card keeps filling — bounded anyway, since the run terminates
    * on its own and the stream caps its buffer. An explicit [[CardCommand.Stop]] still stops
    * it: that is the user choosing to.
    */
  private def autoStopIfTapTable(id: CardId): Unit =
    findNow(id).foreach {
      case tt: TapTableCard if !tt.stopped && tt.target.resumable && !isErrored(tt.entry) => stopCard(id)
      case _ => ()
    }

  /** Tap-table stop freezes the buffer to a static snapshot, marks the entry ended, and
    * hands the target to the host (`onStopTap`) to free the tap subscription — a
    * [[com.thatdot.quine.webapp.resultspanel.streaming.LiveStream]] is not revivable once
    * frozen (`connect` is once-per-session). To the UI a stopped card is just *paused* —
    * the same state a filled budget produces — and its exits ([[CardCommand.FetchMoreSamples]]
    * / [[CardCommand.GoLive]]) reopen the tap as a continuation via [[reopenContinuing]];
    * the host swaps a fresh, seeded entry in via [[replaceTapTableEntry]].
    */
  // Effects run first against a [[findNow]] snapshot, then the state change lands as a
  // pure `updateCard` — the same protocol as [[closeCard]]. `cardsVar.update` mod
  // functions must stay pure: the host callbacks (`onStopTap` & co.) can re-enter the
  // store synchronously via the host's tap-source binders, and a re-entrant read or
  // update from inside a mod function acts on the pre-update state.
  private def stopCard(id: CardId): Unit =
    findNow(id).foreach {
      case TapTableCard(_, target, entry, _, _, _, _) =>
        entry.stream.freeze()
        entry.ended.set(true)
        onStopTap(target)
        updateTapCard(id)(_.copy(stopped = true))
      case _: AdhocCard => ()
    }

  private def closeCard(id: CardId): Unit = {
    findNow(id).foreach {
      case TapTableCard(_, target, entry, _, _, _, _) =>
        // Same freeze protocol as [[stopCard]]: without it the stream's flush interval and
        // record subscription (plus its row buffer) outlive the closed card for the page's
        // lifetime — the card is gone, so nothing else can ever freeze them.
        entry.stream.freeze()
        entry.ended.set(true)
        // Any in-flight reopen marks die with the card: left behind, they would be
        // consumed by the next card to reopen this target (see [[cancelPendingReopen]]).
        cancelPendingReopen(target)
        onCloseTap(target)
      case _: AdhocCard => ()
    }
    cardsVar.update(_.filterNot(_.id == id))
    if (expandedIdVar.now().contains(id)) expandedIdVar.set(None)
  }

  private def goLive(id: CardId): Unit =
    findNow(id).foreach {
      // An ended tap that cannot be reopened has nothing to go live *to*, so this is a full
      // no-op rather than a cap lift the stream could never honor.
      case TapTableCard(_, target, entry, _, _, _, _) if !entry.ended.now() || target.resumable =>
        // Lift the cap first: a frozen tap (filled budget or user Stop alike) reopens
        // through the continuation protocol, and the fresh session's budget thunk must
        // already read unbounded when it connects. `replaceTapTableEntry` clears `stopped`.
        updateTapCard(id)(_.copy(hasSampleLimit = false))
        if (entry.ended.now()) reopenContinuing(target)
      case _: TapTableCard => ()
      // Adhoc cards never get a live button (design §3) — a full no-op preserves the
      // "adhoc never live" invariant even for a stray GoLive command.
      case _: AdhocCard => ()
    }

  /** Reopen a frozen tap as a *continuation*: the swapped-in fresh stream is seeded with
    * the old one's buffer (see [[replaceTapTableEntry]]) so the rows already on screen stay
    * put and the new session appends after them — unlike a session-restore reconnect, which
    * starts an empty buffer.
    */
  private def reopenContinuing(target: TapTarget): Unit =
    if (target.resumable) {
      continuationKeys += target.key
      onRestartTap(target)
    }

  /** One-shot read of an entry's current display status (observe + kill — a `Signal` has
    * no ownerless `now`), the same pattern the host uses on a just-arrived source.
    */
  private def entryStatusNow(entry: TapEntry): SourceStatus = {
    val statusObs = entry.status.observe(unsafeWindowOwner)
    val status = statusObs.now()
    statusObs.killOriginalSubscription()
    status
  }

  /** Whether `entry`'s stream has failed — the dead-end state whose only exit is a fresh
    * open ([[reconnect]], or [[addTapTableCard]]'s revive branch), never the freeze/reopen
    * continuation protocol.
    */
  private def isErrored(entry: TapEntry): Boolean = entryStatusNow(entry) match {
    case SourceStatus.Error(_) => true
    case _ => false
  }

  /** Reconnect an errored tap card: hand the target back to the host for a fresh open.
    * The errored stream is not continuable — its WS is gone, and its buffer is stale
    * against a possibly recreated standing query — so this rides the same open path as
    * the tap modal's ⊕ pick, whose resolution revives this card in place via
    * [[addTapTableCard]] (fresh entry, restarted budget). Guarded on the entry actually
    * being errored: every other state already has a live or continuable stream and its
    * own controls.
    */
  private def reconnect(id: CardId): Unit =
    findNow(id).foreach {
      case TapTableCard(_, target, entry, _, _, _, _) =>
        entryStatusNow(entry) match {
          case SourceStatus.Error(_) => onReopenTap(target)
          case _ => ()
        }
      case _: AdhocCard => ()
    }

  private def reRun(id: CardId): Unit =
    findNow(id).foreach {
      case AdhocCard(_, query, language, _, _, _) =>
        // Associate only once the host accepts the run: an aborted submission (blank
        // query, pending-query alert) must not leave a dangling association that the
        // user's next unrelated query would then route into this card. Safe after the
        // call — the run's first content emission is async, so the association is in
        // place before the run boundary resolves its target.
        if (onReRun(query, language)) associateForEdit(id)
      case _ => () // Re-run is an adhoc-only control (design §3 header row)
    }

  /** Mark `id` as the sole edit-associated card (design doc §3: "the card remembers the
    * association so the *next* run updates this card rather than spawning a new one" — a
    * one-shot association, not a sticky one). Clears the flag on every other card first so
    * at most one card ever holds it, matching `CardId`'s "survives ... re-runs" contract for
    * both the `Edit ↑` and `Re-run` entry points.
    */
  private def associateForEdit(id: CardId): Unit =
    cardsVar.update(_.map(c => withAssociation(c, c.id == id)))

  /** `c` with its edit association set — a no-op for a tap card, which has no association
    * to hold (`Edit ↑` and `Re-run` are adhoc-only controls).
    */
  private def withAssociation(c: Card, associated: Boolean): Card = c match {
    case adhoc: AdhocCard => adhoc.copy(editAssociated = associated)
    case tt: TapTableCard => tt
  }

  val dispatch: Observer[CardCommand] = Observer {
    case CardCommand.OnViewer(id, cmd) => interpretViewer(id, cmd)
    case CardCommand.Expand(id) => expand(id)
    case CardCommand.Minimize(id) => minimize(id)
    case CardCommand.Close(id) => closeCard(id)
    case CardCommand.ReRun(id) => reRun(id)
    case CardCommand.Stop(id) => stopCard(id)
    case CardCommand.GoLive(id) => goLive(id)
    case CardCommand.Reconnect(id) => reconnect(id)
    case CardCommand.SetSampleSize(id, size) =>
      // Writes the batch size only — the live display cap (`sampleSize`) is untouched, so
      // typing a smaller number never truncates the visible table; the new batch applies
      // on the next FetchMoreSamples.
      findNow(id).foreach(_.viewer.sampleBatch.set(size.max(1)))
    case CardCommand.FetchMoreSamples(id) =>
      // From a frozen stream — budget filled, user Stop, or a Stop after Live — this is
      // the "resume sampled" exit: the budget becomes rows-on-screen + batch (not
      // old-budget + batch, since a stopped Live session may hold far more rows than any
      // budget), the mode returns to Sampled, and the tap reopens as a continuation so
      // the visible rows stay put and the next batch appends after them. A session still
      // filling just grows its budget in place; nothing to reopen. Mode lands before the
      // reopen so the fresh session's budget thunk reads Sampled when it connects.
      findNow(id).foreach {
        case tt: TapTableCard =>
          val batch = tt.viewer.sampleBatch.now()
          if (!tt.entry.ended.now()) tt.viewer.sampleSize.update(_ + batch)
          else if (tt.target.resumable) {
            tt.viewer.sampleSize.set(tt.entry.stream.rows.now().size + batch)
            updateTapCard(id)(_.copy(hasSampleLimit = true))
            reopenContinuing(tt.target)
          }
        // else: an ended, non-reopenable tap (a finished background query) has no more rows
        // to fetch. Growing the budget against its frozen buffer would leave the card
        // claiming to be waiting on results that can never arrive.
        case _: AdhocCard => ()
      }
    case CardCommand.EditQuery(id) =>
      findNow(id).foreach {
        case AdhocCard(_, query, _, _, _, _) =>
          onEditQuery.onNext(query)
          associateForEdit(id)
        case _: TapTableCard =>
          // `Edit ↑` is an adhoc-only control: tap definitions are edited through the tap
          // modal, not the query bar, and the header offers no edit button for tap cards.
          ()
      }
    case CardCommand.Search(text) => drawerSearchVar.set(text)
  }

  val cardDispatch: CardId => Observer[ViewerCommand] =
    id => dispatch.contramap(cmd => CardCommand.OnViewer(id, cmd))

  // ── host-facing constructors (not commands — these create cards from outside events,
  //    e.g. a tap being opened via the tap modal) ─────────────────────────────────────

  /** Register a freshly-opened tap-table card (host calls this once the tap modal /
    * `WiretapService` produces a [[TapEntry]] for a `Results card` destination pick —
    * design doc §2 step 2). Auto-expands, same as a fresh adhoc run — routed through
    * [[expand]] (not a direct `expandedIdVar.set`) so a previously-expanded tap-table
    * card correctly auto-stops via [[autoStopIfTapTable]].
    *
    * One card per target is enforced here, not just by the host's ✓-badge/`focusTarget`
    * UI nicety: if a card already taps `target` (e.g. two opens raced, or the badge state
    * is stale), that card is brought to front instead of a duplicate spawning — same
    * lookup [[focusTarget]] uses. `entry` is by-name and only forced when a card actually
    * takes ownership of it (a fresh card, or an errored card being revived below), so the
    * plain duplicate path never builds a stream no card would own.
    * What the duplicate open does own is its tap subscription: `open` is idempotent per
    * `target.key` (see `TapSubscriptions`), so when the existing card's entry is still
    * live the open reused the very subscription that entry consumes — closing the key
    * would kill the live card's stream — but when that entry has ended (user Stop, filled
    * budget, restored placeholder) its subscription is already closed and the duplicate
    * open's is dangling, so it's freed here via `onCloseTap`.
    *
    * An *errored* entry is the exception to focus-only: error is a dead-end state (the
    * popup grays every stream control), so a deliberate re-tap of the target is the one
    * recovery path the user has. The open's fresh entry is swapped into the existing card
    * — empty buffer, `stopped` cleared, the card's resolved query and sample mode kept —
    * instead of focusing a card that can do nothing.
    */
  def addTapTableCard(target: TapTarget, entry: => TapEntry, query: Option[TapPointQuery]): CardId =
    findByTarget(target) match {
      case Some(existing) =>
        existing match {
          case TapTableCard(_, _, prevEntry, _, _, _, _) =>
            entryStatusNow(prevEntry) match {
              case SourceStatus.Error(_) =>
                // Forced outside the update fn, which must stay pure (same rule as the
                // seeding in `replaceTapTableEntry`).
                val fresh = entry
                // An errored entry is never frozen (freeze travels with `ended`, and an
                // ended entry reads `Ended` here, not `Error`), so freeze it before the
                // swap drops the last reference — same hazard [[closeCard]] guards:
                // otherwise its flush interval and record subscription outlive the swap
                // for the page's lifetime.
                prevEntry.stream.freeze()
                prevEntry.ended.set(true)
                updateTapCard(existing.id)(tt =>
                  tt.copy(entry = fresh, query = tt.query.orElse(query), stopped = false),
                )
              case _ if prevEntry.ended.now() => onCloseTap(target)
              case _ => ()
            }
          case _ => ()
        }
        expand(existing.id)
        existing.id
      case None =>
        val card = TapTableCard.fresh(freshId(), target, entry, query)
        cardsVar.update(_ :+ card)
        expand(card.id)
        card.id
    }

  /** Swap a reopened tap's fresh [[TapEntry]] into the existing card tapping `target` and
    * clear its stopped flag — the reopen half of the freeze/reopen protocol (see
    * [[stopCard]]): [[reopenContinuing]] (or the host's session-restore path) asks the
    * host (`onRestartTap`) to reopen the tap, and the host calls this once the fresh
    * source arrives. A continuation reopen is seeded with the previous session's rows; a
    * restore reconnect begins empty. Returns false (installing nothing) when no card taps
    * `target` any more — the card was closed while the reopen was in flight, and the host
    * should free the just-reopened tap and stream.
    *
    * `queryIfMissing` backfills the card's [[TapPointQuery]] when it has none — a card
    * restored from a snapshot saved before query capture existed, or opened while the tap
    * catalog was still loading. A query the card already resolved is never overwritten.
    *
    * A reconnect marked via [[restoreLive]] also puts the card back in `Live` mode here —
    * a restored card always starts `Sampled` (`CardSnapshot.toCard`), so a reopen that
    * never resolves stays honestly `Sampled` rather than a stuck `Live`.
    */
  def replaceTapTableEntry(target: TapTarget, entry: TapEntry, queryIfMissing: Option[TapPointQuery]): Boolean = {
    val prevCard = findByTarget(target)
    val prevEntry = prevCard.collect { case tt: TapTableCard => tt.entry }
    val continuation = continuationKeys.contains(target.key)
    continuationKeys -= target.key
    val restoringLive = restoreLiveKeys.contains(target.key)
    restoreLiveKeys -= target.key
    prevCard.foreach { card =>
      // A continuation reopen (fetch-more / go-live from a frozen stream) carries the
      // previous session's rows forward; a restore reconnect begins empty. Seeding happens
      // here — outside the `cardsVar.update` mod function, which must stay pure.
      if (continuation) prevEntry.foreach(prev => entry.stream.seedFrom(prev.stream))
      // Swap only the one card resolved above, by id — not every card whose kind matches
      // `target` — so a stray pre-invariant duplicate (a session restored from before
      // one-card-per-target was enforced at `addTapTableCard`) doesn't have a second card's
      // entry silently overwritten by this reopen.
      // `.copy` on the existing kind, not a fresh `TapTableCard(...)`, so the card's
      // resolved `query` survives the entry swap; `orElse` only ever fills a `None`.
      updateTapCard(card.id)(tt =>
        tt.copy(
          entry = entry,
          query = tt.query.orElse(queryIfMissing),
          stopped = false,
          hasSampleLimit = if (restoringLive) false else tt.hasSampleLimit,
        ),
      )
    }
    prevCard.isDefined
  }

  /** The row budget the tap on `target` should honor right now: its card's sample budget
    * while sampling, unbounded once the card went live (or if no card taps `target` —
    * a dangling stream the host is about to free). Read by the host's `LiveStream.connect`
    * budget thunk on every append, so budget growth and go-live apply mid-session.
    */
  def sampleBudgetFor(target: TapTarget): Option[Int] =
    findByTarget(target).flatMap {
      case tt: TapTableCard if tt.hasSampleLimit => Some(tt.viewer.sampleSize.now())
      case _: TapTableCard => None // live — unbounded
      case _: AdhocCard => None
    }

  /** Bring an existing tap card to the front, by the [[TapTarget]] it taps — the pipeline
    * tree's ✓-badge "focus its card instead of re-tapping" action (design doc §2).
    * No-op if no card currently taps `target`.
    */
  def focusTarget(target: TapTarget): Unit =
    findByTarget(target).foreach(c => expand(c.id))

  // ── host-facing persistence hooks (design doc §6 / checklist A11) ─────────────────

  /** The current card list, for the host's snapshot path (see `CardSnapshot.fromCard`). */
  def currentCards: Vector[Card] = cardsVar.now()

  /** The currently expanded card's id. */
  def currentExpandedId: Option[CardId] = expandedIdVar.now()

  /** The card list projected for the host's snapshot path, paired with the position of the
    * expanded card within that projection — the two halves [[restore]] reads back. Ids are
    * session-scoped and never persisted, so the expanded card is recorded by position.
    *
    * Both come from one pass because [[CardSnapshot.fromCard]] drops cards that must not
    * outlive the session (a background-query tap card): an index taken against the unfiltered
    * [[currentCards]] would address the wrong card once one of those has dropped out.
    */
  def snapshotCards: (Vector[CardSnapshot], Option[Int]) = {
    val kept = cardsVar.now().flatMap(card => CardSnapshot.fromCard(card).map(card.id -> _))
    (kept.map(_._2), expandedIdVar.now().map(id => kept.indexWhere(_._1 == id)).filter(_ >= 0))
  }

  /** Restore a card list from the host's persistence path (design doc §6). Snapshots are
    * rebuilt into [[Card]]s here, rather than by the caller, because this store is the only
    * minter of [[CardId]]s: a restored card gets a fresh id, so `expandedIdx` addresses the
    * expanded card by its position in `snapshots` instead. The index is resolved against
    * those original positions, not the surviving ones — [[CardSnapshot.toCard]] drops
    * snapshots that no longer decode.
    *
    * Deliberately bypasses [[expand]]: restoring must not auto-stop cards the way a user
    * expand does.
    */
  def restore(snapshots: Seq[CardSnapshot], expandedIdx: Option[Int]): Unit = {
    val restored = snapshots.zipWithIndex.flatMap { case (snap, idx) =>
      CardSnapshot.toCard(snap, freshId()).map(_ -> idx)
    }.toVector
    cardsVar.set(restored.map(_._1))
    expandedIdVar.set(restored.collectFirst { case (card, idx) if expandedIdx.contains(idx) => card.id })
    // The drawer search needle is not part of the snapshot; a stale one from the previous
    // namespace/session would silently pre-filter the restored cards.
    drawerSearchVar.set("")
  }

  /** Mark `target`'s in-flight session-restore reconnect as one that should put the card
    * back in `Live` mode once it succeeds — the host calls this alongside kicking off the
    * reopen for a tap-table snapshot that `CardSnapshot.wasLive` (restored `stopped`,
    * `Sampled` regardless — see `CardSnapshot.toCard`). [[replaceTapTableEntry]]
    * consumes the mark on that same reconnect; a reopen that never resolves is cancelled
    * by the host's pending-timeout path via [[cancelPendingReopen]] — the mark must not
    * outlive its reopen, or the next reopen of the same target (e.g. a fetch-more
    * continuation) would consume it and silently flip the card to `Live`.
    */
  def restoreLive(target: TapTarget): Unit =
    restoreLiveKeys += target.key

  /** Drop `target`'s in-flight reopen marks ([[continuationKeys]] / [[restoreLiveKeys]]) —
    * called by the host when a pending reopen times out without a source arriving, and on
    * card close. Both sets are keyed on the target alone, so a stale mark would otherwise
    * be consumed by whatever reopens that target next: a leftover restore-live mark flips
    * a bounded fetch-more continuation to `Live`/unbounded.
    */
  def cancelPendingReopen(target: TapTarget): Unit = {
    continuationKeys -= target.key
    restoreLiveKeys -= target.key
  }

  /** Drop every card without any server-side effects, for a namespace switch: the
    * wiretap layer renews its per-namespace store on switch (closing the old
    * namespace's sockets itself), so the only cleanup owed here is client-side —
    * freezing tap-table streams so their buffers and throttle timers stop.
    */
  def resetForNamespaceSwitch(): Unit = {
    cardsVar
      .now()
      .foreach {
        case tt: TapTableCard =>
          tt.entry.stream.freeze()
          tt.entry.ended.set(true)
        case _ => ()
      }
    cardsVar.set(Vector.empty)
    expandedIdVar.set(None)
    drawerSearchVar.set("")
    currentRunTarget = None
    // Reopen marks are keyed on target alone, so one surviving the switch would be
    // consumed by the new namespace's first reopen of the same-named target.
    continuationKeys = Set.empty
    restoreLiveKeys = Set.empty
  }

  /** Close every card through the normal close path (freeing tap subscriptions) — the
    * card half of the junk drawer's reset-canvas actions.
    */
  def closeAllCards(): Unit = cardsVar.now().map(_.id).foreach(closeCard)

  /** Refresh an existing adhoc card in place with a new/refined result, recording the
    * language the run actually used. This is the association's one consumption point:
    * `editAssociated` is cleared here so the "next run updates this card" promise
    * (design doc §3) is one-shot rather than sticking to the card for the rest of the
    * session.
    *
    * When the incoming content belongs to a *different run* than what the card showed
    * (first emission after a Re-run / `Edit ↑` resubmit — batches 2..n share the runId),
    * the card's result-scoped viewer state is reset: a filter/sort/selection/column
    * layout from the old result applied to the new one silently shows "0 rows", a
    * selected row from data that no longer exists, or misaligned widths.
    */
  private def refreshAdhocCard(id: CardId, content: ResultsContent): Unit = {
    findNow(id).foreach { c =>
      val sameRun = c match {
        case adhoc: AdhocCard => adhoc.outcome.exists(_.runId == content.runId)
        case _ => false
      }
      if (!sameRun) ViewerState.resetForNewContent(c.viewer)
    }
    updateCard(id) {
      case adhoc: AdhocCard =>
        adhoc.copy(
          query = content.queryEcho,
          language = content.language,
          outcome = Some(content),
          editAssociated = false,
        )
      // A run never targets a tap card — `onLiveEmission` only ever resolves an adhoc one.
      case tt: TapTableCard => tt
    }
  }

  /** The most recent adhoc card holding this query, out of `snapshot`. Two submissions are
    * the same query iff their trimmed text matches exactly (leading/trailing whitespace is
    * submission noise; anything else — including internal reformatting — is a different
    * query). Most-recent (`findLast` over the append-ordered vector) because restored
    * sessions from before the one-card-per-query invariant can still hold same-query
    * duplicates.
    */
  private def adhocCardMatching(snapshot: Vector[Card], query: String): Option[Card] = {
    val trimmed = query.trim
    snapshot.findLast {
      case adhoc: AdhocCard => adhoc.query.trim == trimmed
      case _: TapTableCard => false
    }
  }

  /** One `liveContent` emission (extracted from [[lifecycleMods]] so the run-boundary
    * routing is exercisable without mounting an element — see `CardsStoreTest`).
    *
    * Auto-creates/updates an adhoc card per new run (design doc §3 model: "liveContent
    * Signal[Option[ResultsContent]] auto-creates/updates an adhoc card per new run").
    * The receiving card is resolved once, at the run boundary, in precedence order:
    *
    *   1. the adhoc card already holding this query ([[adhocCardMatching]]) — re-expanded
    *      and refreshed, so resubmitting a query re-opens its card instead of spawning a
    *      duplicate (one card per query identity). Outranks the edit association: routing
    *      an edited-to-match query into the associated card instead would leave two cards
    *      holding the same query.
    *   2. the edit-associated card (the association `editAssociated` records for a
    *      ReRun/EditQuery-triggered run; there is at most one live query stream, so at
    *      most one association is meaningfully "current" at a time) — how an edited query
    *      no card holds yet still updates the card it was edited from.
    *   3. a fresh card, appended and expanded.
    *
    * Every emission of the run — WS text queries emit once per batch — then routes to
    * that same card via `currentRunTarget`, so a re-run of an older card keeps updating
    * *that* card for batches 2..n instead of falling back to the most recently appended
    * adhoc card.
    */
  private[cards] def onLiveEmission(live: Option[ResultsContent]): Unit = {
    // A new run is any content whose runId differs from the previous emission's (which
    // covers `None -> Some` and back-to-back `Some -> Some` runs alike — the class-doc
    // contract), so the boundary holds even if the host stops clearing the signal
    // between submissions. Batches 2..n of one run share the runId and refresh in place.
    val isNewRun = live.exists(content => prevLive.forall(_.runId != content.runId))
    live.foreach { content =>
      if (isNewRun) {
        // Both candidates are resolved from one snapshot, taken before any effectful call
        // below — `expand` can synchronously auto-stop a previously-expanded tap-table card
        // (autoStopIfTapTable -> stopCard -> updateCard), and a `cardsVar.now()` re-read
        // after that mutation would resolve `associated` against post-effect state instead
        // of the state the run boundary is actually routing against.
        val snapshot = cardsVar.now()
        val matching = adhocCardMatching(snapshot, content.queryEcho)
        val associated = snapshot.find {
          case adhoc: AdhocCard => adhoc.editAssociated
          case _: TapTableCard => false
        }
        matching.foreach(c => expand(c.id))
        val targetId = matching.orElse(associated).map(_.id).getOrElse {
          val card = AdhocCard.fresh(freshId(), content.queryEcho, content.language)
          cardsVar.update(_ :+ card)
          expand(card.id)
          card.id
        }
        currentRunTarget = Some(targetId)
        // Flag hygiene at the run boundary: any association that isn't this run's
        // target is stale by definition — orphaned by an earlier aborted submission, or
        // outranked by a query-identity match — and would otherwise hijack a later run.
        cardsVar.update(_.map(c => if (c.id != targetId) withAssociation(c, false) else c))
      }
      currentRunTarget.foreach(id => refreshAdhocCard(id, content))
    }
    if (live.isEmpty) currentRunTarget = None
    prevLive = live
  }

  // ── event bindings (facts in) — applied by the view, so they are element-owned ─────
  val lifecycleMods: Seq[Modifier[HtmlElement]] = Seq(
    liveContent --> (onLiveEmission(_)),
  )
}
