package com.thatdot.quine.webapp.resultspanel.cards

import scala.scalajs.js

import com.raquo.laminar.api.L._

import com.thatdot.quine.routes.{CypherQueryResult, QueryLanguage}
import com.thatdot.quine.webapp.resultspanel.{
  ResultOutcome,
  ResultsContent,
  ResultsData,
  ResultsExport,
  TapEntry,
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
  *     [[QueryKey]] identity — one card per query), else into the edit-associated card
  *     (see [[CardCommand.EditQuery]] / [[CardState.editAssociated]] — the association
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
) {

  // ── state slices (private — views read only the exposed signals) ──────────────────
  private val cardsVar: Var[Vector[CardState]] = Var(Vector.empty)
  private val expandedIdVar: Var[Option[CardId]] = Var(None)
  private val drawerSearchVar: Var[String] = Var("")

  private var prevLive: Option[ResultsContent] = None

  /** Tap targets whose in-flight reopen is a sampling *continuation* (fetch-more / go-live
    * from a frozen stream) rather than a session-restore reconnect — consumed by
    * [[replaceTapTableEntry]] to decide whether to seed the swapped-in stream.
    */
  private var continuationKeys: Set[String] = Set.empty

  /** The card receiving the current run's emissions. Resolved once at the run boundary
    * (the edit-associated card if any, else a freshly appended card) and used for every
    * subsequent emission of the same run — WS text queries emit once per batch, so
    * routing must be per-run, not per-emission, or batches 2..n of a re-run land on
    * whichever adhoc card happens to be newest.
    */
  private var currentRunTarget: Option[CardId] = None

  // ── reads exposed to components ─────────────────────────────────────────────────
  val cards: Signal[Vector[CardState]] = cardsVar.signal
  val expandedId: Signal[Option[CardId]] = expandedIdVar.signal
  val drawerSearch: Signal[String] = drawerSearchVar.signal

  /** The minimized cards, most-recent-first — the order the drawer stacks them in. */
  val minimizedCards: Signal[Vector[CardState]] =
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
  val expandedCard: Signal[Option[CardState]] =
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
  private def cardRenderKey(c: CardState): Any = {
    val kindKey: Any = c.kind match {
      case CardKind.AdhocCard(query, language, outcome) =>
        (query, language, outcome.map(o => (o.runId, o.revision)))
      case CardKind.TapTableCard(target, entry, _) => (target, entry)
    }
    (c.id, c.title, c.createdAt, c.mode, c.stopped, c.editAssociated, kindKey)
  }

  private def findNow(id: CardId): Option[CardState] = cardsVar.now().find(_.id == id)

  private def updateCard(id: CardId)(f: CardState => CardState): Unit =
    cardsVar.update(_.map(c => if (c.id == id) f(c) else c))

  // ── command interpreter ─────────────────────────────────────────────────────────
  private def runExport(vs: ViewerState, outcome: Option[ResultOutcome])(f: CypherQueryResult => Unit): Unit =
    outcome.collect { case ResultOutcome.Tabular(r) => r }.foreach { r =>
      f(ResultsData.derive(r, vs.search.now(), vs.sortCol.now(), vs.sortDir.now()))
      vs.exportOpen.set(false)
    }

  private def outcomeOf(kind: CardKind): Option[ResultOutcome] = kind match {
    case CardKind.AdhocCard(_, _, outcome) => outcome.map(_.outcome)
    case CardKind.TapTableCard(_, entry, _) => Some(ResultOutcome.Tabular(entry.stream.toCypherResult))
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
          runExport(vs, outcomeOf(card.kind))(d => ResultsExport.copyToClipboard(ResultsExport.toJson(d)))
        case ViewerCommand.CopyCsv =>
          runExport(vs, outcomeOf(card.kind))(d =>
            ResultsExport.copyToClipboard(ResultsExport.toCsv(d, vs.csvFlat.now())),
          )
        case ViewerCommand.DownloadJson =>
          runExport(vs, outcomeOf(card.kind))(d =>
            ResultsExport.download(ResultsExport.toJson(d), "json", "application/json"),
          )
        case ViewerCommand.DownloadCsv =>
          runExport(vs, outcomeOf(card.kind))(d =>
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
    */
  private def autoStopIfTapTable(id: CardId): Unit =
    findNow(id).foreach { card =>
      card.kind match {
        case _: CardKind.TapTableCard if !card.stopped => stopCard(id)
        case _ => ()
      }
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
    findNow(id).foreach { card =>
      card.kind match {
        case CardKind.TapTableCard(target, entry, _) =>
          entry.stream.freeze()
          entry.ended.set(true)
          onStopTap(target)
        case _: CardKind.AdhocCard => ()
      }
      updateCard(id)(_.copy(stopped = true))
    }

  private def closeCard(id: CardId): Unit = {
    findNow(id).foreach(_.kind match {
      case CardKind.TapTableCard(target, entry, _) =>
        // Same freeze protocol as [[stopCard]]: without it the stream's flush interval and
        // record subscription (plus its row buffer) outlive the closed card for the page's
        // lifetime — the card is gone, so nothing else can ever freeze them.
        entry.stream.freeze()
        entry.ended.set(true)
        onCloseTap(target)
      case _: CardKind.AdhocCard => ()
    })
    cardsVar.update(_.filterNot(_.id == id))
    if (expandedIdVar.now().contains(id)) expandedIdVar.set(None)
  }

  private def goLive(id: CardId): Unit =
    findNow(id).foreach { card =>
      card.kind match {
        case CardKind.TapTableCard(target, entry, _) =>
          // Mode first: a frozen tap (filled budget or user Stop alike) reopens through the
          // continuation protocol, and the fresh session's budget thunk must already read
          // Live (unbounded) when it connects. `replaceTapTableEntry` clears `stopped`.
          updateCard(id)(_.copy(mode = SampleMode.Live))
          if (entry.ended.now()) reopenContinuing(target)
        // Adhoc cards never get a live button (design §3) — a full no-op preserves the
        // "adhoc never live" invariant even for a stray GoLive command.
        case _: CardKind.AdhocCard => ()
      }
    }

  /** Reopen a frozen tap as a *continuation*: the swapped-in fresh stream is seeded with
    * the old one's buffer (see [[replaceTapTableEntry]]) so the rows already on screen stay
    * put and the new session appends after them — unlike a session-restore reconnect, which
    * starts an empty buffer.
    */
  private def reopenContinuing(target: TapTarget): Unit = {
    continuationKeys += target.key
    onRestartTap(target)
  }

  private def reRun(id: CardId): Unit =
    findNow(id).foreach(_.kind match {
      case CardKind.AdhocCard(query, language, _) =>
        // Associate only once the host accepts the run: an aborted submission (blank
        // query, pending-query alert) must not leave a dangling association that the
        // user's next unrelated query would then route into this card. Safe after the
        // call — the run's first content emission is async, so the association is in
        // place before the run boundary resolves its target.
        if (onReRun(query, language)) associateForEdit(id)
      case _ => () // Re-run is an adhoc-only control (design §3 header row)
    })

  /** Mark `id` as the sole edit-associated card (design doc §3: "the card remembers the
    * association so the *next* run updates this card rather than spawning a new one" — a
    * one-shot association, not a sticky one). Clears the flag on every other card first so
    * at most one card ever holds it, matching `CardId`'s "survives ... re-runs" contract for
    * both the `Edit ↑` and `Re-run` entry points.
    */
  private def associateForEdit(id: CardId): Unit =
    cardsVar.update(_.map(c => c.copy(editAssociated = c.id == id)))

  val dispatch: Observer[CardCommand] = Observer {
    case CardCommand.OnViewer(id, cmd) => interpretViewer(id, cmd)
    case CardCommand.Expand(id) => expand(id)
    case CardCommand.Minimize(id) => minimize(id)
    case CardCommand.Close(id) => closeCard(id)
    case CardCommand.ReRun(id) => reRun(id)
    case CardCommand.Stop(id) => stopCard(id)
    case CardCommand.GoLive(id) => goLive(id)
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
      findNow(id).foreach { c =>
        c.kind match {
          case CardKind.TapTableCard(target, entry, _) =>
            val batch = c.viewer.sampleBatch.now()
            if (entry.ended.now()) {
              c.viewer.sampleSize.set(entry.stream.rows.now().size + batch)
              updateCard(id)(_.copy(mode = SampleMode.Sampled))
              reopenContinuing(target)
            } else c.viewer.sampleSize.update(_ + batch)
          case _: CardKind.AdhocCard => ()
        }
      }
    case CardCommand.EditQuery(id) =>
      findNow(id).foreach(_.kind match {
        case CardKind.AdhocCard(query, _, _) =>
          onEditQuery.onNext(query)
          associateForEdit(id)
        case _: CardKind.TapTableCard =>
          // `Edit ↑` is an adhoc-only control: tap definitions are edited through the tap
          // modal, not the query bar, and the header offers no edit button for tap cards.
          ()
      })
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
    */
  def addTapTableCard(target: TapTarget, entry: TapEntry, query: Option[TapCardQuery]): CardId = {
    val card = CardState.freshTapTable(target, entry, query, CardsStore.nowLabel())
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
    * `queryIfMissing` backfills the card's [[TapCardQuery]] when it has none — a card
    * restored from a snapshot saved before query capture existed, or opened while the tap
    * catalog was still loading. A query the card already resolved is never overwritten.
    */
  def replaceTapTableEntry(target: TapTarget, entry: TapEntry, queryIfMissing: Option[TapCardQuery]): Boolean = {
    val prevEntry = cardsVar
      .now()
      .collectFirst(Function.unlift { c =>
        c.kind match {
          case CardKind.TapTableCard(t, prev, _) if t == target => Some(prev)
          case _ => None
        }
      })
    val continuation = continuationKeys.contains(target.key)
    continuationKeys -= target.key
    prevEntry.foreach { prev =>
      // A continuation reopen (fetch-more / go-live from a frozen stream) carries the
      // previous session's rows forward; a restore reconnect begins empty. Seeding happens
      // here — outside the `cardsVar.update` mod function, which must stay pure.
      if (continuation) entry.stream.seedFrom(prev.stream)
      cardsVar.update(_.map { c =>
        c.kind match {
          // `.copy` on the existing kind, not a fresh `TapTableCard(...)`, so the card's
          // resolved `query` survives the entry swap; `orElse` only ever fills a `None`.
          case tt: CardKind.TapTableCard if tt.target == target =>
            c.copy(kind = tt.copy(entry = entry, query = tt.query.orElse(queryIfMissing)), stopped = false)
          case _ => c
        }
      })
    }
    prevEntry.isDefined
  }

  /** The row budget the tap on `target` should honor right now: its card's sample budget
    * while sampling, unbounded once the card went live (or if no card taps `target` —
    * a dangling stream the host is about to free). Read by the host's `LiveStream.connect`
    * budget thunk on every append, so budget growth and go-live apply mid-session.
    */
  def sampleBudgetFor(target: TapTarget): Option[Int] =
    cardsVar
      .now()
      .find(c =>
        c.kind match {
          case CardKind.TapTableCard(t, _, _) => t == target
          case _: CardKind.AdhocCard => false
        },
      )
      .flatMap(c =>
        c.mode match {
          case SampleMode.Sampled => Some(c.viewer.sampleSize.now())
          case SampleMode.Live => None
        },
      )

  /** Bring an existing tap card to the front, by the [[TapTarget]] it taps — the pipeline
    * tree's ✓-badge "focus its card instead of re-tapping" action (design doc §2).
    * No-op if no card currently taps `target`.
    */
  def focusTarget(target: TapTarget): Unit =
    cardsVar
      .now()
      .find { c =>
        c.kind match {
          case CardKind.TapTableCard(t, _, _) => t == target
          case _: CardKind.AdhocCard => false
        }
      }
      .foreach(c => expand(c.id))

  // ── host-facing persistence hooks (design doc §6 / checklist A11) ─────────────────

  /** The current card list, for the host's snapshot path (see `CardSnapshot.fromState`). */
  def currentCards: Vector[CardState] = cardsVar.now()

  /** The currently expanded card's id, for the host's snapshot path. */
  def currentExpandedId: Option[CardId] = expandedIdVar.now()

  /** Restore a card list from the host's persistence path (design doc §6). Callers
    * construct [[CardState]] values themselves (via [[CardSnapshot.toState]]: `Restored`
    * or full saved outcomes for adhoc cards, placeholder entries for tap-table cards) —
    * this just installs them, along with which card, if any, is expanded. Deliberately
    * bypasses [[expand]]: restoring must not auto-stop cards the way a user expand does.
    */
  def restore(restored: Vector[CardState], expandedId: Option[CardId]): Unit = {
    cardsVar.set(restored)
    expandedIdVar.set(expandedId.filter(id => restored.exists(_.id == id)))
    // The drawer search needle is not part of the snapshot; a stale one from the previous
    // namespace/session would silently pre-filter the restored cards.
    drawerSearchVar.set("")
  }

  /** Drop every card without any server-side effects, for a namespace switch: the
    * wiretap layer renews its per-namespace store on switch (closing the old
    * namespace's sockets itself), so the only cleanup owed here is client-side —
    * freezing tap-table streams so their buffers and throttle timers stop.
    */
  def resetForNamespaceSwitch(): Unit = {
    cardsVar
      .now()
      .foreach(_.kind match {
        case CardKind.TapTableCard(_, entry, _) =>
          entry.stream.freeze()
          entry.ended.set(true)
        case _ => ()
      })
    cardsVar.set(Vector.empty)
    expandedIdVar.set(None)
    drawerSearchVar.set("")
    currentRunTarget = None
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
      val sameRun = c.kind match {
        case CardKind.AdhocCard(_, _, Some(prev)) => prev.runId == content.runId
        case _ => false
      }
      if (!sameRun) ViewerState.resetForNewContent(c.viewer)
    }
    updateCard(id) { c =>
      val kind = CardKind.AdhocCard(content.queryEcho, content.language, Some(content))
      c.copy(kind = kind, title = CardState.titleFor(kind), editAssociated = false)
    }
  }

  /** The most recent adhoc card holding this query, by [[QueryKey]] identity. Most-recent
    * (`findLast` over the append-ordered vector) because restored sessions from before the
    * one-card-per-query invariant can still hold same-query duplicates.
    */
  private def adhocCardMatching(query: String): Option[CardState] = {
    val key = QueryKey.of(query)
    cardsVar
      .now()
      .findLast(c =>
        c.kind match {
          case CardKind.AdhocCard(q, _, _) => QueryKey.of(q) == key
          case _: CardKind.TapTableCard => false
        },
      )
  }

  /** One `liveContent` emission (extracted from [[lifecycleMods]] so the run-boundary
    * routing is exercisable without mounting an element — see `CardsStoreTest`).
    *
    * Auto-creates/updates an adhoc card per new run (design doc §3 model: "liveContent
    * Signal[Option[ResultsContent]] auto-creates/updates an adhoc card per new run").
    * The receiving card is resolved once, at the run boundary, in precedence order:
    *
    *   1. the adhoc card already holding this query ([[QueryKey]] identity) — re-expanded
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
        val matching = adhocCardMatching(content.queryEcho)
        matching.foreach(c => expand(c.id))
        val associated = cardsVar.now().find(c => c.editAssociated && CardState.isAdhoc(c.kind))
        val targetId = matching.orElse(associated).map(_.id).getOrElse {
          val card = CardState.freshAdhoc(content.queryEcho, content.language, CardsStore.nowLabel())
          cardsVar.update(_ :+ card)
          expand(card.id)
          card.id
        }
        currentRunTarget = Some(targetId)
        // Flag hygiene at the run boundary: any association that isn't this run's
        // target is stale by definition — left on a non-adhoc card by EditQuery (only
        // adhoc associations are claimable above), orphaned by an earlier aborted
        // submission, or outranked by a query-identity match — and would otherwise
        // hijack a later run.
        cardsVar.update(_.map(c => if (c.editAssociated && c.id != targetId) c.copy(editAssociated = false) else c))
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

object CardsStore {
  private def nowLabel(): String = {
    val d = new js.Date()
    f"${d.getHours().toInt}%02d:${d.getMinutes().toInt}%02d"
  }
}
