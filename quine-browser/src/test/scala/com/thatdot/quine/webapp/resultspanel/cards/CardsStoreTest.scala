package com.thatdot.quine.webapp.resultspanel.cards

import com.raquo.laminar.api.L._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{LoneElement, OptionValues}

import com.thatdot.quine.routes.QueryLanguage
import com.thatdot.quine.webapp.resultspanel.streaming.{LiveStream, StreamRow}
import com.thatdot.quine.webapp.resultspanel.{
  LiveSource,
  Provenance,
  ResultOutcome,
  ResultsContent,
  SourceKind,
  SourceStatus,
  TapEntry,
  TapPoint,
  TapTarget,
  ViewerState,
}

/** The adhoc-card run-boundary contract: one card per query identity (trimmed text).
  * A run whose query matches an existing adhoc card re-opens and refreshes that card;
  * only a query with no matching card appends a new one.
  */
class CardsStoreTest extends AnyFunSuite with Matchers with OptionValues with LoneElement {

  private def content(query: String, runId: Long, revision: Int = 0): ResultsContent =
    ResultsContent(
      outcome = ResultOutcome.EmptyResult(wasTabular = true, columns = Seq("n")),
      queryEcho = query,
      language = QueryLanguage.Cypher,
      runId = runId,
      revision = revision,
    )

  private def newStore(): CardsStore =
    new CardsStore(liveContent = Var(Option.empty[ResultsContent]).signal)

  private def adhocQuery(c: Card): String = adhocCard(c).query

  private def adhocRunId(c: Card): Option[Long] = adhocCard(c).outcome.map(_.runId)

  private def tapTarget(sqName: String = "sq1"): TapTarget = TapTarget.StandingQuery(sqName, TapPoint.Raw)

  /** A fresh, connected [[TapEntry]] for `target` — not yet ended/frozen (a just-opened or
    * just-reopened tap session).
    */
  private def freshEntry(target: TapTarget): TapEntry = {
    val stream = new LiveStream
    val source = LiveSource(
      id = s"test:${target.key}",
      provenance = Provenance(SourceKind.Tap, target.label),
      status = Val(SourceStatus.Live),
      records = EventStream.empty,
      tapTarget = Some(target),
    )
    stream.connect(source.records)
    new TapEntry(source, stream)
  }

  /** An entry whose producer reports a terminal error (e.g. a restore reconnect that found
    * its standing query deleted) — the dead-end state a re-tap revives the card from.
    */
  private def erroredEntry(target: TapTarget): TapEntry = {
    val stream = new LiveStream
    val source = LiveSource(
      id = s"test:${target.key}",
      provenance = Provenance(SourceKind.Tap, target.label),
      status = Val(SourceStatus.Error("standing query missing")),
      records = EventStream.empty,
      tapTarget = Some(target),
    )
    stream.connect(source.records)
    new TapEntry(source, stream)
  }

  private def tapCard(store: CardsStore, target: TapTarget = tapTarget()): Card = {
    val id = store.addTapTableCard(target, freshEntry(target), query = None)
    store.currentCards.find(_.id == id).value
  }

  /** [[CardSnapshot.toCard]] on its own, for the tests that check the snapshot projection
    * without going through [[CardsStore.restore]] (which mints its own ids). The id is
    * arbitrary — nothing about `toCard` depends on it.
    */
  private def restoreOne(snap: CardSnapshot): Card = CardSnapshot.toCard(snap, CardId(1)).value

  private def tapKind(c: Card): TapTableCard = c match {
    case tt: TapTableCard => tt
    case other => fail(s"expected a tap-table card, got $other")
  }

  private def adhocCard(c: Card): AdhocCard = c match {
    case adhoc: AdhocCard => adhoc
    case other => fail(s"expected an adhoc card, got $other")
  }

  test("a first run creates a card and expands it") {
    val store = newStore()
    store.onLiveEmission(Some(content("MATCH (n) RETURN n", runId = 1)))

    val card = store.currentCards.loneElement
    adhocQuery(card) shouldBe "MATCH (n) RETURN n"
    adhocRunId(card).value shouldBe 1L
    store.currentExpandedId.value shouldBe card.id
  }

  test("resubmitting the same query reuses the existing card: re-expands and refreshes it") {
    val store = newStore()
    store.onLiveEmission(Some(content("MATCH (n) RETURN n", runId = 1)))
    val card = store.currentCards.loneElement
    store.dispatch.onNext(CardCommand.Minimize(card.id))
    store.currentExpandedId shouldBe None

    store.onLiveEmission(Some(content("MATCH (n) RETURN n", runId = 2)))

    val after = store.currentCards.loneElement
    after.id shouldBe card.id // same card, not a new one
    adhocRunId(after).value shouldBe 2L // ...holding the new run's results
    store.currentExpandedId.value shouldBe card.id // ...and pulled back out of the tray
  }

  test("query identity ignores leading/trailing whitespace but nothing else") {
    val store = newStore()
    store.onLiveEmission(Some(content("MATCH (n) RETURN n", runId = 1)))
    store.onLiveEmission(Some(content("  MATCH (n) RETURN n\n", runId = 2)))

    val card = store.currentCards.loneElement
    adhocRunId(card).value shouldBe 2L
    // Internal reformatting is a different query, so it gets its own card.
    store.onLiveEmission(Some(content("MATCH (n)  RETURN n", runId = 3)))
    store.currentCards should have size 2
  }

  test("a different query appends a second card") {
    val store = newStore()
    store.onLiveEmission(Some(content("MATCH (a) RETURN a", runId = 1)))
    store.onLiveEmission(Some(content("MATCH (b) RETURN b", runId = 2)))

    store.currentCards should have size 2
    store.currentExpandedId.value shouldBe store.currentCards.last.id
  }

  test("batches of one run keep refreshing the run's card, not matching anew") {
    val store = newStore()
    store.onLiveEmission(Some(content("MATCH (n) RETURN n", runId = 1, revision = 0)))
    store.onLiveEmission(Some(content("MATCH (n) RETURN n", runId = 1, revision = 1)))

    val card = store.currentCards.loneElement
    val outcome = adhocCard(card).outcome.value
    (outcome.runId, outcome.revision) shouldBe ((1L, 1))
  }

  test("the edit association still routes a query no card holds into the edited card") {
    val store = newStore()
    store.onLiveEmission(Some(content("MATCH (a) RETURN a", runId = 1)))
    val card = store.currentCards.loneElement
    store.dispatch.onNext(CardCommand.EditQuery(card.id))

    store.onLiveEmission(Some(content("MATCH (a) RETURN a LIMIT 5", runId = 2)))

    val after = store.currentCards.loneElement
    after.id shouldBe card.id
    adhocQuery(after) shouldBe "MATCH (a) RETURN a LIMIT 5"
    adhocCard(after).editAssociated shouldBe false // the association is one-shot
  }

  test("a query-identity match takes precedence over the edit association") {
    val store = newStore()
    store.onLiveEmission(Some(content("MATCH (a) RETURN a", runId = 1)))
    store.onLiveEmission(Some(content("MATCH (b) RETURN b", runId = 2)))
    store.currentCards should have size 2
    val (cardA, cardB) = (store.currentCards.head, store.currentCards.last)
    store.dispatch.onNext(CardCommand.EditQuery(cardA.id))

    // Edited from card A, but the submitted text is card B's query: B receives the run.
    store.onLiveEmission(Some(content("MATCH (b) RETURN b", runId = 3)))

    store.currentCards should have size 2
    val (afterA, afterB) = (store.currentCards.head, store.currentCards.last)
    adhocRunId(afterA).value shouldBe 1L // A untouched
    adhocRunId(afterB).value shouldBe 3L // B refreshed
    store.currentExpandedId.value shouldBe cardB.id
    adhocCard(afterA).editAssociated shouldBe false // stale association cleared at the boundary
  }

  // ── tap-table state machine ─────────────────────────────────────────────────────────

  test("Stop freezes a tap card's stream, ends its entry, and sets stopped") {
    val store = newStore()
    val target = tapTarget()
    val card = tapCard(store, target)
    tapKind(card).entry.ended.now() shouldBe false

    store.dispatch.onNext(CardCommand.Stop(card.id))

    val after = store.currentCards.loneElement
    tapKind(after).stopped shouldBe true
    tapKind(after).entry.ended.now() shouldBe true
  }

  test("GoLive lifts the sample cap and reopens a frozen (stopped) card as a continuation") {
    val store = newStore()
    val target = tapTarget()
    val card = tapCard(store, target)
    store.dispatch.onNext(CardCommand.Stop(card.id))
    tapKind(store.currentCards.loneElement).hasSampleLimit shouldBe true

    store.dispatch.onNext(CardCommand.GoLive(card.id))

    tapKind(store.currentCards.loneElement).hasSampleLimit shouldBe false
    // The reopen is a continuation: replacing the entry should seed from the frozen buffer.
    val fresh = freshEntry(target)
    val installed = store.replaceTapTableEntry(target, fresh, queryIfMissing = None)
    installed shouldBe true
    val after = store.currentCards.loneElement
    tapKind(after).stopped shouldBe false
    tapKind(after).hasSampleLimit shouldBe false
  }

  test("GoLive on a still-filling (not stopped) card just lifts the cap, without reopening") {
    val store = newStore()
    val target = tapTarget()
    val card = tapCard(store, target)
    tapKind(card).stopped shouldBe false

    store.dispatch.onNext(CardCommand.GoLive(card.id))

    val after = store.currentCards.loneElement
    tapKind(after).hasSampleLimit shouldBe false
    tapKind(after).stopped shouldBe false // never stopped, so nothing to reopen
  }

  test("FetchMoreSamples on a still-filling card grows the budget in place") {
    val store = newStore()
    val card = tapCard(store)
    val batch = card.viewer.sampleBatch.now()
    val before = card.viewer.sampleSize.now()

    store.dispatch.onNext(CardCommand.FetchMoreSamples(card.id))

    val after = store.currentCards.loneElement
    after.viewer.sampleSize.now() shouldBe (before + batch)
    tapKind(after).stopped shouldBe false
    tapKind(after).hasSampleLimit shouldBe true
  }

  test("FetchMoreSamples on a frozen card resumes sampled: budget = rows-on-screen + batch") {
    val store = newStore()
    val target = tapTarget()
    val card = tapCard(store, target)
    store.dispatch.onNext(CardCommand.Stop(card.id))
    val rowsOnScreen = tapKind(store.currentCards.loneElement).entry.stream.rows.now().size
    val batch = card.viewer.sampleBatch.now()

    store.dispatch.onNext(CardCommand.FetchMoreSamples(card.id))

    val after = store.currentCards.loneElement
    tapKind(after).hasSampleLimit shouldBe true
    after.viewer.sampleSize.now() shouldBe (rowsOnScreen + batch)
    // Still logically "reopening": the card stays on its old (now-detached) entry until the
    // host swaps a fresh one in via replaceTapTableEntry.
    tapKind(after).stopped shouldBe true
  }

  test("replaceTapTableEntry seeds the new stream from the old one on a continuation reopen") {
    val store = newStore()
    val target = tapTarget()
    val card = tapCard(store, target)
    store.dispatch.onNext(CardCommand.Stop(card.id))
    val oldEntry = tapKind(store.currentCards.loneElement).entry
    oldEntry.stream.rows.set(Vector(StreamRow(1, isMatch = true, data = io.circe.Json.obj(), raw = io.circe.Json.Null)))

    store.dispatch.onNext(CardCommand.GoLive(card.id)) // marks this reopen as a continuation
    val fresh = freshEntry(target)
    store.replaceTapTableEntry(target, fresh, queryIfMissing = None) shouldBe true

    fresh.stream.rows.now() shouldBe oldEntry.stream.rows.now()
  }

  test("replaceTapTableEntry on a plain restore-reconnect does not seed (begins empty)") {
    val store = newStore()
    val target = tapTarget()
    val snap = CardSnapshot.fromCard(tapCard(store, target)).value.copy(stopped = true, hasSampleLimit = true)
    store.restore(Seq(snap), expandedIdx = None)

    val fresh = freshEntry(target)
    fresh.stream.rows.now() shouldBe Vector.empty
    store.replaceTapTableEntry(target, fresh, queryIfMissing = None) shouldBe true

    fresh.stream.rows.now() shouldBe Vector.empty // no continuation was marked, so no seeding
    val after = store.currentCards.loneElement
    tapKind(after).stopped shouldBe false
    tapKind(after).hasSampleLimit shouldBe true // wasn't marked restoreLive
  }

  test("replaceTapTableEntry returns false and installs nothing when no card taps the target") {
    val store = newStore()
    val target = tapTarget()

    store.replaceTapTableEntry(target, freshEntry(target), queryIfMissing = None) shouldBe false
    store.currentCards shouldBe empty
  }

  test("addTapTableCard returns the existing card's id when one already taps the target") {
    val store = newStore()
    val target = tapTarget()
    val firstId = store.addTapTableCard(target, freshEntry(target), query = None)

    val secondId = store.addTapTableCard(target, freshEntry(target), query = None)

    secondId shouldBe firstId
    store.currentCards should have size 1
  }

  test("addTapTableCard never builds the entry for a duplicate target") {
    val store = newStore()
    val target = tapTarget()
    var entriesBuilt = 0
    def countedEntry(): TapEntry = { entriesBuilt += 1; freshEntry(target) }
    store.addTapTableCard(target, countedEntry(), query = None)
    entriesBuilt shouldBe 1

    store.addTapTableCard(target, countedEntry(), query = None)

    entriesBuilt shouldBe 1 // the by-name entry is only forced when a fresh card installs
  }

  test("a duplicate-target add on a live existing card leaves its tap subscription alone") {
    var closedTargets = Vector.empty[TapTarget]
    val store = new CardsStore(
      liveContent = Var(Option.empty[ResultsContent]).signal,
      onCloseTap = t => closedTargets :+= t,
    )
    val target = tapTarget()
    store.addTapTableCard(target, freshEntry(target), query = None)

    store.addTapTableCard(target, freshEntry(target), query = None)

    // The existing card's live entry consumes the same target.key subscription the
    // duplicate open reused — closing it would kill that stream.
    closedTargets shouldBe empty
  }

  test("a duplicate-target add on an ended existing card frees the open's dangling subscription") {
    var closedTargets = Vector.empty[TapTarget]
    val store = new CardsStore(
      liveContent = Var(Option.empty[ResultsContent]).signal,
      onCloseTap = t => closedTargets :+= t,
    )
    val target = tapTarget()
    val id = store.addTapTableCard(target, freshEntry(target), query = None)
    store.dispatch.onNext(CardCommand.Stop(id)) // ends the entry; its subscription is already freed

    store.addTapTableCard(target, freshEntry(target), query = None)

    closedTargets shouldBe Vector(target)
    store.currentCards should have size 1
  }

  test("addTapTableCard for an existing target expands (focuses) that card") {
    val store = newStore()
    val target = tapTarget()
    val id = store.addTapTableCard(target, freshEntry(target), query = None)
    store.dispatch.onNext(CardCommand.Minimize(id))
    store.currentExpandedId shouldBe None

    val again = store.addTapTableCard(target, freshEntry(target), query = None)

    again shouldBe id
    store.currentExpandedId.value shouldBe id
  }

  test("re-tapping a target whose card errored revives it in place with the fresh entry") {
    val store = newStore()
    val target = tapTarget()
    val id = store.addTapTableCard(target, erroredEntry(target), query = None)

    val fresh = freshEntry(target)
    val again = store.addTapTableCard(target, fresh, query = None)

    again shouldBe id
    val card = store.currentCards.loneElement
    assert(tapKind(card).entry eq fresh, "the errored entry should be swapped for the fresh one")
    tapKind(card).stopped shouldBe false
    store.currentExpandedId.value shouldBe id
  }

  test("a revive freezes and ends the dropped errored entry — nothing else can reach it after the swap") {
    val store = newStore()
    val target = tapTarget()
    store.addTapTableCard(target, erroredEntry(target), query = None)
    val errored = tapKind(store.currentCards.loneElement).entry
    errored.ended.now() shouldBe false // errored, not ended: its stream is still consuming

    store.addTapTableCard(target, freshEntry(target), query = None)

    // Freeze travels with `ended` (see CardsStore's freeze protocol): ended here means the
    // dropped entry's flush interval and record subscription were torn down with it.
    errored.ended.now() shouldBe true
  }

  test("a revive keeps the reopened tap subscription — the revived card's entry consumes it") {
    var closedTargets = Vector.empty[TapTarget]
    val store = new CardsStore(
      liveContent = Var(Option.empty[ResultsContent]).signal,
      onCloseTap = t => closedTargets :+= t,
    )
    val target = tapTarget()
    store.addTapTableCard(target, erroredEntry(target), query = None)

    store.addTapTableCard(target, freshEntry(target), query = None)

    closedTargets shouldBe empty
  }

  test("Reconnect on an errored card hands its target to the host's reopen path") {
    var reopened = Vector.empty[TapTarget]
    val store = new CardsStore(
      liveContent = Var(Option.empty[ResultsContent]).signal,
      onReopenTap = t => reopened :+= t,
    )
    val target = tapTarget()
    val id = store.addTapTableCard(target, erroredEntry(target), query = None)

    store.dispatch.onNext(CardCommand.Reconnect(id))

    reopened shouldBe Vector(target)
  }

  test("Reconnect is a no-op unless the card's entry is errored") {
    var reopened = Vector.empty[TapTarget]
    val store = new CardsStore(
      liveContent = Var(Option.empty[ResultsContent]).signal,
      onReopenTap = t => reopened :+= t,
    )
    val live = tapCard(store)
    store.dispatch.onNext(CardCommand.Reconnect(live.id))

    reopened shouldBe empty
  }

  test("minimizing an errored card does not auto-stop it — the error survives, unlike a paused card") {
    val store = newStore()
    val target = tapTarget()
    val id = store.addTapTableCard(target, erroredEntry(target), query = None)

    store.dispatch.onNext(CardCommand.Minimize(id))

    // `ended` is what `TapEntry.status` folds into `Ended`, overwriting `Error`: leaving it
    // false is what keeps the banner, ↻ Reconnect, and the modal's ⊕ badge on a failed card.
    val card = tapKind(store.currentCards.loneElement)
    card.entry.ended.now() shouldBe false
    card.stopped shouldBe false
  }

  test("an errored card minimized by expanding another card still revives on a re-tap") {
    val store = newStore()
    val target = tapTarget()
    val id = store.addTapTableCard(target, erroredEntry(target), query = None)
    store.addTapTableCard(tapTarget("other"), freshEntry(tapTarget("other")), query = None)
    store.currentExpandedId.value should not be id // the errored card was minimized by the expand

    val fresh = freshEntry(target)
    store.addTapTableCard(target, fresh, query = None) shouldBe id

    assert(tapKind(store.currentCards.find(_.id == id).value).entry eq fresh)
  }

  test("a non-errored tap card still auto-stops when it is minimized") {
    val store = newStore()
    val card = tapCard(store)

    store.dispatch.onNext(CardCommand.Minimize(card.id))

    tapKind(store.currentCards.loneElement).stopped shouldBe true
  }

  test("addTapTableCard for a different target still appends a second card") {
    val store = newStore()
    val targetA = tapTarget("sqA")
    val targetB = tapTarget("sqB")
    store.addTapTableCard(targetA, freshEntry(targetA), query = None)

    store.addTapTableCard(targetB, freshEntry(targetB), query = None)

    store.currentCards should have size 2
  }

  // ── CardSnapshot round-trip ──────────────────────────────────────────────────────────

  test("CardSnapshot round-trips an adhoc card's outcome as Restored") {
    val store = newStore()
    store.onLiveEmission(Some(content("MATCH (n) RETURN n", runId = 1)))
    val card = store.currentCards.loneElement

    val snap = CardSnapshot.fromCard(card).value
    val restored = restoreOne(snap)

    restored match {
      case AdhocCard(_, query, _, Some(outcome), _, _) =>
        query shouldBe "MATCH (n) RETURN n"
        outcome.outcome shouldBe a[ResultOutcome.EmptyResult]
      case other => fail(s"expected an adhoc card with an outcome, got $other")
    }
  }

  test("CardSnapshot restores a tap-table card stopped, on a placeholder entry, regardless of saved state") {
    val store = newStore()
    val target = tapTarget()
    val card = tapCard(store, target)
    tapKind(card).stopped shouldBe false

    val snap = CardSnapshot.fromCard(card).value // saved while live/unstopped
    snap.stopped shouldBe false
    val restored = restoreOne(snap)

    tapKind(restored).stopped shouldBe true
    tapKind(restored).entry.ended.now() shouldBe true
  }

  test("CardSnapshot keeps a restored tap-table card capped even when saved uncapped") {
    val store = newStore()
    val target = tapTarget()
    val card = tapCard(store, target)
    store.dispatch.onNext(CardCommand.GoLive(card.id))
    tapKind(store.currentCards.loneElement).hasSampleLimit shouldBe false

    val snap = CardSnapshot.fromCard(store.currentCards.loneElement).value
    snap.hasSampleLimit shouldBe false
    val restored = restoreOne(snap)

    tapKind(restored).hasSampleLimit shouldBe true
    tapKind(restored).stopped shouldBe true
    CardSnapshot.wasLive(snap) shouldBe true // the host's cue to restoreLive once reconnected
  }

  test("sampleBudgetFor a stopped restored card saved uncapped is bounded, not unbounded") {
    val store = newStore()
    val target = tapTarget()
    val card = tapCard(store, target)
    store.dispatch.onNext(CardCommand.GoLive(card.id))
    val snap = CardSnapshot.fromCard(store.currentCards.loneElement).value
    store.restore(Seq(snap), expandedIdx = None)

    tapKind(store.currentCards.loneElement).hasSampleLimit shouldBe true
    store.sampleBudgetFor(target) shouldBe Some(snap.sampleSize) // the saved budget, still bounded
  }

  test("a successful restore-reconnect marked via restoreLive lifts the card's cap again") {
    val store = newStore()
    val target = tapTarget()
    val card = tapCard(store, target)
    store.dispatch.onNext(CardCommand.GoLive(card.id))
    val snap = CardSnapshot.fromCard(store.currentCards.loneElement).value
    store.restore(Seq(snap), expandedIdx = None)
    store.sampleBudgetFor(target) shouldBe defined // still bounded pre-reconnect

    store.restoreLive(target)
    val installed = store.replaceTapTableEntry(target, freshEntry(target), queryIfMissing = None)

    installed shouldBe true
    val after = store.currentCards.loneElement
    tapKind(after).hasSampleLimit shouldBe false
    tapKind(after).stopped shouldBe false
    store.sampleBudgetFor(target) shouldBe None // unbounded once truly live again
  }

  test("cancelPendingReopen expires a restore-live mark: a later continuation reopen stays capped") {
    val store = newStore()
    val target = tapTarget()
    val card = tapCard(store, target)
    store.dispatch.onNext(CardCommand.GoLive(card.id))
    val snap = CardSnapshot.fromCard(store.currentCards.loneElement).value
    store.restore(Seq(snap), expandedIdx = None)
    store.restoreLive(target) // restore reconnect kicked off...
    store.cancelPendingReopen(target) // ...but timed out (the host's pending-timeout path)

    // The next reopen of the target is a bounded fetch-more continuation. Without the
    // cancel, the stale mark would be consumed here and silently flip the card to Live.
    store.dispatch.onNext(CardCommand.FetchMoreSamples(store.currentCards.loneElement.id))
    store.replaceTapTableEntry(target, freshEntry(target), queryIfMissing = None) shouldBe true

    tapKind(store.currentCards.loneElement).hasSampleLimit shouldBe true
    store.sampleBudgetFor(target) shouldBe defined // still bounded
  }

  test("cancelPendingReopen expires a continuation mark: the next swap does not seed") {
    val store = newStore()
    val target = tapTarget()
    val card = tapCard(store, target)
    store.dispatch.onNext(CardCommand.Stop(card.id))
    val oldEntry = tapKind(store.currentCards.loneElement).entry
    oldEntry.stream.rows.set(Vector(StreamRow(1, isMatch = true, data = io.circe.Json.obj(), raw = io.circe.Json.Null)))
    store.dispatch.onNext(CardCommand.GoLive(card.id)) // marks a continuation reopen...
    store.cancelPendingReopen(target) // ...that timed out

    val fresh = freshEntry(target)
    store.replaceTapTableEntry(target, fresh, queryIfMissing = None) shouldBe true

    fresh.stream.rows.now() shouldBe Vector.empty
  }

  test("closing a card cancels its reopen marks — they don't leak into the target's next card") {
    val store = newStore()
    val target = tapTarget()
    tapCard(store, target)
    store.restoreLive(target) // a reopen is in flight for this card...
    store.dispatch.onNext(CardCommand.Close(store.currentCards.loneElement.id)) // ...when it closes

    // A fresh card on the same target, later doing its own bounded fetch-more reopen.
    val id2 = store.addTapTableCard(target, freshEntry(target), query = None)
    store.dispatch.onNext(CardCommand.Stop(id2))
    store.dispatch.onNext(CardCommand.FetchMoreSamples(id2))
    store.replaceTapTableEntry(target, freshEntry(target), queryIfMissing = None) shouldBe true

    tapKind(store.currentCards.loneElement).hasSampleLimit shouldBe true // stale mark did not flip it
  }

  test("a reopen not marked via restoreLive leaves the restored card's cap unchanged") {
    val store = newStore()
    val target = tapTarget()
    val card = tapCard(store, target)
    val snap = CardSnapshot.fromCard(card).value.copy(stopped = true, hasSampleLimit = true)
    store.restore(Seq(snap), expandedIdx = None)

    store.replaceTapTableEntry(target, freshEntry(target), queryIfMissing = None) shouldBe true

    tapKind(store.currentCards.loneElement).hasSampleLimit shouldBe true
  }

  // ── pause-vs-mid-fill restore semantics ─────────────────────────────────────────────

  private def streamRows(n: Int): Vector[StreamRow] =
    (1 to n).toVector.map(i =>
      StreamRow(i.toLong, isMatch = true, data = io.circe.Json.obj(), raw = io.circe.Json.Null),
    )

  test("a tap paused by a filled budget persists stopped — a restore never reactivates it") {
    val store = newStore()
    tapCard(store)
    val entry = tapKind(store.currentCards.loneElement).entry
    entry.ended.set(true) // what onBudgetFilled does; the card's own stopped flag stays false
    tapKind(store.currentCards.loneElement).stopped shouldBe false

    val snap = CardSnapshot.fromCard(store.currentCards.loneElement).value

    snap.stopped shouldBe true // ended folds into stopped: the host's reopen gate reads this
  }

  test("a stopped tap restores with its sample count reset to the default") {
    val store = newStore()
    val card = tapCard(store)
    card.viewer.sampleSize.set(37)
    store.dispatch.onNext(CardCommand.Stop(card.id))

    val snap = CardSnapshot.fromCard(store.currentCards.loneElement).value
    val restored = restoreOne(snap)

    tapKind(restored).stopped shouldBe true
    restored.viewer.sampleSize.now() shouldBe ViewerState.DefaultSampleSize
  }

  test("a tap saved mid-fill restores its whole budget — the buffer those rows were in is gone") {
    val store = newStore()
    val target = tapTarget()
    val card = tapCard(store, target)
    card.viewer.sampleSize.set(20)
    tapKind(store.currentCards.loneElement).entry.stream.rows.set(streamRows(12)) // 12 of 20 arrived

    val snap = CardSnapshot.fromCard(store.currentCards.loneElement).value
    snap.stopped shouldBe false // still streaming at save: the host reopens this card

    store.restore(Seq(snap), expandedIdx = None)
    val restored = store.currentCards.loneElement
    tapKind(restored).hasSampleLimit shouldBe true
    // The restored stream is an empty placeholder and its reconnect does not seed, so the
    // budget has to cover every row the card will show — not the 8 that were outstanding
    // against a buffer the restore threw away.
    tapKind(restored).entry.stream.rows.now() shouldBe empty
    restored.viewer.sampleSize.now() shouldBe 20
    store.sampleBudgetFor(target) shouldBe Some(20)
  }

  // ── restore identity: fresh ids, expanded card addressed by index ────────────────────

  test("restore mints fresh ids off the store's own counter, never reusing a saved one") {
    val store = newStore()
    store.onLiveEmission(Some(content("MATCH (a) RETURN a", runId = 1)))
    val before = store.currentCards.loneElement.id
    val snaps = store.currentCards.flatMap(CardSnapshot.fromCard)

    store.restore(snaps, expandedIdx = None)
    // A card the same store mints afterwards: the collision the old persisted-id scheme
    // needed a random suffix to avoid.
    store.onLiveEmission(Some(content("MATCH (b) RETURN b", runId = 2)))

    val ids = store.currentCards.map(_.id)
    ids should have size 2
    ids.distinct should have size 2
    ids should not contain before
  }

  test("restore expands the card at expandedIdx") {
    val store = newStore()
    store.onLiveEmission(Some(content("MATCH (a) RETURN a", runId = 1)))
    store.onLiveEmission(Some(content("MATCH (b) RETURN b", runId = 2)))
    val snaps = store.currentCards.flatMap(CardSnapshot.fromCard)

    store.restore(snaps, expandedIdx = Some(0))

    store.currentExpandedId.value shouldBe store.currentCards.head.id
    store.snapshotCards._2 shouldBe Some(0)
  }

  test("expandedIdx indexes the snapshots, so an undecodable one ahead of it doesn't shift it") {
    val store = newStore()
    store.onLiveEmission(Some(content("MATCH (a) RETURN a", runId = 1)))
    store.onLiveEmission(Some(content("MATCH (b) RETURN b", runId = 2)))
    val snaps = store.currentCards.flatMap(CardSnapshot.fromCard)
    // A snapshot whose kind no longer decodes — dropped by `toCard`, and index 1 must
    // still mean the card that was saved at index 1.
    val withDropped = snaps.head.copy(kind = "tapGraph") +: snaps.tail

    store.restore(withDropped, expandedIdx = Some(1))

    val card = store.currentCards.loneElement
    adhocQuery(card) shouldBe "MATCH (b) RETURN b"
    store.currentExpandedId.value shouldBe card.id
  }
}
