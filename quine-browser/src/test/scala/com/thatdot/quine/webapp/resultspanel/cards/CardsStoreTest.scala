package com.thatdot.quine.webapp.resultspanel.cards

import com.raquo.laminar.api.L._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{LoneElement, OptionValues}

import com.thatdot.quine.routes.QueryLanguage
import com.thatdot.quine.webapp.resultspanel.{ResultOutcome, ResultsContent}

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

  private def adhocQuery(c: CardState): String = c.kind match {
    case CardKind.AdhocCard(queryText, _, _) => queryText
    case other => fail(s"expected an adhoc card, got $other")
  }

  private def adhocRunId(c: CardState): Option[Long] = c.kind match {
    case CardKind.AdhocCard(_, _, outcome) => outcome.map(_.runId)
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
    val outcome = card.kind match {
      case CardKind.AdhocCard(_, _, o) => o.value
      case other => fail(s"expected an adhoc card, got $other")
    }
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
    after.editAssociated shouldBe false // the association is one-shot
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
    afterA.editAssociated shouldBe false // stale association cleared at the boundary
  }
}
