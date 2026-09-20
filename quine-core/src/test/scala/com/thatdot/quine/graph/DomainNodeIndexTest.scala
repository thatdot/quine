package com.thatdot.quine.graph

import scala.collection.mutable

import org.scalacheck.Gen
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import com.thatdot.common.logging.Log.SafeLogger
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.behavior.DomainNodeIndexBehavior.DomainNodeIndex
import com.thatdot.quine.graph.behavior.DomainNodeIndexBehavior.DomainNodeIndex.DomainIndexResult
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId

/** [[DomainNodeIndex]]: what a node holds of its peers' answers about the children of the patterns rooted on it.
  *
  * Every decision this structure makes is a pure function of what it holds, so each is stated here as a case of
  * its own rather than reached through an actor. The suites that drive a graph say what a node does; this says
  * what it may do, which is what those suites are then free to assume.
  *
  * Three of the methods carry a rule that no single call can express, so they are also stated as properties over
  * generated indices: `newIndex` reports exactly whether it changed anything, `updateResult` never touches the
  * query attribution, and `answerWouldChange` agrees with whether `updateResult` would.
  */
class DomainNodeIndexTest extends AnyFunSuite with Matchers with ScalaCheckPropertyChecks {

  implicit private val log: SafeLogger = SafeLogger("thatdot.test.DomainNodeIndexTest")

  private val idProvider: QuineIdLongProvider = QuineIdLongProvider()
  private val peerA: QuineId = idProvider.customIdToQid(1L)
  private val peerB: QuineId = idProvider.customIdToQid(2L)

  private val childX: DomainGraphNodeId = 10L
  private val childY: DomainGraphNodeId = 11L

  private val q1: StandingQueryId = StandingQueryId.fresh()
  private val q2: StandingQueryId = StandingQueryId.fresh()
  private val q3: StandingQueryId = StandingQueryId.fresh()

  /** An immutable reading of the whole index, so that "changed nothing" can be asserted rather than described. */
  private def contentsOf(index: DomainNodeIndex): Map[QuineId, Map[DomainGraphNodeId, DomainIndexResult]] =
    index.index.map { case (peer, byDgn) => peer -> byDgn.toMap }.toMap

  private def indexOf(
    entries: (QuineId, Map[DomainGraphNodeId, DomainIndexResult])*,
  ): DomainNodeIndex =
    new DomainNodeIndex(mutable.Map.from(entries.map { case (peer, byDgn) => peer -> mutable.Map.from(byDgn) }))

  private def running(live: StandingQueryId*): StandingQueryId => Boolean = live.toSet.contains

  // -- DomainIndexResult ----------------------------------------------------------------------------------------

  test("an answer records its queries exactly when it names at least one") {
    DomainIndexResult(None, Set.empty).recordsItsQueries shouldBe false
    DomainIndexResult(Some(true), Set.empty).recordsItsQueries shouldBe false
    DomainIndexResult(None, Set(q1)).recordsItsQueries shouldBe true
  }

  test("an answer naming no queries has no live one, which is unknown rather than unwanted") {
    // The distinction the rest of the bookkeeping turns on: `hasLiveQuery` is false here, and
    // `dropAnswersNoLiveQueryNeeds` reads that as "ask the parent index instead", not as "drop it".
    DomainIndexResult(Some(true), Set.empty).hasLiveQuery(running(q1, q2, q3)) shouldBe false
    DomainIndexResult(Some(true), Set.empty).recordsItsQueries shouldBe false
  }

  test("an answer has a live query when any one of the queries it was asked for still runs") {
    val held = DomainIndexResult(Some(true), Set(q1, q2))
    withClue("both running: ")(held.hasLiveQuery(running(q1, q2)) shouldBe true)
    withClue("one running: ")(held.hasLiveQuery(running(q2)) shouldBe true)
    withClue("neither running: ")(held.hasLiveQuery(running(q3)) shouldBe false)
  }

  // -- newIndex -------------------------------------------------------------------------------------------------

  test("newIndex on a peer this node holds nothing for creates the entry and asks") {
    val index = new DomainNodeIndex()
    index.newIndex(peerA, childX, Set(q1)) shouldBe true
    index.index(peerA)(childX) shouldBe DomainIndexResult(None, Set(q1))
  }

  test("newIndex on a peer already held for another pattern adds to that peer's map") {
    val index = indexOf(peerA -> Map(childX -> DomainIndexResult(Some(true), Set(q1))))
    index.newIndex(peerA, childY, Set(q2)) shouldBe true
    index.index(peerA).keySet shouldBe Set(childX, childY)
    withClue("the entry that was already there is untouched: ")(
      index.index(peerA)(childX) shouldBe DomainIndexResult(Some(true), Set(q1)),
    )
  }

  test("newIndex asked again for queries the entry already records changes nothing and does not ask") {
    val index = indexOf(peerA -> Map(childX -> DomainIndexResult(Some(true), Set(q1, q2))))
    val before = contentsOf(index)
    withClue("the same query: ")(index.newIndex(peerA, childX, Set(q1)) shouldBe false)
    withClue("a subset: ")(index.newIndex(peerA, childX, Set(q1, q2)) shouldBe false)
    withClue("no query at all: ")(index.newIndex(peerA, childX, Set.empty) shouldBe false)
    contentsOf(index) shouldBe before
  }

  test("newIndex asked for a query the peer has not heard of records it and asks again") {
    val index = indexOf(peerA -> Map(childX -> DomainIndexResult(Some(true), Set(q1))))
    index.newIndex(peerA, childX, Set(q2)) shouldBe true
    withClue("the queries are unioned, and the answer already held is kept: ")(
      index.index(peerA)(childX) shouldBe DomainIndexResult(Some(true), Set(q1, q2)),
    )
  }

  test("newIndex does not ask again merely because the answer has not arrived yet") {
    // The change from the behaviour this replaced, which re-asked whenever the entry held no answer: a
    // question already out is answered when the peer gets to it, and asking again costs a message per
    // evaluation. What decides is the query set, not the answer.
    val index = indexOf(peerA -> Map(childX -> DomainIndexResult(None, Set(q1))))
    index.newIndex(peerA, childX, Set(q1)) shouldBe false
    index.index(peerA)(childX).answer shouldBe None
  }

  test("an entry created for no queries is still an entry, and is not created twice") {
    val index = new DomainNodeIndex()
    withClue("created: ")(index.newIndex(peerA, childX, Set.empty) shouldBe true)
    withClue("and not again: ")(index.newIndex(peerA, childX, Set.empty) shouldBe false)
    index.index(peerA)(childX) shouldBe DomainIndexResult(None, Set.empty)
  }

  // -- updateResult / answerWouldChange -------------------------------------------------------------------------

  test("updateResult records an answer into an entry this node created by asking") {
    val index = indexOf(peerA -> Map(childX -> DomainIndexResult(None, Set(q1))))
    index.updateResult(peerA, childX, result = true) shouldBe true
    index.index(peerA)(childX) shouldBe DomainIndexResult(Some(true), Set(q1))
  }

  test("updateResult overwrites an answer the peer has changed its mind about") {
    val index = indexOf(peerA -> Map(childX -> DomainIndexResult(Some(true), Set(q1))))
    index.updateResult(peerA, childX, result = false) shouldBe true
    index.index(peerA)(childX).answer shouldBe Some(false)
  }

  test("updateResult refuses an answer about a pattern this node does not ask that peer about") {
    val index = indexOf(peerA -> Map(childX -> DomainIndexResult(Some(true), Set(q1))))
    val before = contentsOf(index)
    withClue("a child this node does not ask this peer about: ")(
      index.updateResult(peerA, childY, result = true) shouldBe false,
    )
    withClue("a peer this node asks nothing: ")(index.updateResult(peerB, childX, result = true) shouldBe false)
    withClue("and nothing was created for either: ")(contentsOf(index) shouldBe before)
  }

  test("answerWouldChange is false for an entry that is not there, so no journal row is written for a refusal") {
    val index = indexOf(peerA -> Map(childX -> DomainIndexResult(Some(true), Set(q1))))
    index.answerWouldChange(peerB, childX, result = true) shouldBe false
    index.answerWouldChange(peerA, childY, result = true) shouldBe false
  }

  test("answerWouldChange is false for an answer repeating what is recorded") {
    val index = indexOf(peerA -> Map(childX -> DomainIndexResult(Some(true), Set(q1))))
    index.answerWouldChange(peerA, childX, result = true) shouldBe false
    index.answerWouldChange(peerA, childX, result = false) shouldBe true
  }

  test("answerWouldChange is true while the question is still out") {
    val index = indexOf(peerA -> Map(childX -> DomainIndexResult(None, Set(q1))))
    index.answerWouldChange(peerA, childX, result = true) shouldBe true
    index.answerWouldChange(peerA, childX, result = false) shouldBe true
  }

  // -- recordAnswerCreatingEntry --------------------------------------------------------------------------------

  test("recordAnswerCreatingEntry creates the entry an answer belongs to when the fold has none") {
    val index = new DomainNodeIndex()
    index.recordAnswerCreatingEntry(peerA, childX, result = true, askedFor = Set.empty)
    withClue("a result carries no query ids, so the entry records none and is judged by the parent index: ")(
      index.index(peerA)(childX) shouldBe DomainIndexResult(Some(true), Set.empty),
    )
  }

  test("recordAnswerCreatingEntry joins an entry the fold has already re-derived, keeping its queries") {
    val index = indexOf(peerA -> Map(childX -> DomainIndexResult(None, Set(q1, q2))))
    index.recordAnswerCreatingEntry(peerA, childX, result = false, askedFor = Set(q3))
    withClue("the queries the re-derived subscription recorded win; the answer is what the event carries: ")(
      index.index(peerA)(childX) shouldBe DomainIndexResult(Some(false), Set(q1, q2)),
    )
  }

  // -- removeIndex / removeAllIndicesInefficiently --------------------------------------------------------------

  test("removeIndex hands back what was held and drops the peer once nothing is held for it") {
    val index = indexOf(
      peerA -> Map(childX -> DomainIndexResult(Some(true), Set(q1)), childY -> DomainIndexResult(None, Set(q2))),
    )
    index.removeIndex(peerA, childX) shouldBe Some(peerA -> DomainIndexResult(Some(true), Set(q1)))
    withClue("the peer is still held for its other pattern: ")(index.contains(peerA) shouldBe true)
    index.removeIndex(peerA, childY) shouldBe Some(peerA -> DomainIndexResult(None, Set(q2)))
    withClue("and is gone once nothing is held for it: ")(index.contains(peerA) shouldBe false)
  }

  test("removing what is not held changes nothing") {
    val index = indexOf(peerA -> Map(childX -> DomainIndexResult(Some(true), Set(q1))))
    val before = contentsOf(index)
    index.removeIndex(peerA, childY) shouldBe None
    index.removeIndex(peerB, childX) shouldBe None
    contentsOf(index) shouldBe before
  }

  test("removeAllIndicesInefficiently drops one pattern's answers across every peer") {
    val index = indexOf(
      peerA -> Map(childX -> DomainIndexResult(Some(true), Set(q1)), childY -> DomainIndexResult(Some(false), Set(q1))),
      peerB -> Map(childX -> DomainIndexResult(Some(false), Set(q2))),
    )
    val removed = index.removeAllIndicesInefficiently(childX).toSet
    removed shouldBe Set(
      peerA -> DomainIndexResult(Some(true), Set(q1)),
      peerB -> DomainIndexResult(Some(false), Set(q2)),
    )
    withClue("the other pattern's answer is untouched, and the peer holding only childX is gone: ")(
      contentsOf(index) shouldBe Map(peerA -> Map(childY -> DomainIndexResult(Some(false), Set(q1)))),
    )
  }

  // -- retainResults --------------------------------------------------------------------------------------------

  test("retainResults drops what it rejects and reports whether it dropped anything") {
    val index = indexOf(
      peerA -> Map(childX -> DomainIndexResult(Some(true), Set(q1)), childY -> DomainIndexResult(Some(true), Set(q2))),
      peerB -> Map(childX -> DomainIndexResult(Some(true), Set(q1))),
    )
    withClue("keeping everything reports no drop: ")(index.retainResults((_, _) => true) shouldBe false)
    withClue("dropping one pattern reports a drop: ")(index.retainResults((child, _) => child != childX) shouldBe true)
    withClue("and the peer left holding nothing is gone: ")(
      contentsOf(index) shouldBe Map(peerA -> Map(childY -> DomainIndexResult(Some(true), Set(q2)))),
    )
  }

  test("retainResults can judge by what an entry holds, not only by its pattern") {
    val index = indexOf(
      peerA -> Map(childX -> DomainIndexResult(Some(true), Set(q1)), childY -> DomainIndexResult(Some(true), Set(q2))),
    )
    index.retainResults((_, held) => held.hasLiveQuery(running(q2))) shouldBe true
    contentsOf(index) shouldBe Map(peerA -> Map(childY -> DomainIndexResult(Some(true), Set(q2))))
  }

  // -- pruneCancelledQueries ------------------------------------------------------------------------------------

  test("pruneCancelledQueries leaves alone an entry that records no queries") {
    // Unknown rather than stale: this is a snapshot written before the queries were recorded, and narrowing
    // an empty set is not something the entry's own record supports.
    val index = indexOf(peerA -> Map(childX -> DomainIndexResult(Some(true), Set.empty)))
    index.pruneCancelledQueries(running()) shouldBe false
    index.index(peerA)(childX) shouldBe DomainIndexResult(Some(true), Set.empty)
  }

  test("pruneCancelledQueries leaves an entry whose queries all still run") {
    val index = indexOf(peerA -> Map(childX -> DomainIndexResult(Some(true), Set(q1, q2))))
    index.pruneCancelledQueries(running(q1, q2, q3)) shouldBe false
    index.index(peerA)(childX).forQueries shouldBe Set(q1, q2)
  }

  test("pruneCancelledQueries narrows an entry to the queries that still run") {
    val index = indexOf(peerA -> Map(childX -> DomainIndexResult(Some(true), Set(q1, q2))))
    index.pruneCancelledQueries(running(q2)) shouldBe true
    withClue("the answer is kept; only the attribution narrows: ")(
      index.index(peerA)(childX) shouldBe DomainIndexResult(Some(true), Set(q2)),
    )
  }

  test(
    "pruneCancelledQueries on an entry whose every query is gone would empty it, which is why nothing calls it alone",
  ) {
    // Recorded because an emptied set is indistinguishable from one that was never recorded, and the two are
    // judged by different rules. `dropAnswersNoLiveQueryNeeds` is what keeps this unreachable: it drops such an
    // entry outright before pruning what survives. The composition is asserted below.
    val index = indexOf(peerA -> Map(childX -> DomainIndexResult(Some(true), Set(q1))))
    index.pruneCancelledQueries(running(q2)) shouldBe true
    index.index(peerA)(childX).recordsItsQueries shouldBe false
  }

  test("dropping first and pruning after never leaves an entry that recorded queries holding none") {
    // The order `dropAnswersNoLiveQueryNeeds` uses, stated as the invariant it exists to maintain.
    forAll(genIndex, Gen.someOf(Seq(q1, q2, q3)), minSuccessful(200)) { (index, live) =>
      val isRunning = running(live.toSeq: _*)
      val hadRecorded = contentsOf(index).view.mapValues(_.view.mapValues(_.recordsItsQueries).toMap).toMap
      val _ = index.retainResults((_, held) => !held.recordsItsQueries || held.hasLiveQuery(isRunning))
      val _ = index.pruneCancelledQueries(isRunning)
      contentsOf(index).foreach { case (peer, byDgn) =>
        byDgn.foreach { case (child, held) =>
          withClue(s"entry ($peer, $child) recorded queries before and holds none now: ")(
            held.recordsItsQueries shouldBe hadRecorded(peer)(child),
          )
        }
      }
    }
  }

  // -- recordQueriesWhereUnrecorded -----------------------------------------------------------------------------

  test("recordQueriesWhereUnrecorded fills in an entry that records none") {
    val index = indexOf(peerA -> Map(childX -> DomainIndexResult(Some(true), Set.empty)))
    index.recordQueriesWhereUnrecorded(_ => Set(q1, q2)) shouldBe true
    index.index(peerA)(childX) shouldBe DomainIndexResult(Some(true), Set(q1, q2))
  }

  test("recordQueriesWhereUnrecorded leaves an entry that already records its own") {
    val index = indexOf(peerA -> Map(childX -> DomainIndexResult(Some(true), Set(q1))))
    index.recordQueriesWhereUnrecorded(_ => Set(q2, q3)) shouldBe false
    index.index(peerA)(childX).forQueries shouldBe Set(q1)
  }

  test("recordQueriesWhereUnrecorded leaves an entry nothing can be derived for") {
    val index = indexOf(peerA -> Map(childX -> DomainIndexResult(Some(true), Set.empty)))
    index.recordQueriesWhereUnrecorded(_ => Set.empty) shouldBe false
    withClue("it stays unrecorded, for the parent-index rule to judge: ")(
      index.index(peerA)(childX).recordsItsQueries shouldBe false,
    )
  }

  test("recordQueriesWhereUnrecorded derives per child pattern, not per peer") {
    val index = indexOf(
      peerA -> Map(childX -> DomainIndexResult(Some(true), Set.empty), childY -> DomainIndexResult(None, Set.empty)),
    )
    index.recordQueriesWhereUnrecorded(child => if (child == childX) Set(q1) else Set.empty) shouldBe true
    index.index(peerA)(childX).forQueries shouldBe Set(q1)
    index.index(peerA)(childY).forQueries shouldBe Set.empty
  }

  // -- lookup / contains ----------------------------------------------------------------------------------------

  test("lookup gives the answer only where one has arrived") {
    val index = indexOf(
      peerA -> Map(childX -> DomainIndexResult(Some(false), Set(q1)), childY -> DomainIndexResult(None, Set(q1))),
    )
    withClue("an answer that arrived: ")(index.lookup(peerA, childX) shouldBe Some(false))
    withClue("a question still out: ")(index.lookup(peerA, childY) shouldBe None)
    withClue("a pattern not asked about: ")(index.lookup(peerB, childX) shouldBe None)
  }

  // -- properties -----------------------------------------------------------------------------------------------

  private val genQueries: Gen[Set[StandingQueryId]] = Gen.someOf(Seq(q1, q2, q3)).map(_.toSet)

  private val genResult: Gen[DomainIndexResult] = for {
    answer <- Gen.oneOf(None, Some(true), Some(false))
    queries <- genQueries
  } yield DomainIndexResult(answer, queries)

  /** A whole index over two peers and two child patterns, which is enough for every case each method
    * distinguishes: present or absent, answered or not, attributed or not.
    */
  private val genIndex: Gen[DomainNodeIndex] = for {
    cells <- Gen.listOfN(
      4,
      Gen.option(genResult),
    )
    keys = Seq(peerA -> childX, peerA -> childY, peerB -> childX, peerB -> childY)
  } yield {
    val index = new DomainNodeIndex()
    keys.zip(cells).foreach {
      case ((peer, child), Some(held)) =>
        index.index.getOrElseUpdate(peer, mutable.Map.empty) += (child -> held)
      case (_, None) => ()
    }
    index
  }

  private val genKey: Gen[(QuineId, DomainGraphNodeId)] =
    Gen.oneOf(Seq(peerA -> childX, peerA -> childY, peerB -> childX, peerB -> childY))

  test("newIndex reports exactly whether it changed the index") {
    forAll(genIndex, genKey, genQueries, minSuccessful(300)) { (index, key, queries) =>
      val (peer, child) = key
      val before = contentsOf(index)
      val asked = index.newIndex(peer, child, queries)
      withClue(s"newIndex($peer, $child, $queries) on $before returned $asked: ")(
        asked shouldBe (contentsOf(index) != before),
      )
    }
  }

  test("after newIndex the entry records at least the queries it was asked on behalf of") {
    forAll(genIndex, genKey, genQueries, minSuccessful(300)) { (index, key, queries) =>
      val (peer, child) = key
      val _ = index.newIndex(peer, child, queries)
      index.index(peer)(child).forQueries should contain allElementsOf queries
    }
  }

  test("newIndex never disturbs an answer already held, nor any other entry") {
    forAll(genIndex, genKey, genQueries, minSuccessful(300)) { (index, key, queries) =>
      val (peer, child) = key
      val before = contentsOf(index)
      val _ = index.newIndex(peer, child, queries)
      val after = contentsOf(index)
      withClue("the answer at the key asked about: ")(
        after(peer)(child).answer shouldBe before.get(peer).flatMap(_.get(child)).flatMap(_.answer),
      )
      val otherKeysBefore = before.map { case (p, m) => p -> m.filterNot { case (c, _) => p == peer && c == child } }
      val otherKeysAfter = after.map { case (p, m) => p -> m.filterNot { case (c, _) => p == peer && c == child } }
      withClue("every other entry: ")(
        otherKeysAfter.filter(_._2.nonEmpty) shouldBe otherKeysBefore.filter(_._2.nonEmpty),
      )
    }
  }

  test("updateResult never changes which queries an answer is held for") {
    forAll(genIndex, genKey, Gen.oneOf(true, false), minSuccessful(300)) { (index, key, answer) =>
      val (peer, child) = key
      val before = contentsOf(index)
      val _ = index.updateResult(peer, child, answer)
      val after = contentsOf(index)
      withClue("the query attribution of every entry: ")(
        after.map { case (p, m) => p -> m.view.mapValues(_.forQueries).toMap } shouldBe
        before.map { case (p, m) => p -> m.view.mapValues(_.forQueries).toMap },
      )
    }
  }

  test("answerWouldChange agrees with whether updateResult changes the index") {
    forAll(genIndex, genKey, Gen.oneOf(true, false), minSuccessful(300)) { (index, key, answer) =>
      val (peer, child) = key
      val predicted = index.answerWouldChange(peer, child, answer)
      val before = contentsOf(index)
      val _ = index.updateResult(peer, child, answer)
      withClue(s"answerWouldChange said $predicted for ($peer, $child) := $answer on $before: ")(
        predicted shouldBe (contentsOf(index) != before),
      )
    }
  }

  test("updateResult lands exactly where an entry exists") {
    forAll(genIndex, genKey, Gen.oneOf(true, false), minSuccessful(300)) { (index, key, answer) =>
      val (peer, child) = key
      val existed = index.contains(peer, child)
      index.updateResult(peer, child, answer) shouldBe existed
      if (existed) index.lookup(peer, child) shouldBe Some(answer)
      else withClue("a refused answer creates nothing: ")(index.contains(peer, child) shouldBe false)
    }
  }

  test("recordAnswerCreatingEntry always leaves the answer recorded") {
    forAll(genIndex, genKey, Gen.oneOf(true, false), genQueries, minSuccessful(300)) { (index, key, answer, asked) =>
      val (peer, child) = key
      val queriesBefore = index.index.get(peer).flatMap(_.get(child)).map(_.forQueries)
      index.recordAnswerCreatingEntry(peer, child, answer, asked)
      index.lookup(peer, child) shouldBe Some(answer)
      withClue("an entry the fold had already re-derived keeps its own attribution: ")(
        index.index(peer)(child).forQueries shouldBe queriesBefore.getOrElse(asked),
      )
    }
  }

  test("keeping everything, pruning nothing and deriving nothing are each the identity") {
    forAll(genIndex, minSuccessful(200)) { index =>
      val before = contentsOf(index)
      index.retainResults((_, _) => true) shouldBe false
      index.pruneCancelledQueries(_ => true) shouldBe false
      index.recordQueriesWhereUnrecorded(_ => Set.empty) shouldBe false
      contentsOf(index) shouldBe before
    }
  }

  test("retainResults rejecting everything empties the index, peers and all") {
    forAll(genIndex, minSuccessful(200)) { index =>
      val hadAnything = contentsOf(index).exists(_._2.nonEmpty)
      index.retainResults((_, _) => false) shouldBe hadAnything
      index.index shouldBe empty
    }
  }
}
