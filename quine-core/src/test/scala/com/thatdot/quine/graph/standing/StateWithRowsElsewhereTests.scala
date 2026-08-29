package com.thatdot.quine.graph.standing

import java.util.UUID

import scala.collection.mutable

import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.behavior.MultipleValuesStandingQueryPartSubscription
import com.thatdot.quine.graph.cypher.{
  ContributionOutcome,
  EdgeContributionStore,
  EdgeSubscriptionReciprocalState,
  QueryContext,
  SubscribeAcrossEdgeState,
}
import com.thatdot.quine.graph.{MultipleValuesStandingQueryPartId, StandingQueryId}
import com.thatdot.quine.model.{EdgeDirection, HalfEdge}
import com.thatdot.quine.persistor.codecs.MultipleValuesStandingQueryStateCodec
import com.thatdot.quine.util.TestLogging._

/** A state whose blob says its rows are recorded elsewhere, revived on a node that installed no store for them.
  *
  * The rows are still there and nothing on this node will read them. What the state must not do is pretend it knows
  * the answer, and what the blob must not do is forget that the rows exist, because the blob's flag is the only
  * thing that will ever lead anyone to them.
  */
class StateWithRowsElsewhereTests extends AnyFunSuite with OptionValues {

  private val globalId = StandingQueryId(new UUID(1L, 2L))
  private val partId = MultipleValuesStandingQueryPartId(new UUID(3L, 4L))
  private val andThenId = MultipleValuesStandingQueryPartId(new UUID(5L, 6L))
  private val forQuery = MultipleValuesStandingQueryPartId(new UUID(7L, 8L))

  private def subscriptionFor(id: MultipleValuesStandingQueryPartId): MultipleValuesStandingQueryPartSubscription =
    MultipleValuesStandingQueryPartSubscription(id, globalId, mutable.Set.empty)

  private def roundTrip(
    pair: (MultipleValuesStandingQueryPartSubscription, com.thatdot.quine.graph.cypher.MultipleValuesStandingQueryState),
  ): com.thatdot.quine.graph.cypher.MultipleValuesStandingQueryState =
    MultipleValuesStandingQueryStateCodec.format.read(MultipleValuesStandingQueryStateCodec.format.write(pair)).get._2

  test("a subscribe-across-edge that cannot reach its rows says so again every time it is written") {
    val state = SubscribeAcrossEdgeState(partId)
    state.edgeResultsExternalized = true

    // Written from a node holding a heap store, because that is the whole situation: the rows are elsewhere and
    // this node has no way to reach them. Deriving the flag from the store present would clear it here, and with it
    // the only sign that anything exists under this key.
    val revived = roundTrip(subscriptionFor(partId) -> state)
    assert(revived.asInstanceOf[SubscribeAcrossEdgeState].edgeResultsExternalized)

    withClue("and again after that, however many times it is written") {
      assert(
        roundTrip(subscriptionFor(partId) -> revived).asInstanceOf[SubscribeAcrossEdgeState].edgeResultsExternalized,
      )
    }
  }

  test("a reciprocal that cannot reach its subscribers says so again every time it is written") {
    val state = EdgeSubscriptionReciprocalState(
      partId,
      HalfEdge(Symbol("myedge"), EdgeDirection.Incoming, QuineId(Array(1.toByte))),
      andThenId,
    )
    state.subscribersExternalized = true
    state.externalSubscriberForQuery = Some(forQuery)

    val revived = roundTrip(subscriptionFor(partId) -> state).asInstanceOf[EdgeSubscriptionReciprocalState]
    assert(revived.subscribersExternalized)
    assert(revived.externalSubscriberForQuery.value == forQuery)

    withClue("and again after that") {
      val again = roundTrip(subscriptionFor(partId) -> revived).asInstanceOf[EdgeSubscriptionReciprocalState]
      assert(again.subscribersExternalized)
    }
  }

  test("a subscribe-across-edge that cannot reach its rows withholds rather than reporting no match") {
    val state = SubscribeAcrossEdgeState(partId)
    assert(
      state.readResults(Map.empty, Symbol("__LABEL")).value.isEmpty,
      "an ordinary state with no matching edges should affirm that there is no match",
    )

    state.edgeResultsExternalized = true
    assert(
      state.readResults(Map.empty, Symbol("__LABEL")).isEmpty,
      "a state that cannot reach its rows reported an affirmative lack of matches, which withdraws rows that stand",
    )
  }

  test("a subscribe-across-edge speaks again once a store that reaches its rows is installed") {
    // The one way out of withholding: the node hands the state a store that can read what was recorded. Nothing
    // the far side says gets there first: a report lands in the heap store the state was built with, and is read
    // back through the same withheld answer.
    val state = SubscribeAcrossEdgeState(partId)
    state.edgeResultsExternalized = true
    val edge = HalfEdge(Symbol("myedge"), EdgeDirection.Incoming, QuineId(Array(1.toByte)))
    state.contributionStore.track(edge)
    state.contributionStore.contribute(edge, Seq.empty)(_ => ())
    assert(
      state.readResults(Map.empty, Symbol("__LABEL")).isEmpty,
      "an answer arriving from across an edge released the withhold, which the rows it cannot read may contradict",
    )

    state.contributionStore = new RowsElsewhereStore
    assert(
      state.readResults(Map.empty, Symbol("__LABEL")).value.isEmpty,
      "a state handed a store that reaches its rows went on withholding, so adoption would never end it",
    )
  }

  /** A store standing in for one whose rows are recorded outside the state: it reaches them, and has none. */
  final private class RowsElsewhereStore extends EdgeContributionStore {
    override def keepsRowsElsewhere: Boolean = true
    def track(halfEdge: HalfEdge): Unit = ()
    def hasTrackedEdges: Boolean = false
    def answeredEdges: Int = 0
    def total: collection.Map[QueryContext, Int] = Map.empty
    def contribute(halfEdge: HalfEdge, level: Seq[QueryContext])(andThen: ContributionOutcome => Unit): Unit =
      andThen(ContributionOutcome(edgeChanged = false, totalChanged = false))
    def retract(halfEdge: HalfEdge)(andThen: ContributionOutcome => Unit): Unit =
      andThen(ContributionOutcome(edgeChanged = false, totalChanged = false))
    def entries: Iterator[(HalfEdge, Option[Seq[QueryContext]])] = Iterator.empty
    def restore(halfEdge: HalfEdge, level: Option[Seq[QueryContext]]): Unit = ()
  }
}
