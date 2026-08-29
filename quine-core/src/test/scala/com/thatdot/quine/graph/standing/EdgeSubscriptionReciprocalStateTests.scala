package com.thatdot.quine.graph.standing

import java.util.UUID

import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.EdgeEvent.{EdgeAdded, EdgeRemoved}
import com.thatdot.quine.graph.StandingQueryId
import com.thatdot.quine.graph.cypher.{EdgeSubscriptionReciprocalState, Expr, MultipleValuesStandingQuery, QueryContext}
import com.thatdot.quine.graph.messaging.StandingQueryMessage.{
  MultipleValuesStandingQuerySubscriber,
  NewMultipleValuesStateResult,
}
import com.thatdot.quine.model.{EdgeDirection, HalfEdge}

class EdgeSubscriptionReciprocalStateTests extends AnyFunSuite with OptionValues {

  val andThenAliasedAs: Symbol = Symbol("bar")
  val andThen: MultipleValuesStandingQuery.LocalProperty = MultipleValuesStandingQuery
    .LocalProperty(Symbol("foo"), MultipleValuesStandingQuery.LocalProperty.Any, Some(andThenAliasedAs))
  val edgeType: Symbol = Symbol("an_edge")

  /** The reciprocal as some subscribing node would synthesize it. Which node that was does not matter: the query's
    * identity is made of its constraints, so a query built from any node's half edge names the same state.
    */
  val query: MultipleValuesStandingQuery.EdgeSubscriptionReciprocal =
    MultipleValuesStandingQuery.EdgeSubscriptionReciprocal(
      halfEdge = HalfEdge(edgeType, EdgeDirection.Outgoing, QuineId(Array(7.toByte))),
      andThenId = andThen.queryPartId,
    )
  val globalId: StandingQueryId = StandingQueryId(new UUID(12L, 34L))

  private def subscriberOn(node: QuineId): MultipleValuesStandingQuerySubscriber.NodeSubscriber =
    MultipleValuesStandingQuerySubscriber.NodeSubscriber(node, globalId, andThen.queryPartId)

  private def edgeTo(node: QuineId): HalfEdge = HalfEdge(edgeType, EdgeDirection.Outgoing, node)

  private def resultFromAndThen(
    state: StandingQueryStateWrapper[MultipleValuesStandingQuery.EdgeSubscriptionReciprocal],
    value: Long,
  ): NewMultipleValuesStateResult = NewMultipleValuesStateResult(
    state.effects.executingNodeId,
    query.andThenId,
    globalId,
    Some(query.queryPartId),
    Seq(QueryContext(Map(andThenAliasedAs -> Expr.Integer(value)))),
  )

  private def newState(): StandingQueryStateWrapper[MultipleValuesStandingQuery.EdgeSubscriptionReciprocal] = {
    val state = new StandingQueryStateWrapper(query, Seq(andThen))
    state.initialize() { (effects, initialResultOpt) =>
      withClue("The reciprocal subscribes to its andThen once, for as long as it exists") {
        val (onNode, subquery) = effects.subscriptionsCreated.dequeue()
        assert(onNode === effects.executingNodeId)
        assert(subquery.queryPartId === query.andThenId)
        assert(effects.isEmpty)
      }
      withClue("There is no result that is not specific to a subscriber") {
        assert(initialResultOpt.isEmpty)
      }
    }
    state
  }

  test("two nodes' subscriptions to the same constraints name the same state") {
    val otherNodesQuery = MultipleValuesStandingQuery.EdgeSubscriptionReciprocal(
      halfEdge = HalfEdge(edgeType, EdgeDirection.Outgoing, QuineId(Array(8.toByte))),
      andThenId = andThen.queryPartId,
    )
    assert(otherNodesQuery.queryPartId === query.queryPartId)

    withClue("while different constraints name different states") {
      val differentDirection = MultipleValuesStandingQuery.EdgeSubscriptionReciprocal(
        halfEdge = HalfEdge(edgeType, EdgeDirection.Incoming, QuineId(Array(7.toByte))),
        andThenId = andThen.queryPartId,
      )
      assert(differentDirection.queryPartId !== query.queryPartId)
      val differentType = MultipleValuesStandingQuery.EdgeSubscriptionReciprocal(
        halfEdge = HalfEdge(Symbol("other_edge"), EdgeDirection.Outgoing, QuineId(Array(7.toByte))),
        andThenId = andThen.queryPartId,
      )
      assert(differentType.queryPartId !== query.queryPartId)
    }
  }

  test("results are relayed to the subscribers that have an edge, and to no others") {
    val state = newState()
    val withEdge = QuineId(Array(7.toByte))
    val withoutEdge = QuineId(Array(8.toByte))

    state.reportNodeEvents(Seq(EdgeAdded(edgeTo(withEdge))), shouldHaveEffects = false) { effects =>
      assert(effects.isEmpty) // no result to relay yet, and nothing about the edge is recorded
    }
    assert(state.addSubscriber(subscriberOn(withEdge)).isEmpty) // no result to give it yet
    assert(state.addSubscriber(subscriberOn(withoutEdge)).isEmpty)

    val result = resultFromAndThen(state, 2L)
    state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
      assert(effects.resultsReportedToNode.dequeue() === (withEdge -> result.resultGroup))
      assert(effects.isEmpty) // the subscriber with no matching edge is not answered
    }

    withClue("A subscriber whose edge appears later is caught up") {
      state.reportNodeEvents(Seq(EdgeAdded(edgeTo(withoutEdge))), shouldHaveEffects = false) { effects =>
        assert(effects.resultsReportedToNode.dequeue() === (withoutEdge -> result.resultGroup))
        assert(effects.isEmpty)
      }
    }

    withClue("A subscriber arriving after the result is answered from the cache") {
      val late = QuineId(Array(9.toByte))
      assert(state.addSubscriber(subscriberOn(late)).isEmpty) // no edge to it
      state.reportNodeEvents(Seq(EdgeAdded(edgeTo(late))), shouldHaveEffects = false) { effects =>
        effects.resultsReportedToNode.dequeue()
        assert(effects.isEmpty)
      }
      assert(state.addSubscriber(subscriberOn(late)).value === result.resultGroup)
    }
  }

  test("past a bound, a result goes to the remaining subscribers without asking the edges about each one") {
    val state = newState()
    // One more subscriber than the bound, every one of them across a matching edge, so that what stops the asking
    // is the bound rather than the subscribers running out, and so that assuming the rest are entitled is right.
    val subscribers = (1 to EdgeSubscriptionReciprocalState.entitlementChecksPerReport + 1)
      .map(index => QuineId(Array(index.toByte)))
    state.reportNodeEvents(subscribers.map(node => EdgeAdded(edgeTo(node))), shouldHaveEffects = false) { effects =>
      assert(effects.isEmpty) // no result to relay yet
    }
    subscribers.foreach(node => state.addSubscriber(subscriberOn(node)))

    val result = resultFromAndThen(state, 2L)
    state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
      withClue("every subscriber hears the result") {
        assert(effects.resultsReportedToNode.dequeueAll(_ => true).map(_._1).toSet === subscribers.toSet)
      }
      withClue("but the node's edges were consulted only up to the bound") {
        assert(
          effects.entitlementQuestions.dequeueAll(_ => true).size ===
            EdgeSubscriptionReciprocalState.entitlementChecksPerReport,
        )
      }
      assert(effects.isEmpty)
    }
  }

  test("an updated result replaces the previous one for every subscriber across an edge") {
    val state = newState()
    val first = QuineId(Array(7.toByte))
    val second = QuineId(Array(8.toByte))
    state.reportNodeEvents(Seq(EdgeAdded(edgeTo(first)), EdgeAdded(edgeTo(second))), shouldHaveEffects = false) {
      effects =>
        assert(effects.isEmpty)
    }
    state.addSubscriber(subscriberOn(first))
    state.addSubscriber(subscriberOn(second))

    val result = resultFromAndThen(state, 2L)
    state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
      assert(effects.resultsReportedToNode.dequeueAll(_ => true).map(_._1).toSet === Set(first, second))
      assert(effects.isEmpty)
    }

    val updated = resultFromAndThen(state, 4L)
    state.reportNewSubscriptionResult(updated, shouldHaveEffects = true) { effects =>
      assert(
        effects.resultsReportedToNode.dequeueAll(_ => true).toSet === Set(
          first -> updated.resultGroup,
          second -> updated.resultGroup,
        ),
      )
      assert(effects.isEmpty)
    }

    withClue("Reporting the same result again changes nothing") {
      state.reportNewSubscriptionResult(updated, shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }
  }

  test("results are retracted when the edge to a subscriber goes away") {
    val state = newState()
    val subscribingNode = QuineId(Array(7.toByte))

    state.reportNodeEvents(Seq(EdgeAdded(edgeTo(subscribingNode))), shouldHaveEffects = false)(_ => ())
    state.addSubscriber(subscriberOn(subscribingNode))
    val result = resultFromAndThen(state, 2L)
    state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
      effects.resultsReportedToNode.dequeue()
      assert(effects.isEmpty)
    }

    withClue("An edge that does not match the constraints is ignored") {
      val differentType = HalfEdge(Symbol("other_edge"), EdgeDirection.Outgoing, subscribingNode)
      val differentDirection = HalfEdge(edgeType, EdgeDirection.Undirected, subscribingNode)
      state.reportNodeEvents(
        Seq(EdgeAdded(differentType), EdgeRemoved(differentType), EdgeAdded(differentDirection)),
        shouldHaveEffects = false,
      ) { effects =>
        assert(effects.isEmpty)
      }
    }

    withClue("Removing the edge retracts the results across it") {
      state.reportNodeEvents(Seq(EdgeRemoved(edgeTo(subscribingNode))), shouldHaveEffects = false) { effects =>
        assert(effects.resultsReportedToNode.dequeue() === (subscribingNode -> Seq.empty))
        assert(effects.isEmpty)
      }
      assert(state.addSubscriber(subscriberOn(subscribingNode)).isEmpty)
    }

    withClue("A result arriving with no edge left is cached but relayed to nobody") {
      val later = resultFromAndThen(state, 5L)
      state.reportNewSubscriptionResult(later, shouldHaveEffects = true) { effects =>
        assert(effects.isEmpty)
      }
      assert(state.sqState.cachedResult.value === later.resultGroup)
    }
  }

  test("the node's existing edges are watched, but not replayed to a state they cannot inform") {
    val state = newState()
    val labelsProperty = Symbol("__LABEL")

    // What it watches and what it can learn from are different questions, and only the second one costs a read of
    // every edge the node has.
    assert(state.sqState.relevantEventTypes(labelsProperty).nonEmpty)
    assert(state.sqState.initialEventTypes(labelsProperty).isEmpty)

    withClue("because an edge the node already has tells a state with no result to relay nothing") {
      state.reportNodeEvents(Seq(EdgeAdded(edgeTo(QuineId(Array(9.toByte))))), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }
  }
}
