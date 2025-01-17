package com.thatdot.quine.graph.standing

import java.util.UUID

import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.EdgeEvent.{EdgeAdded, EdgeRemoved}
import com.thatdot.quine.graph.StandingQueryId
import com.thatdot.quine.graph.cypher.{Expr, MultipleValuesStandingQuery, QueryContext}
import com.thatdot.quine.graph.messaging.StandingQueryMessage.NewMultipleValuesStateResult
import com.thatdot.quine.model.{EdgeDirection, HalfEdge}

class EdgeSubscriptionReciprocalStateTests extends AnyFunSuite {

  val andThenAliasedAs: Symbol = Symbol("bar")
  val andThen: MultipleValuesStandingQuery.LocalProperty = MultipleValuesStandingQuery
    .LocalProperty(Symbol("foo"), MultipleValuesStandingQuery.LocalProperty.Any, Some(andThenAliasedAs))
  val query: MultipleValuesStandingQuery.EdgeSubscriptionReciprocal =
    MultipleValuesStandingQuery.EdgeSubscriptionReciprocal(
      halfEdge = HalfEdge(Symbol("an_edge"), EdgeDirection.Outgoing, QuineId(Array(7.toByte))),
      andThenId = andThen.queryPartId,
    )
  val globalId: StandingQueryId = StandingQueryId(new UUID(12L, 34L))

  test("edge subscription reciprocal") {

    val state =
      new StandingQueryStateWrapper(query, Seq(andThen))

    withClue("Initializing the state") {
      state.initialize() { (effects, initialResultOpt) =>
        assert(initialResultOpt.isEmpty)
        assert(effects.isEmpty)
      }
    }

    withClue("Set the half edge") {
      val halfEdge = EdgeAdded(query.halfEdge)
      state.reportNodeEvents(Seq(halfEdge), shouldHaveEffects = true) { effects =>
        val (onNode, sq) = effects.subscriptionsCreated.dequeue()
        assert(onNode === effects.executingNodeId)
        assert(sq.queryPartId === query.andThenId)
        assert(effects.isEmpty)
      }
    }

    withClue("Report one result back up") {
      val result = NewMultipleValuesStateResult(
        state.effects.executingNodeId,
        query.andThenId,
        globalId,
        Some(query.queryPartId),
        Seq(QueryContext(Map(andThenAliasedAs -> Expr.Integer(2L)))),
      )
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        val reportedResults = effects.resultsReported.dequeue()
        assert(reportedResults == result.resultGroup)
        assert(effects.isEmpty)
      }
    }

    withClue("Report a second result back up") {
      val result = NewMultipleValuesStateResult(
        state.effects.executingNodeId,
        query.andThenId,
        globalId,
        Some(query.queryPartId),
        Seq(QueryContext(Map(andThenAliasedAs -> Expr.Integer(4L)))),
      )
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        val reportedResults = effects.resultsReported.dequeue()
        assert(reportedResults === result.resultGroup)
        assert(effects.isEmpty)
      }
    }

    withClue("Cancel the half edge") {
      val halfEdge = EdgeRemoved(query.halfEdge)
      state.reportNodeEvents(Seq(halfEdge), shouldHaveEffects = true) { effects =>
        val (onNode, sqId) = effects.subscriptionsCancelled.dequeue()
        assert(onNode === effects.executingNodeId)
        assert(sqId === query.andThenId)
        val results = effects.resultsReported.dequeue()
        assert(results == Seq.empty)
        assert(effects.isEmpty)
      }
    }

    withClue("Report a third result back up along the now-cancelled subscription") {
      val result = NewMultipleValuesStateResult(
        state.effects.executingNodeId,
        query.andThenId,
        globalId,
        Some(query.queryPartId),
        Seq(QueryContext(Map(andThenAliasedAs -> Expr.Integer(5L)))),
      )
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        assert(!state.sqState.currentlyMatching)
        assert(state.sqState.cachedResult.nonEmpty)
        assert(state.sqState.cachedResult.get == result.resultGroup)
        assert(effects.isEmpty)
      }
    }
  }
}
