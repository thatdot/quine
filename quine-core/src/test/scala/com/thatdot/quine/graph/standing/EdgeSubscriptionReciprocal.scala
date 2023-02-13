package com.thatdot.quine.graph.standing

import java.util.UUID

import org.scalactic.source.Position
import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.graph.EdgeEvent.{EdgeAdded, EdgeRemoved}
import com.thatdot.quine.graph.StandingQueryId
import com.thatdot.quine.graph.cypher.{Expr, MultipleValuesStandingQuery, QueryContext}
import com.thatdot.quine.graph.messaging.StandingQueryMessage.{NewMultipleValuesResult, ResultId}
import com.thatdot.quine.model.{EdgeDirection, HalfEdge, QuineId}

class EdgeSubscriptionReciprocalStateTest extends AnyFunSuite {

  val andThenAliasedAs: Symbol = Symbol("bar")
  val andThen: MultipleValuesStandingQuery.LocalProperty = MultipleValuesStandingQuery
    .LocalProperty(Symbol("foo"), MultipleValuesStandingQuery.LocalProperty.Any, Some(andThenAliasedAs))
  val query: MultipleValuesStandingQuery.EdgeSubscriptionReciprocal =
    MultipleValuesStandingQuery.EdgeSubscriptionReciprocal(
      halfEdge = HalfEdge(Symbol("an_edge"), EdgeDirection.Outgoing, QuineId(Array(7.toByte))),
      andThenId = andThen.id
    )
  val globalId: StandingQueryId = StandingQueryId(new UUID(12L, 34L))

  test("edge subscription reciprocal") {

    val state =
      new StandingQueryStateWrapper(query, Seq(andThen)) {

        override def testInvariants()(implicit pos: Position): Unit =
          withClue("Checking invariants") {
            if (!sqState.currentlyMatching) {
              assert(sqState.reverseResultDependency.isEmpty)
              ()
            }
          }
      }

    withClue("Initializing the state") {
      state.initialize() { effects =>
        assert(effects.isEmpty)
      }
    }

    withClue("Set the half edge") {
      val halfEdge = EdgeAdded(query.halfEdge)
      state.reportNodeEvents(Seq(halfEdge), shouldHaveEffects = true) { effects =>
        val (onNode, sq) = effects.subscriptionsCreated.dequeue()
        assert(onNode === effects.node)
        assert(sq.id === query.andThenId)
        assert(effects.isEmpty)
      }
    }

    val resId1 = withClue("Report one result back up") {
      val result = NewMultipleValuesResult(
        state.effects.node,
        query.andThenId,
        globalId,
        Some(query.id),
        ResultId.fresh(),
        QueryContext(Map(andThenAliasedAs -> Expr.Integer(2L)))
      )
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        val (resId1, resultCtx) = effects.resultsReported.dequeue()
        assert(resultCtx === result.result)
        assert(effects.isEmpty)
        resId1
      }
    }

    val resId2 = withClue("Report a second result back up") {
      val result = NewMultipleValuesResult(
        state.effects.node,
        query.andThenId,
        globalId,
        Some(query.id),
        ResultId.fresh(),
        QueryContext(Map(andThenAliasedAs -> Expr.Integer(4L)))
      )
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        val (resId2, resultCtx) = effects.resultsReported.dequeue()
        assert(resultCtx === result.result)
        assert(effects.isEmpty)
        resId2
      }
    }

    withClue("Cancel the half edge") {
      val halfEdge = EdgeRemoved(query.halfEdge)
      state.reportNodeEvents(Seq(halfEdge), shouldHaveEffects = true) { effects =>
        val (onNode, sqId) = effects.subscriptionsCancelled.dequeue()
        assert(onNode === effects.node)
        assert(sqId === query.andThenId)
        val cancelled = Set(effects.resultsCancelled.dequeue(), effects.resultsCancelled.dequeue())
        assert(cancelled === Set(resId1, resId2))
        assert(effects.isEmpty)
      }
    }

    withClue("Report a third result back up") {
      val result = NewMultipleValuesResult(
        state.effects.node,
        query.andThenId,
        globalId,
        Some(query.id),
        ResultId.fresh(),
        QueryContext(Map(andThenAliasedAs -> Expr.Integer(5L)))
      )
      state.reportNewSubscriptionResult(result, shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }
  }
}
