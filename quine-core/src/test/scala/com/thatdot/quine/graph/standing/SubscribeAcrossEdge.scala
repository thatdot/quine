package com.thatdot.quine.graph.standing

import java.util.UUID

import org.scalactic.source.Position
import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.graph.EdgeEvent.{EdgeAdded, EdgeRemoved}
import com.thatdot.quine.graph.cypher.{Expr, MultipleValuesStandingQuery, QueryContext}
import com.thatdot.quine.graph.messaging.StandingQueryMessage.{
  CancelMultipleValuesResult,
  NewMultipleValuesResult,
  ResultId
}
import com.thatdot.quine.graph.{MultipleValuesStandingQueryPartId, StandingQueryId}
import com.thatdot.quine.model.{EdgeDirection, HalfEdge, QuineId}

class SubscribeAcrossEdgeStateTest extends AnyFunSuite {

  def makeState(
    query: MultipleValuesStandingQuery.SubscribeAcrossEdge
  ): StandingQueryStateWrapper[MultipleValuesStandingQuery.SubscribeAcrossEdge] =
    new StandingQueryStateWrapper(query) {

      override def testInvariants()(implicit pos: Position): Unit =
        withClue("Checking invariants") {
          val edgesWatchedSet: Set[(MultipleValuesStandingQueryPartId, HalfEdge)] = sqState.edgesWatched.toSeq.map {
            case (he, (sqId, _)) => sqId -> he
          }.toSet
          val edgeQueryIdsSet: Set[(MultipleValuesStandingQueryPartId, HalfEdge)] = sqState.edgeQueryIds.toSeq.map {
            case ((_, sqId), he) => sqId -> he
          }.toSet

          assert(edgesWatchedSet == edgeQueryIdsSet)
          assert(sqState.edgeQueryIds.forall { case ((other, _), he) => other == he.other })

          ()
        }
    }
  val globalId: StandingQueryId = StandingQueryId(new UUID(12L, 34L))

  test("subscribe across edge with label and direction") {

    val andThenAliasedAs = Symbol("bar")
    val query = MultipleValuesStandingQuery.SubscribeAcrossEdge(
      edgeName = Some(Symbol("myedge")),
      edgeDirection = Some(EdgeDirection.Incoming),
      andThen = MultipleValuesStandingQuery
        .LocalProperty(Symbol("foo"), MultipleValuesStandingQuery.LocalProperty.Any, Some(andThenAliasedAs))
    )
    val state = makeState(query)

    withClue("Initializing the state") {
      state.initialize() { effects =>
        assert(effects.isEmpty)
      }
    }

    val qid7 = QuineId(Array(7.toByte))
    val reciprocal7Id = withClue("Set a matching half edge") {
      val halfEdge = HalfEdge(query.edgeName.get, query.edgeDirection.get, qid7)
      val otherHalfEdge = halfEdge.reflect(state.effects.node)
      val reciprocal7 = MultipleValuesStandingQuery.EdgeSubscriptionReciprocal(otherHalfEdge, query.andThen.id)
      val edgeAdded = EdgeAdded(halfEdge)
      state.reportNodeEvents(Seq(edgeAdded), shouldHaveEffects = true) { effects =>
        val (onNode, sq) = effects.subscriptionsCreated.dequeue()
        assert(onNode == qid7)
        assert(sq == reciprocal7)
        assert(effects.isEmpty)
        reciprocal7.id
      }
    }

    val qid8 = QuineId(Array(8.toByte))
    withClue("Set a non-matching half edge") {
      val halfEdge = HalfEdge(Symbol("otheredge"), query.edgeDirection.get, qid8)
      val edgeAdded = EdgeAdded(halfEdge)
      state.reportNodeEvents(Seq(edgeAdded), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }

    val resId1 = withClue("Report a result for the edge") {
      val result = NewMultipleValuesResult(
        qid7,
        reciprocal7Id,
        globalId,
        Some(query.id),
        ResultId.fresh(),
        QueryContext(Map(andThenAliasedAs -> Expr.Integer(2L)))
      )
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        val (resId1, resultCtx) = effects.resultsReported.dequeue()
        assert(resultCtx == result.result)
        assert(effects.isEmpty)
        resId1
      }
    }

    val (upstreamResId2, resId2) = withClue("Report a second result for the edge") {
      val result = NewMultipleValuesResult(
        qid7,
        reciprocal7Id,
        globalId,
        Some(query.id),
        ResultId.fresh(),
        QueryContext(Map(andThenAliasedAs -> Expr.Integer(3L)))
      )
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        val (resId2, resultCtx) = effects.resultsReported.dequeue()
        assert(resultCtx == result.result)
        assert(effects.isEmpty)
        (result.resultId, resId2)
      }
    }

    withClue("Set a second matching edge") {
      val halfEdge = HalfEdge(query.edgeName.get, query.edgeDirection.get, qid8)
      val otherHalfEdge = halfEdge.reflect(state.effects.node)
      val reciprocal8 = MultipleValuesStandingQuery.EdgeSubscriptionReciprocal(otherHalfEdge, query.andThen.id)
      val edgeAdded = EdgeAdded(halfEdge)
      state.reportNodeEvents(Seq(edgeAdded), shouldHaveEffects = true) { effects =>
        val (onNode, sq) = effects.subscriptionsCreated.dequeue()
        assert(onNode == qid8)
        assert(sq == reciprocal8)
        assert(effects.isEmpty)
      }
    }

    withClue("Cancel a result") {
      val cancelled = CancelMultipleValuesResult(
        qid7,
        reciprocal7Id,
        globalId,
        Some(query.id),
        upstreamResId2
      )
      state.reportCancelledSubscriptionResult(cancelled, shouldHaveEffects = true) { effects =>
        val resId2Cancelled = effects.resultsCancelled.dequeue()
        assert(resId2Cancelled == resId2)
        assert(effects.isEmpty)
      }
    }

    withClue("Remove the first matching edge") {
      val halfEdge = HalfEdge(query.edgeName.get, query.edgeDirection.get, qid7)
      val edgeAdded = EdgeRemoved(halfEdge)
      state.reportNodeEvents(Seq(edgeAdded), shouldHaveEffects = true) { effects =>
        val (onNode, sqId) = effects.subscriptionsCancelled.dequeue()
        assert(onNode == qid7)
        assert(sqId == reciprocal7Id)
        val resId1Cancelled = effects.resultsCancelled.dequeue()
        assert(resId1Cancelled == resId1)
        assert(effects.isEmpty)
      }
    }
  }
}
