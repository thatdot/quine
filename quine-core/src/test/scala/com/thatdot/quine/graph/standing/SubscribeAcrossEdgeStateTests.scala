package com.thatdot.quine.graph.standing

import java.util.UUID

import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.graph.EdgeEvent.{EdgeAdded, EdgeRemoved}
import com.thatdot.quine.graph.StandingQueryId
import com.thatdot.quine.graph.cypher.{Expr, MultipleValuesStandingQuery, QueryContext}
import com.thatdot.quine.graph.messaging.StandingQueryMessage.NewMultipleValuesStateResult
import com.thatdot.quine.model.{EdgeDirection, HalfEdge, QuineId}

class SubscribeAcrossEdgeStateTests extends AnyFunSuite with OptionValues {

  def makeState(
    query: MultipleValuesStandingQuery.SubscribeAcrossEdge,
  ): StandingQueryStateWrapper[MultipleValuesStandingQuery.SubscribeAcrossEdge] =
    new StandingQueryStateWrapper(query)

  val globalId: StandingQueryId = StandingQueryId(new UUID(12L, 34L))

  test("subscribe across edge with label and direction") {

    val andThenAliasedAs = Symbol("bar")
    val query = MultipleValuesStandingQuery.SubscribeAcrossEdge(
      edgeName = Some(Symbol("myedge")),
      edgeDirection = Some(EdgeDirection.Incoming),
      andThen = MultipleValuesStandingQuery
        .LocalProperty(Symbol("foo"), MultipleValuesStandingQuery.LocalProperty.Any, Some(andThenAliasedAs)),
    )
    val state = makeState(query)

    withClue("Initializing the state prepares a 0-result group") {
      state.initialize() { (effects, initialResultsOpt) =>
        val initialResults = initialResultsOpt.value
        assert(initialResults == Seq.empty)
        assert(effects.isEmpty)
      }
    }

    val qid7 = QuineId(Array(7.toByte))
    val reciprocal7Id = withClue("Set a matching half edge") {
      val halfEdge = HalfEdge(query.edgeName.get, query.edgeDirection.get, qid7)
      val otherHalfEdge = halfEdge.reflect(state.effects.executingNodeId)
      val reciprocal7 = MultipleValuesStandingQuery.EdgeSubscriptionReciprocal(otherHalfEdge, query.andThen.queryPartId)
      val edgeAdded = EdgeAdded(halfEdge)
      state.reportNodeEvents(Seq(edgeAdded), shouldHaveEffects = true) { effects =>
        val (onNode, sq) = effects.subscriptionsCreated.dequeue()
        assert(onNode == qid7)
        assert(sq == reciprocal7)
        assert(effects.isEmpty)
      }
      reciprocal7.queryPartId
    }

    val qid8 = QuineId(Array(8.toByte))
    withClue("Set a non-matching half edge") {
      val halfEdge = HalfEdge(Symbol("otheredge"), query.edgeDirection.get, qid8)
      val edgeAdded = EdgeAdded(halfEdge)
      state.reportNodeEvents(Seq(edgeAdded), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }

    withClue("Report a result for the edge") {
      val result = NewMultipleValuesStateResult(
        qid7,
        reciprocal7Id,
        globalId,
        Some(query.queryPartId),
        Seq(QueryContext(Map(andThenAliasedAs -> Expr.Integer(2L)))),
      )
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        val results = effects.resultsReported.dequeue()
        assert(results == result.resultGroup)
        assert(effects.isEmpty)
      }
    }

    withClue("Report a second result for the edge") {
      val result = NewMultipleValuesStateResult(
        qid7,
        reciprocal7Id,
        globalId,
        Some(query.queryPartId),
        Seq(QueryContext(Map(andThenAliasedAs -> Expr.Integer(3L)))),
      )
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        val reportedResults = effects.resultsReported.dequeue()
        assert(reportedResults == result.resultGroup)
        assert(effects.isEmpty)
      }
    }

    val reciprocal8Id = withClue("Set a second matching edge (with no results)") {
      val halfEdge = HalfEdge(query.edgeName.get, query.edgeDirection.get, qid8)
      val otherHalfEdge = halfEdge.reflect(state.effects.executingNodeId)
      val reciprocal8 = MultipleValuesStandingQuery.EdgeSubscriptionReciprocal(otherHalfEdge, query.andThen.queryPartId)
      val edgeAdded = EdgeAdded(halfEdge)
      state.reportNodeEvents(Seq(edgeAdded), shouldHaveEffects = true) { effects =>
        val (onNode, sq) = effects.subscriptionsCreated.dequeue()
        assert(onNode == qid8)
        assert(sq == reciprocal8)
        assert(effects.isEmpty)
      }
      reciprocal8.queryPartId
    }

    withClue("Remove the first matching edge") {
      val halfEdge = HalfEdge(query.edgeName.get, query.edgeDirection.get, qid7)
      val edgeRemoved = EdgeRemoved(halfEdge)
      state.reportNodeEvents(Seq(edgeRemoved), shouldHaveEffects = true) { effects =>
        val (onNode, sqId) = effects.subscriptionsCancelled.dequeue()
        assert(onNode == qid7)

        // We probably should be cancelling the reciprocal (reciprocal7Id) rather than the thing
        // that the reciprocal subscribes to (andThen.queryPartId), but the net effect should be the same.
        assert(sqId == query.andThen.queryPartId)
        // No cancellation sent yet, because at least 1 edge (the one to qid8) is pending
        assert(effects.resultsReported.isEmpty)
        assert(effects.isEmpty)
      }
    }

    withClue("Report a result across the second matching edge") {
      val result = NewMultipleValuesStateResult(
        qid8,
        reciprocal8Id,
        globalId,
        Some(query.queryPartId),
        Seq(QueryContext(Map(andThenAliasedAs -> Expr.Integer(4L)))),
      )
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        val reportedResults = effects.resultsReported.dequeue()
        assert(reportedResults == result.resultGroup) // NB does NOT include the results from the first edge
        assert(effects.isEmpty)
      }
    }

    withClue("Remove the second matching edge") {
      val halfEdge = HalfEdge(query.edgeName.get, query.edgeDirection.get, qid8)
      val edgeRemoved = EdgeRemoved(halfEdge)
      state.reportNodeEvents(Seq(edgeRemoved), shouldHaveEffects = true) { effects =>
        val (onNode, sqId) = effects.subscriptionsCancelled.dequeue()
        val results =
          effects.resultsReported
            .dequeue()
        assert(onNode == qid8)

        // We probably should be cancelling the reciprocal (reciprocal7Id) rather than the thing
        // that the reciprocal subscribes to (andThen.queryPartId), but the net effect should be the same.
        assert(sqId == query.andThen.queryPartId)
        assert(results.isEmpty) // All results should be affirmatively cancelled -- there are no matching edges!
        assert(effects.isEmpty)
      }
    }
  }
}
