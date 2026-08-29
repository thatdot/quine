package com.thatdot.quine.graph.standing

import java.util.UUID

import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.EdgeEvent.{EdgeAdded, EdgeRemoved}
import com.thatdot.quine.graph.cypher.{Expr, MultipleValuesStandingQuery, QueryContext}
import com.thatdot.quine.graph.messaging.StandingQueryMessage.NewMultipleValuesStateResult
import com.thatdot.quine.graph.{MultipleValuesStandingQueryPartId, StandingQueryId}
import com.thatdot.quine.model.{EdgeDirection, HalfEdge}

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

        // The subscription cancelled is the reciprocal this state subscribed to on the far node
        assert(sqId == reciprocal7Id)
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

        // The subscription cancelled is the reciprocal this state subscribed to on the far node
        assert(sqId == reciprocal8Id)
        assert(results.isEmpty) // All results should be affirmatively cancelled: there are no matching edges!
        assert(effects.isEmpty)
      }
    }
  }

  private def edgeQuery(andThenAliasedAs: Symbol): MultipleValuesStandingQuery.SubscribeAcrossEdge =
    MultipleValuesStandingQuery.SubscribeAcrossEdge(
      edgeName = Some(Symbol("myedge")),
      edgeDirection = Some(EdgeDirection.Incoming),
      andThen = MultipleValuesStandingQuery
        .LocalProperty(Symbol("foo"), MultipleValuesStandingQuery.LocalProperty.Any, Some(andThenAliasedAs)),
    )

  test("a restated edge addition is a no-op") {
    val andThenAliasedAs = Symbol("bar")
    val query = edgeQuery(andThenAliasedAs)
    val state = makeState(query)
    state.initialize()((_, _) => ())

    val qid7 = QuineId(Array(7.toByte))
    val halfEdge = HalfEdge(query.edgeName.get, query.edgeDirection.get, qid7)
    val reciprocalId = MultipleValuesStandingQuery
      .EdgeSubscriptionReciprocal(halfEdge.reflect(state.effects.executingNodeId), query.andThen.queryPartId)
      .queryPartId

    withClue("The same edge added twice subscribes to the same query, which the far node recognizes as one") {
      state.reportNodeEvents(Seq(EdgeAdded(halfEdge), EdgeAdded(halfEdge)), shouldHaveEffects = true) { effects =>
        val subscriptions = effects.subscriptionsCreated.dequeueAll(_ => true)
        assert(subscriptions.map(_._1).distinct == Seq(qid7))
        assert(subscriptions.map(_._2).distinct.size == 1)
        assert(effects.isEmpty)
      }
    }

    val resultGroup = Seq(QueryContext(Map(andThenAliasedAs -> Expr.Integer(2L))))
    withClue("Report a result across the edge") {
      val result = NewMultipleValuesStateResult(qid7, reciprocalId, globalId, Some(query.queryPartId), resultGroup)
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        assert(effects.resultsReported.dequeue() == resultGroup)
        assert(effects.isEmpty)
      }
    }

    withClue("Restating the edge addition does not discard the answer already received") {
      state.reportNodeEvents(Seq(EdgeAdded(halfEdge)), shouldHaveEffects = true) { effects =>
        // Re-subscribing is a duplicate the far node drops; what would be a bug is losing the answer below
        effects.subscriptionsCreated.dequeueAll(_ => true)
        assert(effects.isEmpty)
      }
      assert(state.readResults().value == resultGroup)
    }
  }

  test("results are reported as edges answer, not withheld until every edge has answered") {
    val andThenAliasedAs = Symbol("bar")
    val query = edgeQuery(andThenAliasedAs)
    val state = makeState(query)
    state.initialize()((_, _) => ())

    def addEdge(qid: QuineId): MultipleValuesStandingQueryPartId = {
      val halfEdge = HalfEdge(query.edgeName.get, query.edgeDirection.get, qid)
      val reciprocal = MultipleValuesStandingQuery
        .EdgeSubscriptionReciprocal(halfEdge.reflect(state.effects.executingNodeId), query.andThen.queryPartId)
      state.reportNodeEvents(Seq(EdgeAdded(halfEdge)), shouldHaveEffects = true) { effects =>
        effects.subscriptionsCreated.dequeue()
        assert(effects.isEmpty)
      }
      reciprocal.queryPartId
    }

    val qid7 = QuineId(Array(7.toByte))
    val qid8 = QuineId(Array(8.toByte))
    val reciprocal7Id = addEdge(qid7)
    addEdge(qid8)

    withClue("With no edge answered, the state has no results to report") {
      assert(state.readResults().isEmpty)
    }

    val row7 = QueryContext(Map(andThenAliasedAs -> Expr.Integer(7L)))
    withClue("The first edge's answer is reported without waiting for the second edge") {
      val result = NewMultipleValuesStateResult(qid7, reciprocal7Id, globalId, Some(query.queryPartId), Seq(row7))
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        assert(effects.resultsReported.dequeue() == Seq(row7))
        assert(effects.isEmpty)
      }
    }

    val row8 = QueryContext(Map(andThenAliasedAs -> Expr.Integer(8L)))
    withClue("The second edge's answer only adds rows, and the row reported earlier is not retracted") {
      val result = NewMultipleValuesStateResult(qid8, reciprocal7Id, globalId, Some(query.queryPartId), Seq(row8))
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        // Concatenation order across edges is unspecified, so compare as a group
        assert(effects.resultsReported.dequeue().toSet == Set(row7, row8))
        assert(effects.isEmpty)
      }
    }
  }
  test("a result from one reciprocal is credited only to the edges that reciprocal answers for") {
    // A query with the edge direction left open, so that two edges to one node are answered by two different
    // reciprocals, one per direction, because a reciprocal is named by the constraints it answers. The compiler
    // never emits this, but a hand-built query can, and the arithmetic below is the same arithmetic either way.
    val andThenAliasedAs = Symbol("bar")
    val query = MultipleValuesStandingQuery.SubscribeAcrossEdge(
      edgeName = Some(Symbol("myedge")),
      edgeDirection = None,
      andThen = MultipleValuesStandingQuery
        .LocalProperty(Symbol("foo"), MultipleValuesStandingQuery.LocalProperty.Any, Some(andThenAliasedAs)),
    )
    val state = makeState(query)
    state.initialize()((_, _) => ())

    val other = QuineId(Array(7.toByte))
    val incoming = HalfEdge(Symbol("myedge"), EdgeDirection.Incoming, other)
    val outgoing = HalfEdge(Symbol("myedge"), EdgeDirection.Outgoing, other)

    def reciprocalFor(halfEdge: HalfEdge): MultipleValuesStandingQueryPartId =
      MultipleValuesStandingQuery
        .EdgeSubscriptionReciprocal(halfEdge.reflect(state.effects.executingNodeId), query.andThen.queryPartId)
        .queryPartId

    state.reportNodeEvents(Seq(EdgeAdded(incoming), EdgeAdded(outgoing)), shouldHaveEffects = true) { effects =>
      val parts = effects.subscriptionsCreated.dequeueAll(_ => true).map(_._2.queryPartId).toSet
      assert(parts.size == 2, "two edges answered by different reciprocals produced one subscription")
      assert(effects.isEmpty)
    }

    val row = QueryContext(Map(andThenAliasedAs -> Expr.Integer(1L)))
    def deliver(fromPart: MultipleValuesStandingQueryPartId, group: Seq[QueryContext]): Unit = {
      val _ = state.reportNewSubscriptionResult(
        NewMultipleValuesStateResult(other, fromPart, globalId, Some(query.queryPartId), group),
        shouldHaveEffects = true,
      )(_.resultsReported.dequeueAll(_ => true))
    }

    deliver(reciprocalFor(incoming), Seq(row))
    deliver(reciprocalFor(outgoing), Seq(row))
    withClue("each reciprocal's answer is a row of its own, because each edge matches separately") {
      assert(state.readResults().value == Seq(row, row))
    }

    // One of them stops matching. What it takes back is its own row, not its sibling's, which is still true.
    deliver(reciprocalFor(outgoing), Nil)
    assert(
      state.readResults().value == Seq(row),
      "a retraction from one reciprocal wiped what a different reciprocal had said",
    )
  }

}
