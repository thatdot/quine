package com.thatdot.quine.graph.standing

import java.util.UUID

import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.EdgeEvent.EdgeAdded
import com.thatdot.quine.graph.StandingQueryId
import com.thatdot.quine.graph.cypher.{Expr, MultipleValuesStandingQuery, QueryContext}
import com.thatdot.quine.graph.messaging.StandingQueryMessage.{
  MultipleValuesStandingQuerySubscriber,
  NewMultipleValuesStateResult,
}
import com.thatdot.quine.model.{EdgeDirection, HalfEdge}

/** What the edge-keyed states do when the node cannot say which edges it has.
  *
  * On a node whose edges are in the persistor, asking is a read, and a read can fail or time out. There is no answer
  * to fall back on, because both answers are claims: "there is an edge" and "there is no edge" are equally things
  * this node has just demonstrated it cannot establish. So every site takes the branch that neither writes a row nor
  * withdraws a result: it does not credit what it cannot attribute, and reports to a subscriber it cannot rule out
  * rather than withholding from one it cannot confirm.
  *
  * Each test below pairs the two: the same step, once while the node can answer and once while it cannot, so that
  * what is asserted is the difference rather than the absence of an effect that was never going to happen.
  *
  * The two edge-removal sites are deliberately absent. Neither asks any more: an edge collection is a set and each
  * of them was asking after the very half edge that had just been removed from it, so the question had one answer
  * and the read only supplied ways to fail to get it.
  */
class UnreadableEdgesTests extends AnyFunSuite with OptionValues {

  private val globalId: StandingQueryId = StandingQueryId(new UUID(12L, 34L))
  private val edgeType: Symbol = Symbol("an_edge")
  private val andThenAliasedAs: Symbol = Symbol("bar")

  private val andThen: MultipleValuesStandingQuery.LocalProperty = MultipleValuesStandingQuery
    .LocalProperty(Symbol("foo"), MultipleValuesStandingQuery.LocalProperty.Any, Some(andThenAliasedAs))

  private def row(value: Long): QueryContext = QueryContext(Map(andThenAliasedAs -> Expr.Integer(value)))

  // ---------------------------------------------------------------- across an edge

  private val acrossEdge: MultipleValuesStandingQuery.SubscribeAcrossEdge =
    MultipleValuesStandingQuery.SubscribeAcrossEdge(
      edgeName = Some(edgeType),
      edgeDirection = Some(EdgeDirection.Outgoing),
      andThen = andThen,
    )

  private def acrossEdgeState(): StandingQueryStateWrapper[MultipleValuesStandingQuery.SubscribeAcrossEdge] = {
    val state = new StandingQueryStateWrapper(acrossEdge, Seq(andThen))
    state.initialize()((_, _) => ())
    state
  }

  private def resultAcrossEdgeFrom(
    from: QuineId,
    reciprocalPartId: com.thatdot.quine.graph.MultipleValuesStandingQueryPartId,
    values: Long*,
  ): NewMultipleValuesStateResult =
    NewMultipleValuesStateResult(
      from,
      reciprocalPartId,
      globalId,
      Some(acrossEdge.queryPartId),
      values.map(row),
    )

  test("a result the node cannot attribute to an edge is dropped rather than credited") {
    val state = acrossEdgeState()
    val far = QuineId(Array(7.toByte))
    val halfEdge = HalfEdge(edgeType, EdgeDirection.Outgoing, far)

    val reciprocalPartId = state.reportNodeEvents(Seq(EdgeAdded(halfEdge)), shouldHaveEffects = true) { effects =>
      val (_, subquery) = effects.subscriptionsCreated.dequeue()
      subquery.queryPartId
    }

    withClue("a result arriving while the node cannot read its edges changes nothing") {
      state.effects.edgesCanBeRead = false
      state.reportNewSubscriptionResult(
        resultAcrossEdgeFrom(far, reciprocalPartId, 1L),
        shouldHaveEffects = false,
      ) { effects =>
        assert(effects.resultsReported.isEmpty)
        ()
      }
      withClue("nothing was credited, so this part still has no answer to give") {
        assert(state.readResults().isEmpty)
      }
    }

    withClue("the far side restating it once the node can read is what fills the gap") {
      state.effects.edgesCanBeRead = true
      state.reportNewSubscriptionResult(
        resultAcrossEdgeFrom(far, reciprocalPartId, 1L),
        shouldHaveEffects = true,
      ) { effects =>
        assert(effects.resultsReported.dequeue() === Seq(row(1L)))
        ()
      }
      assert(state.readResults().value === Seq(row(1L)))
    }
  }

  // ---------------------------------------------------------------- the reciprocal

  private val reciprocal: MultipleValuesStandingQuery.EdgeSubscriptionReciprocal =
    MultipleValuesStandingQuery.EdgeSubscriptionReciprocal(
      halfEdge = HalfEdge(edgeType, EdgeDirection.Outgoing, QuineId(Array(7.toByte))),
      andThenId = andThen.queryPartId,
    )

  private def reciprocalState(): StandingQueryStateWrapper[MultipleValuesStandingQuery.EdgeSubscriptionReciprocal] = {
    val state = new StandingQueryStateWrapper(reciprocal, Seq(andThen))
    state.initialize() { (effects, _) =>
      effects.subscriptionsCreated.dequeue()
      ()
    }
    state
  }

  private def resultForReciprocal(
    state: StandingQueryStateWrapper[MultipleValuesStandingQuery.EdgeSubscriptionReciprocal],
    value: Long,
  ): NewMultipleValuesStateResult = NewMultipleValuesStateResult(
    state.effects.executingNodeId,
    reciprocal.andThenId,
    globalId,
    Some(reciprocal.queryPartId),
    Seq(row(value)),
  )

  private def subscriberOn(node: QuineId): MultipleValuesStandingQuerySubscriber.NodeSubscriber =
    MultipleValuesStandingQuerySubscriber.NodeSubscriber(node, globalId, andThen.queryPartId)

  test("a subscriber the node cannot rule out is still told, and still answered") {
    val state = reciprocalState()
    val subscriber = QuineId(Array(9.toByte))

    // Subscribed, but this node holds no edge to it, which is exactly the case the read would settle, and cannot.
    state.effects.subscribers += subscriberOn(subscriber)
    state.effects.edgesCanBeRead = false

    withClue("a report goes to a subscriber this node cannot say it has no edge to") {
      state.reportNewSubscriptionResult(resultForReciprocal(state, 1L), shouldHaveEffects = true) { effects =>
        val (onNode, group) = effects.resultsReportedToNode.dequeue()
        assert(onNode === subscriber)
        assert(group === Seq(row(1L)))
        ()
      }
    }

    withClue("and a subscriber that asks is answered rather than met with silence it cannot resolve") {
      assert(state.addSubscriber(subscriberOn(subscriber)).value === Seq(row(1L)))
    }

    withClue("once the node can read, the same subscriber with no edge is correctly passed over") {
      state.effects.edgesCanBeRead = true
      state.reportNewSubscriptionResult(resultForReciprocal(state, 2L), shouldHaveEffects = true) { effects =>
        assert(effects.resultsReportedToNode.isEmpty)
        ()
      }
      assert(state.addSubscriber(subscriberOn(subscriber)).isEmpty)
    }
  }
}
