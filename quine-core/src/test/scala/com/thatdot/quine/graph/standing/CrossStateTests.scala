package com.thatdot.quine.graph.standing

import java.util.UUID

import scala.collection.immutable._

import org.scalactic.source.Position
import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.graph.StandingQueryId
import com.thatdot.quine.graph.cypher.{Expr, MultipleValuesStandingQuery, QueryContext}
import com.thatdot.quine.graph.messaging.StandingQueryMessage.NewMultipleValuesStateResult

class CrossStateTests extends AnyFunSuite {

  def makeState(
    query: MultipleValuesStandingQuery.Cross,
  ): StandingQueryStateWrapper[MultipleValuesStandingQuery.Cross] =
    new StandingQueryStateWrapper(query) {
      override def testInvariants()(implicit pos: Position): Unit =
        ()
    }
  val globalId: StandingQueryId = StandingQueryId(new UUID(12L, 34L))

  test("eager cross state with 1 subquery") {

    val aliasedAs = Symbol("bar")
    val reqQuery = MultipleValuesStandingQuery.SubscribeAcrossEdge(
      None,
      None,
      MultipleValuesStandingQuery.LocalProperty(
        Symbol("foo"),
        MultipleValuesStandingQuery.LocalProperty.Any,
        Some(aliasedAs),
      ),
    )
    val query = MultipleValuesStandingQuery.Cross(
      queries = ArraySeq(reqQuery),
      emitSubscriptionsLazily = false,
    )

    val state = makeState(query)

    withClue("Initializing the state") {
      state.initialize() { effects =>
        val (onNode, sq) = effects.subscriptionsCreated.dequeue()
        assert(onNode == effects.executingNodeId)
        assert(sq == reqQuery)
        assert(effects.isEmpty)
      }
    }

    withClue("Report a result for the sub-query") {
      val result = NewMultipleValuesStateResult(
        state.effects.executingNodeId,
        reqQuery.queryPartId,
        globalId,
        Some(query.queryPartId),
        Seq(QueryContext(Map(aliasedAs -> Expr.Integer(2L)))),
      )
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        val resultCtx = effects.resultsReported.dequeue()
        assert(resultCtx == result.resultGroup)
        assert(effects.isEmpty)
      }
    }

    withClue("Report a second result for the sub-query") {
      val result = NewMultipleValuesStateResult(
        state.effects.executingNodeId,
        reqQuery.queryPartId,
        globalId,
        Some(query.queryPartId),
        Seq(QueryContext(Map(aliasedAs -> Expr.Integer(2L))), QueryContext(Map(aliasedAs -> Expr.Integer(3L)))),
      )
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        val reportedResults = effects.resultsReported.dequeue()
        assert(reportedResults == result.resultGroup)
        assert(effects.isEmpty)
      }
    }
  }

  test("eager cross state with 2 subqueries") {

    val aliasedAs1 = Symbol("bar")
    val reqQuery1 = MultipleValuesStandingQuery.SubscribeAcrossEdge(
      None,
      None,
      MultipleValuesStandingQuery.LocalProperty(
        Symbol("foo"),
        MultipleValuesStandingQuery.LocalProperty.Any,
        Some(aliasedAs1),
      ),
    )

    val aliasedAs2 = Symbol("baz")
    val reqQuery2 = MultipleValuesStandingQuery.SubscribeAcrossEdge(
      None,
      None,
      MultipleValuesStandingQuery.LocalProperty(
        Symbol("qux"),
        MultipleValuesStandingQuery.LocalProperty.Any,
        Some(aliasedAs2),
      ),
    )

    val query = MultipleValuesStandingQuery.Cross(
      queries = ArraySeq(reqQuery1, reqQuery2),
      emitSubscriptionsLazily = false,
    )

    val state = makeState(query)

    withClue("Initializing the state") {
      state.initialize() { effects =>
        val (onNode1, sq1) = effects.subscriptionsCreated.dequeue()
        val (onNode2, sq2) = effects.subscriptionsCreated.dequeue()
        assert(onNode1 == effects.executingNodeId)
        assert(onNode2 == effects.executingNodeId)
        assert(Set(sq1, sq2) == Set(reqQuery1, reqQuery2))
        assert(effects.isEmpty)
      }
    }

    withClue("Report a first result for the first sub-query") {
      val result = NewMultipleValuesStateResult(
        state.effects.executingNodeId,
        reqQuery1.queryPartId,
        globalId,
        Some(query.queryPartId),
        Seq(QueryContext(Map(aliasedAs1 -> Expr.Integer(2L)))),
      )
      state.testInvariants()
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        // This should have stashed the result to be saved for serialization
        assert(state.sqState.resultsAccumulator.contains(reqQuery1.queryPartId))
        assert(state.sqState.resultsAccumulator(reqQuery1.queryPartId).contains(result.resultGroup))
        assert(effects.isEmpty)
      }
    }

    val r1 = QueryContext(Map(aliasedAs1 -> Expr.Integer(2L), aliasedAs2 -> Expr.Integer(3L)))
    withClue("Report a first result for the second sub-query") {
      val result = NewMultipleValuesStateResult(
        state.effects.executingNodeId,
        reqQuery2.queryPartId,
        globalId,
        Some(query.queryPartId),
        Seq(QueryContext(Map(aliasedAs2 -> Expr.Integer(3L)))),
      )
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        val reportedResults = effects.resultsReported.dequeue()
        assert(reportedResults == Seq(r1))
        assert(effects.isEmpty)
      }
    }

    withClue("Report a second result for the first sub-query") {
      val result = NewMultipleValuesStateResult(
        state.effects.executingNodeId,
        reqQuery1.queryPartId,
        globalId,
        Some(query.queryPartId),
        Seq(QueryContext(Map(aliasedAs1 -> Expr.Integer(2L))), QueryContext(Map(aliasedAs1 -> Expr.Integer(4L)))),
      )
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        val reportedResults2 = effects.resultsReported.dequeue()
        val r2 = QueryContext(Map(aliasedAs1 -> Expr.Integer(4L), aliasedAs2 -> Expr.Integer(3L)))
        assert(reportedResults2 == Seq(r1, r2))
        assert(effects.isEmpty)
      }
    }

    withClue("Report a seconds result for the second sub-query") {
      val result = NewMultipleValuesStateResult(
        state.effects.executingNodeId,
        reqQuery2.queryPartId,
        globalId,
        Some(query.queryPartId),
        Seq(QueryContext(Map(aliasedAs2 -> Expr.Integer(3L))), QueryContext(Map(aliasedAs2 -> Expr.Integer(5L)))),
      )
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        val results = effects.resultsReported.dequeue()
        val r3 = QueryContext(Map(aliasedAs1 -> Expr.Integer(2L), aliasedAs2 -> Expr.Integer(5L)))
        val r4 = QueryContext(Map(aliasedAs1 -> Expr.Integer(4L), aliasedAs2 -> Expr.Integer(5L)))
        assert(results.contains(r3))
        assert(results.contains(r4))
        assert(results.size == 4)
        assert(effects.isEmpty)
      }
    }
  }

  test("lazy cross state with 2 subqueries") {

    val aliasedAs1 = Symbol("bar")
    val reqQuery1 = MultipleValuesStandingQuery.SubscribeAcrossEdge(
      None,
      None,
      MultipleValuesStandingQuery.LocalProperty(
        Symbol("foo"),
        MultipleValuesStandingQuery.LocalProperty.Any,
        Some(aliasedAs1),
      ),
    )

    val aliasedAs2 = Symbol("baz")
    val reqQuery2 = MultipleValuesStandingQuery.SubscribeAcrossEdge(
      None,
      None,
      MultipleValuesStandingQuery.LocalProperty(
        Symbol("qux"),
        MultipleValuesStandingQuery.LocalProperty.Any,
        Some(aliasedAs2),
      ),
    )

    val query = MultipleValuesStandingQuery.Cross(
      queries = ArraySeq(reqQuery1, reqQuery2),
      emitSubscriptionsLazily = true,
    )

    val state = makeState(query)

    withClue("Initializing the state") {
      state.initialize() { effects =>
        val (onNode1, sq1) = effects.subscriptionsCreated.dequeue()
        assert(onNode1 == effects.executingNodeId)
        assert(sq1 == reqQuery1)
        assert(effects.isEmpty)
      }
    }

    withClue("Report a first result for the first sub-query") {
      val result = NewMultipleValuesStateResult(
        state.effects.executingNodeId,
        reqQuery1.queryPartId,
        globalId,
        Some(query.queryPartId),
        Seq(QueryContext(Map(aliasedAs1 -> Expr.Integer(2L)))),
      )
      state.testInvariants()
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        val (onNode2, sq2) = effects.subscriptionsCreated.dequeue()
        assert(onNode2 == effects.executingNodeId)
        assert(sq2 == reqQuery2)
        assert(effects.isEmpty)
      }
    }

    val r1 = QueryContext(Map(aliasedAs1 -> Expr.Integer(2L), aliasedAs2 -> Expr.Integer(3L)))
    withClue("Report a first result for the second sub-query") {
      val result = NewMultipleValuesStateResult(
        state.effects.executingNodeId,
        reqQuery2.queryPartId,
        globalId,
        Some(query.queryPartId),
        Seq(QueryContext(Map(aliasedAs2 -> Expr.Integer(3L)))),
      )
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        val reportedResults = effects.resultsReported.dequeue()
        assert(reportedResults == Seq(r1))
        assert(effects.isEmpty)
      }
    }

    val r2 = QueryContext(Map(aliasedAs1 -> Expr.Integer(4L), aliasedAs2 -> Expr.Integer(3L)))
    withClue("Report a second result for the first sub-query") {
      val result = NewMultipleValuesStateResult(
        state.effects.executingNodeId,
        reqQuery1.queryPartId,
        globalId,
        Some(query.queryPartId),
        Seq(QueryContext(Map(aliasedAs1 -> Expr.Integer(2L))), QueryContext(Map(aliasedAs1 -> Expr.Integer(4L)))),
      )
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        val reportedResults2 = effects.resultsReported.dequeue()
        assert(reportedResults2 == Seq(r1, r2))
        assert(effects.isEmpty)
      }
    }

    val r3 = QueryContext(Map(aliasedAs1 -> Expr.Integer(2L), aliasedAs2 -> Expr.Integer(5L)))
    val r4 = QueryContext(Map(aliasedAs1 -> Expr.Integer(4L), aliasedAs2 -> Expr.Integer(5L)))
    withClue("Report a seconds result for the second sub-query") {
      val result = NewMultipleValuesStateResult(
        state.effects.executingNodeId,
        reqQuery2.queryPartId,
        globalId,
        Some(query.queryPartId),
        Seq(QueryContext(Map(aliasedAs2 -> Expr.Integer(3L))), QueryContext(Map(aliasedAs2 -> Expr.Integer(5L)))),
      )
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        val results = effects.resultsReported.dequeue()
        assert(results.contains(r1))
        assert(results.contains(r2))
        assert(results.contains(r3))
        assert(results.contains(r4))
        assert(results.size == 4)
        assert(effects.isEmpty)
      }
    }

    withClue("Remove the first result from the second sub-query") {
      val result = NewMultipleValuesStateResult(
        state.effects.executingNodeId,
        reqQuery2.queryPartId,
        globalId,
        Some(query.queryPartId),
        Seq(QueryContext(Map(aliasedAs2 -> Expr.Integer(5L)))),
      )
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        val results = effects.resultsReported.dequeue()

        assert(results.contains(r3))
        assert(results.contains(r4))
        assert(results.size == 2)
        assert(effects.isEmpty)
      }
    }

    withClue("Remove the first result from the first sub-query") {
      val result = NewMultipleValuesStateResult(
        state.effects.executingNodeId,
        reqQuery1.queryPartId,
        globalId,
        Some(query.queryPartId),
        Seq(QueryContext(Map(aliasedAs1 -> Expr.Integer(4L)))),
      )
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        val results = effects.resultsReported.dequeue()

        assert(results.contains(r4))
        assert(results.size == 1)
        assert(effects.isEmpty)
      }
    }

    withClue("Remove the second result from the first sub-query") {
      val result = NewMultipleValuesStateResult(
        state.effects.executingNodeId,
        reqQuery1.queryPartId,
        globalId,
        Some(query.queryPartId),
        Seq.empty,
      )
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        val results = effects.resultsReported.dequeue()
        assert(results.isEmpty)
        assert(effects.isEmpty)
      }
    }
  }
}
