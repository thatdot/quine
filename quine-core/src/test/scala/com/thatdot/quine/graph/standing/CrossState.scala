package com.thatdot.quine.graph.standing

import java.util.UUID

import scala.collection.compat.immutable._

import org.scalactic.source.Position
import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.graph.StandingQueryId
import com.thatdot.quine.graph.cypher.{Expr, MultipleValuesStandingQuery, QueryContext}
import com.thatdot.quine.graph.messaging.StandingQueryMessage.{
  CancelMultipleValuesResult,
  NewMultipleValuesResult,
  ResultId
}

class CrossStateTest extends AnyFunSuite {

  def makeState(
    query: MultipleValuesStandingQuery.Cross
  ): StandingQueryStateWrapper[MultipleValuesStandingQuery.Cross] =
    new StandingQueryStateWrapper(query) {
      override def testInvariants()(implicit pos: Position): Unit = {

        // `resultDependency` and `reverseResultDependency` store the same dependency info
        val deps1: Set[(ResultId, ResultId)] = sqState.resultDependency.iterator.flatMap { case (res, deps) =>
          deps.map(res -> _)
        }.toSet
        val deps2: Set[(ResultId, ResultId)] = sqState.reverseResultDependency.iterator.flatMap { case (dep, ress) =>
          ress.map(_ -> dep)
        }.toSet
        assert(deps1 == deps2)

        // All deps in `resultDependency` and `reverseResultDependency` are in `accumulatedResults`
        val allDeps: Set[ResultId] = sqState.accumulatedResults.iterator.flatMap(_.keys).toSet
        assert(deps1.map(_._2).subsetOf(allDeps))

        ()
      }
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
        Some(aliasedAs)
      )
    )
    val query = MultipleValuesStandingQuery.Cross(
      queries = ArraySeq(reqQuery),
      emitSubscriptionsLazily = false
    )

    val state = makeState(query)

    withClue("Initializing the state") {
      state.initialize() { effects =>
        val (onNode, sq) = effects.subscriptionsCreated.dequeue()
        assert(onNode == effects.node)
        assert(sq == reqQuery)
        assert(effects.isEmpty)
      }
    }

    val (upstreamResId1, resId1) = withClue("Report a result for the sub-query") {
      val result = NewMultipleValuesResult(
        state.effects.node,
        reqQuery.id,
        globalId,
        Some(query.id),
        ResultId.fresh(),
        QueryContext(Map(aliasedAs -> Expr.Integer(2L)))
      )
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        val (resId1, resultCtx) = effects.resultsReported.dequeue()
        assert(resultCtx == result.result)
        assert(effects.isEmpty)
        (result.resultId, resId1)
      }
    }

    val (upstreamResId2 @ _, resId2 @ _) = withClue("Report a second result for the sub-query") {
      val result = NewMultipleValuesResult(
        state.effects.node,
        reqQuery.id,
        globalId,
        Some(query.id),
        ResultId.fresh(),
        QueryContext(Map(aliasedAs -> Expr.Integer(3L)))
      )
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        val (resId2, resultCtx) = effects.resultsReported.dequeue()
        assert(resultCtx == result.result)
        assert(effects.isEmpty)
        (result.resultId, resId2)
      }
    }

    withClue("Cancel the first result for the sub-query") {
      val cancelled = CancelMultipleValuesResult(
        state.effects.node,
        reqQuery.id,
        globalId,
        Some(query.id),
        upstreamResId1
      )
      state.reportCancelledSubscriptionResult(cancelled, shouldHaveEffects = true) { effects =>
        val resId1Cancelled = effects.resultsCancelled.dequeue()
        assert(resId1Cancelled == resId1)
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
        Some(aliasedAs1)
      )
    )

    val aliasedAs2 = Symbol("baz")
    val reqQuery2 = MultipleValuesStandingQuery.SubscribeAcrossEdge(
      None,
      None,
      MultipleValuesStandingQuery.LocalProperty(
        Symbol("qux"),
        MultipleValuesStandingQuery.LocalProperty.Any,
        Some(aliasedAs2)
      )
    )

    val query = MultipleValuesStandingQuery.Cross(
      queries = ArraySeq(reqQuery1, reqQuery2),
      emitSubscriptionsLazily = false
    )

    val state = makeState(query)

    withClue("Initializing the state") {
      state.initialize() { effects =>
        val (onNode1, sq1) = effects.subscriptionsCreated.dequeue()
        val (onNode2, sq2) = effects.subscriptionsCreated.dequeue()
        assert(onNode1 == effects.node)
        assert(onNode2 == effects.node)
        assert(Set(sq1, sq2) == Set(reqQuery1, reqQuery2))
        assert(effects.isEmpty)
      }
    }

    val upstreamResIdA = withClue("Report a first result for the first sub-query") {
      val result = NewMultipleValuesResult(
        state.effects.node,
        reqQuery1.id,
        globalId,
        Some(query.id),
        ResultId.fresh(),
        QueryContext(Map(aliasedAs1 -> Expr.Integer(2L)))
      )
      state.testInvariants()
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        assert(effects.isEmpty)
        result.resultId
      }
    }

    val (upstreamResIdB, resId1) = withClue("Report a first result for the second sub-query") {
      val result = NewMultipleValuesResult(
        state.effects.node,
        reqQuery2.id,
        globalId,
        Some(query.id),
        ResultId.fresh(),
        QueryContext(Map(aliasedAs2 -> Expr.Integer(3L)))
      )
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        val (resId1, resultCtx1) = effects.resultsReported.dequeue()
        val r1 = QueryContext(Map(aliasedAs1 -> Expr.Integer(2L), aliasedAs2 -> Expr.Integer(3L)))
        assert(resultCtx1 == r1)
        assert(effects.isEmpty)
        (result.resultId, resId1)
      }
    }

    val (upstreamResIdC @ _, resId2) = withClue("Report a second result for the first sub-query") {
      val result = NewMultipleValuesResult(
        state.effects.node,
        reqQuery1.id,
        globalId,
        Some(query.id),
        ResultId.fresh(),
        QueryContext(Map(aliasedAs1 -> Expr.Integer(4L)))
      )
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        val (resId2, resultCtx2) = effects.resultsReported.dequeue()
        val r2 = QueryContext(Map(aliasedAs1 -> Expr.Integer(4L), aliasedAs2 -> Expr.Integer(3L)))
        assert(resultCtx2 == r2)
        assert(effects.isEmpty)
        (result.resultId, resId2)
      }
    }

    val (upstreamResIdD @ _, resId3, resId4 @ _) =
      withClue("Report a seconds result for the second sub-query") {
        val result = NewMultipleValuesResult(
          state.effects.node,
          reqQuery2.id,
          globalId,
          Some(query.id),
          ResultId.fresh(),
          QueryContext(Map(aliasedAs2 -> Expr.Integer(5L)))
        )
        state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
          val results = effects.resultsReported.dequeueAll(_ => true).map(_.swap).toMap
          val r3 = QueryContext(Map(aliasedAs1 -> Expr.Integer(2L), aliasedAs2 -> Expr.Integer(5L)))
          val r4 = QueryContext(Map(aliasedAs1 -> Expr.Integer(4L), aliasedAs2 -> Expr.Integer(5L)))
          assert(results.contains(r3))
          assert(results.contains(r4))
          assert(results.size == 2)
          assert(effects.isEmpty)
          (result.resultId, results(r3), results(r4))
        }
      }

    withClue("Cancel the first result from the second sub-query") {
      val cancelled = CancelMultipleValuesResult(
        state.effects.node,
        reqQuery2.id,
        globalId,
        Some(query.id),
        upstreamResIdB
      )
      state.reportCancelledSubscriptionResult(cancelled, shouldHaveEffects = true) { effects =>
        val resultsCancelled = effects.resultsCancelled.dequeueAll(_ => true).toSet
        assert(resultsCancelled.contains(resId1))
        assert(resultsCancelled.contains(resId2))
        assert(resultsCancelled.size == 2)
        assert(effects.isEmpty)
      }
    }

    withClue("Cancel the first result from the first sub-query") {
      val cancelled = CancelMultipleValuesResult(
        state.effects.node,
        reqQuery1.id,
        globalId,
        Some(query.id),
        upstreamResIdA
      )
      state.reportCancelledSubscriptionResult(cancelled, shouldHaveEffects = true) { effects =>
        val resultCancelled = effects.resultsCancelled.dequeue()
        assert(resultCancelled == resId3)
        assert(effects.isEmpty)
      }
    }

    withClue("Cancel the second result from the first sub-query") {
      val cancelled = CancelMultipleValuesResult(
        state.effects.node,
        reqQuery1.id,
        globalId,
        Some(query.id),
        upstreamResIdC
      )
      state.reportCancelledSubscriptionResult(cancelled, shouldHaveEffects = true) { effects =>
        val resultCancelled = effects.resultsCancelled.dequeue()
        assert(resultCancelled == resId4)
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
        Some(aliasedAs1)
      )
    )

    val aliasedAs2 = Symbol("baz")
    val reqQuery2 = MultipleValuesStandingQuery.SubscribeAcrossEdge(
      None,
      None,
      MultipleValuesStandingQuery.LocalProperty(
        Symbol("qux"),
        MultipleValuesStandingQuery.LocalProperty.Any,
        Some(aliasedAs2)
      )
    )

    val query = MultipleValuesStandingQuery.Cross(
      queries = ArraySeq(reqQuery1, reqQuery2),
      emitSubscriptionsLazily = true
    )

    val state = makeState(query)

    withClue("Initializing the state") {
      state.initialize() { effects =>
        val (onNode1, sq1) = effects.subscriptionsCreated.dequeue()
        assert(onNode1 == effects.node)
        assert(sq1 == reqQuery1)
        assert(effects.isEmpty)
      }
    }

    val upstreamResIdA = withClue("Report a first result for the first sub-query") {
      val result = NewMultipleValuesResult(
        state.effects.node,
        reqQuery1.id,
        globalId,
        Some(query.id),
        ResultId.fresh(),
        QueryContext(Map(aliasedAs1 -> Expr.Integer(2L)))
      )
      state.testInvariants()
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        val (onNode2, sq2) = effects.subscriptionsCreated.dequeue()
        assert(onNode2 == effects.node)
        assert(sq2 == reqQuery2)
        assert(effects.isEmpty)
        result.resultId
      }
    }

    val (upstreamResIdB, resId1) = withClue("Report a first result for the second sub-query") {
      val result = NewMultipleValuesResult(
        state.effects.node,
        reqQuery2.id,
        globalId,
        Some(query.id),
        ResultId.fresh(),
        QueryContext(Map(aliasedAs2 -> Expr.Integer(3L)))
      )
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        val (resId1, resultCtx1) = effects.resultsReported.dequeue()
        val r1 = QueryContext(Map(aliasedAs1 -> Expr.Integer(2L), aliasedAs2 -> Expr.Integer(3L)))
        assert(resultCtx1 == r1)
        assert(effects.isEmpty)
        (result.resultId, resId1)
      }
    }

    val (upstreamResIdC @ _, resId2) = withClue("Report a second result for the first sub-query") {
      val result = NewMultipleValuesResult(
        state.effects.node,
        reqQuery1.id,
        globalId,
        Some(query.id),
        ResultId.fresh(),
        QueryContext(Map(aliasedAs1 -> Expr.Integer(4L)))
      )
      state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
        val (resId2, resultCtx2) = effects.resultsReported.dequeue()
        val r2 = QueryContext(Map(aliasedAs1 -> Expr.Integer(4L), aliasedAs2 -> Expr.Integer(3L)))
        assert(resultCtx2 == r2)
        assert(effects.isEmpty)
        (result.resultId, resId2)
      }
    }

    val (upstreamResIdD @ _, resId3, resId4 @ _) =
      withClue("Report a seconds result for the second sub-query") {
        val result = NewMultipleValuesResult(
          state.effects.node,
          reqQuery2.id,
          globalId,
          Some(query.id),
          ResultId.fresh(),
          QueryContext(Map(aliasedAs2 -> Expr.Integer(5L)))
        )
        state.reportNewSubscriptionResult(result, shouldHaveEffects = true) { effects =>
          val results = effects.resultsReported.dequeueAll(_ => true).map(_.swap).toMap
          val r3 = QueryContext(Map(aliasedAs1 -> Expr.Integer(2L), aliasedAs2 -> Expr.Integer(5L)))
          val r4 = QueryContext(Map(aliasedAs1 -> Expr.Integer(4L), aliasedAs2 -> Expr.Integer(5L)))
          assert(results.contains(r3))
          assert(results.contains(r4))
          assert(results.size == 2)
          assert(effects.isEmpty)
          (result.resultId, results(r3), results(r4))
        }
      }

    withClue("Cancel the first result from the second sub-query") {
      val cancelled = CancelMultipleValuesResult(
        state.effects.node,
        reqQuery2.id,
        globalId,
        Some(query.id),
        upstreamResIdB
      )
      state.reportCancelledSubscriptionResult(cancelled, shouldHaveEffects = true) { effects =>
        val resultsCancelled = effects.resultsCancelled.dequeueAll(_ => true).toSet
        assert(resultsCancelled.contains(resId1))
        assert(resultsCancelled.contains(resId2))
        assert(resultsCancelled.size == 2)
        assert(effects.isEmpty)
      }
    }

    withClue("Cancel the first result from the first sub-query") {
      val cancelled = CancelMultipleValuesResult(
        state.effects.node,
        reqQuery1.id,
        globalId,
        Some(query.id),
        upstreamResIdA
      )
      state.reportCancelledSubscriptionResult(cancelled, shouldHaveEffects = true) { effects =>
        val resultCancelled = effects.resultsCancelled.dequeue()
        assert(resultCancelled == resId3)
        assert(effects.isEmpty)
      }
    }

    withClue("Cancel the second result from the first sub-query") {
      val cancelled = CancelMultipleValuesResult(
        state.effects.node,
        reqQuery1.id,
        globalId,
        Some(query.id),
        upstreamResIdC
      )
      state.reportCancelledSubscriptionResult(cancelled, shouldHaveEffects = true) { effects =>
        val resultCancelled = effects.resultsCancelled.dequeue()
        assert(resultCancelled == resId4)
        assert(effects.isEmpty)
      }
    }
  }
}
