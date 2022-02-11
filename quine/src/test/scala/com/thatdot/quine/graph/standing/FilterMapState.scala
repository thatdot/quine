package com.thatdot.quine.graph.standing

import java.util.UUID

import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.graph.StandingQueryId
import com.thatdot.quine.graph.cypher.{Expr, QueryContext, StandingQuery}
import com.thatdot.quine.graph.messaging.StandingQueryMessage.{CancelCypherResult, NewCypherResult, ResultId}
import com.thatdot.quine.model.QuineId

class FilterMapStateTest extends AnyFunSuite {
  val qid1: QuineId = QuineId(Array(1.toByte))
  val qid2: QuineId = QuineId(Array(2.toByte))
  val globalId: StandingQueryId = StandingQueryId(new UUID(12L, 34L))

  test("no-op filter") {
    // using upstreamQuery from LocalPropertyState's "alias but no value constraint"
    val upstreamQuery = StandingQuery.LocalProperty(
      propKey = Symbol("foo"),
      propConstraint = StandingQuery.LocalProperty.Any,
      aliasedAs = Some(Symbol("fooValue"))
    )
    val query = StandingQuery.FilterMap(
      toFilter = upstreamQuery,
      condition = Some(Expr.True),
      dropExisting = false,
      toAdd = Nil
    )

    val state = new StandingQueryStateWrapper(query)

    withClue("initializing state creates exactly 1 subscription and no results") {
      state.initialize() { effects =>
        assert(effects.subscriptionsCreated.nonEmpty)
        assert(effects.subscriptionsCreated.dequeue()._2 === upstreamQuery)
        assert(effects.isEmpty)
      }
    }

    val (upstreamResId1, resId1) = withClue("upstream creating a result creates a result") {
      val upstreamResultId = ResultId.fresh()
      state.reportNewSubscriptionResult(
        NewCypherResult(
          qid1,
          upstreamQuery.id,
          globalId,
          Some(query.id),
          upstreamResultId,
          QueryContext(
            Map(
              upstreamQuery.aliasedAs.get -> Expr.Integer(1)
            )
          )
        ),
        shouldHaveEffects = true
      ) { effects =>
        assert(effects.resultsReported.nonEmpty)
        val (resultId, result) = effects.resultsReported.dequeue()
        assert(result.environment.size === 1)
        assert(result.get(upstreamQuery.aliasedAs.get) === Some(Expr.Integer(1)))
        assert(effects.isEmpty)
        (upstreamResultId, resultId)
      }
    }
    withClue("upstream creating additional results creates additional results") {
      val upstreamResultId = ResultId.fresh()
      state.reportNewSubscriptionResult(
        NewCypherResult(
          qid2,
          upstreamQuery.id,
          globalId,
          Some(query.id),
          upstreamResultId,
          QueryContext(
            Map(
              upstreamQuery.aliasedAs.get -> Expr.Integer(5)
            )
          )
        ),
        shouldHaveEffects = true
      ) { effects =>
        assert(effects.resultsReported.nonEmpty)
        val (resultId, result) = effects.resultsReported.dequeue()
        assert(result.environment.size === 1)
        assert(result.get(upstreamQuery.aliasedAs.get) === Some(Expr.Integer(5)))
        assert(effects.isEmpty)
        (upstreamResultId, resultId)
      }
    }

    withClue("upstream cancelling a result cancels the corresponding result but not others") {
      state.reportCancelledSubscriptionResult(
        CancelCypherResult(qid1, upstreamQuery.id, globalId, Some(query.id), upstreamResId1),
        shouldHaveEffects = true
      ) { effects =>
        assert(effects.resultsCancelled.nonEmpty)
        assert(effects.resultsCancelled.dequeue() === resId1)
        assert(effects.isEmpty)
      }
    }

    withClue("terminating this query should remove the upstream subscription") {
      state.shutdown() { effects =>
        assert(effects.subscriptionsCancelled.nonEmpty)
        assert(effects.subscriptionsCancelled.dequeue()._2 === upstreamQuery.id)
        assert(effects.isEmpty)
      }
    }
  }

  test("impassible filter") {
    val upstreamQuery = StandingQuery.LocalProperty(
      propKey = Symbol("foo"),
      propConstraint = StandingQuery.LocalProperty.Any,
      aliasedAs = Some(Symbol("fooValue"))
    )
    val query = StandingQuery.FilterMap(
      toFilter = upstreamQuery,
      condition = Some(Expr.False),
      dropExisting = false,
      toAdd = Nil
    )
    val state = new StandingQueryStateWrapper(query)

    withClue("initializing state creates exactly 1 subscription and no results") {
      state.initialize() { effects =>
        assert(effects.subscriptionsCreated.nonEmpty)
        assert(effects.subscriptionsCreated.dequeue()._2 === upstreamQuery)
        assert(effects.isEmpty)
      }
    }

    val upstreamResId1 = withClue("upstream creating a result has no effect") {
      val upstreamResultId = ResultId.fresh()
      state.reportNewSubscriptionResult(
        NewCypherResult(
          qid1,
          upstreamQuery.id,
          globalId,
          Some(query.id),
          upstreamResultId,
          QueryContext(
            Map(
              upstreamQuery.aliasedAs.get -> Expr.Integer(1)
            )
          )
        ),
        shouldHaveEffects = false
      ) { effects =>
        assert(effects.isEmpty)
        upstreamResultId
      }
    }
    withClue("upstream cancelling a result has no effect") {
      state.reportCancelledSubscriptionResult(
        CancelCypherResult(qid1, upstreamQuery.id, globalId, Some(query.id), upstreamResId1),
        shouldHaveEffects = false
      ) { effects =>
        assert(effects.isEmpty)
      }
    }

    withClue("terminating this query should remove the upstream subscription") {
      state.shutdown() { effects =>
        assert(effects.subscriptionsCancelled.nonEmpty)
        assert(effects.subscriptionsCancelled.dequeue()._2 === upstreamQuery.id)
        assert(effects.isEmpty)
      }
    }
  }

  test("doubling map") {
    val upstreamQuery = StandingQuery.LocalProperty(
      propKey = Symbol("foo"),
      propConstraint = StandingQuery.LocalProperty.Any,
      aliasedAs = Some(Symbol("fooValue"))
    )
    val query = StandingQuery.FilterMap(
      toFilter = upstreamQuery,
      condition = Some(Expr.True),
      dropExisting = true,
      toAdd = List(
        Symbol("fooValueDoubled") -> Expr.Multiply(
          Expr.Integer(2),
          Expr.Variable(upstreamQuery.aliasedAs.get)
        )
      )
    )

    val state = new StandingQueryStateWrapper(query)

    withClue("initializing state creates exactly 1 subscription and no results") {
      state.initialize() { effects =>
        assert(effects.subscriptionsCreated.nonEmpty)
        assert(effects.subscriptionsCreated.dequeue()._2 === upstreamQuery)
        assert(effects.isEmpty)
      }
    }

    val (upstreamResId1, resId1) = withClue("upstream creating a result creates a result") {
      val upstreamResultId = ResultId.fresh()
      state.reportNewSubscriptionResult(
        NewCypherResult(
          qid1,
          upstreamQuery.id,
          globalId,
          Some(query.id),
          upstreamResultId,
          QueryContext(
            Map(
              upstreamQuery.aliasedAs.get -> Expr.Integer(1)
            )
          )
        ),
        shouldHaveEffects = true
      ) { effects =>
        assert(effects.resultsReported.nonEmpty)
        val (resultId, result) = effects.resultsReported.dequeue()
        assert(result.environment.size === query.toAdd.length)
        assert(result.get(query.toAdd.head._1) === Some(Expr.Integer(2 * 1)))
        assert(effects.isEmpty)
        (upstreamResultId, resultId)
      }
    }
    withClue("upstream creating additional results creates additional results") {
      val upstreamResultId = ResultId.fresh()
      state.reportNewSubscriptionResult(
        NewCypherResult(
          qid2,
          upstreamQuery.id,
          globalId,
          Some(query.id),
          upstreamResultId,
          QueryContext(
            Map(
              upstreamQuery.aliasedAs.get -> Expr.Integer(5)
            )
          )
        ),
        shouldHaveEffects = true
      ) { effects =>
        assert(effects.resultsReported.nonEmpty)
        val (resultId, result) = effects.resultsReported.dequeue()
        assert(result.environment.size === query.toAdd.length)
        assert(result.get(query.toAdd.head._1) === Some(Expr.Integer(2 * 5)))
        assert(effects.isEmpty)
        (upstreamResultId, resultId)
      }
    }
    withClue("upstream cancelling a result cancels the corresponding result but not others") {
      state.reportCancelledSubscriptionResult(
        CancelCypherResult(qid1, upstreamQuery.id, globalId, Some(query.id), upstreamResId1),
        shouldHaveEffects = true
      ) { effects =>
        assert(effects.resultsCancelled.nonEmpty)
        assert(effects.resultsCancelled.dequeue() === resId1)
        assert(effects.isEmpty)
      }
    }

    withClue("terminating this query should remove the upstream subscription") {
      state.shutdown() { effects =>
        assert(effects.subscriptionsCancelled.nonEmpty)
        assert(effects.subscriptionsCancelled.dequeue()._2 === upstreamQuery.id)
        assert(effects.isEmpty)
      }
    }
  }
  test("add tripled odd values filter+map") {
    val upstreamQuery = StandingQuery.LocalProperty(
      propKey = Symbol("foo"),
      propConstraint = StandingQuery.LocalProperty.Any,
      aliasedAs = Some(Symbol("fooValue"))
    )
    val query = StandingQuery.FilterMap(
      toFilter = upstreamQuery,
      condition = Some(
        Expr.Equal(
          Expr.Integer(1), // == 1
          Expr.Modulo( // fooValue % 2
            Expr.Variable(upstreamQuery.aliasedAs.get),
            Expr.Integer(2)
          )
        )
      ),
      dropExisting = false,
      toAdd = List(
        Symbol("fooValueTripled") -> Expr.Multiply(
          Expr.Integer(3),
          Expr.Variable(upstreamQuery.aliasedAs.get)
        )
      )
    )
    val state = new StandingQueryStateWrapper(query)

    withClue("initializing state creates exactly 1 subscription and no results") {
      state.initialize() { effects =>
        assert(effects.subscriptionsCreated.nonEmpty)
        assert(effects.subscriptionsCreated.dequeue()._2 === upstreamQuery)
        assert(effects.isEmpty)
      }
    }

    val (upstreamResId1, resId1) = withClue("upstream creating a result creates a result") {
      val upstreamResultId = ResultId.fresh()
      state.reportNewSubscriptionResult(
        NewCypherResult(
          qid1,
          upstreamQuery.id,
          globalId,
          Some(query.id),
          upstreamResultId,
          QueryContext(
            Map(
              upstreamQuery.aliasedAs.get -> Expr.Integer(1)
            )
          )
        ),
        shouldHaveEffects = true
      ) { effects =>
        assert(effects.resultsReported.nonEmpty)
        val (resultId, result) = effects.resultsReported.dequeue()
        assert(result.environment.size === query.toAdd.length + 1)
        assert(result.get(upstreamQuery.aliasedAs.get) === Some(Expr.Integer(1)))
        assert(result.get(query.toAdd.head._1) === Some(Expr.Integer(3 * 1)))
        assert(effects.isEmpty)
        (upstreamResultId, resultId)
      }
    }
    withClue("upstream creating additional results creates additional results") {
      val upstreamResultId = ResultId.fresh()
      state.reportNewSubscriptionResult(
        NewCypherResult(
          qid2,
          upstreamQuery.id,
          globalId,
          Some(query.id),
          upstreamResultId,
          QueryContext(
            Map(
              upstreamQuery.aliasedAs.get -> Expr.Integer(5)
            )
          )
        ),
        shouldHaveEffects = true
      ) { effects =>
        assert(effects.resultsReported.nonEmpty)
        val (resultId, result) = effects.resultsReported.dequeue()
        assert(result.environment.size === query.toAdd.length + 1)
        assert(result.get(upstreamQuery.aliasedAs.get) === Some(Expr.Integer(5)))
        assert(result.get(query.toAdd.head._1) === Some(Expr.Integer(3 * 5)))
        assert(effects.isEmpty)
        (upstreamResultId, resultId)
      }
    }
    withClue("upstream cancelling a result cancels the corresponding result but not others") {
      state.reportCancelledSubscriptionResult(
        CancelCypherResult(qid1, upstreamQuery.id, globalId, Some(query.id), upstreamResId1),
        shouldHaveEffects = true
      ) { effects =>
        assert(effects.resultsCancelled.nonEmpty)
        assert(effects.resultsCancelled.dequeue() === resId1)
        assert(effects.isEmpty)
      }
    }

    withClue("terminating this query should remove the upstream subscription") {
      state.shutdown() { effects =>
        assert(effects.subscriptionsCancelled.nonEmpty)
        assert(effects.subscriptionsCancelled.dequeue()._2 === upstreamQuery.id)
        assert(effects.isEmpty)
      }
    }
  }
}
