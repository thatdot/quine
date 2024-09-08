package com.thatdot.quine.graph.standing

import java.util.UUID

import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.graph.StandingQueryId
import com.thatdot.quine.graph.cypher.Expr.Variable
import com.thatdot.quine.graph.cypher.{Expr, MultipleValuesStandingQuery, QueryContext}
import com.thatdot.quine.graph.messaging.StandingQueryMessage.NewMultipleValuesStateResult
import com.thatdot.quine.model.QuineId

class FilterMapStateTests extends AnyFunSuite {
  val qid1: QuineId = QuineId(Array(1.toByte))
  val qid2: QuineId = QuineId(Array(2.toByte))
  val globalId: StandingQueryId = StandingQueryId(new UUID(12L, 34L))

  test("no-op filter") {
    // using upstreamQuery from LocalPropertyState's "alias but no value constraint"
    val upstreamAlias = Symbol("fooValue")
    val upstreamQuery = MultipleValuesStandingQuery.LocalProperty(
      propKey = Symbol("foo"),
      propConstraint = MultipleValuesStandingQuery.LocalProperty.Any,
      aliasedAs = Some(upstreamAlias),
    )
    val mappedAlias = Symbol("fooMapped")
    val query = MultipleValuesStandingQuery.FilterMap(
      toFilter = upstreamQuery,
      condition = Some(Expr.True),
      dropExisting = false,
      toAdd = List(mappedAlias -> Variable(upstreamAlias)),
    )

    val state = new StandingQueryStateWrapper(query)

    withClue("initializing state creates exactly 1 subscription and no results") {
      state.initialize() { effects =>
        assert(effects.subscriptionsCreated.nonEmpty)
        assert(effects.subscriptionsCreated.dequeue()._2 === upstreamQuery)
        assert(effects.isEmpty)
      }
    }

    withClue("upstream creating a result creates a result") {
      state.reportNewSubscriptionResult(
        NewMultipleValuesStateResult(
          qid1,
          upstreamQuery.queryPartId,
          globalId,
          Some(query.queryPartId),
          Seq(
            QueryContext(
              Map(
                upstreamQuery.aliasedAs.get -> Expr.Integer(1),
              ),
            ),
          ),
        ),
        shouldHaveEffects = true,
      ) { effects =>
        assert(effects.resultsReported.nonEmpty)
        val result = effects.resultsReported.dequeue().head
        assert(result.environment.size === 2)
        assert(result.get(upstreamAlias) === Some(Expr.Integer(1)))
        assert(result.get(mappedAlias) === Some(Expr.Integer(1)))
        assert(effects.isEmpty)
      }
    }
    withClue("upstream creating additional results creates additional results") {
      state.reportNewSubscriptionResult(
        NewMultipleValuesStateResult(
          qid2,
          upstreamQuery.queryPartId,
          globalId,
          Some(query.queryPartId),
          Seq(
            QueryContext(
              Map(
                upstreamQuery.aliasedAs.get -> Expr.Integer(5),
              ),
            ),
          ),
        ),
        shouldHaveEffects = true,
      ) { effects =>
        assert(effects.resultsReported.nonEmpty)
        val result = effects.resultsReported.dequeue().head
        assert(result.environment.size === 2)
        assert(result.get(upstreamAlias).contains(Expr.Integer(5)))
        assert(result.get(mappedAlias).contains(Expr.Integer(5)))
        assert(effects.isEmpty)
      }
    }
  }

  test("impassible filter") {
    val upstreamQuery = MultipleValuesStandingQuery.LocalProperty(
      propKey = Symbol("foo"),
      propConstraint = MultipleValuesStandingQuery.LocalProperty.Any,
      aliasedAs = Some(Symbol("fooValue")),
    )
    val query = MultipleValuesStandingQuery.FilterMap(
      toFilter = upstreamQuery,
      condition = Some(Expr.False),
      dropExisting = false,
      toAdd = Nil,
    )
    val state = new StandingQueryStateWrapper(query)

    withClue("initializing state creates exactly 1 subscription and no results") {
      state.initialize() { effects =>
        assert(effects.subscriptionsCreated.nonEmpty)
        assert(effects.subscriptionsCreated.dequeue()._2 === upstreamQuery)
        assert(effects.isEmpty)
      }
    }

    withClue("upstream creating a result has no effect") {
      state.reportNewSubscriptionResult(
        NewMultipleValuesStateResult(
          qid1,
          upstreamQuery.queryPartId,
          globalId,
          Some(query.queryPartId),
          Seq(
            QueryContext(
              Map(
                upstreamQuery.aliasedAs.get -> Expr.Integer(1),
              ),
            ),
          ),
        ),
        shouldHaveEffects = true,
      ) { effects =>
        assert(effects.resultsReported.nonEmpty)
        val results = effects.resultsReported.dequeue()
        assert(results === Seq.empty)
        assert(effects.isEmpty)
      }
    }
  }

  test("doubling map") {
    val upstreamQuery = MultipleValuesStandingQuery.LocalProperty(
      propKey = Symbol("foo"),
      propConstraint = MultipleValuesStandingQuery.LocalProperty.Any,
      aliasedAs = Some(Symbol("fooValue")),
    )
    val query = MultipleValuesStandingQuery.FilterMap(
      toFilter = upstreamQuery,
      condition = Some(Expr.True),
      dropExisting = true,
      toAdd = List(
        Symbol("fooValueDoubled") -> Expr.Multiply(
          Expr.Integer(2),
          Expr.Variable(upstreamQuery.aliasedAs.get),
        ),
      ),
    )

    val state = new StandingQueryStateWrapper(query)

    withClue("initializing state creates exactly 1 subscription and no results") {
      state.initialize() { effects =>
        assert(effects.subscriptionsCreated.nonEmpty)
        assert(effects.subscriptionsCreated.dequeue()._2 === upstreamQuery)
        assert(effects.isEmpty)
      }
    }

    withClue("upstream creating a result creates a result") {
      state.reportNewSubscriptionResult(
        NewMultipleValuesStateResult(
          qid1,
          upstreamQuery.queryPartId,
          globalId,
          Some(query.queryPartId),
          Seq(
            QueryContext(
              Map(
                upstreamQuery.aliasedAs.get -> Expr.Integer(1),
              ),
            ),
          ),
        ),
        shouldHaveEffects = true,
      ) { effects =>
        assert(effects.resultsReported.size == 1)
        val result = effects.resultsReported.dequeue().head
        assert(result.environment.size === query.toAdd.length)
        assert(result.get(query.toAdd.head._1) === Some(Expr.Integer(2 * 1)))
        assert(effects.isEmpty)
      }
    }

    withClue("upstream creating additional results creates additional results") {
      state.reportNewSubscriptionResult(
        NewMultipleValuesStateResult(
          qid2,
          upstreamQuery.queryPartId,
          globalId,
          Some(query.queryPartId),
          Seq(
            QueryContext(
              Map(
                upstreamQuery.aliasedAs.get -> Expr.Integer(5),
              ),
            ),
          ),
        ),
        shouldHaveEffects = true,
      ) { effects =>
        assert(effects.resultsReported.size == 1)
        val result = effects.resultsReported.dequeue().head
        assert(result.environment.size === query.toAdd.length)
        assert(result.get(query.toAdd.head._1) === Some(Expr.Integer(2 * 5)))
        assert(effects.isEmpty)
      }
    }
  }

  test("add tripled odd values filter+map") {
    val upstreamAlias = Symbol("fooValue")
    val outputAlias = Symbol("fooValueTripled")
    val upstreamQuery = MultipleValuesStandingQuery.LocalProperty(
      propKey = Symbol("foo"),
      propConstraint = MultipleValuesStandingQuery.LocalProperty.Any,
      aliasedAs = Some(upstreamAlias),
    )
    val query = MultipleValuesStandingQuery.FilterMap(
      toFilter = upstreamQuery,
      condition = Some(
        Expr.Equal(
          Expr.Integer(1), // == 1
          Expr.Modulo( // fooValue % 2
            Expr.Variable(upstreamAlias),
            Expr.Integer(2),
          ),
        ),
      ),
      dropExisting = true,
      toAdd = List(
        outputAlias -> Expr.Multiply(
          Expr.Integer(3),
          Expr.Variable(upstreamAlias),
        ),
      ),
    )
    val state = new StandingQueryStateWrapper(query)

    withClue("initializing state creates exactly 1 subscription and no results") {
      state.initialize() { effects =>
        assert(effects.subscriptionsCreated.nonEmpty)
        assert(effects.subscriptionsCreated.dequeue()._2 === upstreamQuery)
        assert(effects.isEmpty)
      }
    }

    withClue("upstream creating a result creates a result") {
      state.reportNewSubscriptionResult(
        NewMultipleValuesStateResult(
          qid1,
          upstreamQuery.queryPartId,
          globalId,
          Some(query.queryPartId),
          Seq(
            QueryContext(
              Map(
                upstreamAlias -> Expr.Integer(1),
                Symbol("secondAlias") -> Expr.Integer(2),
              ),
            ),
          ),
        ),
        shouldHaveEffects = true,
      ) { effects =>
        assert(effects.resultsReported.size == 1)
        val result = effects.resultsReported.dequeue().head
        assert(result.environment.size === query.toAdd.length)
        assert(result.get(query.toAdd.head._1) === Some(Expr.Integer(3 * 1)))
        assert(effects.isEmpty)
      }
    }

    withClue("upstream creating additional results creates additional results") {
      state.reportNewSubscriptionResult(
        NewMultipleValuesStateResult(
          qid2,
          upstreamQuery.queryPartId,
          globalId,
          Some(query.queryPartId),
          Seq(
            QueryContext(
              Map(
                upstreamQuery.aliasedAs.get -> Expr.Integer(5),
              ),
            ),
          ),
        ),
        shouldHaveEffects = true,
      ) { effects =>
        assert(effects.resultsReported.size == 1)
        val result = effects.resultsReported.dequeue().head
        assert(result.environment.size === query.toAdd.length)
        assert(result.get(query.toAdd.head._1) === Some(Expr.Integer(3 * 5)))
        assert(effects.isEmpty)
      }
    }
  }
}
