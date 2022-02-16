package com.thatdot.quine.graph.standing

import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.graph.cypher.{Expr, QueryContext, StandingQuery}

class LocalIdStateTest extends AnyFunSuite {

  test("local id state") {

    val query = StandingQuery.LocalId(
      aliasedAs = Symbol("idValue"),
      formatAsString = false
    )

    val state = new StandingQueryStateWrapper(query)

    withClue("Initializing the state") {
      state.initialize() { effects =>
        val (resId @ _, result) = effects.resultsReported.dequeue()
        val selfValue = Expr.fromQuineValue(state.effects.idProvider.qidToValue(state.effects.node))
        assert(result == QueryContext(Map(query.aliasedAs -> selfValue)))
        assert(effects.isEmpty)
      }
    }
  }

  test("local id state (formatting result as string)") {

    val query = StandingQuery.LocalId(
      aliasedAs = Symbol("idValue"),
      formatAsString = true
    )

    val state = new StandingQueryStateWrapper(query)

    withClue("Initializing the state") {
      state.initialize() { effects =>
        val (resId @ _, result) = effects.resultsReported.dequeue()
        val selfValue = Expr.Str(state.effects.idProvider.qidToPrettyString(state.effects.node))
        assert(result == QueryContext(Map(query.aliasedAs -> selfValue)))
        assert(effects.isEmpty)
      }
    }
  }
}
