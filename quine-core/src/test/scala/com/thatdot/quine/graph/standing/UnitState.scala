package com.thatdot.quine.graph.standing

import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.graph.cypher.{MultipleValuesStandingQuery, QueryContext}

class UnitSqStateTest extends AnyFunSuite {

  def freshState() = new StandingQueryStateWrapper(
    MultipleValuesStandingQuery.UnitSq()
  )

  test("Unit state") {

    val state = freshState()

    withClue("Initializing the state") {
      state.initialize() { effects =>
        val (resId @ _, result) = effects.resultsReported.dequeue()
        assert(result == QueryContext.empty)
        assert(effects.isEmpty)
      }
    }
  }

}
