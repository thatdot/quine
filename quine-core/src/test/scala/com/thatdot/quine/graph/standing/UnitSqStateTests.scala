package com.thatdot.quine.graph.standing

import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.graph.cypher.{MultipleValuesStandingQuery, QueryContext}

class UnitSqStateTests extends AnyFunSuite {

  def freshState() = new StandingQueryStateWrapper(
    MultipleValuesStandingQuery.UnitSq.instance,
  )

  test("Unit state") {

    val state = freshState()

    withClue("Initializing the state") {
      state.initialize() { effects =>
        val result = effects.resultsReported.dequeue()
        assert(result == Seq(QueryContext.empty))
        assert(effects.isEmpty)
      }
    }
  }
}
