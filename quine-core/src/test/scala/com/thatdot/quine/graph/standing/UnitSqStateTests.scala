package com.thatdot.quine.graph.standing

import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.graph.cypher.{MultipleValuesStandingQuery, QueryContext}

class UnitSqStateTests extends AnyFunSuite with OptionValues {

  def freshState() = new StandingQueryStateWrapper(
    MultipleValuesStandingQuery.UnitSq.instance,
  )

  test("Unit state") {

    val state = freshState()

    withClue("Initializing the state") {
      state.initialize() { (effects, initialResultsOpt) =>
        val initialResults = initialResultsOpt.value
        assert(initialResults == Seq(QueryContext.empty))
        assert(effects.isEmpty)
      }
    }
    withClue("Reading the state's results") {
      val results = state.readResults().value
      assert(results == Seq(QueryContext.empty))
    }
  }
}
