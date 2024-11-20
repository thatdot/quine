package com.thatdot.quine.graph.standing

import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.graph.cypher.{Expr, MultipleValuesStandingQuery, QueryContext}
import com.thatdot.quine.util.Log._

class LocalIdStateTests extends AnyFunSuite with OptionValues {

  implicit protected val logConfig: LogConfig = LogConfig.permissive

  test("local id state") {

    val query = MultipleValuesStandingQuery.LocalId(
      aliasedAs = Symbol("idValue"),
      formatAsString = false,
    )

    val state = new StandingQueryStateWrapper(query)

    withClue("Initializing the state") {
      state.initialize() { (effects, initialResultsOpt) =>
        val initialResults = initialResultsOpt.value
        val selfValue = Expr.fromQuineValue(state.effects.idProvider.qidToValue(state.effects.executingNodeId))
        assert(initialResults == Seq(QueryContext(Map(query.aliasedAs -> selfValue))))
        assert(effects.isEmpty)
      }
    }
    withClue("Reading the state's results") {
      val results = state.readResults().value
      val selfValue = Expr.fromQuineValue(state.effects.idProvider.qidToValue(state.effects.executingNodeId))
      assert(results == Seq(QueryContext(Map(query.aliasedAs -> selfValue))))
    }
  }

  test("local id state (formatting result as string)") {

    val query = MultipleValuesStandingQuery.LocalId(
      aliasedAs = Symbol("idValue"),
      formatAsString = true,
    )

    val state = new StandingQueryStateWrapper(query)

    withClue("Initializing the state") {
      state.initialize() { (effects, initialResultsOpt) =>
        val initialResults = initialResultsOpt.value
        val selfValue = Expr.Str(state.effects.idProvider.qidToPrettyString(state.effects.executingNodeId))
        assert(initialResults == Seq(QueryContext(Map(query.aliasedAs -> selfValue))))
        assert(effects.isEmpty)
      }
    }

    withClue("Reading the state's results") {
      val results = state.readResults().value
      val selfValue = Expr.Str(state.effects.idProvider.qidToPrettyString(state.effects.executingNodeId))
      assert(results == Seq(QueryContext(Map(query.aliasedAs -> selfValue))))
    }
  }
}
