package com.thatdot.quine.graph.standing

import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.graph.PropertyEvent.{PropertyRemoved, PropertySet}
import com.thatdot.quine.graph.cypher.{Expr, MultipleValuesStandingQuery, QueryContext}
import com.thatdot.quine.model.{PropertyValue, QuineValue}

class LocalPropertyStateTest extends AnyFunSuite {

  test("any value constraint, no alias") {

    val query = MultipleValuesStandingQuery.LocalProperty(
      propKey = Symbol("keyOfInterest"),
      propConstraint = MultipleValuesStandingQuery.LocalProperty.Any,
      aliasedAs = None
    )

    val state = new StandingQueryStateWrapper(query)

    withClue("Initializing the state") {
      state.initialize() { effects =>
        assert(effects.isEmpty)
        ()
      }
    }

    withClue("Setting the wrong property doesn't do anything") {
      val wrongProp = PropertySet(Symbol("notKeyOfInterest"), PropertyValue(QuineValue.True))
      state.reportNodeEvents(Seq(wrongProp), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }

    val resId1 = withClue("Setting the right property") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(1L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        val (resId1, result) = effects.resultsReported.dequeue()
        assert(result == QueryContext.empty)
        assert(effects.isEmpty)
        resId1
      }
    }

    withClue("Changing the right property after it is already set doesn't change anything") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(2L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }

    withClue("Removing the right property invalidates the result") {
      val rightProp = PropertyRemoved(query.propKey, PropertyValue(QuineValue.Integer(2L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        val resId1Cancelled = effects.resultsCancelled.dequeue()
        assert(resId1 == resId1Cancelled)
        assert(effects.isEmpty)
      }
    }
  }

  test("null constraint, no alias") {
    val query = MultipleValuesStandingQuery.LocalProperty(
      propKey = Symbol("keyOfInterest"),
      propConstraint = MultipleValuesStandingQuery.LocalProperty.None,
      aliasedAs = None
    )

    val state = new StandingQueryStateWrapper(query)

    val initialResultId = withClue("Initializing the state should yield a result") {
      state.initialize() { effects =>
        assert(effects.resultsReported.nonEmpty)
        val (initialResultId, initialResult) = effects.resultsReported.dequeue()
        // empty because no alias is provided so the result value is discarded
        assert(initialResult === QueryContext.empty)
        assert(effects.isEmpty)
        initialResultId
      }
    }

    withClue("Setting the wrong property doesn't do anything") {
      val wrongProp = PropertySet(Symbol("notKeyOfInterest"), PropertyValue(QuineValue.True))
      state.reportNodeEvents(Seq(wrongProp), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }

    withClue("Setting the right property should cancel the result") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(1L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        assert(effects.resultsCancelled.nonEmpty)
        val cancelledResultId = effects.resultsCancelled.dequeue()
        assert(cancelledResultId === initialResultId)
        assert(effects.isEmpty)
      }
    }

    withClue("Changing the right property after it is already set doesn't change anything") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(2L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }

    withClue("Removing the right property creates a new result") {
      val rightProp = PropertyRemoved(query.propKey, PropertyValue(QuineValue.Integer(2L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        assert(effects.resultsReported.nonEmpty)
        val (_, result1) = effects.resultsReported.dequeue()
        assert(result1 === QueryContext.empty)
        assert(effects.isEmpty)
      }
    }
  }

  test("null constraint and alias") {

    val query = MultipleValuesStandingQuery.LocalProperty(
      propKey = Symbol("keyOfInterest"),
      propConstraint = MultipleValuesStandingQuery.LocalProperty.None,
      aliasedAs = Some(Symbol("nulled"))
    )

    val state = new StandingQueryStateWrapper(query)

    val initialResultId = withClue("Initializing the state should yield a result") {
      state.initialize() { effects =>
        assert(effects.resultsReported.nonEmpty)
        val (initialResultId, initialResult) = effects.resultsReported.dequeue()
        assert(initialResult === QueryContext(Map(query.aliasedAs.get -> Expr.Null)))
        assert(effects.isEmpty)
        initialResultId
      }
    }

    withClue("Setting the wrong property doesn't do anything") {
      val wrongProp = PropertySet(Symbol("notKeyOfInterest"), PropertyValue(QuineValue.True))
      state.reportNodeEvents(Seq(wrongProp), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }

    withClue("Setting the right property should cancel the result") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(1L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        assert(effects.resultsCancelled.nonEmpty)
        val cancelledResultId = effects.resultsCancelled.dequeue()
        assert(cancelledResultId === initialResultId)
        assert(effects.isEmpty)
      }
    }

    withClue("Changing the right property after it is already set doesn't change anything") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(2L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }

    withClue("Removing the right property creates a new result") {
      val rightProp = PropertyRemoved(query.propKey, PropertyValue(QuineValue.Integer(2L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        assert(effects.resultsReported.nonEmpty)
        val (_, result1) = effects.resultsReported.dequeue()
        assert(result1 === QueryContext(Map(query.aliasedAs.get -> Expr.Null)))
        assert(effects.isEmpty)
      }
    }
  }

  test("alias but no value constraint") {

    val query = MultipleValuesStandingQuery.LocalProperty(
      propKey = Symbol("keyOfInterest"),
      propConstraint = MultipleValuesStandingQuery.LocalProperty.Any,
      aliasedAs = Some(Symbol("interesting"))
    )

    val state = new StandingQueryStateWrapper(query)

    state.initialize() { effects =>
      assert(effects.isEmpty)
    }

    withClue("Setting the wrong property doesn't do anything") {
      val wrongProp = PropertySet(Symbol("notKeyOfInterest"), PropertyValue(QuineValue.True))
      state.reportNodeEvents(Seq(wrongProp), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }

    val resId1 = withClue("Setting the right property") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(1L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        val (resId1, result) = effects.resultsReported.dequeue()
        assert(result == QueryContext(Map(query.aliasedAs.get -> Expr.Integer(1L))))
        assert(effects.isEmpty)
        resId1
      }
    }

    val resId2 = withClue("Changing the right property after it is already set") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(2L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        val resId1Cancelled = effects.resultsCancelled.dequeue()
        assert(resId1 == resId1Cancelled)
        val (resId2, result) = effects.resultsReported.dequeue()
        assert(result == QueryContext(Map(query.aliasedAs.get -> Expr.Integer(2L))))
        assert(effects.isEmpty)
        resId2
      }
    }

    withClue("Removing the right property invalidates the result") {
      val rightProp = PropertyRemoved(query.propKey, PropertyValue(QuineValue.Integer(2L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        val resId2Cancelled = effects.resultsCancelled.dequeue()
        assert(resId2 == resId2Cancelled)
        assert(effects.isEmpty)
      }
    }

    withClue("Multiple events emits only 1 result (assuming events are deduplicated prior to onNodeEvents)") {
      val wrongProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(8675309L)))
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(2L)))

      state.reportNodeEvents(Seq(wrongProp, rightProp), shouldHaveEffects = true) { effects =>
        assert(effects.resultsReported.size == 1)
        val (_, result) = effects.resultsReported.dequeue()
        assert(result == QueryContext(Map(query.aliasedAs.get -> Expr.Integer(2L))))
        assert(effects.isEmpty)
      }
    }
  }

  test("value constraint and no alias") {

    val query = MultipleValuesStandingQuery.LocalProperty(
      propKey = Symbol("keyOfInterest"),
      propConstraint = MultipleValuesStandingQuery.LocalProperty.Equal(Expr.Integer(1L)),
      aliasedAs = None
    )

    val state = new StandingQueryStateWrapper(query)

    state.initialize() { effects =>
      assert(effects.isEmpty)
    }

    withClue("Setting the wrong property doesn't do anything") {
      val wrongProp = PropertySet(Symbol("notKeyOfInterest"), PropertyValue(QuineValue.True))
      state.reportNodeEvents(Seq(wrongProp), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }

    withClue("Setting the right property with the wrong value") {
      val rightPropWrongValue = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(2L)))
      state.reportNodeEvents(Seq(rightPropWrongValue), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }

    val resId1 = withClue("Setting the right property with the right value") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(1L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        val (resId1, result) = effects.resultsReported.dequeue()
        assert(result == QueryContext.empty)
        assert(effects.isEmpty)
        resId1
      }
    }

    withClue("Setting the right property back to the wrong value") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(2L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        val resId1Cancelled = effects.resultsCancelled.dequeue()
        assert(resId1 == resId1Cancelled)
        assert(effects.isEmpty)
      }
    }

    val resId2 = withClue("Setting the right property back to the right value") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(1L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        val (resId2, result) = effects.resultsReported.dequeue()
        assert(result == QueryContext.empty)
        assert(effects.isEmpty)
        resId2
      }
    }

    withClue("Removing the right property invalidates the result") {
      val rightProp = PropertyRemoved(query.propKey, PropertyValue(QuineValue.Integer(1L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        val resId2Cancelled = effects.resultsCancelled.dequeue()
        assert(resId2 == resId2Cancelled)
        assert(effects.isEmpty)
      }
    }
  }

  test("value constraint and alias") {

    val query = MultipleValuesStandingQuery.LocalProperty(
      propKey = Symbol("keyOfInterest"),
      propConstraint = MultipleValuesStandingQuery.LocalProperty.Equal(Expr.Integer(1L)),
      aliasedAs = Some(Symbol("interesting"))
    )

    val state = new StandingQueryStateWrapper(query)

    state.initialize() { effects =>
      assert(effects.isEmpty)
    }

    withClue("Setting the wrong property doesn't do anything") {
      val wrongProp = PropertySet(Symbol("notKeyOfInterest"), PropertyValue(QuineValue.True))
      state.reportNodeEvents(Seq(wrongProp), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }

    withClue("Setting the right property with the wrong value") {
      val rightPropWrongValue = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(2L)))
      state.reportNodeEvents(Seq(rightPropWrongValue), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }

    val resId1 = withClue("Setting the right property with the right value") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(1L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        val (resId1, result) = effects.resultsReported.dequeue()
        assert(result == QueryContext(Map(query.aliasedAs.get -> Expr.Integer(1L))))
        assert(effects.isEmpty)
        resId1
      }
    }

    withClue("Setting the right property back to the wrong value") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(2L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        val resId1Cancelled = effects.resultsCancelled.dequeue()
        assert(resId1 == resId1Cancelled)
        assert(effects.isEmpty)
      }
    }

    val resId2 = withClue("Setting the right property back to the right value") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(1L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        val (resId2, result) = effects.resultsReported.dequeue()
        assert(result == QueryContext(Map(query.aliasedAs.get -> Expr.Integer(1L))))
        assert(effects.isEmpty)
        resId2
      }
    }

    withClue("Removing the right property invalidates the result") {
      val rightProp = PropertyRemoved(query.propKey, PropertyValue(QuineValue.Integer(1L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        val resId2Cancelled = effects.resultsCancelled.dequeue()
        assert(resId2 == resId2Cancelled)
        assert(effects.isEmpty)
      }
    }
  }

  test("non-equal constraint and no alias") {
    val query = MultipleValuesStandingQuery.LocalProperty(
      propKey = Symbol("keyOfInterest"),
      propConstraint = MultipleValuesStandingQuery.LocalProperty.NotEqual(Expr.Integer(1L)),
      aliasedAs = None
    )

    val state = new StandingQueryStateWrapper(query)

    state.initialize() { effects =>
      assert(effects.isEmpty)
    }

    withClue("Setting the wrong property doesn't do anything") {
      val wrongProp = PropertySet(Symbol("notKeyOfInterest"), PropertyValue(QuineValue.True))
      state.reportNodeEvents(Seq(wrongProp), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }

    val resId1 = withClue("Setting the right property with a matching value returns a result") {
      val rightPropWrongValue = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(2L)))
      state.reportNodeEvents(Seq(rightPropWrongValue), shouldHaveEffects = true) { effects =>
        assert(effects.resultsReported.nonEmpty)
        val (resId1, result) = effects.resultsReported.dequeue()
        assert(result === QueryContext.empty)
        assert(effects.isEmpty)
        resId1
      }
    }

    withClue("Setting the right property with an equal value cancels") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(1L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        assert(effects.resultsCancelled.nonEmpty)
        val cancelledResultId = effects.resultsCancelled.dequeue()
        assert(cancelledResultId === resId1)
        assert(effects.isEmpty)
      }
    }

    val resId2 =
      withClue("Setting the right property back to a matching value creates a new result") {
        val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(2L)))
        state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
          assert(effects.resultsReported.nonEmpty)
          val (resId2, result) = effects.resultsReported.dequeue()
          assert(result === QueryContext.empty)
          assert(effects.isEmpty)
          resId2
        }
      }

    withClue("Changing the right property to another matching value does nothing") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(5L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }

    withClue("Removing the right property invalidates the result") {
      val rightProp = PropertyRemoved(query.propKey, PropertyValue(QuineValue.Integer(5L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        val resId2Cancelled = effects.resultsCancelled.dequeue()
        assert(resId2 === resId2Cancelled)
        assert(effects.isEmpty)
      }
    }
  }

  test("non-equal constraint and alias") {
    val query = MultipleValuesStandingQuery.LocalProperty(
      propKey = Symbol("keyOfInterest"),
      propConstraint = MultipleValuesStandingQuery.LocalProperty.NotEqual(Expr.Integer(1L)),
      aliasedAs = Some(Symbol("cathy"))
    )

    val state = new StandingQueryStateWrapper(query)

    state.initialize() { effects =>
      assert(effects.isEmpty)
    }

    withClue("Setting the wrong property doesn't do anything") {
      val wrongProp = PropertySet(Symbol("notKeyOfInterest"), PropertyValue(QuineValue.True))
      state.reportNodeEvents(Seq(wrongProp), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }

    val resId1 = withClue("Setting the right property with a matching value returns a result") {
      val rightPropWrongValue = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(2L)))
      state.reportNodeEvents(Seq(rightPropWrongValue), shouldHaveEffects = true) { effects =>
        assert(effects.resultsReported.nonEmpty)
        val (resId1, result) = effects.resultsReported.dequeue()
        assert(result === QueryContext(Map(query.aliasedAs.get -> Expr.Integer(2L))))
        assert(effects.isEmpty)
        resId1
      }
    }

    withClue("Setting the right property with an equal value cancels") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(1L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        assert(effects.resultsCancelled.nonEmpty)
        val cancelledResultId = effects.resultsCancelled.dequeue()
        assert(cancelledResultId === resId1)
        assert(effects.isEmpty)
      }
    }

    val resId2 =
      withClue("Setting the right property back to a matching value creates a new result") {
        val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(2L)))
        state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
          assert(effects.resultsReported.nonEmpty)
          val (resId2, result) = effects.resultsReported.dequeue()
          assert(result === QueryContext(Map(query.aliasedAs.get -> Expr.Integer(2L))))
          assert(effects.isEmpty)
          resId2
        }
      }

    val resId3 = withClue(
      "Changing the right property to another matching value issues a new match and cancels the old one"
    ) {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(5L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        //old result cancelled
        assert(effects.resultsCancelled.nonEmpty)
        val resId2Cancelled = effects.resultsCancelled.dequeue()
        assert(resId2Cancelled === resId2)

        // new result
        assert(effects.resultsReported.nonEmpty)
        val (resId3, result) = effects.resultsReported.dequeue()
        assert(result === QueryContext(Map(query.aliasedAs.get -> Expr.Integer(5L))))

        assert(effects.isEmpty)
        resId3
      }
    }

    withClue("Removing the right property invalidates the result") {
      val rightProp = PropertyRemoved(query.propKey, PropertyValue(QuineValue.Integer(5L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        val resId3Cancelled = effects.resultsCancelled.dequeue()
        assert(resId3 === resId3Cancelled)
        assert(effects.isEmpty)
      }
    }
  }
}
