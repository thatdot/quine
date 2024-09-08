package com.thatdot.quine.graph.standing

import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.graph.PropertyEvent.{PropertyRemoved, PropertySet}
import com.thatdot.quine.graph.cypher.{Expr, MultipleValuesStandingQuery, QueryContext}
import com.thatdot.quine.model.{PropertyValue, QuineValue}

class LocalPropertyStateTests extends AnyFunSuite {

  test("any value constraint, no alias") {

    val query = MultipleValuesStandingQuery.LocalProperty(
      propKey = Symbol("keyOfInterest"),
      propConstraint = MultipleValuesStandingQuery.LocalProperty.Any,
      aliasedAs = None,
    )

    val state = new StandingQueryStateWrapper(query)

    withClue("Initializing the state") {
      state.initialize() { effects =>
        assert(effects.isEmpty)
      }
    }

    withClue("Setting the wrong property doesn't do anything") {
      val wrongProp = PropertySet(Symbol("notKeyOfInterest"), PropertyValue(QuineValue.True))
      state.reportNodeEvents(Seq(wrongProp), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }

    withClue("Setting the right property issues a 1-result group") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(1L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        val results = effects.resultsReported.dequeue()
        assert(results == Seq(QueryContext.empty))
        assert(effects.isEmpty)
      }
    }

    withClue("Changing the right property after it is already set doesn't change anything") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(2L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }

    withClue("Removing the right property reports an empty result group") {
      val rightProp = PropertyRemoved(query.propKey, PropertyValue(QuineValue.Integer(2L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        val results = effects.resultsReported.dequeue()
        assert(results == Seq.empty)
        assert(effects.isEmpty)
      }
    }
  }

  test("null constraint, no alias") {
    val query = MultipleValuesStandingQuery.LocalProperty(
      propKey = Symbol("keyOfInterest"),
      propConstraint = MultipleValuesStandingQuery.LocalProperty.None,
      aliasedAs = None,
    )

    val state = new StandingQueryStateWrapper(query)

    withClue("Initializing the state emits a 1-result group") {
      state.initialize() { effects =>
        val initialResultFromNull = effects.resultsReported.dequeue()
        assert(initialResultFromNull == Seq(QueryContext.empty))
        assert(effects.isEmpty)
      }
    }

    withClue("Setting the wrong property doesn't do anything") {
      val wrongProp = PropertySet(Symbol("notKeyOfInterest"), PropertyValue(QuineValue.True))
      state.reportNodeEvents(Seq(wrongProp), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }

    withClue("Setting the right property reports an empty result group") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(1L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        val results = effects.resultsReported.dequeue()
        assert(results == Seq.empty)
        assert(effects.isEmpty)
      }
    }

    withClue("Changing the right property after it is already set doesn't change anything") {
      // this is an optimization to reduce extra intermediate events
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(2L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }

    withClue("Removing the right property emits a 1-result group") {
      val rightProp = PropertyRemoved(query.propKey, PropertyValue(QuineValue.Integer(2L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        assert(effects.resultsReported.nonEmpty)
        val results = effects.resultsReported.dequeue()
        assert(results === Seq(QueryContext.empty))
        assert(effects.isEmpty)
      }
    }
  }

  test("null constraint and alias") {

    val query = MultipleValuesStandingQuery.LocalProperty(
      propKey = Symbol("keyOfInterest"),
      propConstraint = MultipleValuesStandingQuery.LocalProperty.None,
      aliasedAs = Some(Symbol("nulled")),
    )

    val state = new StandingQueryStateWrapper(query)

    withClue("Initializing the state should yield a 1-result group") {
      state.initialize() { effects =>
        val initialResultFromNull = effects.resultsReported.dequeue()
        assert(initialResultFromNull == Seq(QueryContext(Map(query.aliasedAs.get -> Expr.Null))))
        assert(effects.isEmpty)
      }
    }

    withClue("Setting the wrong property doesn't do anything") {
      val wrongProp = PropertySet(Symbol("notKeyOfInterest"), PropertyValue(QuineValue.True))
      state.reportNodeEvents(Seq(wrongProp), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }

    withClue("Setting the right property emits a 0-result group") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(1L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        val results = effects.resultsReported.dequeue()
        assert(results == Seq.empty)
        assert(effects.isEmpty)
      }
    }

    withClue("Changing the right property after it is already set doesn't change anything") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(2L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }

    withClue("Removing the right property emits a 1-result group") {
      val rightProp = PropertyRemoved(query.propKey, PropertyValue(QuineValue.Integer(2L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        assert(effects.resultsReported.nonEmpty)
        val results = effects.resultsReported.dequeue()
        assert(results === Seq(QueryContext(Map(query.aliasedAs.get -> Expr.Null))))
        assert(effects.isEmpty)
      }
    }
  }

  test("alias but no value constraint") {

    val query = MultipleValuesStandingQuery.LocalProperty(
      propKey = Symbol("keyOfInterest"),
      propConstraint = MultipleValuesStandingQuery.LocalProperty.Any,
      aliasedAs = Some(Symbol("interesting")),
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

    withClue("Setting the right property issues a 1-result group") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(1L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        val results = effects.resultsReported.dequeue()
        assert(results == Seq(QueryContext(Map(query.aliasedAs.get -> Expr.Integer(1L)))))
        assert(effects.isEmpty)
      }
    }

    withClue("Changing the right property after it is already set issues a 1-result group") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(2L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        val results = effects.resultsReported.dequeue()
        assert(results == Seq(QueryContext(Map(query.aliasedAs.get -> Expr.Integer(2L)))))
        assert(effects.isEmpty)
      }
    }

    withClue("Removing the right property issues an empty result group") {
      val rightProp = PropertyRemoved(query.propKey, PropertyValue(QuineValue.Integer(2L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        val results = effects.resultsReported.dequeue()
        assert(results == Seq.empty)
        assert(effects.isEmpty)
      }
    }

    withClue("Multiple events emits only 1 result group (assuming events are deduplicated prior to onNodeEvents)") {
      val wrongProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(8675309L)))
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(2L)))

      state.reportNodeEvents(Seq(wrongProp, rightProp), shouldHaveEffects = true) { effects =>
        assert(effects.resultsReported.size == 1)
        val results = effects.resultsReported.dequeue()
        assert(results == Seq(QueryContext(Map(query.aliasedAs.get -> Expr.Integer(2L)))))
        assert(effects.isEmpty)
      }
    }
  }

  test("value constraint and no alias") {

    val query = MultipleValuesStandingQuery.LocalProperty(
      propKey = Symbol("keyOfInterest"),
      propConstraint = MultipleValuesStandingQuery.LocalProperty.Equal(Expr.Integer(1L)),
      aliasedAs = None,
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

    withClue("Setting the right property with the wrong value emits a 0-result group") {
      val rightPropWrongValue = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(2L)))
      state.reportNodeEvents(Seq(rightPropWrongValue), shouldHaveEffects = true) { effects =>
        val results = effects.resultsReported.dequeue()
        assert(results == Seq.empty)
        assert(effects.isEmpty)
      }
    }

    withClue("Setting the right property with the right value should emit a 1-result group") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(1L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        val results = effects.resultsReported.dequeue()
        assert(results == Seq(QueryContext.empty))
        assert(effects.isEmpty)
      }
    }

    withClue("Setting the right property back to the wrong value should emit a 0-result group") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(2L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        val results = effects.resultsReported.dequeue()
        assert(results == Seq.empty)
        assert(effects.isEmpty)
      }
    }

    withClue("Setting the right property back to the right value emits a 1-result group") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(1L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        val results = effects.resultsReported.dequeue()
        assert(results == Seq(QueryContext.empty))
        assert(effects.isEmpty)
      }
    }

    withClue("Removing the right property emits a 0-result group") {
      val rightProp = PropertyRemoved(query.propKey, PropertyValue(QuineValue.Integer(1L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        val results = effects.resultsReported.dequeue()
        assert(results == Seq.empty)
        assert(effects.isEmpty)
      }
    }
  }

  test("value constraint and alias") {

    val query = MultipleValuesStandingQuery.LocalProperty(
      propKey = Symbol("keyOfInterest"),
      propConstraint = MultipleValuesStandingQuery.LocalProperty.Equal(Expr.Integer(1L)),
      aliasedAs = Some(Symbol("interesting")),
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

    withClue("Setting the right property with the wrong value emits a 0-result group") {
      val rightPropWrongValue = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(2L)))
      state.reportNodeEvents(Seq(rightPropWrongValue), shouldHaveEffects = true) { effects =>
        val results = effects.resultsReported.dequeue()
        assert(results == Seq.empty)
        assert(effects.isEmpty)
      }
    }

    withClue("Setting the right property with the right value") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(1L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        val results = effects.resultsReported.dequeue()
        assert(results == Seq(QueryContext(Map(query.aliasedAs.get -> Expr.Integer(1L)))))
        assert(effects.isEmpty)
      }
    }

    withClue("Setting the right property back to the wrong value emits a 0-result group") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(2L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        val results = effects.resultsReported.dequeue()
        assert(results == Seq.empty)
        assert(effects.isEmpty)
      }
    }

    withClue("Setting the right property back to the right value") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(1L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        val results = effects.resultsReported.dequeue()
        assert(results == Seq(QueryContext(Map(query.aliasedAs.get -> Expr.Integer(1L)))))
        assert(effects.isEmpty)
      }
    }

    withClue("Removing the right property emits a 0-result group") {
      val rightProp = PropertyRemoved(query.propKey, PropertyValue(QuineValue.Integer(1L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        val results = effects.resultsReported.dequeue()
        assert(results == Seq.empty)
        assert(effects.isEmpty)
      }
    }
  }

  test("non-equal constraint and no alias") {
    val query = MultipleValuesStandingQuery.LocalProperty(
      propKey = Symbol("keyOfInterest"),
      propConstraint = MultipleValuesStandingQuery.LocalProperty.NotEqual(Expr.Integer(1L)),
      aliasedAs = None,
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

    withClue("Setting the right property with a matching value emits a 1-result group") {
      val rightPropWrongValue = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(2L)))
      state.reportNodeEvents(Seq(rightPropWrongValue), shouldHaveEffects = true) { effects =>
        assert(effects.resultsReported.nonEmpty)
        val results = effects.resultsReported.dequeue()
        assert(results === Seq(QueryContext.empty))
        assert(effects.isEmpty)
      }
    }

    withClue("Setting the right property with an equal value emits a 0-result group") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(1L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        val results = effects.resultsReported.dequeue()
        assert(results == Seq.empty)
        assert(effects.isEmpty)
      }
    }

    withClue("Setting the right property back to a matching value emits a 1-result group") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(2L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        assert(effects.resultsReported.nonEmpty)
        val results = effects.resultsReported.dequeue()
        assert(results === Seq(QueryContext.empty))
        assert(effects.isEmpty)
      }
    }

    withClue("Changing the right property to another matching value doesn't do anything") {
      // this is an optimization to reduce extra intermediate events
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(5L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }

    withClue("Removing the right property emits a 0-result group") {
      val rightProp = PropertyRemoved(query.propKey, PropertyValue(QuineValue.Integer(5L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        val results = effects.resultsReported.dequeue()
        assert(results == Seq.empty)
        assert(effects.isEmpty)
      }
    }
  }

  test("non-equal constraint and alias") {
    val query = MultipleValuesStandingQuery.LocalProperty(
      propKey = Symbol("keyOfInterest"),
      propConstraint = MultipleValuesStandingQuery.LocalProperty.NotEqual(Expr.Integer(1L)),
      aliasedAs = Some(Symbol("cathy")),
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

    withClue("Setting the right property with a matching value emits a 1-result group") {
      val rightPropWrongValue = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(2L)))
      state.reportNodeEvents(Seq(rightPropWrongValue), shouldHaveEffects = true) { effects =>
        assert(effects.resultsReported.nonEmpty)
        val results = effects.resultsReported.dequeue()
        assert(results === Seq(QueryContext(Map(query.aliasedAs.get -> Expr.Integer(2L)))))
        assert(effects.isEmpty)
      }
    }

    withClue("Setting the right property with an equal value emits a 0-result group") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(1L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        val results = effects.resultsReported.dequeue()
        assert(results == Seq.empty)
        assert(effects.isEmpty)
      }
    }

    withClue("Setting the right property back to a matching value emits a 1-result group") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(2L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        assert(effects.resultsReported.nonEmpty)
        val results = effects.resultsReported.dequeue()
        assert(results === Seq(QueryContext(Map(query.aliasedAs.get -> Expr.Integer(2L)))))
        assert(effects.isEmpty)
      }
    }

    withClue("Changing the right property to another matching value emits a 1-result group") {
      val rightProp = PropertySet(query.propKey, PropertyValue(QuineValue.Integer(5L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        assert(effects.resultsReported.nonEmpty)
        val results = effects.resultsReported.dequeue()
        assert(results === Seq(QueryContext(Map(query.aliasedAs.get -> Expr.Integer(5L)))))
        assert(effects.isEmpty)
      }
    }

    withClue("Removing the right property emits a 0-result group") {
      val rightProp = PropertyRemoved(query.propKey, PropertyValue(QuineValue.Integer(5L)))
      state.reportNodeEvents(Seq(rightProp), shouldHaveEffects = true) { effects =>
        assert(effects.resultsReported.nonEmpty)
        val results = effects.resultsReported.dequeue()
        assert(results === Seq.empty)
        assert(effects.isEmpty)
      }
    }
  }
}
