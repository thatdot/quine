package com.thatdot.quine.graph.standing

import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.graph.PropertyEvent.{PropertyRemoved, PropertySet}
import com.thatdot.quine.graph.cypher.MultipleValuesStandingQuery.Labels
import com.thatdot.quine.graph.cypher.{Expr, MultipleValuesStandingQuery, QueryContext}
import com.thatdot.quine.model.{PropertyValue, QuineValue}

class LabelsStateTests extends AnyFunSuite with OptionValues {

  val labelsPropertyKey = MultipleValuesStandingQueryEffectsTester.labelsProperty

  /** Makes a row with 1 column (alias) whose value is a List containing the strings described by
    * `labels`.
    *
    * @example makeLabelsRow('n, Set('A, 'B, 'C)) ==
    *            QueryContext(Map('n -> Expr.List(Vector(Expr.Str("A"), Expr.Str("B"), Expr.Str("C")))))
    * @example makeLabelsRow('n, Set.empty) ==
    *            QueryContext(Map('n -> Expr.List.empty))
    *
    * @param alias the column name
    * @param labels the list entries
    */
  def makeLabelsRow(alias: Symbol, labels: Set[Symbol]): QueryContext = QueryContext(
    Map(alias -> Expr.List(labels.map(_.name).map(Expr.Str).toVector)),
  )

  val deleteLabelsPropertyEvent: PropertyRemoved = PropertyRemoved(
    labelsPropertyKey,
    PropertyValue(
      QuineValue.Null,
    ), // incorrect (this should be whatever the previous value was) but it doesn't matter to these tests
  )
  def makeSetLabelsEvent(labels: Set[Symbol]): PropertySet =
    PropertySet(labelsPropertyKey, PropertyValue(QuineValue.List(labels.map(_.name).map(QuineValue.Str).toVector)))

  test("unconditional constraint, no alias") {
    val query = MultipleValuesStandingQuery.Labels(
      aliasedAs = None,
      constraint = Labels.Unconditional,
    )

    withClue("Initializing a copy of the state with labels prepares a 1-result group") {
      val tempState = new StandingQueryStateWrapper(query)
      tempState.initialize(
        Map(
          labelsPropertyKey -> PropertyValue(
            QuineValue.List(Vector(QuineValue.Str("A"), QuineValue.Str("B"), QuineValue.Str("C"))),
          ),
        ),
      ) { (effects, initialResultOpt) =>
        val initialResultFromThreeLabels = initialResultOpt.value
        assert(initialResultFromThreeLabels == Seq(QueryContext.empty))
        assert(effects.isEmpty)
      }
    }

    withClue("Initializing a copy of the state with empty labels prepares a 1-result group") {
      val tempState = new StandingQueryStateWrapper(query)
      tempState.initialize(
        Map(
          labelsPropertyKey -> PropertyValue(
            QuineValue.List(Vector.empty),
          ),
        ),
      ) { (effects, initialResultOpt) =>
        val initialResultFromEmptyLabels = initialResultOpt.value
        assert(initialResultFromEmptyLabels == Seq(QueryContext.empty))
        assert(effects.isEmpty)
      }
    }

    val state = new StandingQueryStateWrapper(query)

    withClue("Initializing the state prepares a 1-result group") {
      state.initialize() { (effects, initialResultOpt) =>
        val initialResultFromNull = initialResultOpt.value
        assert(initialResultFromNull == Seq(QueryContext.empty))
        assert(effects.isEmpty)
      }
    }

    val arbitraryPropKey = withClue("Setting a property does nothing") {
      val propKey = Symbol("notKeyOfInterest")
      val wrongProp = PropertySet(propKey, PropertyValue(QuineValue.True))
      state.reportNodeEvents(Seq(wrongProp), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
      propKey
    }

    withClue("Setting a label does nothing") {
      val setLabels = makeSetLabelsEvent(Set(Symbol("A")))
      state.reportNodeEvents(Seq(setLabels), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }

    withClue("Setting a second label does nothing") {
      val setLabels = makeSetLabelsEvent(
        Set(
          Symbol("A"),
          Symbol("B"),
        ),
      )
      state.reportNodeEvents(Seq(setLabels), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }

    withClue("Setting the labels to empty does nothing") {
      val setLabels = makeSetLabelsEvent(Set.empty)
      state.reportNodeEvents(Seq(setLabels), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }

    withClue("Setting multiple labels at once does nothing") {
      val setLabels = makeSetLabelsEvent(
        Set(
          Symbol("A"),
          Symbol("B"),
          Symbol("C"),
        ),
      )
      state.reportNodeEvents(Seq(setLabels), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }

    withClue("Removing the label property does nothing") {
      state.reportNodeEvents(Seq(deleteLabelsPropertyEvent), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }

    withClue("Removing a property does nothing") {
      val removeProp = PropertyRemoved(arbitraryPropKey, PropertyValue(QuineValue.True))
      state.reportNodeEvents(Seq(removeProp), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }
  }

  test("unconditional constraint, aliased") {
    val alias = Symbol("theLabels")
    val query = MultipleValuesStandingQuery.Labels(
      aliasedAs = Some(alias),
      constraint = Labels.Unconditional,
    )

    withClue("Initializing a copy of the state with labels prepares a 1-result group") {
      val tempState = new StandingQueryStateWrapper(query)
      tempState.initialize(
        Map(
          labelsPropertyKey -> PropertyValue(
            QuineValue.List(Vector(QuineValue.Str("A"), QuineValue.Str("B"), QuineValue.Str("C"))),
          ),
        ),
      ) { (effects, initialResultOpt) =>
        val initialResultFromThreeLabels = initialResultOpt.value
        assert(initialResultFromThreeLabels == Seq(makeLabelsRow(alias, Set(Symbol("A"), Symbol("B"), Symbol("C")))))
        assert(effects.isEmpty)
      }
    }

    withClue("Initializing a copy of the state with empty labels prepares a 1-result group") {
      val tempState = new StandingQueryStateWrapper(query)
      tempState.initialize(
        Map(
          labelsPropertyKey -> PropertyValue(
            QuineValue.List(Vector.empty),
          ),
        ),
      ) { (effects, initialResultOpt) =>
        val initialResultFromEmptyLabels = initialResultOpt.value
        assert(initialResultFromEmptyLabels == Seq(makeLabelsRow(alias, Set.empty)))
        assert(effects.isEmpty)
      }
    }

    val state = new StandingQueryStateWrapper(query)

    withClue("Initializing the state prepares a 1-result group") {
      state.initialize() { (effects, initialResultOpt) =>
        val initialResultFromNull = initialResultOpt.value
        assert(initialResultFromNull == Seq(makeLabelsRow(alias, Set.empty)))
        assert(effects.isEmpty)
      }
    }

    val arbitraryPropKey = withClue("Setting a property does nothing") {
      val propKey = Symbol("notKeyOfInterest")
      val wrongProp = PropertySet(propKey, PropertyValue(QuineValue.True))
      state.reportNodeEvents(Seq(wrongProp), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
      propKey
    }

    withClue("Setting a label emits a 1-result group") {
      val setLabels = makeSetLabelsEvent(Set(Symbol("A")))
      state.reportNodeEvents(Seq(setLabels), shouldHaveEffects = true) { effects =>
        val resultGroup = effects.resultsReported.dequeue()
        assert(resultGroup == Seq(makeLabelsRow(alias, Set(Symbol("A")))))
        assert(effects.isEmpty)
      }
    }

    withClue("Setting a second label emits a 1-result group") {
      val setLabels = makeSetLabelsEvent(
        Set(
          Symbol("A"),
          Symbol("B"),
        ),
      )
      state.reportNodeEvents(Seq(setLabels), shouldHaveEffects = true) { effects =>
        val resultGroup = effects.resultsReported.dequeue()
        assert(resultGroup == Seq(makeLabelsRow(alias, Set(Symbol("A"), Symbol("B")))))
        assert(effects.isEmpty)
      }
    }

    withClue("Setting the labels to empty emits a 1-result group") {
      val setLabels = makeSetLabelsEvent(Set.empty)
      state.reportNodeEvents(Seq(setLabels), shouldHaveEffects = true) { effects =>
        val resultGroup = effects.resultsReported.dequeue()
        assert(resultGroup == Seq(makeLabelsRow(alias, Set.empty)))
        assert(effects.isEmpty)
      }
    }

    withClue("Setting multiple labels at once emits a 1-result group") {
      val setLabels = makeSetLabelsEvent(
        Set(
          Symbol("A"),
          Symbol("B"),
          Symbol("C"),
        ),
      )
      state.reportNodeEvents(Seq(setLabels), shouldHaveEffects = true) { effects =>
        val resultGroup = effects.resultsReported.dequeue()
        assert(resultGroup == Seq(makeLabelsRow(alias, Set(Symbol("A"), Symbol("B"), Symbol("C")))))
        assert(effects.isEmpty)
      }
    }

    withClue("Removing the label property emits a 1-result group") {
      state.reportNodeEvents(Seq(deleteLabelsPropertyEvent), shouldHaveEffects = true) { effects =>
        val resultGroup = effects.resultsReported.dequeue()
        assert(resultGroup == Seq(makeLabelsRow(alias, Set.empty)))
        assert(effects.isEmpty)
      }
    }

    withClue("Removing a property does nothing") {
      val removeProp = PropertyRemoved(arbitraryPropKey, PropertyValue(QuineValue.True))
      state.reportNodeEvents(Seq(removeProp), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }
  }

  test("contains labels constraint, no alias") {
    val query = MultipleValuesStandingQuery.Labels(
      aliasedAs = None,
      constraint = Labels.Contains(Set(Symbol("A"))),
    )

    withClue("Initializing a copy of the state with labels prepares a 1-result group") {
      val tempState = new StandingQueryStateWrapper(query)
      tempState.initialize(
        Map(
          labelsPropertyKey -> PropertyValue(
            QuineValue.List(Vector(QuineValue.Str("A"), QuineValue.Str("B"), QuineValue.Str("C"))),
          ),
        ),
      ) { (effects, initialResultOpt) =>
        val initialResultFromThreeLabels = initialResultOpt.value
        assert(initialResultFromThreeLabels == Seq(QueryContext.empty))
        assert(effects.isEmpty)
      }
    }

    withClue("Initializing a copy of the state with empty labels prepares a 0-result group") {
      val tempState = new StandingQueryStateWrapper(query)
      tempState.initialize(
        Map(
          labelsPropertyKey -> PropertyValue(
            QuineValue.List(Vector.empty),
          ),
        ),
      ) { (effects, initialResultOpt) =>
        val initialResultFromEmptyLabels = initialResultOpt.value
        assert(initialResultFromEmptyLabels == Seq.empty)
        assert(effects.isEmpty)
      }
    }

    val state = new StandingQueryStateWrapper(query)

    withClue("Initializing the state prepares a 0-result group") {
      state.initialize() { (effects, initialResultOpt) =>
        val initialResultFromNull = initialResultOpt.value
        assert(initialResultFromNull == Seq.empty)
        assert(effects.isEmpty)
      }
    }

    val arbitraryPropKey = withClue("Setting a property does nothing") {
      val propKey = Symbol("notKeyOfInterest")
      val wrongProp = PropertySet(propKey, PropertyValue(QuineValue.True))
      state.reportNodeEvents(Seq(wrongProp), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
      propKey
    }

    withClue("Setting a matching label emits a 1-result group") {
      val setLabels = makeSetLabelsEvent(Set(Symbol("A")))
      state.reportNodeEvents(Seq(setLabels), shouldHaveEffects = true) { effects =>
        val resultGroup = effects.resultsReported.dequeue()
        assert(resultGroup == Seq(QueryContext.empty))
        assert(effects.isEmpty)
      }
    }

    withClue("Setting a second label does nothing") {
      val setLabels = makeSetLabelsEvent(
        Set(
          Symbol("A"),
          Symbol("B"),
        ),
      )
      state.reportNodeEvents(Seq(setLabels), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }

    withClue("Setting the labels to empty emits a 0-result group") {
      val setLabels = makeSetLabelsEvent(Set.empty)
      state.reportNodeEvents(Seq(setLabels), shouldHaveEffects = true) { effects =>
        val resultGroup = effects.resultsReported.dequeue()
        assert(resultGroup == Seq.empty)
        assert(effects.isEmpty)
      }
    }
    withClue("Setting multiple labels at once emits a 1-result group") {
      val setLabels = makeSetLabelsEvent(
        Set(
          Symbol("A"),
          Symbol("B"),
          Symbol("C"),
        ),
      )
      state.reportNodeEvents(Seq(setLabels), shouldHaveEffects = true) { effects =>
        val resultGroup = effects.resultsReported.dequeue()
        assert(resultGroup == Seq(QueryContext.empty))
        assert(effects.isEmpty)
      }
    }

    withClue("Removing the label property emits a 0-result group") {
      state.reportNodeEvents(Seq(deleteLabelsPropertyEvent), shouldHaveEffects = true) { effects =>
        val resultGroup = effects.resultsReported.dequeue()
        assert(resultGroup == Seq.empty)
        assert(effects.isEmpty)
      }
    }

    withClue("Removing a property does nothing") {
      val removeProp = PropertyRemoved(arbitraryPropKey, PropertyValue(QuineValue.True))
      state.reportNodeEvents(Seq(removeProp), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }

  }

  test("contains labels constraint, aliased") {
    val alias = Symbol("theLabels")
    val query = MultipleValuesStandingQuery.Labels(
      aliasedAs = Some(alias),
      constraint = Labels.Contains(Set(Symbol("A"))),
    )

    withClue("Initializing a copy of the state with labels prepares a 1-result group") {
      val tempState = new StandingQueryStateWrapper(query)
      tempState.initialize(
        Map(
          labelsPropertyKey -> PropertyValue(
            QuineValue.List(Vector(QuineValue.Str("A"), QuineValue.Str("B"), QuineValue.Str("C"))),
          ),
        ),
      ) { (effects, initialResultOpt) =>
        val initialResultFromThreeLabels = initialResultOpt.value
        assert(initialResultFromThreeLabels == Seq(makeLabelsRow(alias, Set(Symbol("A"), Symbol("B"), Symbol("C")))))
        assert(effects.isEmpty)
      }
    }

    withClue("Initializing a copy of the state with empty labels prepares a 0-result group") {
      val tempState = new StandingQueryStateWrapper(query)
      tempState.initialize(
        Map(
          labelsPropertyKey -> PropertyValue(
            QuineValue.List(Vector.empty),
          ),
        ),
      ) { (effects, initialResultOpt) =>
        val initialResultFromEmptyLabels = initialResultOpt.value
        assert(initialResultFromEmptyLabels == Seq.empty)
        assert(effects.isEmpty)
      }
    }

    val state = new StandingQueryStateWrapper(query)

    withClue("Initializing the state prepares a 0-result group") {
      state.initialize() { (effects, initialResultOpt) =>
        val initialResultFromNull = initialResultOpt.value
        assert(initialResultFromNull == Seq.empty)
        assert(effects.isEmpty)
      }
    }

    val arbitraryPropKey = withClue("Setting a property does nothing") {
      val propKey = Symbol("notKeyOfInterest")
      val wrongProp = PropertySet(propKey, PropertyValue(QuineValue.True))
      state.reportNodeEvents(Seq(wrongProp), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
      propKey
    }

    withClue("Setting a label emits a 1-result group") {
      val setLabels = makeSetLabelsEvent(Set(Symbol("A")))
      state.reportNodeEvents(Seq(setLabels), shouldHaveEffects = true) { effects =>
        val resultGroup = effects.resultsReported.dequeue()
        assert(resultGroup == Seq(makeLabelsRow(alias, Set(Symbol("A")))))
        assert(effects.isEmpty)
      }
    }

    withClue("Setting a second label emits a 1-result group") {
      val setLabels = makeSetLabelsEvent(
        Set(
          Symbol("A"),
          Symbol("B"),
        ),
      )
      state.reportNodeEvents(Seq(setLabels), shouldHaveEffects = true) { effects =>
        val resultGroup = effects.resultsReported.dequeue()
        assert(resultGroup == Seq(makeLabelsRow(alias, Set(Symbol("A"), Symbol("B")))))
        assert(effects.isEmpty)
      }
    }

    withClue("Setting the labels to empty emits a 0-result group") {
      val setLabels = makeSetLabelsEvent(Set.empty)
      state.reportNodeEvents(Seq(setLabels), shouldHaveEffects = true) { effects =>
        val resultGroup = effects.resultsReported.dequeue()
        assert(resultGroup == Seq.empty)
        assert(effects.isEmpty)
      }
    }
    withClue("Setting multiple labels at once emits a 1-result group") {
      val setLabels = makeSetLabelsEvent(
        Set(
          Symbol("A"),
          Symbol("B"),
          Symbol("C"),
        ),
      )
      state.reportNodeEvents(Seq(setLabels), shouldHaveEffects = true) { effects =>
        val resultGroup = effects.resultsReported.dequeue()
        assert(resultGroup == Seq(makeLabelsRow(alias, Set(Symbol("A"), Symbol("B"), Symbol("C")))))
        assert(effects.isEmpty)
      }
    }
    withClue("Setting non-matching labels emits a 0-result group") {
      val setLabels = makeSetLabelsEvent(
        Set(
          Symbol("B"),
          Symbol("C"),
        ),
      )
      state.reportNodeEvents(Seq(setLabels), shouldHaveEffects = true) { effects =>
        val resultGroup = effects.resultsReported.dequeue()
        assert(resultGroup == Seq.empty)
        assert(effects.isEmpty)
      }
    }

    withClue("Removing the label property emits does nothing") {
      state.reportNodeEvents(Seq(deleteLabelsPropertyEvent), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }

    withClue("Removing a property does nothing") {
      val removeProp = PropertyRemoved(arbitraryPropKey, PropertyValue(QuineValue.True))
      state.reportNodeEvents(Seq(removeProp), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }
  }

  test("contains (multiple) labels constraint, aliased") {
    val alias = Symbol("theLabels")
    val query = MultipleValuesStandingQuery.Labels(
      aliasedAs = Some(alias),
      constraint = Labels.Contains(Set(Symbol("A"), Symbol("B"))),
    )

    withClue("Initializing a copy of the state with labels prepares a 1-result group") {
      val tempState = new StandingQueryStateWrapper(query)
      tempState.initialize(
        Map(
          labelsPropertyKey -> PropertyValue(
            QuineValue.List(Vector(QuineValue.Str("A"), QuineValue.Str("B"), QuineValue.Str("C"))),
          ),
        ),
      ) { (effects, initialResultOpt) =>
        val initialResultFromThreeLabels = initialResultOpt.value
        assert(initialResultFromThreeLabels == Seq(makeLabelsRow(alias, Set(Symbol("A"), Symbol("B"), Symbol("C")))))
        assert(effects.isEmpty)
      }
    }

    withClue("Initializing a copy of the state with empty labels prepares a 0-result group") {
      val tempState = new StandingQueryStateWrapper(query)
      tempState.initialize(
        Map(
          labelsPropertyKey -> PropertyValue(
            QuineValue.List(Vector.empty),
          ),
        ),
      ) { (effects, initialResultOpt) =>
        val initialResultFromEmptyLabels = initialResultOpt.value
        assert(initialResultFromEmptyLabels == Seq.empty)
        assert(effects.isEmpty)
      }
    }

    val state = new StandingQueryStateWrapper(query)

    withClue("Initializing the state prepares a 0-result group") {
      state.initialize() { (effects, initialResultOpt) =>
        val initialResultFromNull = initialResultOpt.value
        assert(initialResultFromNull == Seq.empty)
        assert(effects.isEmpty)
      }
    }

    val arbitraryPropKey = withClue("Setting a property does nothing") {
      val propKey = Symbol("notKeyOfInterest")
      val wrongProp = PropertySet(propKey, PropertyValue(QuineValue.True))
      state.reportNodeEvents(Seq(wrongProp), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
      propKey
    }

    withClue("Setting only some matching labels does nothing") {
      val setLabels = makeSetLabelsEvent(Set(Symbol("A")))
      state.reportNodeEvents(Seq(setLabels), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }

    withClue("Setting all required labels emits a 1-result group") {
      val setLabels = makeSetLabelsEvent(
        Set(
          Symbol("A"),
          Symbol("B"),
        ),
      )
      state.reportNodeEvents(Seq(setLabels), shouldHaveEffects = true) { effects =>
        val resultGroup = effects.resultsReported.dequeue()
        assert(resultGroup == Seq(makeLabelsRow(alias, Set(Symbol("A"), Symbol("B")))))
        assert(effects.isEmpty)
      }
    }

    withClue("Setting the labels to empty emits a 0-result group") {
      val setLabels = makeSetLabelsEvent(Set.empty)
      state.reportNodeEvents(Seq(setLabels), shouldHaveEffects = true) { effects =>
        val resultGroup = effects.resultsReported.dequeue()
        assert(resultGroup == Seq.empty)
        assert(effects.isEmpty)
      }
    }
    withClue("Setting multiple labels at once emits a 1-result group") {
      val setLabels = makeSetLabelsEvent(
        Set(
          Symbol("A"),
          Symbol("B"),
          Symbol("C"),
        ),
      )
      state.reportNodeEvents(Seq(setLabels), shouldHaveEffects = true) { effects =>
        val resultGroup = effects.resultsReported.dequeue()
        assert(resultGroup == Seq(makeLabelsRow(alias, Set(Symbol("A"), Symbol("B"), Symbol("C")))))
        assert(effects.isEmpty)
      }
    }
    withClue("Setting non-matching labels emits a 0-result group") {
      val setLabels = makeSetLabelsEvent(
        Set(
          Symbol("B"),
          Symbol("C"),
        ),
      )
      state.reportNodeEvents(Seq(setLabels), shouldHaveEffects = true) { effects =>
        val resultGroup = effects.resultsReported.dequeue()
        assert(resultGroup == Seq.empty)
        assert(effects.isEmpty)
      }
    }

    withClue("Removing the label property emits does nothing") {
      state.reportNodeEvents(Seq(deleteLabelsPropertyEvent), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }

    withClue("Removing a property does nothing") {
      val removeProp = PropertyRemoved(arbitraryPropKey, PropertyValue(QuineValue.True))
      state.reportNodeEvents(Seq(removeProp), shouldHaveEffects = false) { effects =>
        assert(effects.isEmpty)
      }
    }
  }
}
