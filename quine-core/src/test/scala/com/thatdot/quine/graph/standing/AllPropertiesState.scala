package com.thatdot.quine.graph.standing

import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.graph.PropertyEvent.{PropertyRemoved, PropertySet}
import com.thatdot.quine.graph.cypher.{Expr, MultipleValuesStandingQuery, QueryContext}
import com.thatdot.quine.model.{PropertyValue, QuineValue}

class AllPropertiesStateTest extends AnyFunSuite with OptionValues {
  val query: MultipleValuesStandingQuery.AllProperties = MultipleValuesStandingQuery.AllProperties(
    aliasedAs = Symbol("props"),
  )

  test("all properties state with bootstrapped properties") {
    val state = new StandingQueryStateWrapper(query)

    withClue("Initializing the state prepares an initial 1-result group") {
      val initialProperties = Map(Symbol("one") -> QuineValue(1L), Symbol("two") -> QuineValue(2L))

      state.initialize(initialProperties.map { case (k, v) => k -> PropertyValue(v) }) { (effects, initialResultsOpt) =>
        val results = initialResultsOpt.value
        assert(
          results == Seq(
            QueryContext(
              Map(
                query.aliasedAs ->
                Expr.Map(initialProperties.map { case (k, v) => k.name -> Expr.fromQuineValue(v) }),
              ),
            ),
          ),
        )
        assert(effects.isEmpty)
      }
    }
  }

  test("all properties state") {

    val state = new StandingQueryStateWrapper(query)
    val prop1 = Symbol("one") -> QuineValue(1L)
    val prop2 = Symbol("two") -> QuineValue(2L)
    val prop3 = Symbol("three") -> QuineValue(3L)
    val prop4 = Symbol("four") -> QuineValue(4L)
    val prop5 = Symbol("five") -> QuineValue(5L)
    val prop6 = Symbol("six") -> QuineValue(6L)
    val prop1ButFunky = Symbol("one") -> QuineValue(-1L)
    val prop2ButFunky = Symbol("two") -> QuineValue(-2L)

    def propsAsCypher(props: (Symbol, QuineValue)*): Expr.Map = Expr.Map(props.map { case (k, v) =>
      k.name -> Expr.fromQuineValue(v)
    })
    def makeSetEvent(prop: (Symbol, QuineValue)): PropertySet = PropertySet(prop._1, PropertyValue(prop._2))
    def makeDeleteEvent(prop: (Symbol, QuineValue)): PropertyRemoved = PropertyRemoved(prop._1, PropertyValue(prop._2))

    withClue("Initializing the state prepares a 1-result group with an empty properties map") {
      state.initialize() { (effects, initialResultOpt) =>
        val initialResult = initialResultOpt.value
        assert(initialResult == Seq(QueryContext(Map(query.aliasedAs -> Expr.Map.empty))))
        assert(effects.isEmpty)
      }
    }
    withClue("Adding a single property reports a new 1-result group") {
      val events = Seq(prop1).map(makeSetEvent)
      state.reportNodeEvents(events, shouldHaveEffects = true) { effects =>
        val results = effects.resultsReported.dequeue()
        val expected = propsAsCypher(prop1)
        assert(results == Seq(QueryContext(Map(query.aliasedAs -> expected))))
        assert(effects.isEmpty)
      }
    }
    withClue("Adding multiple properties reports a new 1-result group") {
      val events = Seq(prop2, prop3, prop4, prop5, prop6).map(makeSetEvent)
      state.reportNodeEvents(events, shouldHaveEffects = true) { effects =>
        val results = effects.resultsReported.dequeue()
        val expected = propsAsCypher(prop1, prop2, prop3, prop4, prop5, prop6)
        assert(results == Seq(QueryContext(Map(query.aliasedAs -> expected))))
        assert(effects.isEmpty)
      }
    }
    withClue("Changing multiple properties reports a new 1-result group") {
      val events = Seq(prop1ButFunky, prop2ButFunky).map(makeSetEvent)
      state.reportNodeEvents(events, shouldHaveEffects = true) { effects =>
        val results = effects.resultsReported.dequeue()
        val expected = propsAsCypher(prop1ButFunky, prop2ButFunky, prop3, prop4, prop5, prop6)
        assert(results == Seq(QueryContext(Map(query.aliasedAs -> expected))))
        assert(effects.isEmpty)
      }
    }
    withClue("Removing a single property reports a new 1-result group") {
      val events = Seq(prop6).map(makeDeleteEvent)
      state.reportNodeEvents(events, shouldHaveEffects = true) { effects =>
        val results = effects.resultsReported.dequeue()
        val expected = propsAsCypher(prop1ButFunky, prop2ButFunky, prop3, prop4, prop5)
        assert(results == Seq(QueryContext(Map(query.aliasedAs -> expected))))
        assert(effects.isEmpty)
      }
    }
    withClue("Removing multiple properties reports a new 1-result group") {
      val events = Seq(prop1ButFunky, prop3).map(makeDeleteEvent)
      state.reportNodeEvents(events, shouldHaveEffects = true) { effects =>
        val results = effects.resultsReported.dequeue()
        val expected = propsAsCypher(prop2ButFunky, prop4, prop5)
        assert(results == Seq(QueryContext(Map(query.aliasedAs -> expected))))
        assert(effects.isEmpty)
      }
    }
    withClue(
      "Removing a single property and changing an existing property reports a new 1-result group",
    ) {
      val events = Seq(makeDeleteEvent(prop4), makeSetEvent(prop2))
      state.reportNodeEvents(events, shouldHaveEffects = true) { effects =>
        val results = effects.resultsReported.dequeue()
        val expected = propsAsCypher(prop2, prop5)
        assert(results == Seq(QueryContext(Map(query.aliasedAs -> expected))))
        assert(effects.isEmpty)
      }
    }

    /** The following commented-out test represents a user-facing invariant we want to preserve:
      * If a batch of events has no net effect on the set of properties on a node, no new result should be emitted.
      *
      * This invariant _is_ in place, but is not implemented by the MVSQ states. Rather, the [[AbstractNodeActor]]
      * itself is responsible for deciding which events are duplicate both within a batch and against the node's
      * current state. The node itself will filter out any duplicates _before_ making the events available to the
      * MVSQ state (analogous to "before calling reportNodeEvents")
      *
      * @see [[NodeActorTest.scala:141]] in quine-enterprise
      */
//    withClue("Setting multiple properties to their current value does nothing") {
//      val events = Seq(prop2, prop5).map(makeSetEvent)
//      state.reportNodeEvents(events, false) { effects =>
//        assert(effects.isEmpty)
//      }
//    }
  }

}
