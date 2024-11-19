package com.thatdot.quine.graph.standing

import scala.collection.mutable

import org.scalactic.source.Position
import org.scalatest.Assertions

import com.thatdot.quine.graph.PropertyEvent.{PropertyRemoved, PropertySet}
import com.thatdot.quine.graph.cypher.{MultipleValuesStandingQuery, MultipleValuesStandingQueryEffects, QueryContext}
import com.thatdot.quine.graph.messaging.StandingQueryMessage.NewMultipleValuesStateResult
import com.thatdot.quine.graph.{
  AbstractNodeActor,
  EdgeEvent,
  MultipleValuesStandingQueryPartId,
  NodeChangeEvent,
  PropertyEvent,
  QuineIdLongProvider,
}
import com.thatdot.quine.model.{PropertyValue, QuineId, QuineIdProvider}
import com.thatdot.quine.util.TestLogging._

/** Mocked up handler of standing query effects - instead of actually doing anything with the
  * effects, they just get queued up for easy testing
  *
  * @param subscriptionsCreated queue of calls made to `createSubscription`
  * @param subscriptionsCancelled queue of calls made to `cancelSubscription`
  * @param resultsReported queue of calls made to `reportNewResult`
  * @param executingNodeId ID of the fake node on which this is running
  * @param idProvider ID provider
  */
final case class MultipleValuesStandingQueryEffectsTester(
  subscriptionsCreated: mutable.Queue[(QuineId, MultipleValuesStandingQuery)],
  subscriptionsCancelled: mutable.Queue[(QuineId, MultipleValuesStandingQueryPartId)],
  resultsReported: mutable.Queue[Seq[QueryContext]],
  executingNodeId: QuineId,
  idProvider: QuineIdProvider,
  knownQueries: mutable.Map[MultipleValuesStandingQueryPartId, MultipleValuesStandingQuery],
) extends MultipleValuesStandingQueryEffects {

  var currentProperties: Map[Symbol, PropertyValue] = Map.empty
  def trackPropertyEffects(events: Seq[NodeChangeEvent]): Unit = events.foreach {
    case PropertySet(key, value) => currentProperties += key -> value
    case PropertyRemoved(key, _) => currentProperties -= key
    case _: EdgeEvent => ()
  }

  def createSubscription(onNode: QuineId, query: MultipleValuesStandingQuery): Unit = {
    knownQueries += query.queryPartId -> query
    subscriptionsCreated.enqueue(onNode -> query)
  }

  def cancelSubscription(onNode: QuineId, queryId: MultipleValuesStandingQueryPartId): Unit =
    subscriptionsCancelled.enqueue(onNode -> queryId)

  def reportUpdatedResults(resultGroup: Seq[QueryContext]): Unit =
    resultsReported.enqueue(resultGroup)

  def isEmpty: Boolean =
    subscriptionsCreated.isEmpty && subscriptionsCancelled.isEmpty &&
    resultsReported.isEmpty

  def lookupQuery(queryPartId: MultipleValuesStandingQueryPartId): MultipleValuesStandingQuery = knownQueries(
    queryPartId,
  )
}
object MultipleValuesStandingQueryEffectsTester {

  /** Create an empty effects tester
    *
    * @param idProvider ID provider
    * @return empty effects tester
    */
  def empty(
    query: MultipleValuesStandingQuery,
    initiallyKnownQueries: Seq[MultipleValuesStandingQuery] = Seq.empty,
    idProvider: QuineIdProvider = QuineIdLongProvider(),
  ): MultipleValuesStandingQueryEffectsTester =
    new MultipleValuesStandingQueryEffectsTester(
      mutable.Queue.empty,
      mutable.Queue.empty,
      mutable.Queue.empty,
      idProvider.newQid(),
      idProvider,
      knownQueries =
        mutable.Map(query.queryPartId -> query) ++= initiallyKnownQueries.map(sq => sq.queryPartId -> sq).toMap,
    )
}

/** Harness for checking the behaviour of a [[StandingQueryState]] when it receives different
  * data
  *
  * @param query the query being checked
  * @param effects how effects are mocked up
  */
class StandingQueryStateWrapper[S <: MultipleValuesStandingQuery](
  final val query: S,
  final val knownQueries: Seq[MultipleValuesStandingQuery] = Seq.empty,
) extends Assertions {
  final val sqState: query.State = query.createState()
  final val effects: MultipleValuesStandingQueryEffectsTester =
    MultipleValuesStandingQueryEffectsTester.empty(query, knownQueries)

  def testInvariants()(implicit pos: Position): Unit = ()

  def initialize[A](
    initialProperties: Map[Symbol, PropertyValue] = Map.empty,
  )(thenCheck: MultipleValuesStandingQueryEffectsTester => A)(implicit pos: Position): A = {
    val initialPropertyEvents: Seq[NodeChangeEvent] = initialProperties.map { case (k, v) => PropertySet(k, v) }.toSeq
    effects.trackPropertyEffects(initialPropertyEvents)
    sqState.rehydrate(effects)
    sqState.onInitialize(effects)
    sqState.onNodeEvents(initialPropertyEvents, effects)
    testInvariants()
    thenCheck(effects)
  }

  /** Simulate node change events
    *
    * @param events events being simulated
    * @param shouldHaveEffects assert whether this should cause an update in the state
    * @param thenCheck after processing the events, check something about the state
    * @return output of the check
    */
  def reportNodeEvents[A](events: Seq[NodeChangeEvent], shouldHaveEffects: Boolean)(
    thenCheck: MultipleValuesStandingQueryEffectsTester => A,
  )(implicit pos: Position): A = {
    // emulate deduplication behavior of nodes w.r.t propertyevents
    val finalEvents =
      if (events.forall(_.isInstanceOf[PropertyEvent]))
        AbstractNodeActor.internallyDeduplicatePropertyEvents(
          events.collect { case pe: PropertyEvent => pe }.toList,
        )
      else events
    // emulate property processing
    effects.trackPropertyEffects(finalEvents)
    val hadEffects = sqState.onNodeEvents(finalEvents, effects)
    assert(
      shouldHaveEffects == hadEffects,
      "New node events did not have the expected effects analysis",
    )
    testInvariants()
    thenCheck(effects)
  }

  /** Simulate new subscription results
    *
    * @param result subscription result simulated
    * @param shouldHaveEffects assert whether this should cause an update in the state
    * @param thenCheck after processing the subscription, check something about the state
    * @return output of the check
    */
  def reportNewSubscriptionResult[A](result: NewMultipleValuesStateResult, shouldHaveEffects: Boolean)(
    thenCheck: MultipleValuesStandingQueryEffectsTester => A,
  )(implicit pos: Position): A = {
    val hadEffects = sqState.onNewSubscriptionResult(result, effects)
    assert(
      shouldHaveEffects == hadEffects,
      "New node events did not have the expected effects analysis",
    )
    testInvariants()
    thenCheck(effects)
  }
}
