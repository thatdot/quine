package com.thatdot.quine.graph.standing

import scala.collection.mutable

import org.scalactic.source.Position

import com.thatdot.quine.graph.cypher.{QueryContext, StandingQuery, StandingQueryEffects}
import com.thatdot.quine.graph.messaging.StandingQueryMessage.{CancelCypherResult, NewCypherResult, ResultId}
import com.thatdot.quine.graph.{NodeChangeEvent, QuineIdLongProvider, StandingQueryPartId}
import com.thatdot.quine.model.{QuineId, QuineIdProvider}

/** Mocked up handler of standing query effects - instead of actually doing anything with the
  * effects, they just get queued up for easy testing
  *
  * @param subscriptionsCreated queue of calls made to `createSubscription`
  * @param subscriptionsCancelled queue of calls made to `cancelSubscription`
  * @param resultsReported queue of calls made to `reportNewResult`
  * @param resultsCancelled queue of calls made to `cancelOldResult`
  * @param node ID of the fake node on which this is running
  * @param idProvider ID provider
  */
final case class StandingQueryEffectsTester(
  subscriptionsCreated: mutable.Queue[(QuineId, StandingQuery)],
  subscriptionsCancelled: mutable.Queue[(QuineId, StandingQueryPartId)],
  resultsReported: mutable.Queue[(ResultId, QueryContext)],
  resultsCancelled: mutable.Queue[ResultId],
  node: QuineId,
  idProvider: QuineIdProvider,
  knownQueries: mutable.Map[StandingQueryPartId, StandingQuery]
) extends StandingQueryEffects {

  def createSubscription(onNode: QuineId, query: StandingQuery): Unit = {
    knownQueries += query.id -> query
    subscriptionsCreated.enqueue(onNode -> query)
  }

  def cancelSubscription(onNode: QuineId, queryId: StandingQueryPartId): Unit =
    subscriptionsCancelled.enqueue(onNode -> queryId)

  def reportNewResult(resultId: ResultId, result: QueryContext): Unit =
    resultsReported.enqueue(resultId -> result)

  def cancelOldResult(resultId: ResultId): Unit =
    resultsCancelled.enqueue(resultId)

  def isEmpty: Boolean =
    subscriptionsCreated.isEmpty && subscriptionsCancelled.isEmpty &&
    resultsReported.isEmpty && resultsCancelled.isEmpty

  def lookupQuery(queryPartId: StandingQueryPartId): StandingQuery = knownQueries(queryPartId)
}
object StandingQueryEffectsTester {

  /** Create an empty effects tester
    *
    * @param idProvider ID provider
    * @return empty effects tester
    */
  def empty(
    query: StandingQuery,
    initiallyKnownQueries: Seq[StandingQuery] = Seq.empty,
    idProvider: QuineIdProvider = QuineIdLongProvider()
  ): StandingQueryEffectsTester =
    new StandingQueryEffectsTester(
      mutable.Queue.empty,
      mutable.Queue.empty,
      mutable.Queue.empty,
      mutable.Queue.empty,
      idProvider.newQid(),
      idProvider,
      knownQueries = mutable.Map(query.id -> query) ++= initiallyKnownQueries.map(sq => sq.id -> sq).toMap
    )
}

/** Harness for checking the behaviour of a [[StandingQueryState]] when it receives different
  * data
  *
  * @param query the query being checked
  * @param effects how effects are mocked up
  */
class StandingQueryStateWrapper[S <: StandingQuery](
  final val query: S,
  final val knownQueries: Seq[StandingQuery] = Seq.empty
) {
  final val sqState: query.State = query.createState()
  final val effects: StandingQueryEffectsTester = StandingQueryEffectsTester.empty(query, knownQueries)

  def testInvariants()(implicit pos: Position): Unit = ()

  def initialize[A]()(thenCheck: StandingQueryEffectsTester => A)(implicit pos: Position): A = {
    sqState.preStart(effects)
    sqState.onInitialize(effects)
    testInvariants()
    thenCheck(effects)
  }

  def shutdown[A]()(thenCheck: StandingQueryEffectsTester => A)(implicit pos: Position): A = {
    sqState.onShutdown(effects)
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
    thenCheck: StandingQueryEffectsTester => A
  )(implicit pos: Position): A = {
    val hadEffects = sqState.onNodeEvents(events, effects)
    assert(
      shouldHaveEffects == hadEffects,
      "New node events did not have the expected effects (or lack thereof)"
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
  def reportNewSubscriptionResult[A](result: NewCypherResult, shouldHaveEffects: Boolean)(
    thenCheck: StandingQueryEffectsTester => A
  )(implicit pos: Position): A = {
    val hadEffects = sqState.onNewSubscriptionResult(result, effects)
    assert(
      shouldHaveEffects == hadEffects,
      "New subscription did not have the expected effects (or lack thereof)"
    )
    testInvariants()
    thenCheck(effects)
  }

  /** Simulate new subscription result cancellations
    *
    * @param result subscription cancellation simulated
    * @param shouldHaveEffects assert whether this should cause an update in the state
    * @param thenCheck after processing the subscription, check something about the state
    * @return output of the check
    */
  def reportCancelledSubscriptionResult[A](result: CancelCypherResult, shouldHaveEffects: Boolean)(
    thenCheck: StandingQueryEffectsTester => A
  )(implicit pos: Position): A = {
    val hadEffects = sqState.onCancelledSubscriptionResult(result, effects)
    assert(
      shouldHaveEffects == hadEffects,
      "Subscription cancellation did not have the expected effects (or lack thereof)"
    )
    testInvariants()
    thenCheck(effects)
  }
}
