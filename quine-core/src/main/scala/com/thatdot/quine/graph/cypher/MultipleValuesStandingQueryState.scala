package com.thatdot.quine.graph.cypher

import scala.collection.compat.immutable._
import scala.collection.mutable

import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.graph.NodeChangeEvent.{EdgeAdded, EdgeRemoved, PropertyRemoved, PropertySet}
import com.thatdot.quine.graph.messaging.StandingQueryMessage.{
  CancelMultipleValuesResult,
  NewMultipleValuesResult,
  ResultId
}
import com.thatdot.quine.graph.{MultipleValuesStandingQueryPartId, NodeChangeEvent, StandingQueryLocalEvents}
import com.thatdot.quine.model.{HalfEdge, Properties, QuineId, QuineIdProvider}

/** The stateful component of a standing query, holding on to the information
  * necessary for:
  *
  *   - reporting new matches
  *   - invalidating previous matches
  *   - retracing previous matches
  *
  * Performance note: There are very likely a *lot* of these in memory at a given time. Therefore,
  * every effort should be made to keep the in-memory size of instances small. For example, rather
  * than serializing and reconstructing the StandingQuery instance associated with a State (which
  * would create multiple identical copies of the same Query objects in memory) the States leverage
  * a global registry of StandingQuery instances, and only serialize as much information as
  * necessary to `replayResults` when requested.
  */
sealed abstract class MultipleValuesStandingQueryState extends LazyLogging {

  /** Type of standing query from which this state was created
    *
    * For any `S <: StandingQuery` and `sq: S`, it should be the case that
    * `sq.createState().StateOf =:= S`. In other words `StandingQueryState#StateOf`
    * is the inverse of `StandingQuery#State`.
    */
  type StateOf <: MultipleValuesStandingQuery

  /** the ID of the StandingQuery (part) associated with this state */
  def queryPartId: MultipleValuesStandingQueryPartId

  /** Non-overlapping group of possible node event categories that state wants to be notified of */
  def relevantEvents: Seq[StandingQueryLocalEvents] = Seq.empty

  /** Called on state creation or deserialization/wakeup, before `onInitialize`
    * or any other external events/results.
    *
    * This is used to rehydrate fields which we don't want serialized.
    */
  def preStart(effectHandler: MultipleValuesStandingQueryLookupInfo): Unit = ()

  /** Called the first time the state is created (but not when it is merely being woken up)
    */
  def onInitialize(
    effectHandler: MultipleValuesStandingQueryEffects
  ): Unit = ()

  /** Called when the state (and its associated query) are deleted (but not when they are merely
    * being put to sleep). This should cancel the query represented by this state (and clean up)
    * TODO: consider return type `Future[Unit]`
    */
  def onShutdown(
    effectHandler: MultipleValuesStandingQueryEffects
  ): Unit = ()

  /** Relevant node events that have happened
    *
    * @param events which node-events happened
    * @param effectHandler handler for external effects
    * @return whether the standing query state was updated (eg. is there anything new to save?)
    */
  def onNodeEvents(
    events: Seq[NodeChangeEvent],
    effectHandler: MultipleValuesStandingQueryEffects
  ): Boolean = false

  /** Called when one of the sub-queries delivers a new result
    *
    * @param result subscription result
    * @param effectHandler handler for external effects
    * @return whether the standing query state was updated (eg. is there anything new to save?)
    */
  def onNewSubscriptionResult(
    result: NewMultipleValuesResult,
    effectHandler: MultipleValuesStandingQueryEffects
  ): Boolean = false

  /** Called when one of the sub-queries invalidates a previous result
    *
    * @param result cancelled subscription result
    * @param effectHandler handler for external effects
    * @return whether the standing query state was updated (eg. is there anything new to save?)
    */
  def onCancelledSubscriptionResult(
    result: CancelMultipleValuesResult,
    effectHandler: MultipleValuesStandingQueryEffects
  ): Boolean = false

  /** Re-create all past (not cancelled) results
    *
    * @note passing in the current node properties is done to enable some storage optimizations
    *
    * Suppose that we were to keep a `Map[ResultId, QueryContext]` from the moment that the query
    * is initialized and every time a new result is reported with [[reportNewResult]] we add it
    * to this map and every time a new result is cancelled with [[cancelOldResult]] we remove it
    * from the map.Calling `replayResults` at any point in time should produce a `Map` matching
    * that (hypothetical) map's contents, though the implementation may be optimized..
    *
    * @param localProperties current local node properties
    * @return a map of all past (not cancelled) results, keyed by their original IDs
    */
  def replayResults(localProperties: Properties): Map[ResultId, QueryContext]
}

/** A [[MultipleValuesStandingQueryState]] that refers to a [[MultipleValuesStandingQuery]] in the system (graph)'s cache
  * `query` may be safely used in any other function
  */
sealed trait CachableQueryMultipleValues extends MultipleValuesStandingQueryState {
  private[this] var _query: StateOf = _ // late-init
  protected[this] def query: StateOf = _query // readonly access for implementations

  override def preStart(effectHandler: MultipleValuesStandingQueryLookupInfo): Unit = {
    super.preStart(effectHandler)

    // Cast here is safe thanks to the invariant documented on [[StateOf]]
    _query = effectHandler.lookupQuery(queryPartId).asInstanceOf[StateOf]
  }
}

trait MultipleValuesStandingQueryLookupInfo {

  /** Get a [[MultipleValuesStandingQuery]] instance from the current graph
    *
    * @param queryPartId
    * @return
    */
  def lookupQuery(queryPartId: MultipleValuesStandingQueryPartId): MultipleValuesStandingQuery

  /** Current node */
  val node: QuineId

  /** ID provider */
  val idProvider: QuineIdProvider
}
object MultipleValuesStandingQueryLookupInfo {
  def apply(
    lookupQuery: MultipleValuesStandingQueryPartId => MultipleValuesStandingQuery,
    id: QuineId,
    idProv: QuineIdProvider
  ): MultipleValuesStandingQueryLookupInfo = new MultipleValuesStandingQueryLookupInfo {
    def lookupQuery(queryPartId: MultipleValuesStandingQueryPartId): MultipleValuesStandingQuery = lookupQuery(
      queryPartId
    )
    val node: QuineId = id
    val idProvider: QuineIdProvider = idProv
  }
}

/** Limited scope of actions that a [[MultipleValuesStandingQueryState]] is allowed to make */
trait MultipleValuesStandingQueryEffects extends MultipleValuesStandingQueryLookupInfo {

  /** Issue a subscription to a node
    *
    * @param onNode node to which the subscription is delivered
    * @param query standing query whose results are being subscribed to
    */
  def createSubscription(onNode: QuineId, query: MultipleValuesStandingQuery): Unit

  /** Cancel a previously issued subscription
    *
    * @param onNode node to which the cancellation is delivered
    * @param queryId ID of the standing query whose results were being subscribed to
    */
  def cancelSubscription(onNode: QuineId, queryId: MultipleValuesStandingQueryPartId): Unit

  /** Report a fresh result
    *
    * @param resultId unique identifier for the new result
    * @param result contents of the new results
    */
  def reportNewResult(resultId: ResultId, result: QueryContext): Unit

  /** Cancel an existing result
    *
    * @param resultId uniqe identifier for the old result
    */
  def cancelOldResult(resultId: ResultId): Unit
}

/** State needed to process a [[MultipleValuesStandingQuery.UnitSq]]
  *
  * @param queryPartId the ID of the unit query with this State
  * @param resultId the ID of the one and only result that is created
  */
final case class UnitState(
  queryPartId: MultipleValuesStandingQueryPartId,
  var resultId: Option[ResultId]
) extends MultipleValuesStandingQueryState
    with CachableQueryMultipleValues {

  override def onInitialize(
    effectHandler: MultipleValuesStandingQueryEffects
  ): Unit =
    if (resultId.isEmpty) {
      val freshResultId = ResultId.fresh()
      effectHandler.reportNewResult(freshResultId, QueryContext.empty)
      resultId = Some(freshResultId)
    } else
      logger.error(s"$this cannot be re-initialized")

  def replayResults(localProperties: Properties): Map[ResultId, QueryContext] =
    resultId.view.map(_ -> QueryContext.empty).toMap
}

/** State needed to process a [[MultipleValuesStandingQuery.Cross]]
  *
  * @param queryPartId the ID of the cross-product query with this State
  * @param subscriptionsEmitted total number of subscriptions emitted so far (this only grows)
  * @param accumulatedResults results produced so far by each part of the cross-product
  * @param resultDependency map of results emitted to their dependencies
  */
final case class CrossState(
  queryPartId: MultipleValuesStandingQueryPartId,
  var subscriptionsEmitted: Int,
  accumulatedResults: ArraySeq[mutable.Map[ResultId, QueryContext]],
  resultDependency: mutable.Map[ResultId, List[ResultId]]
) extends MultipleValuesStandingQueryState
    with CachableQueryMultipleValues {
  type StateOf = MultipleValuesStandingQuery.Cross

  /** map of results received to the set of results emitted
    *
    * Invariant:
    *
    * {{{
    * val deps1 = resultDependency.toSet.flatMap { case (res, deps) => deps.map(res -> _) }
    * val deps2 = reverseResultDependency.toSet.flatMap { case (dep, ress) => ress.map(_ -> dep) }
    * deps1 == deps2
    * }}}
    */
  val reverseResultDependency: mutable.Map[ResultId, mutable.Set[ResultId]] = mutable.Map.empty

  /** just like `accumulatedResults`, but tracks the query ID alongside the results */
  private[this] var accumulatedResultsWithId
    : ArraySeq[(MultipleValuesStandingQueryPartId, mutable.Map[ResultId, QueryContext])] = _

  override def preStart(effectHandler: MultipleValuesStandingQueryLookupInfo): Unit = {
    super.preStart(effectHandler)

    // Populate `reverseResultDependency`
    for {
      (resultId, dependsOn) <- resultDependency
      dependedUpon <- dependsOn
    } reverseResultDependency.getOrElseUpdate(dependedUpon, mutable.Set.empty).add(resultId)

    // Populate `accumulatedResultsWithId`
    accumulatedResultsWithId = ArraySeq
      .newBuilder[(MultipleValuesStandingQueryPartId, mutable.Map[ResultId, QueryContext])]
      .++=(query.queries.map(_.id).zip(accumulatedResults))
      .result()
  }

  /** Record and emit a new result
    *
    * @param resultId new result ID
    * @param result new result
    * @param dependsOn recursive results which need to hold for this result to remain valid
    */
  private def newResult(
    resultId: ResultId,
    result: QueryContext,
    dependsOn: List[ResultId],
    effectHandler: MultipleValuesStandingQueryEffects
  ): Unit = {
    effectHandler.reportNewResult(resultId, result)
    resultDependency += resultId -> dependsOn
    for (dependedUpon <- dependsOn)
      reverseResultDependency.getOrElseUpdate(dependedUpon, mutable.Set.empty).add(resultId)
  }

  /** Cancel results that are no longer valid due to a dependency that is no longer valid
    *
    * @param dependency recusive result which is no longer valid
    */
  private def cancelResults(
    dependencyId: ResultId,
    effectHandler: MultipleValuesStandingQueryEffects
  ): Unit =
    for (invalidated <- reverseResultDependency.remove(dependencyId).getOrElse(Nil)) {
      effectHandler.cancelOldResult(invalidated)
      for (otherDep <- resultDependency.remove(invalidated).get; if otherDep != dependencyId) {
        val resSet = reverseResultDependency(otherDep)
        resSet.remove(invalidated)
        if (resSet.isEmpty) reverseResultDependency.remove(otherDep)
      }
    }

  override def onInitialize(
    effectHandler: MultipleValuesStandingQueryEffects
  ): Unit =
    for (sq <- if (query.emitSubscriptionsLazily) query.queries.view.take(1) else query.queries.view) {
      effectHandler.createSubscription(effectHandler.node, sq)
      subscriptionsEmitted += 1
    }

  override def onShutdown(
    effectHandler: MultipleValuesStandingQueryEffects
  ): Unit =
    for (sq <- query.queries.take(subscriptionsEmitted))
      effectHandler.cancelSubscription(effectHandler.node, sq.id)

  override def onNewSubscriptionResult(
    result: NewMultipleValuesResult,
    effectHandler: MultipleValuesStandingQueryEffects
  ): Boolean = {
    if (!query.queries.exists(_.id == result.queryId)) {
      logger.error(s"$this received subscription result it didn't subscribe to: $result")
      return false
    }

    if (subscriptionsEmitted != query.queries.length) {
      // Don't bother trying to build up results - all subscriptions haven't been emitted yet!
      val resultsIdx = query.queries.indexWhere(_.id == result.queryId)
      val resultMap = accumulatedResults(resultsIdx)

      // If this is the first result, make sure a subscription has been emitted for the next query
      if (resultMap.isEmpty && resultsIdx == subscriptionsEmitted - 1) {
        effectHandler.createSubscription(effectHandler.node, query.queries(subscriptionsEmitted))
        subscriptionsEmitted += 1
      }

      resultMap += result.resultId -> result.result
    } else {
      // Build up an iterator of fresh results
      val newResults = accumulatedResultsWithId.foldLeft(Iterator(List.empty[ResultId] -> QueryContext.empty)) {
        case (crossAccumulator, (subId, resultMap)) =>
          // What rows to use in the cross-product for this subscription?
          val resultsIterator: Iterable[(ResultId, QueryContext)] = if (subId != result.queryId) {
            resultMap
          } else {
            resultMap += result.resultId -> result.result
            List(result.resultId -> result.result)
          }

          // Take a cross-product
          for {
            (dependsOnResultIds: List[ResultId @unchecked], crossRow: QueryContext) <-
              crossAccumulator
            (resultId, row) <- resultsIterator
          } yield (resultId :: dependsOnResultIds, crossRow ++ row)
      }

      // Emit new results
      for ((dependsOnResultIds: List[ResultId @unchecked], result: QueryContext) <- newResults)
        newResult(ResultId.fresh(), result, dependsOnResultIds, effectHandler)
    }

    true
  }

  override def onCancelledSubscriptionResult(
    result: CancelMultipleValuesResult,
    effectHandler: MultipleValuesStandingQueryEffects
  ): Boolean =
    accumulatedResultsWithId.find(_._1 == result.queryId) match {
      case None =>
        logger.error(s"$this revieved subscription result it didn't subscribe to: $result")
        false

      case Some((_, results)) =>
        // Remove the invalidated result
        results.remove(result.resultId)

        // Invalidate results that depended on this
        cancelResults(result.resultId, effectHandler)

        true
    }

  def replayResults(localProperties: Properties): Map[ResultId, QueryContext] =
    resultDependency.view.map { case (resultId, dependsOnResulIds) =>
      val recreatedResult = accumulatedResults.view
        .map(
          dependsOnResulIds.collectFirst(_).get
        ) // TODO: handle `get` with an error explaining what invariant is violated
        .foldLeft(QueryContext.empty)(_ ++ _)
      resultId -> recreatedResult
    }.toMap
}

/** State needed to process a [[MultipleValuesStandingQuery.LocalProperty]]
  *
  * @param queryPartId the ID of the local property query with this State
  * @param currentResult ID of a result if it has been returned
  */
final case class LocalPropertyState(
  queryPartId: MultipleValuesStandingQueryPartId,
  var currentResult: Option[ResultId]
) extends MultipleValuesStandingQueryState
    with CachableQueryMultipleValues {
  type StateOf = MultipleValuesStandingQuery.LocalProperty

  override def relevantEvents: Seq[StandingQueryLocalEvents.Property] = Seq(
    StandingQueryLocalEvents.Property(query.propKey)
  )

  override def onInitialize(effectHandler: MultipleValuesStandingQueryEffects): Unit =
    if (currentResult.nonEmpty) {
      logger.error(s"$this cannot be re-initialized")
    } else if (query.propConstraint.satisfiedByNone) {
      currentResult = {
        val result = query.aliasedAs match {
          case None => QueryContext.empty
          case Some(aliased) => QueryContext.empty + (aliased -> Expr.Null)
        }
        val freshResultId = ResultId.fresh()
        effectHandler.reportNewResult(freshResultId, result)
        Some(freshResultId)
      }
    }

  override def onNodeEvents(
    events: Seq[NodeChangeEvent],
    effectHandler: MultipleValuesStandingQueryEffects
  ): Boolean = {
    val oldResult = currentResult
    events.foreach {
      case PropertySet(propKey, propVal) if query.propKey == propKey =>
        // Does it pass the property value test?
        val satisfiesValueConstraint =
          query.propConstraint(propVal.deserialized.get) // TODO: review `Try.get`

        currentResult match {
          case Some(prev @ _) if query.aliasedAs.isEmpty && satisfiesValueConstraint =>
          /* This is the case where: We already had a result, that result has changed (but still
           * matches), but because the query doesn't return the property value, there is nothing
           * new to report
           */

          case _ =>
            // Invalidate the previous result
            for (resultId <- currentResult)
              effectHandler.cancelOldResult(resultId)

            if (satisfiesValueConstraint) {
              // Produce a new result
              val freshResultId = ResultId.fresh()
              val resultValue = Expr.fromQuineValue(propVal.deserialized.get)
              val result = query.aliasedAs match {
                case None => QueryContext.empty
                case Some(aliased) => QueryContext.empty + (aliased -> resultValue)
              }
              currentResult = Some(freshResultId)
              effectHandler.reportNewResult(freshResultId, result)
            } else {
              currentResult = None
            }
        }

      case PropertyRemoved(propKey, _) if query.propKey == propKey =>
        if (query.propConstraint.satisfiedByNone && (currentResult.isEmpty || query.aliasedAs.isDefined)) {
          currentResult = currentResult.orElse {
            val result = query.aliasedAs match {
              case None => QueryContext.empty
              case Some(aliased) => QueryContext.empty + (aliased -> Expr.Null)
            }
            val freshResultId = ResultId.fresh()
            effectHandler.reportNewResult(freshResultId, result)
            Some(freshResultId)
          }
        } else
          // Invalidate the previous result
          currentResult = currentResult.flatMap { resultId =>
            effectHandler.cancelOldResult(resultId)
            None
          }

      case _ => // Ignore
    }
    oldResult != currentResult
  }

  override def onNewSubscriptionResult(
    result: NewMultipleValuesResult,
    effectHandler: MultipleValuesStandingQueryEffects
  ): Boolean = {
    logger.error(s"$this received subscription result it didn't subscribe to: $result")
    false
  }

  override def onCancelledSubscriptionResult(
    result: CancelMultipleValuesResult,
    effectHandler: MultipleValuesStandingQueryEffects
  ): Boolean = {
    logger.error(s"$this received subscription result it didn't subscribe to: $result")
    false
  }

  def replayResults(localProperties: Properties): Map[ResultId, QueryContext] = {
    val resId = currentResult match {
      case Some(resId) => resId
      case None => return Map.empty
    }
    val result = query.aliasedAs match {
      case None => QueryContext.empty
      case Some(aliased) =>
        val propVal = localProperties(query.propKey)
        val resultValue = Expr.fromQuineValue(propVal.deserialized.get) // TODO: review `get`
        QueryContext.empty + (aliased -> resultValue)
    }
    Map(resId -> result)
  }
}

/** State needed to process a [[MultipleValuesStandingQuery.LocalId]]
  *
  * @param queryPartId the ID of the localId query with this State
  * @param resultId the ID of the one and only result that is created
  */
final case class LocalIdState(
  queryPartId: MultipleValuesStandingQueryPartId,
  var resultId: Option[ResultId]
) extends MultipleValuesStandingQueryState
    with CachableQueryMultipleValues {
  type StateOf = MultipleValuesStandingQuery.LocalId

  private[this] var idValue: Value = _

  override def preStart(effectHandler: MultipleValuesStandingQueryLookupInfo): Unit = {
    super.preStart(effectHandler)

    // Pre-compute the ID result value
    idValue = if (query.formatAsString) {
      Expr.Str(effectHandler.idProvider.qidToPrettyString(effectHandler.node))
    } else {
      Expr.fromQuineValue(effectHandler.idProvider.qidToValue(effectHandler.node))
    }
  }

  override def onInitialize(
    effectHandler: MultipleValuesStandingQueryEffects
  ): Unit = {
    if (resultId.nonEmpty) {
      logger.error(s"$this cannot be re-initialized")
      return ()
    }
    val freshResultId = ResultId.fresh()
    effectHandler.reportNewResult(freshResultId, QueryContext.empty + (query.aliasedAs -> idValue))
    resultId = Some(freshResultId)
  }

  def replayResults(localProperties: Properties): Map[ResultId, QueryContext] =
    resultId match {
      case Some(resId) => Map(resId -> (QueryContext.empty + (query.aliasedAs -> idValue)))
      case None => Map.empty
    }
}

/** State needed to process a [[MultipleValuesStandingQuery.SubscribeAcrossEdge]]
  *
  * @param queryPartId the ID of the subscribe-across-edge query with this State
  * @param edgesWatched mapping of matching half edges that to results received & produced
  */
final case class SubscribeAcrossEdgeState(
  queryPartId: MultipleValuesStandingQueryPartId,
  edgesWatched: mutable.Map[
    HalfEdge,
    (MultipleValuesStandingQueryPartId, mutable.Map[ResultId, (ResultId, QueryContext)])
  ]
) extends MultipleValuesStandingQueryState
    with CachableQueryMultipleValues {
  type StateOf = MultipleValuesStandingQuery.SubscribeAcrossEdge

  /** mapping from standing queries emitted to the corresponding half edge
    *
    * Invariants:
    *
    * {{{
    * val edges1 = edgesWatched.iterator.map { case (he, (sqId, _)) => sqId -> he }.toSet
    * val edges2 = edgeQueryIds.iterator.map { case ((_, sqId), he) => sqId -> he }.toSet
    * edges1 == edges2
    *
    * edgeQueryIds.forall { case ((other, _), he) => other == he.other }
    * }}}
    */
  val edgeQueryIds: mutable.Map[(QuineId, MultipleValuesStandingQueryPartId), HalfEdge] =
    mutable.Map.empty[(QuineId, MultipleValuesStandingQueryPartId), HalfEdge]

  override def relevantEvents: Seq[StandingQueryLocalEvents.Edge] = Seq(StandingQueryLocalEvents.Edge(query.edgeName))

  override def preStart(effectHandler: MultipleValuesStandingQueryLookupInfo): Unit = {
    super.preStart(effectHandler)

    for ((he, (sqId, _)) <- edgesWatched)
      edgeQueryIds += (he.other -> sqId) -> he
  }

  override def onNodeEvents(
    events: Seq[NodeChangeEvent],
    effectHandler: MultipleValuesStandingQueryEffects
  ): Boolean = {
    var somethingChanged = false
    events.foreach {
      case EdgeAdded(halfEdge)
          if query.edgeName.forall(_ == halfEdge.edgeType) &&
            query.edgeDirection.forall(_ == halfEdge.direction) =>
        // Create a new subscription
        val freshEdgeQuery = MultipleValuesStandingQuery.EdgeSubscriptionReciprocal(
          halfEdge.reflect(effectHandler.node),
          query.andThen.id,
          query.columns
        )
        effectHandler.createSubscription(halfEdge.other, freshEdgeQuery)
        edgesWatched += halfEdge -> (freshEdgeQuery.id -> mutable.Map.empty)
        edgeQueryIds += (halfEdge.other -> freshEdgeQuery.id) -> halfEdge

        somethingChanged = true

      case EdgeRemoved(halfEdge) if edgesWatched.contains(halfEdge) =>
        val (invalidatedQueryId, invalidatedResults) = edgesWatched.remove(halfEdge).get
        edgeQueryIds -= (halfEdge.other -> invalidatedQueryId)

        // Remove the subscription
        effectHandler.cancelSubscription(halfEdge.other, invalidatedQueryId)

        // Invalidate all results through this edge
        for ((_, (resultId, _)) <- invalidatedResults)
          effectHandler.cancelOldResult(resultId)

        somethingChanged = true

      case _ => // Ignore
    }
    somethingChanged
  }

  override def onShutdown(effectHandler: MultipleValuesStandingQueryEffects): Unit =
    for ((heOther, edgeQueryId) <- edgeQueryIds.keys)
      effectHandler.cancelSubscription(heOther, edgeQueryId)

  override def onNewSubscriptionResult(
    result: NewMultipleValuesResult,
    effectHandler: MultipleValuesStandingQueryEffects
  ): Boolean = {
    val correspondingHalfEdge: HalfEdge = edgeQueryIds.get(result.from -> result.queryId) match {
      case None => return false // can happen if a subscription cancellation races a new result
      case Some(he) => he
    }

    val outputResultId = ResultId.fresh()
    val resultToCache = outputResultId -> result.result
    edgesWatched(correspondingHalfEdge)._2 += (result.resultId -> resultToCache)
    effectHandler.reportNewResult(outputResultId, result.result)

    true
  }

  override def onCancelledSubscriptionResult(
    result: CancelMultipleValuesResult,
    effectHandler: MultipleValuesStandingQueryEffects
  ): Boolean = {
    val correspondingHalfEdge: HalfEdge = edgeQueryIds.get(result.from -> result.queryId) match {
      case None => return false // can happen if a subscription cancellation races a cancelled result
      case Some(he) => he
    }

    // Find the results across this edge and invalidate them
    val invalidated = edgesWatched(correspondingHalfEdge)._2.remove(result.resultId).get._1
    effectHandler.cancelOldResult(invalidated)

    true
  }

  def replayResults(localProperties: Properties): Map[ResultId, QueryContext] =
    edgesWatched.values.flatMap(_._2.values).toMap
}

/** State needed to process a [[MultipleValuesStandingQuery.EdgeSubscriptionReciprocal]]
  *
  * Since reciprocal queries are generated on the fly in [[SubscribeAcrossEdgeState]], they won't
  * show up when you try to look them up by ID globally. This is why this state inlines fields from
  * [[MultipleValuesStandingQuery.EdgeSubscriptionReciprocal]], but only stores an ID for the `andThenId`.
  *
  * @param queryPartId the ID of the edge-subscript-reciprocal query with this State
  * @param halfEdge the half-edge descriptor to match on replay -- this should match the query's half-edge
  * @param currentlyMatching is there currently a reciprocal half edge?
  * @param reverseResultDependency mapping from received subquery result ID to what was emitted
  * @param andThenId ID of the standing query part following the completion of this cross-edge match
  */
final case class EdgeSubscriptionReciprocalState(
  queryPartId: MultipleValuesStandingQueryPartId,
  halfEdge: HalfEdge,
  var currentlyMatching: Boolean,
  reverseResultDependency: mutable.Map[ResultId, (ResultId, QueryContext)],
  andThenId: MultipleValuesStandingQueryPartId
) extends MultipleValuesStandingQueryState {

  type StateOf = MultipleValuesStandingQuery.EdgeSubscriptionReciprocal

  private[this] var andThen: MultipleValuesStandingQuery = _

  override def preStart(effectHandler: MultipleValuesStandingQueryLookupInfo): Unit =
    andThen = effectHandler.lookupQuery(andThenId)

  override def relevantEvents: Seq[StandingQueryLocalEvents.Edge] = Seq(
    StandingQueryLocalEvents.Edge(Some(halfEdge.edgeType))
  )

  override def onNodeEvents(
    events: Seq[NodeChangeEvent],
    effectHandler: MultipleValuesStandingQueryEffects
  ): Boolean = {
    var somethingChanged = false
    events.foreach {
      case EdgeAdded(newHalfEdge) if halfEdge == newHalfEdge =>
        currentlyMatching = true
        effectHandler.createSubscription(effectHandler.node, andThen)
        somethingChanged = true

      case EdgeRemoved(oldHalfEdge) if halfEdge == oldHalfEdge =>
        currentlyMatching = false
        effectHandler.cancelSubscription(effectHandler.node, andThenId)

        // Invalidate all results
        for ((_, (resultId, _)) <- reverseResultDependency)
          effectHandler.cancelOldResult(resultId)
        reverseResultDependency.clear()

        somethingChanged = true

      case _ => // Ignore
    }
    somethingChanged
  }

  override def onShutdown(effectHandler: MultipleValuesStandingQueryEffects): Unit =
    if (currentlyMatching) {
      effectHandler.cancelSubscription(effectHandler.node, andThenId)
    }

  override def onNewSubscriptionResult(
    result: NewMultipleValuesResult,
    effectHandler: MultipleValuesStandingQueryEffects
  ): Boolean = {
    if (!currentlyMatching) return false

    val outputResultId = ResultId.fresh()
    reverseResultDependency += result.resultId -> (outputResultId -> result.result)
    effectHandler.reportNewResult(outputResultId, result.result)

    true
  }

  override def onCancelledSubscriptionResult(
    result: CancelMultipleValuesResult,
    effectHandler: MultipleValuesStandingQueryEffects
  ): Boolean = {
    if (currentlyMatching) return false

    if (!reverseResultDependency.contains(result.resultId)) {
      logger.error(s"$this received subscription result it didn't subscribe to: $result")
      return false
    }

    // Find the results across this edge and invalidate them
    val invalidated = reverseResultDependency.remove(result.resultId).get._1
    effectHandler.cancelOldResult(invalidated)

    true
  }

  def replayResults(localProperties: Properties): Map[ResultId, QueryContext] =
    reverseResultDependency.values.toMap
}

/** State needed to process a [[MultipleValuesStandingQuery.FilterMap]]
  *
  * @param queryPartId the ID of the filter/map query with this State
  * @param keptResults map of input result IDs that passed the filter to output result IDs
  */
final case class FilterMapState(
  queryPartId: MultipleValuesStandingQueryPartId,
  keptResults: mutable.Map[ResultId, (ResultId, QueryContext)]
) extends MultipleValuesStandingQueryState
    with CachableQueryMultipleValues {
  type StateOf = MultipleValuesStandingQuery.FilterMap

  override def onInitialize(effectHandler: MultipleValuesStandingQueryEffects): Unit =
    effectHandler.createSubscription(effectHandler.node, query.toFilter)

  override def onShutdown(effectHandler: MultipleValuesStandingQueryEffects): Unit =
    effectHandler.cancelSubscription(effectHandler.node, query.toFilter.id)

  override def onNewSubscriptionResult(
    result: NewMultipleValuesResult,
    effectHandler: MultipleValuesStandingQueryEffects
  ): Boolean = {
    // Does the result pass the filter?
    val passesFilter = query.condition.fold(true) { (cond: Expr) =>
      // TODO: parameters
      cond.eval(result.result)(effectHandler.idProvider, Parameters.empty) match {
        case Expr.True => true
        case Expr.List(l) => l.nonEmpty
        case _ => false
      }
    }

    if (!passesFilter) return false

    // Construct the output result
    val initial = if (query.dropExisting) QueryContext.empty else result.result
    val newResult = query.toAdd.foldLeft(initial) { case (acc, (aliasedAs, exprToAdd)) =>
      acc + (aliasedAs -> exprToAdd.eval(result.result)(
        effectHandler.idProvider,
        Parameters.empty
      ))
    }

    val outputResultId = ResultId.fresh()
    effectHandler.reportNewResult(outputResultId, newResult)
    keptResults += (result.resultId -> (outputResultId -> newResult))

    true
  }

  override def onCancelledSubscriptionResult(
    result: CancelMultipleValuesResult,
    effectHandler: MultipleValuesStandingQueryEffects
  ): Boolean =
    keptResults.remove(result.resultId) match {
      // the result had been filtered out
      case None => false

      // the result needs to be invalidatedd
      case Some((invalidated: ResultId, _)) =>
        effectHandler.cancelOldResult(invalidated)
        true
    }

  def replayResults(localProperties: Properties): Map[ResultId, QueryContext] =
    keptResults.values.toMap
}
