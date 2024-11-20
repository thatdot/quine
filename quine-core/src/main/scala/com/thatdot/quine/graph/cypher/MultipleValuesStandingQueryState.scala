package com.thatdot.quine.graph.cypher

import scala.annotation.unused
import scala.collection.{View, mutable}

import com.thatdot.quine.graph.EdgeEvent.{EdgeAdded, EdgeRemoved}
import com.thatdot.quine.graph.PropertyEvent.{PropertyRemoved, PropertySet}
import com.thatdot.quine.graph.cypher.LabelsState.extractLabels
import com.thatdot.quine.graph.messaging.StandingQueryMessage.NewMultipleValuesStateResult
import com.thatdot.quine.graph.{MultipleValuesStandingQueryPartId, NodeChangeEvent, PropertyEvent, WatchableEventType}
import com.thatdot.quine.model
import com.thatdot.quine.model.{HalfEdge, Properties, PropertyValue, QuineId, QuineIdProvider, QuineType, QuineValue}
import com.thatdot.quine.util.Log._
import com.thatdot.quine.util.Log.implicits._

/** The stateful component of a standing query, holding on to the information necessary for:
  *
  *   - Recording subscribers to this node and the query for which they are interested in receiving results
  *   - issuing subqueries
  *   - caching results to those subqueries
  *   - reporting new results
  *
  * A StandingQueryState is uniquely defined by the product of: (QuineId, globalSqId, and queryPartId).
  * The QuineId portion of that is maintained on the node, and thus from the node's perspective, it manages a collection
  * of states defined by (globalSqId, queryPartId). The node maintains a Map in `multipleValuesStandingQueries` of
  * (globalSqId, queryPartId) -> (subscribers, state)  Each of those "states" maintains a cache of subquery results.
  * When a new result comes in for the subquery, the cache is updated. Results are sent out from each state in the case
  * of two kind of events: 1.) a new result comes in that is different than the result previously sent; 2.) a change to
  * this node occurs (via NodeChangeEvent) which causes a meaningful alteration of the locally cached results (e.g. a
  * property changes).
  *
  * Performance note: There are very likely a *lot* of these in memory at a given time. Therefore, every effort should
  * be made to keep the in-memory size of instances small. For example, rather than serializing and reconstructing the
  * StandingQuery instance associated with a State (which would create multiple identical copies of the same Query
  * objects in memory) the States leverage a global registry of StandingQuery instances, and only serialize as much
  * information as necessary to produce results when requested.
  *
  * All operations on these classes must be done on an Actor within the single-threaded flow of message processing.
  * These operations **are not thread safe**.
  */
sealed abstract class MultipleValuesStandingQueryState extends LazySafeLogging {

  /** Type of standing query from which this state was created
    *
    * For any `S <: StandingQuery` and `sq: S`, it should be the case that
    * `sq.createState().StateOf =:= S`. In other words `StandingQueryState#StateOf`
    * is the inverse of `StandingQuery#State`.
    */
  type StateOf <: MultipleValuesStandingQuery

  /** Refers to a [[MultipleValuesStandingQuery]] in the system's cache. `def query` may be safely used in any
    * other function.
    */
  protected var _query: StateOf = _ // late-init
  def query: StateOf = _query // readonly access for implementations

  /** the ID of the StandingQuery (part) associated with this state */
  def queryPartId: MultipleValuesStandingQueryPartId

  /** Non-overlapping group of possible node event categories that this state wants to be notified of */
  def relevantEventTypes(labelsPropertyKey: Symbol): Seq[WatchableEventType] = Seq.empty

  /** Called on state creation or deserialization/wakeup, before `onInitialize` or any other external events/results.
    *
    * This is used to rehydrate fields which we don't want serialized.
    */
  def rehydrate(effectHandler: MultipleValuesStandingQueryLookupInfo)(implicit @unused logConfig: LogConfig): Unit =
    // Cast here is safe thanks to the invariant documented on [[StateOf]]
    _query = effectHandler.lookupQuery(queryPartId).asInstanceOf[StateOf]

  /** Called the first time the state is created (but not when it is merely being woken up).
    * This SHOULD NOT emit any results derived from state that may change as a result of new events,
    * but MUST emit any results derived from state that is known to never change (i.e., the node's ID
    * or a Unit state result). The code that materializes this state will also compute the relevant
    * initial events to issue to this state, and explicitly call [[onNodeEvents]]: see the behavior
    * for [[CreateMultipleValuesStandingQuerySubscription]] messages.
    */
  def onInitialize(effectHandler: MultipleValuesStandingQueryEffects): Unit = ()

  /** Process node events. Note that this may re-report any results that were previously reported,
    * and is guaranteed to do so during node wake, if there is any relevant state in the node's properties/edges.
    *
    * Always called on the node's thread
    *
    * @param events which node-events happened (after node-side deduplication against current node state)
    *               NB: multiple edge events within the same batch are no longer [1] deduplicated against
    *               one another, but property events still are [2]
    * @see https://github.com/thatdot/quine-plus/pull/2280#discussion_r1115372792
    * @see https://github.com/thatdot/quine-plus/pull/2522
    * @param effectHandler handler for external effects
    * @return whether the standing query state was updated (eg. is there anything new to save?)
    */
  def onNodeEvents(
    events: Seq[NodeChangeEvent],
    effectHandler: MultipleValuesStandingQueryEffects,
  )(implicit logConfig: LogConfig): Boolean = false

  /** Called when one of the sub-queries delivers a new result
    *
    * @param result subscription result
    * @param effectHandler handler for external effects
    * @return whether the standing query state was updated (eg. is there anything new to save?)
    */
  def onNewSubscriptionResult(
    result: NewMultipleValuesStateResult,
    effectHandler: MultipleValuesStandingQueryEffects,
  )(implicit logConfig: LogConfig): Boolean = false

  /** Read the current results for this SQ state.
    *
    * @note passing in the current node properties is done to enable some storage optimizations. Be aware that
    *       this will return results according to the properties that are passed in -- which may differ from the
    *       properties returned by `effectHandler.currentProperties`
    *
    * INV: this returns the same rows as the last call to `effectHandler.reportUpdatedResults` made by either
    *      `onNewSubscriptionResult` or `onNodeEvents`.
    *
    * @param localProperties current local node properties, including the labels property (labelsKey), which is not
    *                        seen by the ad-hoc cypher interpreter
    * @param labelsPropertyKey       the property key used to store labels on a node, according to startup-time configuration
    * @return Accumulated results at this moment.
    *         `None` when the internal state has not yet received/produced a result (e.g. still waiting for subqueries).
    *         `Some(Seq.empty)` when a result was produced but yielded no results
    *         `Some(Seq(...))` when accumulated results have been resolved into Seq of resulting rows according to whatever
    *         the StandingQueryState is meant to compute from its cached state.
    */
  def readResults(localProperties: Properties, labelsPropertyKey: Symbol)(implicit
    logConfig: LogConfig,
  ): Option[Seq[QueryContext]]

  def pretty(implicit @unused idProvider: QuineIdProvider): String = this.toString
}

trait MultipleValuesStandingQueryLookupInfo {

  /** Get a [[MultipleValuesStandingQuery]] instance from the current graph
    *
    * @param queryPartId the identifier for a subquery saved in the system's standing query registry
    * @return the relevant subquery for this standing query part ID
    */

  @throws[NoSuchElementException]("When a MultipleValuesStandingQueryPartId is not known to this graph")
  def lookupQuery(queryPartId: MultipleValuesStandingQueryPartId): MultipleValuesStandingQuery

  /** Current node */
  val executingNodeId: QuineId

  /** ID provider */
  val idProvider: QuineIdProvider
}

/** Limited scope of actions that a [[MultipleValuesStandingQueryState]] is allowed to make */
trait MultipleValuesStandingQueryEffects extends MultipleValuesStandingQueryLookupInfo {

  /** @return a readonly view on the current node properties, including the labels property, which is not seen by the
    *         ad-hoc cypher interpreter. Includes updates made as a result of the event that triggered MVSQ-related
    *         work.
    */
  def currentProperties: Map[Symbol, model.PropertyValue]

  /** @return The property key used to store labels on a node
    */
  def labelsProperty: Symbol

  /** Issue a subscription to a node
    *
    * @param onNode node to which the subscription is delivered
    * @param query standing query whose results are being subscribed to
    */
  def createSubscription(onNode: QuineId, query: MultipleValuesStandingQuery): Unit

  /** Cancel a previously issued subscription. This method call is only initiated if an edge is removed, causing the
    * tree of subqueries to become selectively irrelevant, and cancelled recursively. This method is not called when a
    * standing query is cancelled.
    *
    * @param onNode node to which the cancellation is delivered
    * @param queryId ID of the standing query whose results were being subscribed to
    */
  def cancelSubscription(onNode: QuineId, queryId: MultipleValuesStandingQueryPartId): Unit

  /** Report a new or updated result
    *
    * @param resultGroup Each item in the sequence represents on "row" of results.
    *               (may be concatenated, appended, or crossed later with other results)
    */
  def reportUpdatedResults(resultGroup: Seq[QueryContext]): Unit
}

/** State needed to process a [[MultipleValuesStandingQuery.UnitSq]]
  *
  * Algebraically, acts as an emitter for the 0-value for the cross product operation.
  * Contextually, this is only ever used as the far side of a SubscribeAcrossEdge, eg in the pattern:
  *
  * MATCH (a)-->() WHERE a.x = 1 RETURN a
  *
  * In such a case, the only thing we care about of the unnamed node is that it exists (and that its
  * half edge agrees with a's, but that concern is handled by the implicit EdgeSubscriptionReciprocal).
  *
  * In other words, this SQ's semantics are "confirm a node is here to run this SQ". This is so
  * similar to what LocalId does that we could eliminate UnitSq and UnitState by merging them in
  * to LocalId and LocalIdState.
  */
final case class UnitState() extends MultipleValuesStandingQueryState {
  type StateOf = MultipleValuesStandingQuery.UnitSq

  def queryPartId: MultipleValuesStandingQueryPartId = MultipleValuesStandingQuery.UnitSq.instance.queryPartId

  /** There is only one possible result. It represents a positive result (1 row) with no data. It should not be only
    * `Nil` because it should be able to be combined with other results in `Cross` with no effect.
    * Not persisted.
    */
  private val resultGroup = Seq(QueryContext.empty)

  override def onInitialize(
    effectHandler: MultipleValuesStandingQueryEffects,
  ): Unit =
    // this state is more similar to an local event driven state than a subquery-driven state,
    // so we report an initial result (per the scaladoc on [[super]])
    effectHandler.reportUpdatedResults(resultGroup)

  /** There is only one unit query, and we don't need to do a lookup to know its value. */
  override def rehydrate(effectHandler: MultipleValuesStandingQueryLookupInfo)(implicit logConfig: LogConfig): Unit =
    _query = MultipleValuesStandingQuery.UnitSq.instance

  def readResults(localProperties: Properties, labelsPropertyKey: Symbol)(implicit
    logConfig: LogConfig,
  ): Some[Seq[QueryContext]] = Some(resultGroup)
}

/** Produce a Cartesian product from a sequence of subqueries. The subscriptions for subqueries might be emitted lazily.
  *
  * State needed to process a [[MultipleValuesStandingQuery.Cross]]
  *
  * @param queryPartId the ID of the cross-product query with this State
  */
final case class CrossState(
  queryPartId: MultipleValuesStandingQueryPartId,
) extends MultipleValuesStandingQueryState {

  type StateOf = MultipleValuesStandingQuery.Cross

  /** Internally cached state accumulated by this SQ State component. Persisted. */
  val resultsAccumulator: mutable.Map[MultipleValuesStandingQueryPartId, Option[Seq[QueryContext]]] = mutable.Map.empty

  private def subscriptionsEmittedCount: Int = resultsAccumulator.size

  /** Initialization for a `Cross` is a matter of issuing subscriptions to other nodes for subqueries.
    * As an optimization, this uses the `emitSubscriptionsLazily` value to emit only the first subscription on init.
    * When `emitSubscriptionsLazily` is `true`, new subscriptions for subsequent subqueries will be emitted only when
    * there is one or more result returned for the prior query. This works because a Cartesian product that crosses any
    * size collection with an empty set will itself always be empty. Additional subqueries are added in
    * `def onNewSubscriptionResult`.
    */
  override def onInitialize(
    effectHandler: MultipleValuesStandingQueryEffects,
  ): Unit =
    for (sq <- if (query.emitSubscriptionsLazily) query.queries.view.take(1) else query.queries.view) {
      // In a `Cross`, `createSubscription` always ends up going to the same node as the Cross itself,
      // so we don't need to store the QuineId.
      effectHandler.createSubscription(effectHandler.executingNodeId, sq)
      resultsAccumulator += (sq.queryPartId -> None)
    }

  /** An internal optimization to track whether this state is ready to report results--because it has received at
    * least one result for each subquery. This transition from `false` to `true` is always monotonic.
    */
  object isReadyToReport {
    private[this] var isReadyToReportState = false
    def apply(): Boolean = isReadyToReportState || { // short-circuits if `true`
      val haveOneResultPerSubquery = resultsAccumulator.values.forall(_.isDefined) // avoid iterating this if possible!
      if (haveOneResultPerSubquery) isReadyToReportState = true
      haveOneResultPerSubquery
    }
  }

  override def onNewSubscriptionResult(
    result: NewMultipleValuesStateResult,
    effectHandler: MultipleValuesStandingQueryEffects,
  )(implicit logConfig: LogConfig): Boolean =
    resultsAccumulator.get(result.queryPartId) match {
      case None =>
        logger.error {
          val subscriptions = resultsAccumulator.keys
            .mkString("[", ",", "]")
          log"""MVSQ CrossState: ${this.toString} for SQ part: $query received subscription result: $result not
               |in the list of subscriptions: ${Safe(subscriptions)}""".cleanLines
        }
        false
      case Some(previousResultsFromChild) =>
        if (subscriptionsEmittedCount != query.queries.length) {
          // NB query.emitSubscriptionsLazily must be true if we made it here

          // Which index (in the query list) does this result correspond to?
          def queryIdxForResult: Int = query.queries.indexWhere(_.queryPartId == result.queryPartId)

          if (queryIdxForResult == subscriptionsEmittedCount - 1) {
            // If this is the first result for the most recently-emitted subscription, make sure another subscription has
            // been emitted for the NEXT query (because of the `emitSubscriptionsLazily` optimization).
            val nextSubscriptionQuery = query.queries(subscriptionsEmittedCount)
            effectHandler.createSubscription(effectHandler.executingNodeId, nextSubscriptionQuery)
            resultsAccumulator += (nextSubscriptionQuery.queryPartId -> None) // Add new subscription with empty result.
          }

          // Don't bother trying to build up cross-product results - all subscriptions haven't been emitted yet!
          // Instead, just cache the result and wait for the next one.
          resultsAccumulator += (result.queryPartId -> Some(result.resultGroup)) // Cache the newly arrived result.
        } else { // All subscriptions have been issued
          resultsAccumulator += (result.queryPartId -> Some(result.resultGroup)) // Cache the newly arrived result.
          val isNewResultGroup = !previousResultsFromChild.contains(result.resultGroup)
          // Report results only if this result is new, and only when we have at least one result received for each subquery.
          if (isNewResultGroup && isReadyToReport()) {
            generateCrossProductResults.foreach(effectHandler.reportUpdatedResults)
          }
        }
        true
    }

  private[this] def generateCrossProductResults: Option[List[QueryContext]] = {
    import cats.implicits._
    val results: List[Option[Seq[QueryContext]]] = resultsAccumulator.values.toList
    // first, fish out any None value. This would mean we haven't yet gotten results
    // from all subqueries. If everything is Some, we're good to continue.
    val resultsOrNone: Option[List[Seq[QueryContext]]] = results.sequence

    resultsOrNone.map { resultsFromAllChildren: List[Seq[QueryContext]] =>
      resultsFromAllChildren.foldLeft(
        // Before considering any subqueries, but knowing we want to emit a match,
        // start with a single, empty row
        List(QueryContext.empty),
      ) { case (allRowsFromCombiningEarlierChildQueries, nextResultGroup) =>
        // We're working through the child queries one by one, accumulating the cross product into the first argument.
        // One by one, each child query's results gets a turn being the `nextResultGroup`, at which time, we
        // zip each row from the previous cross product with each row from the new result group.
        for {
          rowSoFar: QueryContext <- allRowsFromCombiningEarlierChildQueries
          newResultRowAddition: QueryContext <- nextResultGroup
        } yield rowSoFar ++ newResultRowAddition
      }
    }
  }

  def readResults(localProperties: Properties, labelsPropertyKey: Symbol)(implicit
    logConfig: LogConfig,
  ): Option[Seq[QueryContext]] =
    if (subscriptionsEmittedCount == query.queries.length && isReadyToReport()) generateCrossProductResults
    else None
}

final case class AllPropertiesState(queryPartId: MultipleValuesStandingQueryPartId)
    extends MultipleValuesStandingQueryState {

  /** NB not serialized. We know that properties can only change when the node is awake, so
    * we don't need to record the last-known properties when the node goes to sleep.
    * Because we don't explicitly rehydrate this, the first call to [[onNodeEvents]]
    * will duplicate the last result set reported. This is okay, as the event will be
    * deduplicated by the subscriber (or, at least, some subscriber upstream -- at worst,
    * the [[MultipleValuesResultsReporter]] will deduplicate it).
    */
  private[this] var lastReportedProperties: Option[Properties] = None

  override type StateOf = MultipleValuesStandingQuery.AllProperties

  private def projectProperties(properties: Properties, labelsPropertyKey: Symbol): View[(String, Value)] =
    properties.view.collect {
      case (k, v) if k != labelsPropertyKey =>
        k.name -> v.deserialized.fold[Value](_ => Expr.Null, qv => Expr.fromQuineValue(qv))
    }

  private def propertiesAsCypher(properties: Properties, labelsPropertyKey: Symbol): Expr.Map =
    Expr.Map(projectProperties(properties, labelsPropertyKey))

  override def relevantEventTypes(labelsPropertyKey: Symbol): Seq[WatchableEventType] = Seq(
    // This will slightly overtrigger, as it will include changes to the labels property, but that's okay.
    WatchableEventType.AnyPropertyChange,
  )

  /** NB this rolls up all property-related changes in [[events]] into one downstream event. Alternatively, we _could_
    * emit one downstream event per incoming event, but since Cross et al is already the default mode of event
    * combination, this could quickly spiral out of control.
    *
    * Ex:
    * `MATCH (n) SET n = {hello: "world", fizz: "buzz"}` will cause a single SQ match with the map
    * `{hello: "world", fizz: "buzz"}`, rather than 2 matches, one with `{hello: "world"}` and one with `{fizz: "buzz"}`
    */
  override def onNodeEvents(
    events: Seq[NodeChangeEvent],
    effectHandler: MultipleValuesStandingQueryEffects,
  )(implicit logConfig: LogConfig): Boolean = {
    val somePropertyChanged = events.exists {
      case pe: PropertyEvent if pe.key != effectHandler.labelsProperty => true
      case _ => false
    }

    if (somePropertyChanged) {
      // The events contained a property update, so confirm that the set of properties really did change since our
      // last recorded report
      val previousProperties = lastReportedProperties
      lastReportedProperties = Some(effectHandler.currentProperties)
      if (previousProperties == lastReportedProperties) {
        // the result has not changed, no need to report. This case is only expected when the node is first woken up.
        false
      } else {
        val result = QueryContext.empty + (query.aliasedAs -> propertiesAsCypher(
          lastReportedProperties.get,
          effectHandler.labelsProperty,
        ))
        effectHandler.reportUpdatedResults(result :: Nil)
        true
      }
    } else {
      // The events had no changes to properties, so do nothing
      false
    }

  }

  override def onNewSubscriptionResult(
    result: NewMultipleValuesStateResult,
    effectHandler: MultipleValuesStandingQueryEffects,
  )(implicit logConfig: LogConfig): Boolean = {
    logger.warn(
      log"""MVSQ state: ${this.toString} for Part ID: ${Safe(queryPartId)} received subscription
           |result it didn't subscribe to: $result""".cleanLines,
    )
    false
  }

  def readResults(localProperties: Properties, labelsPropertyKey: Symbol)(implicit
    logConfig: LogConfig,
  ): Some[Seq[QueryContext]] = Some(
    (QueryContext.empty + (query.aliasedAs -> propertiesAsCypher(localProperties, labelsPropertyKey))) :: Nil,
  )
}

/** Returns data from local properties. It completes immediately and always succeeds.
  * It issues no subquery subscriptions.
  *
  * State needed to process a [[MultipleValuesStandingQuery.LocalProperty]]
  *
  * @param queryPartId the ID of the local property query with this State
  */
final case class LocalPropertyState(
  queryPartId: MultipleValuesStandingQueryPartId,
) extends MultipleValuesStandingQueryState {

  type StateOf = MultipleValuesStandingQuery.LocalProperty

  /** The value of the watched property as of the last time we made a report
    * This is either:
    * None: we have not yet made a report since registering/waking the query
    * Some(None): our last report was based on the property being absent
    * Some(Some(value)): our last report was based on the property having the given value
    *
    * NB on Null: It should not be possible to write a property with the Null value because
    * the only interpreter that can write values (the ad-hoc cypher query interpreter) considers
    * SETing a property to NULL to have the semantics of removing the property. However, this
    * Standing Query is designed to be agnostic to the ad-hoc interpreter, and so will consider
    * Null a valid, present value, distinct from the absence of the property. This means that a
    * property with a Null value will be represented as Some(Some(Null)) in this state.
    *
    * NB not serialized. We know that properties can only change when the node is awake, so
    * we don't need to record the last-known properties when the node goes to sleep.
    * Because we don't explicitly rehydrate this, the first call to [[onNodeEvents]]
    * will duplicate the last result set reported. This is okay, as the event will be
    * deduplicated by the subscriber (or, at least, some subscriber upstream -- at worst,
    * the [[MultipleValuesResultsReporter]] will deduplicate it).
    *
    * Not persisted, but will be appropriately initialized by first call to [[onNodeEvents]]
    */
  var valueAtLastReport: Option[Option[model.PropertyValue]] = None

  /** Whether we have affirmatively matched based on [[valueAtLastReport]].
    * If we haven't yet reported since registering/waking, this is false.
    *
    * Not persisted, but will be appropriately initialized by first call to [[onNodeEvents]]
    */
  var lastReportWasAMatch: Boolean = false

  override def relevantEventTypes(labelsPropertyKey: Symbol): Seq[WatchableEventType.PropertyChange] = {
    if (query.propKey == labelsPropertyKey) {
      logger.warn(
        safe"""LocalProperty MultipleValues standing query part with ID $queryPartId is configured to watch the labels
              |property (`${Safe(labelsPropertyKey)}`). This is not supported and may result in lost or inconsistent
              |matches for this standing query. To fix this warning, if your query does not explicitly refer to
              |`${Safe(labelsPropertyKey)}`, please re-register it. If your query does, either choose a different
              |property name for your standing query, or else or change the `quine.labels-property` configuration
              |setting.""".cleanLines,
      )
    }
    Seq(
      WatchableEventType.PropertyChange(query.propKey),
    )
  }

  override def onNodeEvents(
    events: Seq[NodeChangeEvent],
    effectHandler: MultipleValuesStandingQueryEffects,
  )(implicit logConfig: LogConfig): Boolean = {
    require(
      events.collect { case pe: PropertyEvent if pe.key == query.propKey => pe }.drop(1).isEmpty,
      "Invariant violated: MVSQ received multiple events for the same property key in the same batch",
    )

    // NB by the scaladoc on [[super]], there is only one (or zero) property event that will affect [[query.propKey]]
    val relevantChange: Option[PropertyEvent] = events.collectFirst {
      case pe: PropertyEvent if pe.key == query.propKey => pe
    }
    relevantChange
      .map { event =>
        val currentProperty: Option[PropertyValue] = event match {
          case PropertySet(_, value) => Some(value)
          case PropertyRemoved(_, _) => None
        }
        lazy val currentPropertyDoesMatch = currentProperty match {
          case Some(value) => query.propConstraint(value.deserialized.get)
          case None => query.propConstraint.satisfiedByNone
        }

        val somethingChanged = query.aliasedAs match {
          case Some(alias) =>
            // the query cares about all changes to the property, even those that bring it from matching to still matching
            if (!valueAtLastReport.contains(currentProperty) && currentPropertyDoesMatch) {
              val currentPropertyExpr =
                currentProperty
                  .map(pv =>
                    // assume the value is a QuineValue
                    pv.deserialized.map(Expr.fromQuineValue).get,
                  )
                  .getOrElse(Expr.Null)
              val result = QueryContext.empty + (alias -> currentPropertyExpr)
              lastReportWasAMatch = true
              effectHandler.reportUpdatedResults(result :: Nil)
              true // we issued a new result
            } else if (valueAtLastReport.contains(currentProperty)) {
              // the property hasn't actually changed, so we don't need to do anything
              false
            } else if (valueAtLastReport.isEmpty) {
              // we haven't yet reported whether we match, but we don't -- send a report.
              // lastReportWasAMatch = false is unnecessary, because it initializes as false.
              effectHandler.reportUpdatedResults(Nil)
              true // we issued a new result
            } else if (lastReportWasAMatch) {
              // we used to match but no longer do -- cancel the previous positive result
              lastReportWasAMatch = false
              effectHandler.reportUpdatedResults(Nil)
              true // we issued a new result
            } else {
              // we didn't previously match and we still don't, nothing to do.
              false
            }
          case None =>
            // the query only cares about changes that bring the property from not matching to matching or vice versa
            if (lastReportWasAMatch != currentPropertyDoesMatch) {
              val resultGroup =
                if (currentPropertyDoesMatch) {
                  // we do match, but we didn't use to -- so emit one empty (but positive!) result.
                  lastReportWasAMatch = true
                  QueryContext.empty :: Nil
                } else {
                  // we don't match, but we used to -- so emit that nothing matches.
                  lastReportWasAMatch = false
                  Nil
                }
              effectHandler.reportUpdatedResults(resultGroup)
              true
            } else if (valueAtLastReport.isEmpty) {
              // send initial report that nothing matches
              // lastReportWasAMatch = false is unnecessary, because it initializes as false.
              effectHandler.reportUpdatedResults(Nil)
              true // we issued a new result
            } else {
              // nothing changed that we need to report - no-op.
              false
            }
        }
        valueAtLastReport = Some(currentProperty)
        somethingChanged
      }
      .getOrElse {
        // there was no relevant change. We only have something to do if the query wants results for null values.
        // As an optimization, we only do this if we haven't yet reported a result since waking. If this node is
        // waking up, this may be a duplicate event, but that will be deduplicated by the subscriber / result reporter.
        if (query.propConstraint.satisfiedByNone && valueAtLastReport.isEmpty) {
          val alwaysReportedPart: QueryContext = QueryContext.empty
          val aliasedPart: Iterable[(Symbol, Value)] = query.aliasedAs.map(_ -> Expr.Null)
          val result = Seq(alwaysReportedPart ++ aliasedPart)
          valueAtLastReport = Some(None)
          lastReportWasAMatch = true
          effectHandler.reportUpdatedResults(result)
          true // we issued a new result
        } else {
          // nothing changed that we need to report - no-op.
          false
        }
      }
  }

  override def onNewSubscriptionResult(
    result: NewMultipleValuesStateResult,
    effectHandler: MultipleValuesStandingQueryEffects,
  )(implicit logConfig: LogConfig): Boolean = {
    // this query issues no subscriptions, so ignore any results that come in from subscriptions
    logger.warn(
      log"""MVSQ LocalPropertyState: ${this.toString} for SQ part: $query received subscription
           |result it didn't subscribe to: $result""".cleanLines,
    )
    false
  }

  def readResults(localProperties: Properties, labelsPropertyKey: Symbol)(implicit
    logConfig: LogConfig,
  ): Some[Seq[QueryContext]] = Some {
    val theProperty: Option[Value] =
      localProperties
        .get(query.propKey)
        .map(_.deserialized.get) // Assume the value is a valid QuineValue
        .map(Expr.fromQuineValue)
    val currentPropertyValueMatches: Option[Boolean] = theProperty.map(query.propConstraint.apply)

    val currentPropertyStateMatches: Boolean =
      currentPropertyValueMatches.getOrElse(query.propConstraint.satisfiedByNone)

    if (!currentPropertyStateMatches) Nil
    else
      query.aliasedAs match {
        case Some(alias) => Seq(QueryContext(Map(alias -> theProperty.getOrElse(Expr.Null))))
        case None => Seq(QueryContext.empty)
      }
  }
}

final case class LabelsState(queryPartId: MultipleValuesStandingQueryPartId) extends MultipleValuesStandingQueryState {
  type StateOf = MultipleValuesStandingQuery.Labels

  /** The value of the labels as of the last time we made a report, or None if we have not
    * made a report since registering/waking.
    *
    * NB not serialized. We know that properties can only change when the node is awake, so
    * we don't need to record the last-known labels when the node goes to sleep.
    * Because we don't explicitly rehydrate this, the first call to [[onNodeEvents]]
    * will duplicate the last result set reported. This is okay, as the event will be
    * deduplicated by the subscriber (or, at least, some subscriber upstream -- at worst,
    * the [[MultipleValuesResultsReporter]] will deduplicate it).
    *
    * Not persisted, but will be appropriately initialized by first call to [[onNodeEvents]]
    */
  var lastReportedLabels: Option[Set[Symbol]] = None

  /** Whether we have affirmatively matched based on [[lastReportedLabels]].
    * If we haven't yet reported since registering/waking, this is false.
    *
    * Not persisted, but will be appropriately initialized by first call to [[onNodeEvents]]
    */
  var lastReportWasAMatch: Boolean = false

  override def relevantEventTypes(labelsPropertyKey: Symbol): Seq[WatchableEventType] = Seq(
    WatchableEventType.PropertyChange(labelsPropertyKey),
  )

  override def onNodeEvents(events: Seq[NodeChangeEvent], effectHandler: MultipleValuesStandingQueryEffects)(implicit
    logConfig: LogConfig,
  ): Boolean = {
    require(
      events.collect { case pe: PropertyEvent if pe.key == effectHandler.labelsProperty => pe }.drop(1).isEmpty,
      "Invariant violated: MVSQ received multiple events for the same node's labels in the same batch",
    )

    // NB by the scaladoc on [[super]], there is only one (or zero) property event that will affect [[query.propKey]]
    val relevantChange: Option[PropertyEvent] = events.collectFirst {
      case pe: PropertyEvent if pe.key == effectHandler.labelsProperty => pe
    }
    relevantChange
      .map { event =>
        val labelsValue: Option[QuineValue] = event match {
          case PropertySet(_, value) => Some(value.deserialized.get) // assume the value is a valid QuineValue
          case PropertyRemoved(_, _) => None
        }
        val currentLabels = extractLabels(labelsValue)
        lazy val matched = query.constraint(currentLabels)

        val somethingChanged: Boolean = query.aliasedAs match {
          case Some(alias) =>
            // the query cares about all changes to the node's labels, even those that bring it from matching to still
            // matching
            if (!lastReportedLabels.contains(currentLabels) && matched) {
              val labelsAsExpr = Expr.List(currentLabels.map(_.name).map(Expr.Str).toVector)
              val result = QueryContext.empty + (alias -> labelsAsExpr)
              lastReportWasAMatch = true
              effectHandler.reportUpdatedResults(result :: Nil)
              true // we issued a new result
            } else if (lastReportedLabels.contains(currentLabels)) {
              // the property hasn't actually changed, so we don't need to do anything
              false
            } else if (lastReportedLabels.isEmpty) {
              // we haven't yet reported whether we match, but we don't -- send a report.
              // lastReportWasAMatch = false is unnecessary, because it initializes as false.
              effectHandler.reportUpdatedResults(Nil)
              true // we issued a new result
            } else if (lastReportWasAMatch) {
              // we used to match but no longer do -- cancel the previous positive result
              lastReportWasAMatch = false
              effectHandler.reportUpdatedResults(Nil)
              true // we issued a new result
            } else {
              // we didn't use to match and we still don't, nothing to do.
              false
            }
          case None =>
            // the query only cares about the presence or absense of labels, not their values -- we only
            // need to send a report when we go from matching to not matching or visa versa
            if (lastReportWasAMatch != matched) {
              val resultGroup =
                if (matched) {
                  // we do match, but we didn't use to -- so emit one empty (but positive!) result.
                  lastReportWasAMatch = true
                  QueryContext.empty :: Nil
                } else {
                  // we don't match, but we used to -- so emit that nothing matches.
                  lastReportWasAMatch = false
                  Nil
                }
              effectHandler.reportUpdatedResults(resultGroup)
              true
            } else if (lastReportedLabels.isEmpty) {
              // send initial report that nothing matches
              // lastReportWasAMatch = false is unnecessary, because it initializes as false.
              effectHandler.reportUpdatedResults(Nil)
              true // we issued a new result
            } else {
              // nothing changed that we need to report - no-op.
              false
            }
        }

        lastReportedLabels = Some(currentLabels)
        somethingChanged
      }
      .getOrElse {
        // there was no relevant change. We only have something to do if the query wants results for empty labels.
        // As an optimization, we only do this if we haven't yet reported a result since waking. If this node is
        // waking up, this may be a duplicate event, but that will be deduplicated by the subscriber / result reporter.
        if (query.constraint.apply(Set.empty) && lastReportedLabels.isEmpty) {
          val alwaysReportedPart: QueryContext = QueryContext.empty
          val aliasedPart: Iterable[(Symbol, Value)] = query.aliasedAs.map(_ -> Expr.List.empty)
          val result = Seq(alwaysReportedPart ++ aliasedPart)
          lastReportedLabels = Some(Set.empty)
          lastReportWasAMatch = true
          effectHandler.reportUpdatedResults(result)
          true // we issued a new result
        } else {
          // nothing changed that we need to report - no-op.
          false
        }
      }
  }

  override def onNewSubscriptionResult(
    result: NewMultipleValuesStateResult,
    effectHandler: MultipleValuesStandingQueryEffects,
  )(implicit logConfig: LogConfig): Boolean = {
    // this query issues no subscriptions, so ignore any results that come in from subscriptions
    logger.warn(
      log"""MVSQ LabelsState: ${this.toString} for SQ part: $query received subscription
           |result it didn't subscribe to: $result""".cleanLines,
    )
    false
  }

  def readResults(localProperties: Properties, labelsPropertyKey: Symbol)(implicit
    logConfig: LogConfig,
  ): Some[Seq[QueryContext]] = Some {
    val labels = extractLabels(
      localProperties
        .get(labelsPropertyKey)
        .map(_.deserialized.get), // assume the value is a valid QuineValue
    )

    val matched = query.constraint(labels)

    if (!matched) Nil
    else {
      query.aliasedAs match {
        case Some(alias) => Seq(QueryContext(Map(alias -> Expr.List(labels.map(_.name).map(Expr.Str).toVector))))
        case None => Seq(QueryContext.empty)
      }
    }
  }
}
object LabelsState extends LazySafeLogging {
  private def extractLabels(labelsProperty: Option[QuineValue])(implicit logConfig: LogConfig): Set[Symbol] =
    // type-checker needs some assistance here
    (labelsProperty: Iterable[QuineValue]).flatMap {
      case QuineValue.List(labels) =>
        labels.flatMap {
          case QuineValue.Str(label) => Seq(Symbol(label))
          case other =>
            logger.warn(
              log"""Parsing labels from property: ${Safe(labelsProperty)} failed. Expected ${QuineType.Str} but
                     |found: ${other.quineType} with value: $other. Discarding this value and using all
                     |${QuineType.Str} as labels""".cleanLines,
            )
            Seq.empty
        }
      case other =>
        logger.info(
          log"""Parsing labels property ${Safe(labelsProperty)} failed. Expected ${QuineType.List} of ${QuineType.Str}
                 |but found: ${other.quineType} with value: $other. Defaulting to no labels.""".cleanLines,
        )
        Seq.empty
    }.toSet
}

/** Returns the ID of the node receiving this. It completes immediately, always succeeds, and behaves essentially
  * like [[UnitState]] except that it stores a preference for string formatting.
  *
  * Note: the serialization code eliminates this state so that it isn't stored on disk.
  *
  * State needed to process a [[MultipleValuesStandingQuery.LocalId]]
  *
  * @param queryPartId the ID of the localId query with this State
  */
final case class LocalIdState(
  queryPartId: MultipleValuesStandingQueryPartId,
) extends MultipleValuesStandingQueryState {

  type StateOf = MultipleValuesStandingQuery.LocalId

  private var result: Seq[QueryContext] = _ // Set during `hydrate`

  override def rehydrate(effectHandler: MultipleValuesStandingQueryLookupInfo)(implicit logConfig: LogConfig): Unit = {
    super.rehydrate(effectHandler) // Sets `query`
    // Pre-compute the ID result value
    val idValue = if (query.formatAsString) {
      Expr.Str(effectHandler.idProvider.qidToPrettyString(effectHandler.executingNodeId))
    } else {
      Expr.fromQuineValue(effectHandler.idProvider.qidToValue(effectHandler.executingNodeId))
    }
    result = (QueryContext.empty + (query.aliasedAs -> idValue)) :: Nil
  }

  override def onInitialize(effectHandler: MultipleValuesStandingQueryEffects): Unit =
    // this result is not dependent on any events, so we must report it immediately
    effectHandler.reportUpdatedResults(result)

  def readResults(localProperties: Properties, labelsPropertyKey: Symbol)(implicit
    logConfig: LogConfig,
  ): Some[Seq[QueryContext]] = Some(result)
}

/** Issues the subquery across all edges which match the locally testable edge conditions. The reciprocal edge will be
  * checked on the other side with [[EdgeSubscriptionReciprocalState]].
  *
  * State needed to process a [[MultipleValuesStandingQuery.SubscribeAcrossEdge]]
  *
  * @param queryPartId the ID of the subscribe-across-edge query with this State
  */
final case class SubscribeAcrossEdgeState(
  queryPartId: MultipleValuesStandingQueryPartId,
) extends MultipleValuesStandingQueryState {

  type StateOf = MultipleValuesStandingQuery.SubscribeAcrossEdge

  /** The results for this query state are cached by the edges along which that result is produced. The value will be
    * `None` if a subscription has been made but no result received. If the value is `Some`, a response has been
    * received from the node at `_.other` on the HalfEdge key.
    *
    * The keys in this map are always a subset of what's in the node's `EdgeCollection`.
    *
    * Persisted.
    */
  val edgeResults: mutable.Map[HalfEdge, Option[Seq[QueryContext]]] = mutable.Map.empty

  override def relevantEventTypes(labelsPropertyKey: Symbol): Seq[WatchableEventType.EdgeChange] =
    Seq(WatchableEventType.EdgeChange(query.edgeName))

  private[this] def edgeMatchesPattern(halfEdge: HalfEdge): Boolean =
    query.edgeName.forall(_ == halfEdge.edgeType) &&
    query.edgeDirection.forall(_ == halfEdge.direction)

  override def onNodeEvents(
    events: Seq[NodeChangeEvent],
    effectHandler: MultipleValuesStandingQueryEffects,
  )(implicit logConfig: LogConfig): Boolean = {
    var somethingChanged = false
    events.foreach {
      case EdgeAdded(halfEdge) if edgeMatchesPattern(halfEdge) =>
        // Create a new subscription
        val freshEdgeQuery = MultipleValuesStandingQuery.EdgeSubscriptionReciprocal(
          halfEdge.reflect(effectHandler.executingNodeId),
          query.andThen.queryPartId,
          query.columns,
        )
        effectHandler.createSubscription(halfEdge.other, freshEdgeQuery)
        // Record that the subscription has been made, but no result (from the andThen via the reciprocal) yet.
        edgeResults += (halfEdge -> None)
        somethingChanged = true

      case EdgeRemoved(halfEdge) if edgeResults.contains(halfEdge) =>
        val oldResult: Option[Seq[QueryContext]] = edgeResults.remove(halfEdge).get
        effectHandler.cancelSubscription(halfEdge.other, query.andThen.queryPartId)

        if (oldResult.exists(_.nonEmpty)) {
          // There was (1) a result based on this edge, that (2) had rows we may want to cancel

          // NB this may not immediately issue a cancellation, if any other edges have not yet reported their results.
          // However, those edges should eventually report results, at which point this will issue a cancellation (and
          // any new matches from those edges)
          readResults(effectHandler.currentProperties, effectHandler.labelsProperty).foreach(
            effectHandler.reportUpdatedResults,
          )
        }

        somethingChanged = true

      case _ => () // Ignore all other events.
    }
    somethingChanged
  }

  override def onNewSubscriptionResult(
    result: NewMultipleValuesStateResult,
    effectHandler: MultipleValuesStandingQueryEffects,
  )(implicit logConfig: LogConfig): Boolean = {
    // Silently drop the result (with an empty `needsUpdate`) if we aren't expecting a result from `result.other`.
    // This can happen if the edge is removed (here first) then the other side reports no longer matching the reciprocal
    // TODO does this race during creation?
    val needsUpdate: Option[(HalfEdge, Option[Seq[QueryContext]])] =
      edgeResults.find { case (he, _) =>
        he.other == result.from && edgeMatchesPattern(he)
      }

    needsUpdate match {
      case Some((edge, oldResult)) if !oldResult.contains(result.resultGroup) =>
        edgeResults += (edge -> Some(result.resultGroup))
        readResults(effectHandler.currentProperties, effectHandler.labelsProperty).foreach(
          effectHandler.reportUpdatedResults,
        )
        true
      case Some(_) => false // we found a matching edge, but its result didn't change
      case _ => false // we found no matching edge
    }
  }

  def readResults(localProperties: Properties, labelsKey: Symbol)(implicit
    logConfig: LogConfig,
  ): Option[Seq[QueryContext]] =
    if (edgeResults.isEmpty) {
      // There are no matching edges, so there is an affirmative lack of matches
      Some(Nil)
    } else {
      // If we don't know about _any_ edge, we can't know our results
      lazy val existentialCheck =
        if (edgeResults.view.values.exists(maybeRows => maybeRows.isEmpty)) None
        else Some(edgeResults.view.values.flatten.flatten.toSeq)

      // Alternative semantics: If we don't know about some edges, we still know enough about the others to generate
      // a result
      @unused lazy val universalCheck =
        if (edgeResults.view.values.forall(_.isEmpty)) None
        else Some(edgeResults.view.values.flatten.flatten.toSeq)

      existentialCheck
    }

  // the result set of a SubscribeAcrossEdge, when defined, is the concatenation of all the result rows
  // from the all edges that could match the query's edge (because a MVSQ should report a row for each way
  // by which it matches)

  override def pretty(implicit idProvider: QuineIdProvider): String =
    s"${this.getClass.getSimpleName}($queryPartId, ${edgeResults.map { case (he, v) => he.pretty -> v }})"
}

/** Validates this concluding half edge side of the edge and propagates results back to the subscribing side when
  * available and when the edge is matching.
  *
  * State needed to process a [[MultipleValuesStandingQuery.EdgeSubscriptionReciprocal]]
  *
  * Since reciprocal queries are generated on the fly in [[SubscribeAcrossEdgeState]], they won't
  * show up when you try to look them up by ID globally. This is why this state inlines fields from
  * [[MultipleValuesStandingQuery.EdgeSubscriptionReciprocal]], but only stores an ID for the `andThenId`.
  *
  * @param queryPartId the ID of the edge-subscript-reciprocal query with this State
  * @param halfEdge the half-edge descriptor to match on replay -- this should match the query's half-edge
  * @param andThenId ID of the standing query part following the completion of this cross-edge match
  */
final case class EdgeSubscriptionReciprocalState(
  queryPartId: MultipleValuesStandingQueryPartId,
  halfEdge: HalfEdge,
  andThenId: MultipleValuesStandingQueryPartId,
) extends MultipleValuesStandingQueryState {
  require(
    queryPartId != andThenId,
    """Invariant violated: EdgeSubscriptionReciprocal had a matching andThen queryPartId and [self] queryPartId.
      |An EdgeSubscriptionReciprocal's original query should not also be that query's andThen.
      |""".stripMargin.replace('\n', ' '),
  )

  type StateOf = MultipleValuesStandingQuery.EdgeSubscriptionReciprocal

  /** Boolean to indicate whether there is currently a locally-matching reciprocal half edge. Persisted */
  var currentlyMatching: Boolean = false

  /** Saved state from `andThen` query. Persisted. */
  var cachedResult: Option[Seq[QueryContext]] = None // Result from the `andThen` query cached here.

  /** The subquery to run when the reciprocal edge has been verified. */
  private[this] var andThen: MultipleValuesStandingQuery = _

  override def rehydrate(
    effectHandler: MultipleValuesStandingQueryLookupInfo,
  )(implicit logConfig: LogConfig): Unit =
    // Do not call `super.preStart(effectHandler)` here because this `EdgeSubscriptionReciprocalState` is synthesized
    // and its `queryPartId` is not in the global registry.
    andThen = effectHandler.lookupQuery(andThenId)

  override def relevantEventTypes(labelsPropertyKey: Symbol): Seq[WatchableEventType.EdgeChange] = Seq(
    WatchableEventType.EdgeChange(
      Some(halfEdge.edgeType),
    ),
  )

  override def onNodeEvents(
    events: Seq[NodeChangeEvent],
    effectHandler: MultipleValuesStandingQueryEffects,
  )(implicit logConfig: LogConfig): Boolean = {
    var somethingChanged = false
    events.foreach {
      case EdgeAdded(newHalfEdge) if halfEdge == newHalfEdge =>
        currentlyMatching = true
        effectHandler.createSubscription(effectHandler.executingNodeId, andThen)
        somethingChanged = true
        readResults(effectHandler.currentProperties, effectHandler.labelsProperty).foreach(
          effectHandler.reportUpdatedResults,
        )

      case EdgeRemoved(oldHalfEdge) if halfEdge == oldHalfEdge =>
        currentlyMatching = false
        effectHandler.cancelSubscription(effectHandler.executingNodeId, andThenId)
        effectHandler.reportUpdatedResults(Nil)

        somethingChanged = true

      case _ => // Ignore
    }
    somethingChanged
  }

  override def onNewSubscriptionResult( // Happens when the subscription for the `andThen` returns a result
    result: NewMultipleValuesStateResult,
    effectHandler: MultipleValuesStandingQueryEffects,
  )(implicit logConfig: LogConfig): Boolean = {
    val resultIsUpdate = !cachedResult.contains(result.resultGroup)
    cachedResult = Some(result.resultGroup)
    // only propagate a result across an edge if that edge still exists, but cache the result regardless
    if (resultIsUpdate && currentlyMatching) effectHandler.reportUpdatedResults(result.resultGroup)
    resultIsUpdate
  }

  def readResults(localProperties: Properties, labelsPropertyKey: Symbol)(implicit
    logConfig: LogConfig,
  ): Option[Seq[QueryContext]] =
    if (currentlyMatching && cachedResult.isDefined) cachedResult else None

  override def pretty(implicit idProvider: QuineIdProvider): String =
    s"${this.getClass.getSimpleName}($queryPartId, ${halfEdge.pretty}, $currentlyMatching, ${cachedResult.map(_.mkString("[", ",", "]"))}, $andThenId)"
}

/** Filters incoming results (optionally) and transforms each result that passes the filter (optionally).
  * State needed to process a [[MultipleValuesStandingQuery.FilterMap]]
  *
  * @param queryPartId the ID of the filter/map query with this State
  */
final case class FilterMapState(
  queryPartId: MultipleValuesStandingQueryPartId,
) extends MultipleValuesStandingQueryState {

  type StateOf = MultipleValuesStandingQuery.FilterMap

  /** The results of this query state are cached here. Persisted.
    */
  var keptResults: Option[Seq[QueryContext]] = None

  override def onInitialize(effectHandler: MultipleValuesStandingQueryEffects): Unit =
    effectHandler.createSubscription(effectHandler.executingNodeId, query.toFilter)

  private var condition: QueryContext => Boolean = _ // Set during `rehydrate`
  private var mapper: QueryContext => QueryContext = _ // Set during `rehydrate`

  override def rehydrate(effectHandler: MultipleValuesStandingQueryLookupInfo)(implicit logConfig: LogConfig): Unit = {
    super.rehydrate(effectHandler)
    condition = query.condition.fold((r: QueryContext) => true) { (cond: Expr) => (r: QueryContext) =>
      cond.evalUnsafe(r)(effectHandler.idProvider, Parameters.empty, logConfig) == Expr.True
    }
    mapper = (row: QueryContext) =>
      query.toAdd.foldLeft(if (query.dropExisting) QueryContext.empty else row) { case (acc, (aliasedAs, exprToAdd)) =>
        acc + (aliasedAs -> exprToAdd.evalUnsafe(row)(
          effectHandler.idProvider,
          Parameters.empty,
          logConfig,
        ))
      }
  }

  override def onNewSubscriptionResult(
    result: NewMultipleValuesStateResult,
    effectHandler: MultipleValuesStandingQueryEffects,
  )(implicit logConfig: LogConfig): Boolean = {
    val newResults = result.resultGroup.collect {
      case row if condition(row) => mapper(row)
    }
    val isUpdated = !keptResults.contains(newResults)
    if (isUpdated) {
      effectHandler.reportUpdatedResults(newResults)
      keptResults = Some(newResults)
    }
    isUpdated
  }

  def readResults(localProperties: Properties, labelsPropertyKey: Symbol)(implicit
    logConfig: LogConfig,
  ): Option[Seq[QueryContext]] = keptResults

  override def pretty(implicit idProvider: QuineIdProvider): String =
    s"${this.getClass.getSimpleName}($queryPartId, ${keptResults.mkString("[", ",", "]")})"
}
