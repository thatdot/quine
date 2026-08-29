package com.thatdot.quine.graph.cypher

import scala.annotation.unused
import scala.collection.{View, mutable}

import com.thatdot.common.logging.Log.{LazySafeLogging, LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.EdgeEvent.{EdgeAdded, EdgeRemoved}
import com.thatdot.quine.graph.PropertyEvent.{PropertyRemoved, PropertySet}
import com.thatdot.quine.graph.cypher.LabelsState.extractLabels
import com.thatdot.quine.graph.messaging.StandingQueryMessage.{
  MultipleValuesStandingQuerySubscriber,
  NewMultipleValuesStateResult,
}
import com.thatdot.quine.graph.{MultipleValuesStandingQueryPartId, NodeChangeEvent, PropertyEvent, WatchableEventType}
import com.thatdot.quine.model
import com.thatdot.quine.model.{
  EdgeDirection,
  HalfEdge,
  Properties,
  PropertyValue,
  QuineIdProvider,
  QuineType,
  QuineValue,
}
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
  * information as necessary to produce results when requested. When data is omitted from serialization, it must be
  * managed according to the following criteria:
  * 1) the first call to [[onNodeEvents]] after node wake must set the object's internal state such that subsequent
  *    calls to [[onNodeEvents]] do not produce duplicate results
  * 2) [[readResults]] must return the correct results for the state at any point in time after the state
  *    initialization is completed, including after a node is re-awoken, even if the node does not run [[onNodeEvents]]
  *    again after waking
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

  /** The query this state runs, when it is one the graph knows by id.
    *
    * Empty for [[EdgeSubscriptionReciprocalState]], which is synthesized by the nodes that subscribe to it rather
    * than registered, and so inlines what it needs instead of resolving a query.
    */
  def resolvedQuery: Option[MultipleValuesStandingQuery] = Option(_query)

  /** the ID of the StandingQuery (part) associated with this state */
  def queryPartId: MultipleValuesStandingQueryPartId

  /** Non-overlapping group of possible node event categories that this state wants to be notified of */
  def relevantEventTypes(labelsPropertyKey: Symbol): Seq[WatchableEventType] = Seq.empty

  /** Of those, the categories whose replay of what the node already is tells this state something.
    *
    * Replaying the node to a state it has just created is how that state learns the node it was created on, so by
    * default this is everything the state watches. A state whose reaction to an event depends on something it has
    * not been told yet learns nothing that way, and naming no categories is what keeps a node from reading every
    * edge it has in order to hand each one to a method that will do nothing with it.
    */
  def initialEventTypes(labelsPropertyKey: Symbol): Seq[WatchableEventType] = relevantEventTypes(labelsPropertyKey)

  /** Called on state creation or deserialization/wakeup, before `onInitialize` or any other external events/results.
    *
    * This is used to rehydrate fields which we don't want serialized.
    */
  def rehydrate(effectHandler: MultipleValuesStandingQueryLookupInfo)(implicit @unused logConfig: LogConfig): Unit =
    // Cast here is safe thanks to the invariant documented on [[StateOf]]
    _query = effectHandler.lookupQuery(queryPartId).asInstanceOf[StateOf]

  /** Called the first time the state is created (but not when it is merely being woken up).
    * This MUST set any internal state so that the next call to [[readResults]] generates any results which do not
    * depend on node state (for example, a LocalId's result).
    * The code that materializes this state is architected to also compute the relevant initial events to issue to
    * this state, and explicitly call [[onNodeEvents]]: see the behavior for
    * [[CreateMultipleValuesStandingQuerySubscription]] messages. It should then call [[readResults]] to get the
    * initial results.
    */
  def onInitialize(effectHandler: MultipleValuesInitializationEffects): Unit = ()

  /** Process node events.
    *
    * Always called on the node's thread.
    *
    * This both processes events as-they-happen, as well as accepts replays of mock events to represent current node
    * state. The latter mode occurs when the query is initially registered, and should pass an empty set of subscribers
    * so that any calls to [[effectHandler.reportUpdatedResults]] are no-ops. The results should then be conclusively
    * decided by a call to [[readResults]], and emitted to any initial subscriber[s].
    *
    * The implementation of this function should guarantee that a result group from this state will be reported in
    * finite time. For example, if this state depends only on node-local data, this must report any changed result
    * immediately. If this state depends on subqueries, it must ensure that any subqueries will report any changed
    * results as quickly as they can. Put another way: Once this is called, [[readResults]] should at most
    * temporarily return None.
    *
    * @param events which node-events happened (after node-side deduplication against current node state)
    *               NB: multiple edge events within the same batch are no longer [1] deduplicated against
    *               one another, but property events still are [2]
    * @see https://github.com/thatdot/quine-plus/pull/2280#discussion_r1115372792
    * @see https://github.com/thatdot/quine-plus/pull/2522
    * @param effectHandler handler for external effects
    * @return whether the standing query state may have been updated (eg. is there anything new to save?)
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
    * INV: this returns the same rows as the last call to [[effectHandler.reportUpdatedResults]] made by either
    *      [[onNewSubscriptionResult]] or [[onNodeEvents]].
    *
    * [[onNodeEvents]] and [[onNewSubscriptionResult]] should work together across a standing query to ensure that this
    * function returns [[None]] as little as possible, and only ever temporarily.
    *
    * @param localProperties   current local node properties, including the labels property (labelsKey), which is not
    *                          seen by the ad-hoc cypher interpreter
    * @param labelsPropertyKey the property key used to store labels on a node, according to startup-time
    *                          configuration
    * @return Accumulated results at this moment.
    *         `None` when the internal state has not yet received/produced a result (i.e, still waiting for necessary
    *         subqueries).
    *         `Some(Seq.empty)` when a result group was produced but yielded no result rows
    *         `Some(Seq(...))` when accumulated state have been resolved into a nonempty result group according to
    *         whatever the StandingQueryState is meant to compute from its cached state.
    */
  def readResults(localProperties: Properties, labelsPropertyKey: Symbol)(implicit
    logConfig: LogConfig,
  ): Option[Seq[QueryContext]]

  /** Read the current results owed to one particular subscriber, used when that subscriber first arrives.
    *
    * Almost every state owes the same group to all of its subscribers, and so answers with [[readResults]].
    * [[EdgeSubscriptionReciprocalState]] is the exception: it answers a subscriber only across an edge that exists,
    * so what it can report is a function of who is asking.
    */
  def readResultsFor(
    subscriber: MultipleValuesStandingQuerySubscriber,
    effectHandler: MultipleValuesStandingQueryEffects,
  )(implicit logConfig: LogConfig): Option[Seq[QueryContext]] =
    readResults(effectHandler.currentProperties, effectHandler.labelsProperty)

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

/** Callbacks available to an MVSQ during `onInitialize` -- i.e., after its `query` is resolved but before
  * it is able to issue results.
  */
trait MultipleValuesInitializationEffects {

  /** Current node */
  val executingNodeId: QuineId

  /** ID provider */
  val idProvider: QuineIdProvider

  /** Issue a subscription to a node
    *
    * @param onNode node to which the subscription is delivered
    * @param query standing query whose results are being subscribed to
    */
  def createSubscription(onNode: QuineId, query: MultipleValuesStandingQuery): Unit
}

/** Limited scope of actions that a [[MultipleValuesStandingQueryState]] is allowed to make during regular
  * (post-initialization) operation
  */
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

  /** Report a new or updated result to one subscriber, leaving the others as they are.
    *
    * Most states owe the same result group to every subscriber and use [[reportUpdatedResults]]. A state whose answer
    * depends on who is asking ([[EdgeSubscriptionReciprocalState]], which answers only across edges that exist)
    * reports per subscriber instead.
    */
  def reportUpdatedResultsTo(subscriber: MultipleValuesStandingQuerySubscriber, resultGroup: Seq[QueryContext]): Unit

  /** Report a new or updated result to every subscriber running on the given node. */
  def reportUpdatedResultsToNode(onNode: QuineId, resultGroup: Seq[QueryContext]): Unit

  /** Report a new or updated result to the given query part running on the given node.
    *
    * [[reportUpdatedResultsTo]] and [[reportUpdatedResultsToNode]] address subscribers the node holds in its
    * subscriber set. A state whose subscribers are recorded somewhere else (an
    * [[EdgeSubscriptionReciprocalState]] with a [[ReciprocalSubscriberStore]]) has no such entry to address, so it
    * names a subscriber by what the store records: the node it runs on, and the part it subscribed for.
    *
    * This is the one effect that may be invoked off the node's thread. The store answers "which subscribers" as a
    * stream, sending as it goes, and the node's message processing is paused for the length of it, so nothing the
    * node might otherwise do can interleave with the sends. The implementation must therefore touch nothing of the
    * node's own state.
    */
  def reportUpdatedResultsToRemotePart(
    onNode: QuineId,
    forPart: MultipleValuesStandingQueryPartId,
    resultGroup: Seq[QueryContext],
  ): Unit

  /** Report a new or updated result to the node subscribers this node holds in its own subscriber set, asking
    * `entitled` which of them it is owed to.
    *
    * One pass over the subscribers, rather than one report addressed per node, because addressing a node is itself
    * a pass over them: a node may be on record more than once. `entitled` is asked at most once per subscribing
    * node however many entries it has, and at most `checkAtMost` times in all, after which every node still to
    * come is treated as entitled.
    *
    * That bound is the point of the parameter. Answering can be a read of the node's edges, taken on the node's
    * thread, so a state with many subscribers here would hold the node for one round trip each. Where the budget
    * runs out the question is dropped rather than answered, which is the same trade
    * [[ReciprocalSubscriberStore.reportToEntitledSubscribers]] makes when it cannot work out who is entitled: a
    * report to a node that is not costs one message and tells it a level it can only agree with.
    */
  def reportUpdatedResultsToEntitledNodes(resultGroup: Seq[QueryContext], checkAtMost: Int)(
    entitled: QuineId => Boolean,
  ): Unit

  /** This node's half edges to `other` matching the given constraints (`None` matches anything).
    *
    * Answered from the node's edge collection, by index, and it already reflects the events being processed. A state
    * keyed by edges asks this rather than keeping a set of them: the edges are the record of which edges there are,
    * and a second copy is a copy that can disagree.
    *
    * `None` where the node could not find out. On a node whose edges live in the persistor this is a read, and a
    * read can fail or time out; there is no answer to fall back on, because both of them are claims. A caller
    * handed `None` takes the branch that neither writes a row nor withdraws a result: it does not cancel, does not
    * retract, and reports to a subscriber it cannot rule out rather than withholding from one it cannot confirm.
    * Every answer here is bounded by the edges to one node, so it is a sequence rather than a lazy iterator: a
    * lazy one would carry the read, and therefore the failure, back out past whoever asked.
    */
  def matchingEdgesTo(
    edgeName: Option[Symbol],
    edgeDirection: Option[EdgeDirection],
    other: QuineId,
  ): Option[Seq[HalfEdge]]
}

object MultipleValuesStandingQueryEffects {

  /** One pass over a state's subscribers, handing each entitled node subscriber to `report`.
    *
    * `entitled` is asked at most once per subscribing node, however many entries that node has, and at most
    * `checkAtMost` times in all; every node reached after that is treated as entitled. Past the budget the
    * question is dropped rather than answered, because the question is an optimization and this is the cheap way
    * to be wrong: one message carrying a level its receiver already agrees with.
    *
    * Here rather than in each effect handler so that the bound has one implementation. A handler decides what
    * reporting is, and what to do about a standing query that has since been cancelled; how many times the node's
    * edges get asked is the same question wherever it is answered.
    */
  def eachEntitledNodeSubscriber(
    subscribers: Iterable[MultipleValuesStandingQuerySubscriber],
    checkAtMost: Int,
    entitled: QuineId => Boolean,
  )(report: MultipleValuesStandingQuerySubscriber.NodeSubscriber => Unit): Unit = {
    val decided = mutable.Map.empty[QuineId, Boolean]
    subscribers.foreach {
      case subscriber: MultipleValuesStandingQuerySubscriber.NodeSubscriber =>
        val onNode = subscriber.subscribingNode
        val isEntitled = decided.get(onNode) match {
          case Some(answered) => answered
          case None =>
            val answer = decided.size >= checkAtMost || entitled(onNode)
            decided += (onNode -> answer)
            answer
        }
        if (isEntitled) report(subscriber)
      case _ => ()
    }
  }
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
    effectHandler: MultipleValuesInitializationEffects,
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
    *
    * This is not persisted, and meets the 2 criteria specified by [[MultipleValuesStandingQueryState]]:
    *
    * 1) The first call to [[onNodeEvents]] will always set this to Some, so subsequent calls will only report if the
    *    properties differ from this value
    * 2) [[readResults]] will always return results according to the properties it is provided, and therefore operates
    *    independently of the internal state of this object.
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
    val previousProperties = lastReportedProperties
    lastReportedProperties = Some(effectHandler.currentProperties)

    val somePropertyChanged = events.exists {
      case pe: PropertyEvent if pe.key != effectHandler.labelsProperty => true
      case _ => false
    }
    if (somePropertyChanged) {
      // The events contained a property update, so confirm that the set of properties really did change since our
      // last recorded report
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
    * NB not persisted. We know that properties can only change when the node is awake, so
    * we don't need to record the last-known properties when the node goes to sleep.
    * This satisfies the 2 criteria specified by
    * [[MultipleValuesStandingQueryState]]:
    * 1) The first call to [[onNodeEvents]] after wake (or, the first that contains an update for the tracked property,
    *    which is also be the first call because of the [[WatchableEventType]]) will record value of the watched
    *    property as a Some here. Subsequent calls will only report if [[lastReportWasAMatch]] or the Some value have
    *    changed, depending on the query's property constraint and aliasing rule.
    * 2) [[readResults]] will always return results according to the properties it is provided, and therefore operates
    *    independently of the internal state of this object.
    */
  var valueAtLastReport: Option[Option[model.PropertyValue]] = None

  // TODO: Clarify the conditionals that depend on valueAtLastReport and lastReportWasAMatch, potentially by collapsing
  //  both vars into a single var with a composite value.

  /** Whether we have affirmatively matched based on [[valueAtLastReport]].
    * If we haven't yet reported since registering/waking, this is None.
    *
    * Not persisted, but will be appropriately initialized by first call to [[onNodeEvents]]
    * @see [[valueAtLastReport]]
    */
  var lastReportWasAMatch: Option[Boolean] = None

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
            val knowSameResultReported = valueAtLastReport.contains(currentProperty)
            val unknownIfChangedOrKnowChanged = !knowSameResultReported
            if (unknownIfChangedOrKnowChanged && currentPropertyDoesMatch) {
              val currentPropertyExpr =
                currentProperty
                  .map(pv =>
                    // assume the value is a QuineValue
                    pv.deserialized.map(Expr.fromQuineValue).get,
                  )
                  .getOrElse(Expr.Null)
              val result = QueryContext.empty + (alias -> currentPropertyExpr)

              effectHandler.reportUpdatedResults(result :: Nil)

              true // we issued a new result
            } else if (knowSameResultReported) {
              // the property hasn't actually changed, so we don't need to do anything
              false
            } else if (lastReportWasAMatch.isEmpty || lastReportWasAMatch.contains(true)) { // !currentPropertyDoesMatch
              // we used to match but no longer do, or we aren't sure -- cancel any previous positive result
              effectHandler.reportUpdatedResults(Nil)
              true // we issued a new result
            } else {
              // we didn't previously match and we still don't, nothing to do.
              false
            }
          case None =>
            // the query only cares about changes that bring the property from not matching to matching or vice versa
            if (!lastReportWasAMatch.contains(currentPropertyDoesMatch)) {
              val resultGroup =
                if (currentPropertyDoesMatch) {
                  // we do match, but we didn't use to -- so emit one empty (but positive!) result.
                  QueryContext.empty :: Nil
                } else {
                  // we don't match, but we used to -- so emit that nothing matches.
                  Nil
                }

              effectHandler.reportUpdatedResults(resultGroup)
              true
            } else {
              // nothing changed that we need to report - no-op.
              false
            }
        }
        valueAtLastReport = Some(currentProperty)
        lastReportWasAMatch = Some(currentPropertyDoesMatch)
        somethingChanged
      }
      .getOrElse {
        // valueAtLastReport is defined for all but the first time onNodeEvents is called.
        // If this is the first call to [[onNodeEvents]] since wake, the property must be None/null, so track that
        if (valueAtLastReport.isEmpty) {
          valueAtLastReport = Some(None)
          lastReportWasAMatch = Some(query.propConstraint.satisfiedByNone)
        }
        // nothing changed that needs persistence
        false
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
    * NB not persisted. We know that labels can only change when the node is awake, so
    * we don't need to record the last-known labels when the node goes to sleep.
    * Because we don't explicitly rehydrate this, the first call to [[onNodeEvents]]
    * will duplicate the last result set reported. This satisfies the 2 criteria specified by
    * [[MultipleValuesStandingQueryState]]:
    * 1) The first call to [[onNodeEvents]] after wake will record the current value of the labels, setting this to
    *    Some. Subsequent calls will only report if [[lastReportWasAMatch]] or the Some value have changed, depending
    *    on the query's property constraint and aliasing rule.
    * 2) [[readResults]] will always return results according to the properties it is provided, and therefore operates
    *    independently of the internal state of this object.
    */
  var lastReportedLabels: Option[Set[Symbol]] = None

  /** Whether we have affirmatively matched based on [[lastReportedLabels]].
    * If we haven't yet reported since registering/waking, this is None.
    *
    * Not persisted, but will be appropriately initialized by first call to [[onNodeEvents]]
    *
    * @see [[lastReportedLabels]]
    */
  var lastReportWasAMatch: Option[Boolean] = None

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
        val matched = query.constraint(currentLabels)

        val somethingChanged: Boolean = query.aliasedAs match {
          case Some(alias) =>
            // the query cares about all changes to the node's labels, even those that bring it from matching to still
            // matching
            val knowSameResultReported = lastReportedLabels.contains(currentLabels)
            val unknownIfChangedOrKnowChanged = !knowSameResultReported
            if (unknownIfChangedOrKnowChanged && matched) {
              val labelsAsExpr = Expr.List(currentLabels.map(_.name).map(Expr.Str).toVector)
              val result = QueryContext.empty + (alias -> labelsAsExpr)

              effectHandler.reportUpdatedResults(result :: Nil)
              true // we issued a new result
            } else if (knowSameResultReported) {
              // the property hasn't actually changed, so we don't need to do anything
              false
            } else if (lastReportWasAMatch.isEmpty || lastReportWasAMatch.contains(true)) { // !matched
              // we used to match but no longer do -- cancel the previous positive result
              effectHandler.reportUpdatedResults(Nil)
              true // we issued a new result
            } else {
              // we didn't use to match and we still don't, nothing to do.
              false
            }
          case None =>
            // the query only cares about the presence or absense of labels, not their values -- we only
            // need to send a report when we go from matching to not matching or visa versa
            if (!lastReportWasAMatch.contains(matched)) {
              val resultGroup =
                if (matched) {
                  // we do match, but we didn't use to -- so emit one empty (but positive!) result.
                  QueryContext.empty :: Nil
                } else {
                  // we don't match, but we used to -- so emit that nothing matches.
                  Nil
                }

              effectHandler.reportUpdatedResults(resultGroup)
              true
            } else {
              // nothing changed that we need to report - no-op.
              false
            }
        }

        lastReportedLabels = Some(currentLabels)
        lastReportWasAMatch = Some(matched)
        somethingChanged
      }
      .getOrElse {
        // lastReportedLabels is defined for all but the first time onNodeEvents is called.
        // If this is the first call to [[onNodeEvents]] since wake, there must be no labels, so track that
        if (lastReportedLabels.isEmpty) {
          lastReportedLabels = Some(Set.empty)
          lastReportWasAMatch = Some(query.constraint(Set.empty))
        }
        // nothing changed that needs persistence
        false
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

  /** Not persisted. This satisfies the 2 criteria specified by [[MultipleValuesStandingQueryState]]:
    * 1) Results are never proactively reported by this state (only by [[readResults]] when something subscribes
    *    to this state), so [[onNodeEvents]] does not need to worry about duplicating them
    * 2) [[readResults]] will always be run after [[rehydrate]], and only [[rehydrate]] changes this value, so
    *    [[readResults]] will always report the right value
    */
  private var result: Seq[QueryContext] = _ // Set during [[rehydrate]]

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

  /** What each edge along which this query matches currently contributes, and the running total of those
    * contributions. An edge with no contribution yet is one that has been subscribed to but has not answered.
    *
    * These are meant to be a subset of the node's `EdgeCollection`, and are one for as long as the node is the only
    * thing writing to both. A wake is where they can part: an edge change is journalled per update while this is
    * durable in the state's blob, and journal replay puts the edge back without telling any standing query state
    * (`EdgeProcessor.updateEdgeCollection`). So an edge removed after the last blob write is gone from the
    * collection and still recorded here.
    *
    * Persisted. Held here behind a seam because a node whose edges do not fit in memory cannot keep one entry per
    * edge either: what changes there is where the rows live, not what they mean.
    */
  private[this] var contributions: EdgeContributionStore = new HeapEdgeContributionStore

  /** Where this state keeps each edge's contribution. Replacing it is how a node moves those rows out of the heap,
    * and is only sound while the state holds none of its own: everything this state reports is the total, which the
    * store derives from the rows it was given.
    */
  def contributionStore: EdgeContributionStore = contributions
  def contributionStore_=(store: EdgeContributionStore): Unit = contributions = store

  /** Whether the blob this state was read from said its per-edge rows are recorded outside it.
    *
    * Set by the codec, read at wake. A node that installs a store for them adopts the rows. A node that installs
    * none has a state whose rows exist and cannot be reached, which deserves an error rather than a silent report
    * of nothing.
    */
  var edgeResultsExternalized: Boolean = false

  override def relevantEventTypes(labelsPropertyKey: Symbol): Seq[WatchableEventType.EdgeChange] =
    Seq(WatchableEventType.EdgeChange(query.edgeName))

  private[this] def edgeMatchesPattern(halfEdge: HalfEdge): Boolean =
    query.edgeName.forall(_ == halfEdge.edgeType) &&
    query.edgeDirection.forall(_ == halfEdge.direction)

  /** The query asked of the node across `halfEdge`. Its identity ignores which node is asking, so subscriptions from
    * different nodes with the same constraints all land on one state there. It does not ignore the edge's own type
    * or direction: edges to one node differing in either are asked as different queries, and answered separately.
    */
  private[this] def reciprocalQueryFor(
    halfEdge: HalfEdge,
    executingNodeId: QuineId,
  ): MultipleValuesStandingQuery.EdgeSubscriptionReciprocal =
    MultipleValuesStandingQuery.EdgeSubscriptionReciprocal(
      halfEdge.reflect(executingNodeId),
      query.andThen.queryPartId,
      query.columns,
    )

  override def onNodeEvents(
    events: Seq[NodeChangeEvent],
    effectHandler: MultipleValuesStandingQueryEffects,
  )(implicit logConfig: LogConfig): Boolean = {
    var somethingChanged = false
    events.foreach {
      // Applying the same `EdgeAdded` twice is a no-op. Not from the event path, which drops an event that would
      // change nothing and requires a batch to name each edge at most once, but from the description of the node
      // given at registration, which replays edges already being tracked. What must never happen is the restatement
      // discarding an answer already received, which is why tracking an edge never clears what it contributed.
      case EdgeAdded(halfEdge) if edgeMatchesPattern(halfEdge) =>
        // Create a new subscription. A restated edge re-sends the same query, and the far node treats the repeat as
        // the duplicate it is. Two different edges to one node cannot do this: an edge collection is a set, so they
        // differ in type or direction, and the query each is asked under differs with them.
        effectHandler.createSubscription(halfEdge.other, reciprocalQueryFor(halfEdge, effectHandler.executingNodeId))
        // Record that the subscription has been made, but no result (from the andThen via the reciprocal) yet.
        contributions.track(halfEdge)
        // Only where the rows are in this state's own blob. A store that keeps them elsewhere writes nothing into
        // the blob and tracks nothing, so there is nothing here for a write to make durable. On the node that
        // arrangement exists for, an edge event would otherwise rewrite the blob for no reason at all.
        somethingChanged ||= !contributions.keepsRowsElsewhere

      case EdgeRemoved(halfEdge) if edgeMatchesPattern(halfEdge) =>
        // Unconditional, because the subscription this cancels served exactly the edge that just went away. It is
        // named by that edge's own type and direction, and an edge collection is a set, so one half edge is the
        // most a node can have with those constraints to that node, and it is this one. Asking whether another
        // remains asks about the half edge just removed from a collection that already reflects the removal, which
        // has one answer. On a node whose edges are in the persistor it was also a round trip to get it.
        effectHandler.cancelSubscription(
          halfEdge.other,
          reciprocalQueryFor(halfEdge, effectHandler.executingNodeId).queryPartId,
        )

        // Whether there is anything to write down cannot wait for the outcome (this is the answer to a question
        // the node asks as soon as this returns), and the event only reaches here because the edge really went away.
        // As above, there is nothing to write down at all when the rows are elsewhere.
        somethingChanged ||= !contributions.keepsRowsElsewhere

        contributions.retract(halfEdge) { outcome =>
          if (outcome.totalChanged) {
            // This edge had contributed rows, which are now withdrawn.

            // NB this may not immediately issue a cancellation, if any other edges have not yet reported their results.
            // However, those edges should eventually report results, at which point this will issue a cancellation (and
            // any new matches from those edges)
            readResults(effectHandler.currentProperties, effectHandler.labelsProperty).foreach(
              effectHandler.reportUpdatedResults,
            )
          }
        }

      case _ => () // Ignore all other events.
    }
    somethingChanged
  }

  override def onNewSubscriptionResult(
    result: NewMultipleValuesStateResult,
    effectHandler: MultipleValuesStandingQueryEffects,
  )(implicit logConfig: LogConfig): Boolean = {
    // The far node reports once per node rather than once per edge, so this result belongs to every matching edge to
    // it that the reporting part answers for: the query matches along each edge separately, and so each of them
    // contributes its own rows.
    //
    // Silently drop the result if there is no matching edge to `result.from`. This can happen if the edge is removed
    // (here first) then the other side reports no longer matching the reciprocal
    // TODO does this race during creation?
    //
    // Dropped too where the node could not find out which edges this result speaks for. What that leaves standing is
    // whatever those edges last contributed: nothing where none had answered yet, and the superseded group where
    // one had, which this part goes on reporting. Not merely a gap, then: a stale answer.
    //
    // Crediting instead would write a row for an edge that may not exist, and nothing would ever take it back: the
    // `EdgeRemoved` that clears a row has already passed by the time a result races in behind it. Between two
    // wrongnesses that both persist, this is the one that ends by itself: the far side sends a level whenever its
    // own result changes, and the next such change corrects this edge. A row for a departed edge is corrected only
    // by that edge being added and removed again, which may never happen.
    val matchingEdges: Seq[HalfEdge] =
      effectHandler.matchingEdgesTo(query.edgeName, query.edgeDirection, result.from).getOrElse(Seq.empty)

    // Which of them this result speaks for. A reciprocal is named by the constraints it answers, so where this
    // query's constraints are concrete (which is every query the compiler emits), every matching edge to one node
    // is answered by the same part, and this selects all of them. Where a constraint is left open, edges of
    // different types or directions to one node are answered by *different* parts, and crediting one part's answer
    // to all of them means a retraction from one wiping what its siblings said.
    //
    // So nothing matching credits nothing. A result under an id no matching edge can produce speaks for an edge this
    // node no longer has, which is the same case as having no matching edge at all.
    val contributingEdges: Seq[HalfEdge] = matchingEdges.filter { halfEdge =>
      reciprocalQueryFor(halfEdge, effectHandler.executingNodeId).queryPartId == result.queryPartId
    }

    if (contributingEdges.isEmpty) false // no matching edge to the node this result came from
    else {
      // The report has to be made from inside the outcomes, not after the calls: a store that keeps its rows outside
      // the heap answers after a round trip, so at the end of this method it has not decided anything yet. Counting
      // the outcomes down is what keeps that to one report per delivery rather than one per edge: they arrive on
      // this node's thread, and in the order the calls were made.
      var awaitingOutcome = contributingEdges.size
      var totalChanged = false
      contributingEdges.foreach { halfEdge =>
        contributions.contribute(halfEdge, result.resultGroup) { outcome =>
          totalChanged ||= outcome.totalChanged
          awaitingOutcome -= 1
          if (awaitingOutcome == 0 && totalChanged)
            readResults(effectHandler.currentProperties, effectHandler.labelsProperty).foreach(
              effectHandler.reportUpdatedResults,
            )
        }
      }
      true
    }
  }

  def readResults(localProperties: Properties, labelsKey: Symbol)(implicit
    logConfig: LogConfig,
  ): Option[Seq[QueryContext]] =
    if (edgeResultsExternalized && !contributions.keepsRowsElsewhere) {
      // The blob said this part's rows are recorded elsewhere, and no store that can reach them was installed. What
      // this state holds is not "no matching edges". It is no idea at all, and saying `Some` here would report an
      // affirmative lack of matches, withdrawing rows that are still perfectly good. Withhold instead.
      //
      // One thing ends this, and it is not a far node reporting: a report lands in the heap store this state was
      // built with, and is read back here, which is still this branch. What ends it is the node installing a
      // store that does reach them, which adopts the rows and merges whatever the heap collected in the meantime.
      // Short of that the part stays silent for as long as the state lives, and
      // only recreating the standing query builds a state that can speak. Say so where an operator will read it,
      // rather than describing a recovery that does not arrive.
      //
      // Adoption is also why the rows are left where they are rather than dropped on finding them unreachable: they
      // are what every far node last said, and all of it stays true except for edges removed while this state could
      // not act. Dropping them would trade a bounded wrongness for a total one.
      None
    } else if (!contributions.hasTrackedEdges) {
      // There are no matching edges, so there is an affirmative lack of matches
      Some(Nil)
    } else {
      // Report what is known as soon as any edge has answered, withholding only while *every* edge is
      // unanswered. Since this state's result group is the concatenation of its edges' rows, an edge's rows
      // are valid regardless of whether other edges have answered: filling in a later edge only adds rows,
      // and so never retracts a row reported earlier.
      //
      // The alternative (withhold while *any* edge is unanswered) trades fewer intermediate results for
      // unbounded latency, because the gate re-closes on every newly added matching edge: a node under
      // continuous edge ingest may never report at all. It also requires tracking one unanswered placeholder
      // per edge, which a node whose edges live outside the heap cannot afford.
      if (contributions.answeredEdges == 0) None
      else Some(EdgeContributionStore.expand(contributions.total))
    }

  // the result set of a SubscribeAcrossEdge, when defined, is the concatenation of all the result rows
  // from the all edges that could match the query's edge (because a MVSQ should report a row for each way
  // by which it matches), which is what the running total counts

  override def pretty(implicit idProvider: QuineIdProvider): String =
    s"${this.getClass.getSimpleName}($queryPartId, ${contributions.entries.map { case (he, v) => he.pretty -> v }.mkString("{", ", ", "}")})"
}

/** Validates this concluding half edge side of the edge and propagates results back to each subscribing side that
  * currently has a matching edge.
  *
  * State needed to process a [[MultipleValuesStandingQuery.EdgeSubscriptionReciprocal]]
  *
  * One of these serves every node that subscribes with the same edge constraints, rather than one per edge: the
  * constraints, not any particular edge, are what its identity is made of. So a node with a million edges into it
  * runs one of these, holds one copy of the `andThen` result, and subscribes to the `andThen` once.
  *
  * That subscription is this state's one standing invariant: for as long as the state exists, the `andThen`'s
  * subscriber set on this same node names this part. The two facts persist together and dissolve together (the
  * last subscriber's cancel discards the state and cancels the subscription), so a wake decodes the pair intact.
  * [[onInitialize]] establishes it for a state the behavior creates; the wake-time fold of pre-collapse rows
  * carries an existing entry over; and `updateMultipleValuesStandingQueriesOnNode` re-checks it on every wake,
  * establishing it for the one kind of state that can be born without it: one the fold assembled entirely out
  * of rows that had never subscribed. Everything below assumes the invariant rather than re-checking it: an
  * [[EdgeAdded]] relays the cached result instead of subscribing, and [[readResultsFor]] answers from the cache,
  * both sound only because the cache is being kept current by a subscription that always exists.
  *
  * Which subscribers are currently entitled to results is not tracked here. It is asked of the node's edge
  * collection at the moments it matters (when a result arrives, when an edge appears or disappears, and when a
  * subscriber first arrives), because the edges are the record of it, indexed by the node they lead to. Where the
  * subscribers themselves are recorded (the node's subscriber set, or [[subscriberStore]]) is a separate question
  * from everything above, and the two do not interact: the `andThen` link is one entry however many subscribers
  * there are, and moving subscribers between set and store never touches it.
  *
  * Since reciprocal queries are generated on the fly in [[SubscribeAcrossEdgeState]], they won't
  * show up when you try to look them up by ID globally. This is why this state inlines fields from
  * [[MultipleValuesStandingQuery.EdgeSubscriptionReciprocal]], but only stores an ID for the `andThenId`.
  *
  * @param queryPartId the ID of the edge-subscript-reciprocal query with this State
  * @param halfEdge a subscriber's edge as seen from this node. Everything that matters is its type and direction
  *                 (the constraints a subscriber's edge must match), while `halfEdge.other` merely records which
  *                 node's subscription created (or, for a state written before identities were shared, once owned)
  *                 this state. Nothing may read `other`
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

  /** Saved state from `andThen` query. Persisted. */
  var cachedResult: Option[Seq[QueryContext]] = None // Result from the `andThen` query cached here.

  /** Where this state's subscribers are recorded, when somewhere other than the node's subscriber set.
    *
    * A node with too many subscribing neighbours to hold one entry each, in the heap or in this state's blob,
    * substitutes a store that records them outside both. Absent everywhere else, and then the node's subscriber set
    * is the record, exactly as before. Installed by the node, never persisted.
    *
    * Even when present it is not the whole record: a subscriber the store cannot record stays in the node's set,
    * and the two hold disjoint subscribers rather than two views of the same ones. Whatever reports to every
    * subscriber has to go through both. See [[externalSubscriberForQuery]] for which ones the store takes.
    */
  var subscriberStore: Option[ReciprocalSubscriberStore] = None

  /** The one query part every subscriber recorded in [[subscriberStore]] subscribes for.
    *
    * The store records a subscriber by node alone, so answering one needs the part to address, and there is
    * exactly one, because every subscription with these edge constraints comes from the same compiled query part. A
    * subscriber citing a different part (possible only for a hand-built query) stays in the node's subscriber set
    * instead: correct, merely unshared. Set by the first subscriber the store records. Persisted.
    */
  var externalSubscriberForQuery: Option[MultipleValuesStandingQueryPartId] = None

  /** Whether the blob this state was read from said its subscribers are recorded outside it.
    *
    * Set by the codec, read at wake. A node that installs a store for them answers its subscribers from it. A node
    * that installs none has subscribers that exist and cannot be reached, which deserves an error rather than
    * silence.
    */
  var subscribersExternalized: Boolean = false

  /** The subquery whose results are relayed across matching edges. */
  private[this] var andThen: MultipleValuesStandingQuery = _

  override def rehydrate(
    effectHandler: MultipleValuesStandingQueryLookupInfo,
  )(implicit logConfig: LogConfig): Unit =
    // Do not call `super.preStart(effectHandler)` here because this `EdgeSubscriptionReciprocalState` is synthesized
    // and its `queryPartId` is not in the global registry.
    andThen = effectHandler.lookupQuery(andThenId)

  /** Subscribe to the `andThen` once, for as long as this state exists. Since one state serves all subscribers, it
    * cannot start and stop with any one subscriber's edge. A node asked about an edge that only exists on the
    * other side answers that it does not match, without needing to have avoided the question.
    */
  override def onInitialize(effectHandler: MultipleValuesInitializationEffects): Unit =
    effectHandler.createSubscription(effectHandler.executingNodeId, andThen)

  override def relevantEventTypes(labelsPropertyKey: Symbol): Seq[WatchableEventType.EdgeChange] = Seq(
    WatchableEventType.EdgeChange(
      Some(halfEdge.edgeType),
    ),
  )

  /** Nothing: the edges this node already has cannot tell this state anything.
    *
    * All this state does with an edge's appearance is relay the `andThen` result it is holding, and a state that has
    * just been created is holding none: its subscription to the `andThen` is a message it has not sent yet when
    * the replay runs. So every edge the node already has would be read only to be ignored, which on the node in the
    * middle of a pattern is a scan of every edge pointing at it for no effect at all. Nothing is lost by not asking:
    * when the `andThen` does answer, that answer reaches each subscriber by asking the edges then.
    */
  override def initialEventTypes(labelsPropertyKey: Symbol): Seq[WatchableEventType] = Seq.empty

  private[this] def edgeMatchesPattern(edge: HalfEdge): Boolean =
    edge.edgeType == halfEdge.edgeType && edge.direction == halfEdge.direction

  override def onNodeEvents(
    events: Seq[NodeChangeEvent],
    effectHandler: MultipleValuesStandingQueryEffects,
  )(implicit logConfig: LogConfig): Boolean = {
    events.foreach {
      case EdgeAdded(newHalfEdge) if edgeMatchesPattern(newHalfEdge) =>
        // The node across this edge may be a subscriber that was not being answered until now. Reporting to a node
        // that is not subscribed, or is already being answered, costs a message: results are levels, so a repeat of
        // one already delivered changes nothing.
        cachedResult.foreach { result =>
          effectHandler.reportUpdatedResultsToNode(newHalfEdge.other, result)
          reportToExternalSubscriberOn(newHalfEdge.other, result, effectHandler)
        }

      case EdgeRemoved(oldHalfEdge) if edgeMatchesPattern(oldHalfEdge) =>
        // Unconditional, for the reason the guard here used to ask about: this state answers one edge type and
        // direction, an edge collection is a set, and the event only reaches here because that edge matched. So the
        // edge that just went away was the only one entitling that node, and the collection the question would have
        // been put to already reflects its removal. On a supernode the question was a read of the persistor, whose
        // one answer was "none remains", and whose failure retracted nothing, leaving that node holding a result.
        effectHandler.reportUpdatedResultsToNode(oldHalfEdge.other, Nil)
        reportToExternalSubscriberOn(oldHalfEdge.other, Nil, effectHandler)

      case _ => // Ignore
    }
    // Nothing persisted changed: which subscribers are entitled to results is derived from the node's edges.
    false
  }

  override def onNewSubscriptionResult( // Happens when the subscription for the `andThen` returns a result
    result: NewMultipleValuesStateResult,
    effectHandler: MultipleValuesStandingQueryEffects,
  )(implicit logConfig: LogConfig): Boolean = {
    val resultIsUpdate = !cachedResult.contains(result.resultGroup)
    cachedResult = Some(result.resultGroup)
    // only propagate a result across an edge that exists, but cache the result regardless
    if (resultIsUpdate) {
      // Withheld only from a node this one can say it has no edge to. Where it cannot say, the result goes: a
      // report to a node that is no longer entitled costs one message and changes nothing there, since results
      // are levels, while withholding leaves a subscriber that *is* entitled holding a stale one. Asked of a
      // bounded number of them, because on a node whose edges are in the persistor the question is a read.
      effectHandler.reportUpdatedResultsToEntitledNodes(
        result.resultGroup,
        EdgeSubscriptionReciprocalState.entitlementChecksPerReport,
      ) { subscribingNode =>
        effectHandler
          .matchingEdgesTo(Some(halfEdge.edgeType), Some(halfEdge.direction), subscribingNode)
          .forall(_.nonEmpty)
      }
      // The subscribers recorded outside the node's subscriber set hear it from the store, which already knows how
      // to reach exactly the entitled ones.
      for {
        store <- subscriberStore
        forPart <- externalSubscriberForQuery
      } store.reportToEntitledSubscribers(
        effectHandler.reportUpdatedResultsToRemotePart(_, forPart, result.resultGroup),
      )
    }
    resultIsUpdate
  }

  /** Report to the given node only if it is a subscriber recorded in [[subscriberStore]]: a point membership
    * question, answered after a round trip, with the report made from inside the answer. Nothing at all when the
    * subscribers are not externalized, where [[MultipleValuesStandingQueryEffects.reportUpdatedResultsToNode]]
    * already reaches everyone.
    */
  private[this] def reportToExternalSubscriberOn(
    node: QuineId,
    resultGroup: Seq[QueryContext],
    effectHandler: MultipleValuesStandingQueryEffects,
  ): Unit =
    for {
      store <- subscriberStore
      forPart <- externalSubscriberForQuery
    } store.ifSubscribed(node)(() => effectHandler.reportUpdatedResultsToRemotePart(node, forPart, resultGroup))

  /** There is no answer here that is not specific to a subscriber (see [[readResultsFor]]). This state's
    * subscribers are all remote, so nothing local ever reads its results without saying who is asking.
    */
  def readResults(localProperties: Properties, labelsPropertyKey: Symbol)(implicit
    logConfig: LogConfig,
  ): Option[Seq[QueryContext]] = None

  override def readResultsFor(
    subscriber: MultipleValuesStandingQuerySubscriber,
    effectHandler: MultipleValuesStandingQueryEffects,
  )(implicit logConfig: LogConfig): Option[Seq[QueryContext]] = subscriber match {
    // As in `onNewSubscriptionResult`: withheld only from a node this one can say it has no edge to. Where it
    // cannot say, the subscriber that just asked gets the cached result rather than a silence it has no way to
    // resolve: it asked because it has just subscribed, and nothing else will prompt this part to answer it.
    case MultipleValuesStandingQuerySubscriber.NodeSubscriber(subscribingNode, _, _)
        if effectHandler
          .matchingEdgesTo(Some(halfEdge.edgeType), Some(halfEdge.direction), subscribingNode)
          .forall(_.nonEmpty) =>
      cachedResult
    case _ => None
  }

  override def pretty(implicit idProvider: QuineIdProvider): String =
    s"${this.getClass.getSimpleName}($queryPartId, ${halfEdge.edgeType}/${halfEdge.direction}, ${cachedResult
      .map(_.mkString("[", ",", "]"))}, $andThenId)"
}

object EdgeSubscriptionReciprocalState {

  /** How many of this state's own subscribers it will ask the node's edges about before reporting to the rest
    * without asking.
    *
    * Only the subscribers the node holds in its subscriber set are counted here. The ones recorded in a
    * [[ReciprocalSubscriberStore]] are not asked about one at a time at all: the store answers who is entitled in
    * one pass over its rows and the node's edges together, which is what it is for. So this bounds the case the
    * store was not installed for, or could not be installed in, where the number of subscribers can still be
    * large enough that a read each would hold the node's thread for a very long time.
    */
  val entitlementChecksPerReport: Int = 32
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

  override def onInitialize(effectHandler: MultipleValuesInitializationEffects): Unit =
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
