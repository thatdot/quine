package com.thatdot.quine.graph

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.locks.StampedLock

import scala.collection.compat._
import scala.collection.mutable
import scala.compat.ExecutionContexts
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

import akka.actor.{Actor, ActorLogging}

import cats.data.NonEmptyList

import com.thatdot.quine.graph.DomainIndexEvent.CreateDomainStandingQuerySubscription
import com.thatdot.quine.graph.EdgeEvent.{EdgeAdded, EdgeRemoved}
import com.thatdot.quine.graph.PropertyEvent.{PropertyRemoved, PropertySet}
import com.thatdot.quine.graph.behavior.DomainNodeIndexBehavior.{
  DomainNodeIndex,
  NodeParentIndex,
  SubscribersToThisNodeUtil
}
import com.thatdot.quine.graph.behavior._
import com.thatdot.quine.graph.cypher.{
  MultipleValuesStandingQuery,
  MultipleValuesStandingQueryLookupInfo,
  MultipleValuesStandingQueryState
}
import com.thatdot.quine.graph.edgecollection.EdgeCollection
import com.thatdot.quine.graph.messaging.BaseMessage.Done
import com.thatdot.quine.graph.messaging.CypherMessage._
import com.thatdot.quine.graph.messaging.LiteralMessage.{
  DgnLocalEventIndexSummary,
  LiteralCommand,
  LocallyRegisteredStandingQuery,
  NodeInternalState,
  SqStateResult,
  SqStateResults
}
import com.thatdot.quine.graph.messaging.StandingQueryMessage._
import com.thatdot.quine.graph.messaging.{AlgorithmCommand, QuineIdAtTime, QuineIdOps, QuineRefOps}
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.model.{HalfEdge, Milliseconds, PropertyValue, QuineId, QuineIdProvider}
import com.thatdot.quine.persistor.codecs.{MultipleValuesStandingQueryStateCodec, SnapshotCodec}
import com.thatdot.quine.persistor.{EventEffectOrder, PersistenceAgent, PersistenceConfig}
import com.thatdot.quine.util.HexConversions

case class NodeActorConstructorArgs(
  properties: Map[Symbol, PropertyValue],
  edges: Iterable[HalfEdge],
  distinctIdSubscribers: mutable.Map[
    DomainGraphNodeId,
    SubscribersToThisNodeUtil.DistinctIdSubscription
  ],
  domainNodeIndex: DomainNodeIndexBehavior.DomainNodeIndex,
  multipleValuesStandingQueryStates: NodeActor.MultipleValuesStandingQueries,
  initialJournal: NodeActor.Journal
)
object NodeActorConstructorArgs {
  def empty: NodeActorConstructorArgs = NodeActorConstructorArgs(
    properties = Map.empty,
    edges = Iterable.empty,
    distinctIdSubscribers = mutable.Map.empty,
    domainNodeIndex = DomainNodeIndexBehavior.DomainNodeIndex(mutable.Map.empty),
    multipleValuesStandingQueryStates = mutable.Map.empty,
    initialJournal = Iterable.empty
  )

  /** Create NodeActorConstructorArgs from a NodeSnapshot
    *
    * @param snapshot node snapshot
    */
  def fromSnapshot(snapshot: NodeSnapshot): NodeActorConstructorArgs =
    NodeActorConstructorArgs(
      properties = snapshot.properties,
      edges = snapshot.edges,
      distinctIdSubscribers = snapshot.subscribersToThisNode,
      domainNodeIndex = DomainNodeIndex(snapshot.domainNodeIndex),
      multipleValuesStandingQueryStates = mutable.Map.empty,
      initialJournal = Iterable.empty
    )

}

/** The fundamental graph unit for both data storage (eg [[com.thatdot.quine.graph.NodeActor#properties()]]) and
  * computation (as an Akka actor).
  * At most one [[NodeActor]] exists in the actor system ([[graph.system]]) per node per moment in
  * time (see [[atTime]]).
  *
  * @param qidAtTime the ID that comprises this node's notion of nominal identity -- analogous to akka's ActorRef
  * @param graph a reference to the graph in which this node exists
  * @param costToSleep @see [[CostToSleep]]
  * @param wakefulState an atomic reference used like a variable to track the current lifecycle state of this node.
  *                     This is (and may be expected to be) threadsafe, so that [[GraphShardActor]]s can access it
  * @param actorRefLock a lock on this node's [[ActorRef]] used to hard-stop messages when sleeping the node (relayTell uses
  *                     tryReadLock during its tell, so if a write lock is held for a node's actor, no messages can be
  *                     sent to it)
  */
private[graph] class NodeActor(
  val qidAtTime: QuineIdAtTime,
  val graph: StandingQueryOpsGraph with CypherOpsGraph,
  costToSleep: CostToSleep,
  protected val wakefulState: AtomicReference[WakefulState],
  protected val actorRefLock: StampedLock,
  protected var properties: Map[Symbol, PropertyValue],
  initialEdges: Iterable[HalfEdge],
  initialDomainGraphSubscribers: mutable.Map[
    DomainGraphNodeId,
    SubscribersToThisNodeUtil.DistinctIdSubscription
  ],
  protected val domainNodeIndex: DomainNodeIndexBehavior.DomainNodeIndex,
  protected val multipleValuesStandingQueries: NodeActor.MultipleValuesStandingQueries,
  initialJournal: NodeActor.Journal
) extends Actor
    with ActorLogging
    with BaseNodeActor
    with QuineRefOps
    with QuineIdOps
    with LiteralCommandBehavior
    with AlgorithmBehavior
    with DomainNodeIndexBehavior
    with GoToSleepBehavior
    with PriorityStashingBehavior
    with CypherBehavior
    with MultipleValuesStandingQueryBehavior
    with ActorClock {

  def receive: Receive = actorClockBehavior {
    case control: NodeControlMessage => goToSleepBehavior(control)
    case StashedMessage(message) => receive(message)
    case query: CypherQueryInstruction => cypherBehavior(query)
    case command: LiteralCommand => literalCommandBehavior(command)
    case command: AlgorithmCommand => algorithmBehavior(command)
    case command: DomainNodeSubscriptionCommand => domainNodeIndexBehavior(command)
    case command: MultipleValuesStandingQueryCommand => multipleValuesStandingQueryBehavior(command)
    case command: UpdateStandingQueriesCommand => updateStandingQueriesBehavior(command)
    case msg => log.error("Node received an unknown message (from {}): {}", sender(), msg)
  }

  val qid: QuineId = qidAtTime.id
  val atTime: Option[Milliseconds] = qidAtTime.atTime
  implicit val idProvider: QuineIdProvider = graph.idProvider
  protected val persistor: PersistenceAgent = graph.persistor
  protected val persistenceConfig: PersistenceConfig = persistor.persistenceConfig
  protected val metrics: HostQuineMetrics = graph.metrics
  protected val edges: EdgeCollection = graph.edgeCollectionFactory()
  protected val dgnRegistry: DomainGraphNodeRegistry = graph.dgnRegistry
  protected val domainGraphSubscribers: SubscribersToThisNode = SubscribersToThisNode(initialDomainGraphSubscribers)

  protected var latestUpdateAfterSnapshot: Option[EventTime] = None
  protected var lastWriteMillis: Long = 0

  protected def updateRelevantToSnapshotOccurred(): Unit = {
    if (atTime.nonEmpty) {
      log.warning("Attempted to flag a historical node as being updated -- this update will not be persisted.")
    }
    // TODO: should this update `lastWriteMillis` too?
    latestUpdateAfterSnapshot = Some(peekEventSequence())
  }

  /** @see [[StandingQueryLocalEventIndex]]
    */
  protected var localEventIndex: StandingQueryLocalEventIndex =
    // NB this initialization is non-authoritative: only after journal restoration is complete can this be
    // comprehensively reconstructed (see the block below the definition of [[nodeParentIndex]]). However, journal
    // restoration may access [[localEventIndex]] and/or [[nodeParentIndex]] so they must be at least initialized
    StandingQueryLocalEventIndex
      .from(
        dgnRegistry,
        domainGraphSubscribers.subscribersToThisNode.keysIterator,
        multipleValuesStandingQueries.iterator.map { case (sqIdAndPartId, (_, state)) => sqIdAndPartId -> state }
      )
      ._1 // take the index, ignoring the record of which DGNs no longer exist (addressed in the aforementioned block)

  /** @see [[NodeParentIndex]]
    */
  protected var domainGraphNodeParentIndex: NodeParentIndex =
    // NB this initialization is non-authoritative: only after journal restoration is complete can this be
    // comprehensively reconstructed (see the block below the definition of [[nodeParentIndex]]). However, journal
    // restoration may access [[localEventIndex]] and/or [[nodeParentIndex]] so they must be at least initialized
    NodeParentIndex
      .reconstruct(domainNodeIndex, domainGraphSubscribers.subscribersToThisNode.keys, dgnRegistry)
      ._1 // take the index, ignoring the record of which DGNs no longer exist (addressed in the aforementioned block)

  { // here be the side-effects performed by the constructor
    initialEdges.foreach(edges +=)

    initialJournal foreach {
      case event: PropertyEvent => applyPropertyEffect(event)
      case event: EdgeEvent => applyEdgeEffect(event)
      case event: DomainIndexEvent => applyDomainIndexEffect(event, shouldCauseSideEffects = false)
    }

    // Once edge map is updated, recompute cost to sleep:
    costToSleep.set(Math.round(Math.round(edges.size.toDouble) / Math.log(2) - 2))

    // Make a best-effort attempt at restoring the localEventIndex: This will fail for DGNs that no longer exist,
    // so also make note of which those are for further cleanup. Now that the journal and snapshot have both been
    // applied, we know that this reconstruction + removal detection will be as complete as possible
    val (localEventIndexRestored, locallyWatchedDgnsToRemove) = StandingQueryLocalEventIndex.from(
      dgnRegistry,
      domainGraphSubscribers.subscribersToThisNode.keysIterator,
      multipleValuesStandingQueries.iterator.map { case (sqIdAndPartId, (_, state)) => sqIdAndPartId -> state }
    )
    this.localEventIndex = localEventIndexRestored

    // Phase: The node has caught up to the target time, but some actions locally on the node need to catch up
    // with what happened with the graph while this node was asleep.

    // stop tracking subscribers of deleted DGNs that were previously watching for local events
    domainGraphSubscribers.removeSubscribersOf(locallyWatchedDgnsToRemove)

    // determine newly-registered DistinctId SQs and the DGN IDs they track (returns only those DGN IDs that are
    // potentially-rooted on this node)
    // see: [[updateDistinctIdStandingQueriesOnNode]]
    val newDistinctIdSqDgns = for {
      (sqId, runningSq) <- graph.runningStandingQueries
      dgnId <- runningSq.query.query match {
        case dgnPattern: StandingQueryPattern.DomainGraphNodeStandingQueryPattern => Some(dgnPattern.dgnId)
        case _ => None
      }
      subscriber = Right(sqId)
      alreadySubscribed = domainGraphSubscribers.containsSubscriber(dgnId, subscriber, sqId)
      if !alreadySubscribed
    } yield sqId -> dgnId

    // Make a best-effort attempt at restoring the nodeParentIndex: This will fail for DGNs that no longer exist,
    // so also make note of which those are for further cleanup.
    // By doing this after removing `locallyWatchedDgnsToRemove`, we'll have fewer wasted entries in the
    // reconstructed index. By doing this after journal restoration, we ensure that this reconstruction + removal
    // detection will be as complete as possible
    val (nodeParentIndexPruned, propogationDgnsToRemove) =
      NodeParentIndex.reconstruct(domainNodeIndex, domainGraphSubscribers.subscribersToThisNode.keys, dgnRegistry)
    this.domainGraphNodeParentIndex = nodeParentIndexPruned

    // stop tracking subscribers of deleted DGNs that were previously propogating messages
    domainGraphSubscribers.removeSubscribersOf(propogationDgnsToRemove)

    // Now that we have a comprehensive diff of the SQs added/removed, debug-log that diff.
    if (log.isDebugEnabled) {
      // serializing DGN collections is potentially nontrivial work, so only do it when the target log level is enabled
      log.debug(
        s"""Detected Standing Query changes while asleep. Removed DGNs:
           |${(propogationDgnsToRemove ++ locallyWatchedDgnsToRemove).toList.distinct}.
           |Added DGNs: ${newDistinctIdSqDgns}. Catching up now.""".stripMargin.replace('\n', ' ')
      )
    }

    // TODO ensure replay related to a dgn is no-op when that dgn is absent

    // TODO clear expired DGN/DistinctId data out of snapshots (at least, avoid re-snapshotting abandoned data,
    //      but also to avoid reusing expired caches)

    // Conceptually, during this phase we only need to synchronously compute+store initial local state for the
    // newly-registered SQs. However, in practice this is unnecessary and inefficient, since in order to cause off-node
    // effects in the final phase, we'll need to re-run most of the computation anyway (in the loop over
    // `newDistinctIdSqDgns` towards the end of this block). If we wish to make the final phase asynchronous, we'll need
    // to apply the local effects as follows:
    //    newDistinctIdSqDgns.foreach { case (sqId, dgnId) =>
    //      receiveDomainNodeSubscription(Right(sqId), dgnId, Set(sqId), shouldSendReplies = false)
    //    }

    // Standing query information restored before this point is for state/answers already processed, and so it
    // caused no effects off this node while restoring itself.
    // Phase: Having fully caught up with the target time, and applied local effects that occurred while the node
    // was asleep, we can move on to do other catch-up-work-while-sleeping which does cause effects off this node:

    // Finish computing (and send) initial results for each of the newly-registered DGNs
    // as this can cause off-node effects (notably: SQ results may be issued to a user), we opt out of this stage on
    // historical nodes.
    //
    // By corollary, a thoroughgoing node at time X may have a more complete DistinctId Standing Query index than a
    // reconstruction of that same node as a historical (atTime=Some(X)) node. This is acceptable, as historical nodes
    // should not receive updates and therefore should not propogate standing query effects.
    if (atTime.isEmpty) {
      newDistinctIdSqDgns.foreach { case (sqId, dgnId) =>
        receive(CreateDomainNodeSubscription(dgnId, Right(sqId), Set(sqId)))
      }

      // Final phase: sync MultipleValues SQs (mixes local + off-node effects)
      updateMultipleValuesStandingQueriesOnNode()
    }
  }

  /** Synchronizes this node's operating standing queries with those currently active on the thoroughgoing graph.
    * After a node is woken and restored to the state it was in before sleeping, it may need to catch up on new/deleted
    * standing queries which changed while it was asleep. This function catches the node up to the current collection
    * of live standing queries. If called from a historical node, this function is a no-op.
    * - Registers and emits initial results for any standing queries not yet registered on this node
    * - Removes any standing queries defined on this node but no longer known to the graph
    */
  protected def syncStandingQueries(): Unit =
    if (atTime.isEmpty) {
      updateDistinctIdStandingQueriesOnNode(shouldSendReplies = true)
      updateMultipleValuesStandingQueriesOnNode()
    }

  /** Fast check for if a number is a power of 2 */
  private[this] def isPowerOfTwo(n: Int): Boolean = (n & (n - 1)) == 0

  protected def edgeEventHasEffect(event: EdgeEvent): Boolean = event match {
    case EdgeAdded(edge) => !edges.contains(edge)
    case EdgeRemoved(edge) => edges.contains(edge)
  }
  protected def propertyEventHasEffect(event: PropertyEvent): Boolean = event match {
    case PropertySet(key, value) => !properties.get(key).contains(value)
    case PropertyRemoved(key, _) => properties.contains(key)
  }

  /** Enforces processEvents invariants before delegating to `onEffecting` (see block comment in [[BaseNodeActor]]
    * @param hasEffectPredicate A function that, given an event, returns true if and only if the event would change the
    *                           state of the node
    * @param events             The events to apply to this node, in the order they should be applied
    * @param atTimeOverride     Supply a number if you wish to override the number produced by the node's actor clock,
    *                           recorded as the timestamp of the event when writing to the journal.
    * @param onEffecting        The effect to be run -- this will be provided the final, deduplicated list of events to
    *                           apply, in order. The events represent the minimal set of events that will change node
    *                           state in a way equivalent to if all of the original `events` were applied.
    */
  protected[this] def guardEvents[E <: NodeChangeEvent](
    hasEffectPredicate: E => Boolean,
    events: List[E],
    atTimeOverride: Option[EventTime],
    onEffecting: NonEmptyList[NodeEvent.WithTime[E]] => Future[Done.type]
  ): Future[Done.type] = {
    val produceEventTime = atTimeOverride.fold(() => tickEventSequence())(() => _)
    refuseHistoricalUpdates(events)(
      NonEmptyList.fromList(events.filter(hasEffectPredicate)) match {
        case Some(effectfulEvents) => onEffecting(effectfulEvents.map(e => NodeEvent.WithTime(e, produceEventTime())))
        case None => Future.successful(Done)
      }
    )
  }

  // This is marked private and wrapped with two separate callable methods that either allow a collection or allow passing a custom `atTime`, but not both.
  private[this] def propertyEvents(events: List[PropertyEvent], atTime: Option[EventTime]): Future[Done.type] =
    guardEvents[PropertyEvent](
      propertyEventHasEffect,
      events,
      atTime,
      persistAndApplyEventsEffectsInMemory[PropertyEvent](_, _.toList.foreach(applyPropertyEffect))
    )

  protected def processPropertyEvent(
    event: PropertyEvent,
    atTimeOverride: Option[EventTime] = None
  ): Future[Done.type] = propertyEvents(event :: Nil, atTimeOverride)

  protected def processPropertyEvents(events: List[PropertyEvent]): Future[Done.type] =
    propertyEvents(events, None)

  private[this] def edgeEvents(events: List[EdgeEvent], atTime: Option[EventTime]): Future[Done.type] =
    guardEvents[EdgeEvent](
      edgeEventHasEffect,
      events,
      atTime,
      persistAndApplyEventsEffectsInMemory[EdgeEvent](_, _.toList.foreach(applyEdgeEffect))
    )

  protected def processEdgeEvents(
    events: List[EdgeEvent]
  ): Future[Done.type] = edgeEvents(events, None)
  protected def processEdgeEvent(
    event: EdgeEvent,
    atTimeOverride: Option[EventTime]
  ): Future[Done.type] = edgeEvents(event :: Nil, atTimeOverride)

  /** This is just an assertion to guard against programmer error.
    * @param events Just for the [[IllegalHistoricalUpdate]] error returned, which doesn't even use it in its message? Maybe it should be passed-through as an arg to [[action]], so callers don't have to specify it twice?
    * @param action The action to run if this is indeed not a hisorical node.
    * @tparam A
    * @return
    */
  def refuseHistoricalUpdates[A](events: Seq[NodeEvent])(action: => Future[A]): Future[A] =
    atTime.fold(action)(historicalTime => Future.failed(IllegalHistoricalUpdate(events, qid, historicalTime)))

  protected def processDomainIndexEvent(
    event: DomainIndexEvent
  ): Future[Done.type] =
    refuseHistoricalUpdates(event :: Nil)(
      persistAndApplyEventsEffectsInMemory[DomainIndexEvent](
        NonEmptyList.one(NodeEvent.WithTime(event, tickEventSequence())),
        // We know there is only one event here, because we're only passing one above.
        // So just calling .head works as well as .foreach
        events => applyDomainIndexEffect(events.head, shouldCauseSideEffects = true)
      )
    )

  protected def persistAndApplyEventsEffectsInMemory[A <: NodeEvent](
    effectingEvents: NonEmptyList[NodeEvent.WithTime[A]],
    applyEventsEffectsInMemory: NonEmptyList[A] => Unit
  ): Future[Done.type] = {
    val persistAttempts = new AtomicInteger(1)
    def persistEventsToJournal(): Future[Done.type] =
      if (persistenceConfig.journalEnabled) {
        metrics.persistorPersistEventTimer
          .time(persistor.persistEvents(qid, effectingEvents.toList))
          .transform(
            _ =>
              // TODO: add a metric to count `persistAttempts`
              Done,
            (e: Throwable) => {
              val attemptCount = persistAttempts.getAndIncrement()
              log.info(
                s"Retrying persistence from node: ${qid.pretty} with events: $effectingEvents after: " +
                s"$attemptCount attempts, with error: $e"
              )
              e
            }
          )(cypherEc)
      } else Future.successful(Done)

    graph.effectOrder match {
      case EventEffectOrder.MemoryFirst =>
        val events = effectingEvents.map(_.event)
        applyEventsEffectsInMemory(events)
        notifyNodeUpdate(events collect { case e: NodeChangeEvent => e })
        akka.pattern.retry(
          () => persistEventsToJournal(),
          Int.MaxValue,
          1.millisecond,
          10.seconds,
          randomFactor = 0.1d
        )(cypherEc, context.system.scheduler)
      case EventEffectOrder.PersistorFirst =>
        pauseMessageProcessingUntil[Done.type](
          persistEventsToJournal(),
          {
            case Success(_) =>
              // Executed by this actor (which is not slept), in order before any other messages are processed.
              val events = effectingEvents.map(_.event)
              applyEventsEffectsInMemory(events)
              notifyNodeUpdate(events collect { case e: NodeChangeEvent => e })
            case Failure(e) =>
              log.info(
                s"Persistor error occurred when writing events to journal on node: ${qid.pretty} Will not apply " +
                s"events: $effectingEvents to in-memory state. Returning failed result. Error: $e"
              )
          }
        ).map(_ => Done)(ExecutionContexts.parasitic)
    }

  }

  private[this] def persistSnapshot(): Unit = if (atTime.isEmpty) {
    val occurredAt: EventTime = tickEventSequence()
    val snapshot = toSnapshotBytes(occurredAt)
    metrics.snapshotSize.update(snapshot.length)

    def persistSnapshot(): Future[Unit] =
      metrics.persistorPersistSnapshotTimer.time(
        persistor.persistSnapshot(
          qid,
          if (persistenceConfig.snapshotSingleton) EventTime.MaxValue else occurredAt,
          snapshot
        )
      )

    def infinitePersisting(logFunc: String => Unit, f: Future[Unit] = persistSnapshot()): Future[Unit] =
      f.recoverWith { case NonFatal(e) =>
        logFunc(s"Persisting snapshot for: $occurredAt is being retried after the error: $e")
        infinitePersisting(logFunc, persistSnapshot())
      }(cypherEc)

    graph.effectOrder match {
      case EventEffectOrder.MemoryFirst =>
        infinitePersisting(log.info)
      case EventEffectOrder.PersistorFirst =>
        // There's nothing sane to do if this fails; there's no query result to fail. Just retry forever and deadlock.
        // The important intention here is to disallow any subsequent message (e.g. query) until the persist succeeds,
        // and to disallow `runPostActions` until persistence succeeds.
        val _ = pauseMessageProcessingUntil(infinitePersisting(log.warning))
    }
    latestUpdateAfterSnapshot = None
  } else {
    log.debug("persistSnapshot called on historical node: This indicates programmer error.")
  }

  /** The folling three methods apply effects of the provided events to the node state.
    * For [[EdgeEvent]], [[PropertyEvent]], and [[DomainIndexEvent]], respectively
    * @param event                 thee event to apply
    */
  private[this] def applyEdgeEffect(event: EdgeEvent): Unit = event match {
    case EdgeAdded(edge) =>
      // The more edges you get, the worse it is to sleep
      val len = edges.size
      if (len > 7 && isPowerOfTwo(len)) costToSleep.incrementAndGet()

      val edgeCollectionSizeWarningInterval = 10000
      if (log.isWarningEnabled && (len + 1) % edgeCollectionSizeWarningInterval == 0)
        log.warning(s"Node: ${qid.pretty} has: ${len + 1} edges")

      metrics.nodeEdgesCounter.increment(previousCount = len)
      edges += edge
      ()

    case EdgeRemoved(edge) =>
      metrics.nodeEdgesCounter.decrement(previousCount = edges.size)
      edges -= edge
      ()

  }

  private[this] def applyPropertyEffect(event: PropertyEvent): Unit = event match {
    case PropertySet(key, value) =>
      metrics.nodePropertyCounter.increment(previousCount = properties.size)
      properties = properties + (key -> value)
    case PropertyRemoved(key, _) =>
      metrics.nodePropertyCounter.decrement(previousCount = properties.size)
      properties = properties - key
  }

  /** Apply a [[DomainIndexEvent]] to the node state
    * @param event the event to apply
    * @param shouldCauseSideEffects whether the application of this event should cause off-node side effects, such
    *                               as Standing Query results. This value should be false when restoring
    *                               events from a journal.
    */
  private[this] def applyDomainIndexEffect(event: DomainIndexEvent, shouldCauseSideEffects: Boolean): Unit = {
    import DomainIndexEvent._
    event match {
      case CreateDomainNodeSubscription(dgnId, nodeId, forQuery) =>
        receiveDomainNodeSubscription(Left(nodeId), dgnId, forQuery, shouldSendReplies = shouldCauseSideEffects)

      case CreateDomainStandingQuerySubscription(dgnId, sqId, forQuery) =>
        receiveDomainNodeSubscription(Right(sqId), dgnId, forQuery, shouldSendReplies = shouldCauseSideEffects)

      case DomainNodeSubscriptionResult(from, dgnId, result) =>
        receiveIndexUpdate(from, dgnId, result, shouldSendReplies = shouldCauseSideEffects)

      case CancelDomainNodeSubscription(dgnId, fromSubscriber) =>
        cancelSubscription(dgnId, Some(Left(fromSubscriber)), shouldSendReplies = shouldCauseSideEffects)

    }
  }

  /** Call this if effects were applied to the node state (it was modified)
    * to update the "last update" timestamp, save a snapshot (if configured to),
    * and notify any subscribers of the applied [[NodeChangeEvent]]s
    * @param events
    */
  private[this] def notifyNodeUpdate(events: List[NodeChangeEvent]): Unit = {
    latestUpdateAfterSnapshot = Some(peekEventSequence())
    lastWriteMillis = previousMessageMillis()
    if (persistenceConfig.snapshotOnUpdate) persistSnapshot()
    runPostActions(events)
  }

  /** Hook for registering some arbitrary action after processing a node event. Right now, all this
    * does is advance standing queries
    *
    * @param events ordered sequence of node events produced from a single message.
    */
  private[this] def runPostActions(events: List[NodeChangeEvent]): Unit = events.foreach { event =>
    localEventIndex.standingQueriesWatchingNodeEvent(
      event,
      {
        case cypherSubscriber: StandingQueryLocalEventIndex.StandingQueryWithId =>
          updateMultipleValuesSqs(event, cypherSubscriber)
          false
        case StandingQueryLocalEventIndex.DomainNodeIndexSubscription(dgnId) =>
          dgnRegistry.getIdentifiedDomainGraphNode(dgnId) match {
            case Some(dgn) =>
              // ensure that this node is subscribed to all other necessary nodes to continue processing the DGN
              ensureSubscriptionToDomainEdges(
                dgn,
                domainGraphSubscribers.getRelatedQueries(dgnId),
                shouldSendReplies = true
              )
              // inform all subscribers to this node about any relevant changes caused by the recent event
              domainGraphSubscribers.updateAnswerAndNotifySubscribers(dgn, shouldSendReplies = true)
              false
            case None =>
              true // true returned to standingQueriesWatchingNodeEvent indicates record should be removed
          }
      }
    )
  }

  /** Serialize node state into a binary node snapshot
    *
    * @note returning just bytes instead of [[NodeSnapshot]] means that we don't need to worry
    * about accidentally leaking references to (potentially thread-unsafe) internal actor state
    *
    * @return serialized node snapshot
    */
  def toSnapshotBytes(time: EventTime): Array[Byte] = {
    latestUpdateAfterSnapshot = None // TODO: reconsider what to do if saving the snapshot fails!
    SnapshotCodec.format.write(
      NodeSnapshot(
        time,
        properties,
        edges.toSerialize,
        domainGraphSubscribers.subscribersToThisNode,
        domainNodeIndex.index
      )
    )
  }

  def debugNodeInternalState(): Future[NodeInternalState] = {
    // Return a string that (if possible) shows the deserialized representation
    def propertyValue2String(propertyValue: PropertyValue): String =
      propertyValue.deserialized.fold(
        _ => HexConversions.formatHexBinary(propertyValue.serialized),
        _.toString
      )

    val subscribersStrings = domainGraphSubscribers.subscribersToThisNode.toList
      .map { case (a, c) =>
        a -> c.subscribers.map {
          case Left(q) => q.pretty
          case Right(x) => x
        } -> c.lastNotification -> c.relatedQueries
      }
      .map(_.toString)

    val domainNodeIndexStrings = domainNodeIndex.index.toList
      .map(t => t._1.pretty -> t._2.map { case (a, c) => a -> c })
      .map(_.toString)

    val dgnLocalEventIndexSummary = {
      val propsIdx = localEventIndex.watchingForProperty.view.map { case (propertyName, notifiables) =>
        propertyName.name -> notifiables.toList.collect {
          case StandingQueryLocalEventIndex.DomainNodeIndexSubscription(dgnId) =>
            dgnId
        }
      }.toMap
      val edgesIdx = localEventIndex.watchingForEdge.view.map { case (edgeLabel, notifiables) =>
        edgeLabel.name -> notifiables.toList.collect {
          case StandingQueryLocalEventIndex.DomainNodeIndexSubscription(dgnId) =>
            dgnId
        }
      }.toMap
      val anyEdgesIdx = localEventIndex.watchingForAnyEdge.collect {
        case StandingQueryLocalEventIndex.DomainNodeIndexSubscription(dgnId) =>
          dgnId
      }

      DgnLocalEventIndexSummary(
        propsIdx,
        edgesIdx,
        anyEdgesIdx.toList
      )
    }

    persistor
      .getJournalWithTime(
        qid,
        startingAt = EventTime.MinValue,
        endingAt =
          atTime.map(EventTime.fromMillis).map(_.largestEventTimeInThisMillisecond).getOrElse(EventTime.MaxValue),
        includeDomainIndexEvents = false
      )
      .recover { case err =>
        log.error(err, "failed to get journal for node: {}", qidAtTime.debug)
        Iterable.empty
      }(context.dispatcher)
      .map { journal =>
        NodeInternalState(
          atTime,
          properties.view.mapValues(propertyValue2String).toMap,
          edges.toSet,
          latestUpdateAfterSnapshot,
          subscribersStrings,
          domainNodeIndexStrings,
          getSqState(),
          dgnLocalEventIndexSummary,
          multipleValuesStandingQueries.view.map {
            case ((globalId, sqId), (MultipleValuesStandingQuerySubscribers(_, _, subs), st)) =>
              LocallyRegisteredStandingQuery(
                sqId.toString,
                globalId.toString,
                subs.map(_.toString).toSet,
                st.toString
              )
          }.toVector,
          journal.toSet,
          getNodeHashCode().value
        )
      }(context.dispatcher)
  }

  def getNodeHashCode(): GraphNodeHashCode =
    GraphNodeHashCode(qid, properties, edges.toSet)

  def getSqState(): SqStateResults =
    SqStateResults(
      domainGraphSubscribers.subscribersToThisNode.toList.flatMap { case (dgnId, subs) =>
        subs.subscribers.toList.collect { case Left(q) => // filters out receivers outside the graph
          SqStateResult(dgnId, q, subs.lastNotification)
        }
      },
      domainNodeIndex.index.toList.flatMap { case (q, m) =>
        m.toList.map { case (dgnId, lastN) =>
          SqStateResult(dgnId, q, lastN)
        }
      }
    )
}

object NodeActor {

  type Journal = Iterable[NodeEvent]
  type MultipleValuesStandingQueries = mutable.Map[
    (StandingQueryId, MultipleValuesStandingQueryPartId),
    (MultipleValuesStandingQuerySubscribers, MultipleValuesStandingQueryState)
  ]

  @throws[NodeWakeupFailedException]("When snapshot could not be deserialized")
  private[this] def deserializeSnapshotBytes(
    snapshotBytes: Array[Byte],
    qidForDebugging: QuineIdAtTime
  )(implicit idProvider: QuineIdProvider): NodeSnapshot =
    SnapshotCodec.format
      .read(snapshotBytes)
      .fold(
        err =>
          throw new NodeWakeupFailedException(
            s"Snapshot could not be loaded for: ${qidForDebugging.debug}",
            err
          ),
        identity
      )

  def create(
    quineIdAtTime: QuineIdAtTime,
    recoverySnapshotBytes: Option[Array[Byte]],
    graph: BaseGraph
  ): Future[NodeActorConstructorArgs] =
    recoverySnapshotBytes match {
      case Some(recoverySnapshotBytes) =>
        val snapshot = deserializeSnapshotBytes(recoverySnapshotBytes, quineIdAtTime)(graph.idProvider)
        val multipleValuesStandingQueryStatesFut: Future[MultipleValuesStandingQueries] =
          getMultipleValuesStandingQueryStates(quineIdAtTime, graph)
        multipleValuesStandingQueryStatesFut.map(multipleValuesStandingQueryStates =>
          // this snapshot was created as the node slept, so there are no journal events after the snapshot
          NodeActorConstructorArgs
            .fromSnapshot(snapshot)
            .copy(multipleValuesStandingQueryStates = multipleValuesStandingQueryStates)
        )(graph.nodeDispatcherEC)

      case None => restoreFromSnapshotAndJournal(quineIdAtTime, graph)
    }

  private[this] def getMultipleValuesStandingQueryStates(
    qidAtTime: QuineIdAtTime,
    graph: BaseGraph
  ): Future[MultipleValuesStandingQueries] = (graph -> qidAtTime) match {
    case (sqGraph: StandingQueryOpsGraph, QuineIdAtTime(qid, None)) =>
      val idProv: QuineIdProvider = sqGraph.idProvider
      val lookupInfo = new MultipleValuesStandingQueryLookupInfo {
        def lookupQuery(queryPartId: MultipleValuesStandingQueryPartId): MultipleValuesStandingQuery =
          sqGraph.getStandingQueryPart(queryPartId)
        val node: QuineId = qid
        val idProvider: QuineIdProvider = idProv
      }
      sqGraph.metrics.persistorGetMultipleValuesStandingQueryStatesTimer
        .time {
          sqGraph.persistor.getMultipleValuesStandingQueryStates(qid)
        }
        .map { multipleValuesStandingQueryStates =>
          multipleValuesStandingQueryStates.map { case (sqIdAndPartId, bytes) =>
            val sqState = MultipleValuesStandingQueryStateCodec.format
              .read(bytes)
              .fold(
                err =>
                  throw new NodeWakeupFailedException(
                    s"NodeActor state (Standing Query States) for node: ${qidAtTime.debug(idProv)} could not be loaded",
                    err
                  ),
                identity
              )
            sqState._2.preStart(lookupInfo)
            sqIdAndPartId -> sqState
          }
        }(sqGraph.nodeDispatcherEC)
        .map(map => mutable.Map.from(map))(sqGraph.nodeDispatcherEC)
    case (_: StandingQueryOpsGraph, QuineIdAtTime(_, hasHistoricalTimestamp @ _)) =>
      // this is the right kind of graph, but by definition, historical nodes (ie, atTime != None)
      // have no multipleValues states
      Future.successful(mutable.Map.empty)
    case (nonStandingQueryGraph @ _, _) =>
      // wrong kind of graph: only [[StandingQueryOpsGraph]]s can manage MultipleValues Standing Queries
      Future.successful(mutable.Map.empty)

  }

  /** Load the state of specified the node at the specified time. The resultant NodeActorConstructorArgs should allow
    * the node to restore itself to its state prior to sleeping (up to removed Standing Queries) without any additional
    * persistor calls.
    *
    * @param untilOpt load changes made up to and including this time
    */
  private[this] def restoreFromSnapshotAndJournal(
    quineIdAtTime: QuineIdAtTime,
    graph: BaseGraph
  ): Future[NodeActorConstructorArgs] = {
    val QuineIdAtTime(qid, atTime) = quineIdAtTime
    val persistenceConfig = graph.persistor.persistenceConfig

    def getSnapshot(): Future[Option[NodeSnapshot]] =
      if (!persistenceConfig.snapshotEnabled) Future.successful(None)
      else {
        val upToTime = atTime match {
          case Some(historicalTime) if !persistenceConfig.snapshotSingleton =>
            EventTime.fromMillis(historicalTime)
          case _ =>
            EventTime.MaxValue
        }
        graph.metrics.persistorGetLatestSnapshotTimer
          .time {
            graph.persistor.getLatestSnapshot(qid, upToTime)
          }
          .map { maybeBytes =>
            maybeBytes.map(deserializeSnapshotBytes(_, quineIdAtTime)(graph.idProvider))
          }(graph.nodeDispatcherEC)
      }

    def getJournalAfter(after: Option[EventTime], includeDomainIndexEvents: Boolean): Future[Iterable[NodeEvent]] = {
      val startingAt = after.fold(EventTime.MinValue)(_.tickEventSequence(None))
      val endingAt = atTime match {
        case Some(until) => EventTime.fromMillis(until).largestEventTimeInThisMillisecond
        case None => EventTime.MaxValue
      }
      graph.metrics.persistorGetJournalTimer.time {
        graph.persistor.getJournal(qid, startingAt, endingAt, includeDomainIndexEvents)
      }
    }

    // Get the snapshot and journal events
    val snapshotAndJournal =
      getSnapshot()
        .flatMap { latestSnapshotOpt =>
          val journalAfterSnapshot: Future[Journal] = if (persistenceConfig.journalEnabled) {
            getJournalAfter(latestSnapshotOpt.map(_.time), includeDomainIndexEvents = atTime.isEmpty)
            // QU-429 to avoid extra retries, consider unifying the Failure types of `persistor.getJournal`, and adding a
            // recoverWith here to map any that represent irrecoverable failures to a [[NodeWakeupFailedException]]
          } else
            Future.successful(Vector.empty)

          journalAfterSnapshot.map(journalAfterSnapshot => (latestSnapshotOpt, journalAfterSnapshot))(
            ExecutionContexts.parasitic
          )
        }(graph.nodeDispatcherEC)

    // Get the materialized standing query states for MultipleValues.
    val multipleValuesStandingQueryStates: Future[MultipleValuesStandingQueries] =
      getMultipleValuesStandingQueryStates(quineIdAtTime, graph)

    // Will defer all other message processing until the Future is complete.
    // It is OK to ignore the returned future from `pauseMessageProcessingUntil` because nothing else happens during
    // initialization of this actor. Additional message processing is deferred by `pauseMessageProcessingUntil`'s
    // message stashing.
    snapshotAndJournal
      .zip(multipleValuesStandingQueryStates)
  }.map { case ((snapshotOpt, journal), multipleValuesStates) =>
    snapshotOpt
      .fold(NodeActorConstructorArgs.empty)(NodeActorConstructorArgs.fromSnapshot)
      .copy(
        initialJournal = journal,
        multipleValuesStandingQueryStates = multipleValuesStates
      )
  }(graph.nodeDispatcherEC)
}
