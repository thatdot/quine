package com.thatdot.quine.graph

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.locks.StampedLock

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.Actor
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}

import cats.data.NonEmptyList
import cats.implicits._
import org.apache.pekko

import com.thatdot.common.logging.Log.{ActorSafeLogging, LogConfig, Safe, SafeInterpolator, SafeLoggableInterpolator}
import com.thatdot.common.logging.Pretty.PrettyHelper
import com.thatdot.common.quineid.QuineId
import com.thatdot.common.util.ByteConversions
import com.thatdot.quine.graph.AbstractNodeActor.internallyDeduplicatePropertyEvents
import com.thatdot.quine.graph.NodeEvent.WithTime
import com.thatdot.quine.graph.PropertyEvent.{PropertyRemoved, PropertySet}
import com.thatdot.quine.graph.behavior.DomainNodeIndexBehavior.{NodeParentIndex, SubscribersToThisNodeUtil}
import com.thatdot.quine.graph.behavior.{
  ActorClock,
  AlgorithmBehavior,
  CypherBehavior,
  DomainNodeIndexBehavior,
  GoToSleepBehavior,
  LiteralCommandBehavior,
  MultipleValuesStandingQueryBehavior,
  MultipleValuesStandingQueryPartSubscription,
  PriorityStashingBehavior,
  QuinePatternQueryBehavior,
}
import com.thatdot.quine.graph.cypher.MultipleValuesResultsReporter
import com.thatdot.quine.graph.cypher.quinepattern.GraphEvent
import com.thatdot.quine.graph.edges.{EdgeProcessor, MemoryFirstEdgeProcessor, PersistorFirstEdgeProcessor}
import com.thatdot.quine.graph.messaging.BaseMessage.Done
import com.thatdot.quine.graph.messaging.LiteralMessage.{
  DgnWatchableEventIndexSummary,
  DistinctIdIndexState,
  DistinctIdParentLink,
  DistinctIdSubscriberState,
  JournalEntry,
  LocallyRegisteredStandingQuery,
  NodeInternalState,
  SqStateResults,
}
import com.thatdot.quine.graph.messaging.{QuineIdOps, QuineRefOps, SpaceTimeQuineId}
import com.thatdot.quine.graph.metrics.HostQuineMetrics
import com.thatdot.quine.graph.metrics.implicits.TimeFuture
import com.thatdot.quine.graph.quinepattern.QuinePatternOpsGraph
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.model.{HalfEdge, Milliseconds, PropertyValue, QuineIdProvider, QuineValue}
import com.thatdot.quine.persistor.{EventEffectOrder, NamespacedPersistenceAgent, PersistenceConfig}
import com.thatdot.quine.util.Log.implicits._

/** The fundamental graph unit for both data storage (eg [[properties]]) and
  * computation (as a Pekko actor).
  * At most one [[AbstractNodeActor]] exists in the actor system ([[graph.system]]) per node per moment in
  * time (see [[qidAtTime]]).
  *
  * [[AbstractNodeActor]] is the default place to define implementation of interfaces exposed by [[BaseNodeActor]] and
  * [[BaseNodeActorView]]. Classes extending [[AbstractNodeActor]] (e.g., [[NodeActor]]) should be kept as lightweight
  * as possible, ideally including only construction-time logic and an [[Actor.receive]] implementation.
  *
  * @param qidAtTime    the ID that comprises this node's notion of nominal identity -- analogous to pekko's ActorRef
  * @param graph        a reference to the graph in which this node exists
  * @param costToSleep  see [[CostToSleep]]
  * @param wakefulState an atomic reference used like a variable to track the current lifecycle state of this node.
  *                     This is (and may be expected to be) threadsafe, so that [[GraphShardActor]]s can access it
  * @param actorRefLock a lock on this node's [[ActorRef]] used to hard-stop messages when sleeping the node (relayTell uses
  *                     tryReadLock during its tell, so if a write lock is held for a node's actor, no messages can be
  *                     sent to it)
  * @param properties   the properties of this node. This must be a var of an immutable Map, as references to it are
  *                     closed over (and expected to be immutable) by MultipleValuesStandingQueries
  */
abstract private[graph] class AbstractNodeActor(
  val qidAtTime: SpaceTimeQuineId,
  val graph: QuinePatternOpsGraph with StandingQueryOpsGraph with CypherOpsGraph,
  costToSleep: CostToSleep,
  protected val wakefulState: AtomicReference[WakefulState],
  protected val actorRefLock: StampedLock,
  protected var properties: Map[Symbol, PropertyValue],
  initialEdges: Iterable[HalfEdge],
  initialDomainGraphSubscribers: mutable.Map[
    DomainGraphNodeId,
    SubscribersToThisNodeUtil.DistinctIdSubscription,
  ],
  protected val domainNodeIndex: DomainNodeIndexBehavior.DomainNodeIndex,
  protected val multipleValuesStandingQueries: NodeActor.MultipleValuesStandingQueries,
)(implicit protected val logConfig: LogConfig)
    extends Actor
    with ActorSafeLogging
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
    with QuinePatternQueryBehavior
    with ActorClock {
  val qid: QuineId = qidAtTime.id
  val namespace: NamespaceId = qidAtTime.namespace
  val atTime: Option[Milliseconds] = qidAtTime.atTime
  implicit val idProvider: QuineIdProvider = graph.idProvider
  protected val persistor: NamespacedPersistenceAgent = graph.namespacePersistor(namespace).get // or throw!
  protected val persistenceConfig: PersistenceConfig = persistor.persistenceConfig
  protected val metrics: HostQuineMetrics = graph.metrics

  /** Utility for inheritors to choose a default EdgeProcessor. Accounts for configuration, and returns an edge
    * processor appropriate for arbitrary usage by this node, and only this node
    */
  protected[this] def defaultSynchronousEdgeProcessor: EdgeProcessor = {
    val edgeCollection = graph.edgeCollectionFactory(qid)
    initialEdges.foreach(edgeCollection.addEdge)
    val persistEventsToJournal: NonEmptyList[WithTime[EdgeEvent]] => Future[Unit] =
      if (persistor.persistenceConfig.journalEnabled)
        events => metrics.persistorPersistEventTimer.time(persistor.persistNodeChangeEvents(qid, events))
      else
        _ => Future.unit

    graph.effectOrder match {
      case EventEffectOrder.PersistorFirst =>
        new PersistorFirstEdgeProcessor(
          edges = edgeCollection,
          persistToJournal = persistEventsToJournal,
          pauseMessageProcessingUntil = pauseMessageProcessingUntil,
          onBatchApplied = countJournaledEdgesAndUpdateSnapshotTimestamp,
          runPostActions = runPostActions,
          qid = qid,
          costToSleep = costToSleep,
          nodeEdgesCounter = metrics.nodeEdgesCounter(namespace),
        )
      case EventEffectOrder.MemoryFirst =>
        new MemoryFirstEdgeProcessor(
          edges = edgeCollection,
          persistToJournal = persistEventsToJournal,
          onBatchApplied = countJournaledEdgesAndUpdateSnapshotTimestamp,
          runPostActions = runPostActions,
          qid = qid,
          costToSleep = costToSleep,
          nodeEdgesCounter = metrics.nodeEdgesCounter(namespace),
        )(graph.system, idProvider, logConfig)
    }
  }

  protected val dgnRegistry: DomainGraphNodeRegistry = graph.dgnRegistry
  protected val domainGraphSubscribers: SubscribersToThisNode = SubscribersToThisNode(initialDomainGraphSubscribers)

  protected var latestUpdateAfterSnapshot: Option[EventTime] = None
  protected var lastWriteMillis: Long = 0

  /** Journal events applied to this node since its last persisted snapshot: the replay the next
    * snapshot would save. [[replayJournal]] seeds it with the length of the journal replayed at
    * wake, so it spans sleep/wake cycles and a node that repeatedly sleeps below the threshold
    * still accumulates towards it. Only advanced under a journal; without one there is no
    * threshold to read it.
    * @see [[HostQuineMetrics.SnapshotEconomics]]
    */
  protected var journaledEventsAppliedSinceSnapshot: Int = 0

  /** Bound to the edge processors' `onBatchApplied`. Edge events reach the journal through
    * [[defaultSynchronousEdgeProcessor]] rather than [[persistAndApplyEventsEffectsInMemory]], so they are
    * counted here.
    */
  private[this] def countJournaledEdgesAndUpdateSnapshotTimestamp(journaled: Int): Unit = {
    if (persistenceConfig.journalEnabled) journaledEventsAppliedSinceSnapshot += journaled
    updateLastWriteAfterSnapshot()
  }

  protected def updateRelevantToSnapshotOccurred(): Unit = {
    if (atTime.nonEmpty) {
      log.warn(safe"Attempted to flag a historical node as being updated -- this update will not be persisted.")
    }
    // TODO: should this update `lastWriteMillis` too?
    latestUpdateAfterSnapshot = Some(peekEventSequence())
  }

  /** @see [[StandingQueryWatchableEventIndex]]
    */
  protected var watchableEventIndex: StandingQueryWatchableEventIndex =
    // NB this initialization is non-authoritative: only after journal restoration is complete can this be
    // comprehensively reconstructed (see the block below the definition of [[nodeParentIndex]]). However, journal
    // restoration may access [[localEventIndex]] and/or [[nodeParentIndex]] so they must be at least initialized
    StandingQueryWatchableEventIndex
      .from(
        dgnRegistry,
        domainGraphSubscribers.subscribersToThisNode.keysIterator,
        multipleValuesStandingQueries.iterator.map { case (sqIdAndPartId, (_, state)) => sqIdAndPartId -> state },
        graph.labelsProperty,
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

  protected var multipleValuesResultReporters: Map[StandingQueryId, MultipleValuesResultsReporter] =
    MultipleValuesResultsReporter.rehydrateReportersOnNode(
      multipleValuesStandingQueries.values,
      properties,
      graph,
      namespace,
    )

  /** Synchronizes this node's operating standing queries with those currently active on the thoroughgoing graph.
    * If called from a historical node, this function is a no-op
    * - Registers and emits initial results for any standing queries not yet registered on this node
    * - Removes any standing queries defined on this node but no longer known to the graph
    */
  protected def syncStandingQueries(): Unit =
    if (atTime.isEmpty) {
      updateDistinctIdStandingQueriesOnNode()
      updateMultipleValuesStandingQueriesOnNode()
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
    onEffecting: NonEmptyList[NodeEvent.WithTime[E]] => Future[Done.type],
  ): Future[Done.type] = {
    val produceEventTime = atTimeOverride.fold(() => tickEventSequence())(() => _)
    refuseHistoricalUpdates(events)(
      NonEmptyList.fromList(events.filter(hasEffectPredicate)) match {
        case Some(effectfulEvents) => onEffecting(effectfulEvents.map(e => NodeEvent.WithTime(e, produceEventTime())))
        case None => Future.successful(Done)
      },
    )
  }

  // This is marked private and wrapped with two separate callable methods that either allow a collection or allow passing a custom `atTime`, but not both.
  private[this] def propertyEvents(events: List[PropertyEvent], atTime: Option[EventTime]): Future[Done.type] =
    guardEvents[PropertyEvent](
      propertyEventHasEffect,
      events,
      atTime,
      persistAndApplyEventsEffectsInMemory[PropertyEvent](
        _,
        persistor.persistNodeChangeEvents(qid, _),
        events =>
          events.toList.foreach { e =>
            e match {
              case PropertySet(_, value) =>
                // Record the size of the property to the appropriate histogram. NB while this may cause the property to be
                // serialized, it is not an _extra_ serialization, because PropertyValues cache their serialized form. Any
                // later persistence will simply reuse the serialization performed here.
                metrics.propertySizes(namespace).update(value.serialized.length)
              case PropertyRemoved(_, _) => // no relevant metric updates
            }
            applyPropertyEffect(e)
          },
      ),
    )

  protected def processPropertyEvent(
    event: PropertyEvent,
    atTimeOverride: Option[EventTime] = None,
  ): Future[Done.type] = propertyEvents(event :: Nil, atTimeOverride)

  protected def processPropertyEvents(events: List[PropertyEvent]): Future[Done.type] =
    propertyEvents(internallyDeduplicatePropertyEvents(events), None)

  protected[this] def edgeEvents(events: List[EdgeEvent], atTime: Option[EventTime]): Future[Done.type] =
    refuseHistoricalUpdates(events)(
      edges.processEdgeEvents(events, atTime.fold(() => tickEventSequence())(() => _)),
    ).map(_ => Done)(ExecutionContext.parasitic)

  protected def processEdgeEvents(
    events: List[EdgeEvent],
  ): Future[Done.type] =
    edgeEvents(events, None)

  protected def processEdgeEvent(
    event: EdgeEvent,
    atTimeOverride: Option[EventTime],
  ): Future[Done.type] = edgeEvents(event :: Nil, atTimeOverride)

  /** This is just an assertion to guard against programmer error.
    * @param events Just for the [[IllegalHistoricalUpdate]] error returned, which doesn't even use it in its message?
    *               Maybe it should be passed-through as an arg to [[action]], so callers don't have to specify it
    *               twice?
    * @param action The action to run if this is indeed not a historical node.
    * @tparam A
    * @return
    */
  def refuseHistoricalUpdates[A](events: Seq[NodeEvent])(action: => Future[A]): Future[A] =
    atTime.fold(action)(historicalTime => Future.failed(IllegalHistoricalUpdate(events, qid, historicalTime)))

  /** Whether this event would change the DistinctId state that a journal replay rebuilds.
    *
    * The counterpart of [[propertyEventHasEffect]], holding DistinctId bookkeeping to the same rule: a journal row
    * stands for a change. Each case asks the state the event lands in whether the effect is already there, by the
    * same test the applying code itself uses, so the two cannot drift apart.
    */
  protected def domainIndexEventHasEffect(event: DomainIndexEvent): Boolean = {
    import DomainIndexEvent._
    event match {
      case CreateDomainStandingQuerySubscription(dgnId, sqId, forQueries) =>
        domainGraphSubscribers.subscriptionWouldChange(dgnId, Right(sqId), forQueries)
      case CreateDomainNodeSubscription(dgnId, nodeId, forQueries) =>
        domainGraphSubscribers.subscriptionWouldChange(dgnId, Left(nodeId), forQueries)
      // Cancelling for someone who is not a subscriber removes nothing, and the teardown that follows a removal
      // is reached only when one happened. See `cancelSubscription`.
      case CancelDomainNodeSubscription(dgnId, fromSubscriber) =>
        domainGraphSubscribers.hasSubscriber(dgnId, Left(fromSubscriber))
      case CancelDomainStandingQuerySubscription(dgnId, fromSubscriber) =>
        domainGraphSubscribers.hasSubscriber(dgnId, Right(fromSubscriber))
      // An answer repeating what is already recorded leaves this node holding the same thing, and one about a
      // pattern it does not ask about is refused outright. Keeping the refused ones out matters for more than
      // volume: a fold records an answer even where it has yet to re-derive the subscription, so a row the live
      // node threw away would otherwise come back as an entry it never had.
      case DomainNodeSubscriptionResult(from, dgnId, result) =>
        domainNodeIndex.answerWouldChange(from, dgnId, result)
    }
  }

  /** Handle a DistinctId command, journaling it only if it changes this node.
    *
    * Unlike a property event, a command that changes nothing here can still owe its sender a reply: a subscriber
    * re-asking a question it has already asked needs the answer again, because it may have lost the one it was
    * given. So the effect is applied either way and only the journal write is gated. Skipping that write also
    * keeps a no-op command from counting towards the events a snapshot would replace.
    */
  protected def processDomainIndexEvent(
    event: DomainIndexEvent,
  ): Future[Done.type] =
    refuseHistoricalUpdates(event :: Nil)(
      if (domainIndexEventHasEffect(event))
        persistAndApplyEventsEffectsInMemory[DomainIndexEvent](
          NonEmptyList.one(NodeEvent.WithTime(event, tickEventSequence())),
          persistor.persistDomainIndexEvents(qid, _),
          // We know there is only one event here, because we're only passing one above.
          // So just calling .head works as well as .foreach
          events => applyDomainIndexEffect(events.head, shouldCauseSideEffects = true),
        )
      else {
        applyDomainIndexEffect(event, shouldCauseSideEffects = true)
        Future.successful(Done)
      },
    )

  /** Record an event whose effect this node has already applied.
    *
    * For what a node works out for itself rather than being told: a standing query it serves is gone, so it retires
    * that subscription. The teardown follows from the query's absence and happens either way, and it must not be
    * done twice, so the caller applies it and this only writes the record. What the record adds is *when* it
    * happened, which absence cannot say and a replay needs; see
    * [[DomainIndexEvent.CancelDomainStandingQuerySubscription]]. Replay applies it from the event, at this
    * position, with no side effects.
    */
  protected[this] def journalAlreadyAppliedDomainIndexEvent(event: DomainIndexEvent): Unit = if (atTime.isEmpty) {
    val _ = persistAndApplyEventsEffectsInMemory[DomainIndexEvent](
      NonEmptyList.one(NodeEvent.WithTime(event, tickEventSequence())),
      persistor.persistDomainIndexEvents(qid, _),
      _ => (), // applied by the caller, which is what discovered it
    )
  }

  protected def persistAndApplyEventsEffectsInMemory[A <: NodeEvent](
    effectingEvents: NonEmptyList[NodeEvent.WithTime[A]],
    persistEvents: NonEmptyList[WithTime[A]] => Future[Unit],
    applyEventsEffectsInMemory: NonEmptyList[A] => Unit,
  ): Future[Done.type] = {
    val persistAttempts = new AtomicInteger(1)
    def persistEventsToJournal(): Future[Unit] =
      if (persistenceConfig.journalEnabled) {
        metrics.persistorPersistEventTimer
          .time(persistEvents(effectingEvents))
          .transform(
            _ =>
              // TODO: add a metric to count `persistAttempts`
              (),
            (e: Throwable) => {
              val attemptCount = persistAttempts.getAndIncrement()
              log.info(
                log"""Retrying persistence from node: $qid with events:
                     |${effectingEvents.toString} after: ${Safe(attemptCount)} attempts
                     |""".cleanLines withException e,
              )
              e
            },
          )(cypherEc)
      } else Future.unit

    // Counted here rather than in `persistEventsToJournal` so that a batch counts once, however
    // many times the persistor write is retried.
    def applyEffectsAndNotify(): Unit = {
      val events = effectingEvents.map(_.event)
      if (persistenceConfig.journalEnabled) journaledEventsAppliedSinceSnapshot += effectingEvents.size
      applyEventsEffectsInMemory(events)
      notifyNodeUpdate(events collect { case e: NodeChangeEvent => e })
    }

    graph.effectOrder match {
      case EventEffectOrder.MemoryFirst =>
        applyEffectsAndNotify()
        pekko.pattern
          .retry(
            () => persistEventsToJournal(),
            Int.MaxValue,
            1.millisecond,
            10.seconds,
            randomFactor = 0.1d,
          )(cypherEc, context.system.scheduler)
          .map(_ => Done)(ExecutionContext.parasitic)
      case EventEffectOrder.PersistorFirst =>
        pauseMessageProcessingUntil[Unit](
          persistEventsToJournal(),
          {
            case Success(_) =>
              // Executed by this actor (which is not slept), in order before any other messages are processed.
              applyEffectsAndNotify()
            case Failure(e) =>
              log.info(
                log"Persistor error occurred when writing events to journal on node: $qid Will not apply " +
                log"events: ${effectingEvents.toString} to in-memory state. Returning failed result" withException e,
              )
          },
          true,
        ).map(_ => Done)(ExecutionContext.parasitic)
    }

  }

  /** Write a snapshot now, rather than recording that one is owed.
    *
    * Ordinary nodes never need this: everything a snapshot holds either goes through the journal or is written on
    * update or sleep. A node whose state changes in a way none of those paths can see has to say so itself.
    */
  protected[this] def persistSnapshot(): Unit = if (atTime.isEmpty) {
    val occurredAt: EventTime = tickEventSequence()
    val snapshot = toSnapshotBytes(occurredAt)
    metrics.snapshotSize.update(snapshot.length)

    def persistSnapshot(): Future[Unit] =
      metrics.persistorPersistSnapshotTimer
        .time(
          persistor.persistSnapshot(
            qid,
            if (persistenceConfig.snapshotSingleton) EventTime.MaxValue else occurredAt,
            snapshot,
          ),
        )

    def infinitePersisting(logFunc: SafeInterpolator => Unit, f: => Future[Unit]): Future[Unit] =
      f.recoverWith { case NonFatal(e) =>
        logFunc(log"Persisting snapshot for: $occurredAt is being retried after the error:" withException e)
        infinitePersisting(logFunc, f)
      }(cypherEc)

    graph.effectOrder match {
      case EventEffectOrder.MemoryFirst =>
        infinitePersisting(s => log.info(s), persistSnapshot())
      case EventEffectOrder.PersistorFirst =>
        // There's nothing sane to do if this fails; there's no query result to fail. Just retry forever and deadlock.
        // The important intention here is to disallow any subsequent message (e.g. query) until the persist succeeds,
        // and to disallow `runPostActions` until persistence succeeds.
        val _ =
          pauseMessageProcessingUntil[Unit](infinitePersisting(s => log.warn(s), persistSnapshot()), _ => (), true)
    }
    latestUpdateAfterSnapshot = None
  } else {
    log.debug(safe"persistSnapshot called on historical node: This indicates programmer error.")
  }

  /** Apply a [[PropertyEvent]] to the node's properties map and update aggregate metrics on node property counts,
    * if applicable
    * @param event the event to apply
    */
  protected[this] def applyPropertyEffect(event: PropertyEvent): Unit = event match {
    case PropertySet(key, value) =>
      if (value.deserializedReady && value == PropertyValue(QuineValue.Null)) {
        // Should be impossible. If it's not, we'd like to know and fix it.
        logger.warn(safe"Setting a null property on key: ${Safe(key.name)}. This should have been a property removal.")
      }
      val oldValue = properties.get(key)
      metrics.nodePropertyCounter(namespace).increment(previousCount = properties.size)
      properties = properties + (key -> value)
      // State notification for property change
      handleGraphEvent(cypher.quinepattern.GraphEvent.PropertyChanged(key, oldValue, Some(value)))
      // State notification for label change (labels are stored in a special property)
      if (key == graph.labelsProperty) {
        val oldLabels = extractLabelsFromProperty(oldValue)
        val newLabels = extractLabelsFromProperty(Some(value))
        handleGraphEvent(cypher.quinepattern.GraphEvent.LabelsChanged(oldLabels, newLabels))
      }
    case PropertyRemoved(key, _) =>
      val oldPropValue = properties.get(key)
      metrics.nodePropertyCounter(namespace).decrement(previousCount = properties.size)
      properties = properties - key
      // State notification for property change
      handleGraphEvent(cypher.quinepattern.GraphEvent.PropertyChanged(key, oldPropValue, None))
      // State notification for label change (labels are stored in a special property)
      if (key == graph.labelsProperty) {
        val oldLabels = extractLabelsFromProperty(oldPropValue)
        handleGraphEvent(cypher.quinepattern.GraphEvent.LabelsChanged(oldLabels, Set.empty))
      }
  }

  /** Extract labels from a property value (used for V2 label change notifications) */
  private def extractLabelsFromProperty(propValue: Option[PropertyValue]): Set[Symbol] =
    propValue match {
      case Some(pv) =>
        pv.deserialized match {
          case Success(QuineValue.List(values)) =>
            values.flatMap {
              case QuineValue.Str(s) => Some(Symbol(s))
              case _ => None
            }.toSet
          case _ => Set.empty
        }
      case None => Set.empty
    }

  /** Apply a [[DomainIndexEvent]] to the node state, updating its DGB bookkeeping and potentially (only if
    * shouldCauseSideEffects) messaging other nodes with any relevant updates.
    * @param event                  the event to apply
    * @param shouldCauseSideEffects whether the application of this event should cause off-node side effects, such
    *                               as Standing Query results. This value should be false when restoring
    *                               events from a journal.
    */
  /** Which standing queries count as running, for an event happening now or one being replayed.
    *
    * Now, the graph is the authority. Mid-replay it is not: what runs now is the answer for now, not for the point
    * in this node's history being replayed, so the node's own tally of what its journal has introduced and retired
    * is what applies. See [[queriesLiveWhileReplaying]].
    */
  private[this] def isRunningWhen(live: Boolean): StandingQueryId => Boolean =
    if (live) q => graph.standingQueries(namespace).exists(_.runningStandingQuery(q).isDefined)
    else queriesLiveWhileReplaying.contains

  /** Follow what an event says about which standing queries were live when it happened.
    *
    * A subscription names the queries it is for, so they were live; a retirement says one is gone. A node
    * withdrawing its own subscription says nothing either way -- its queries may still be served elsewhere -- so it
    * is passed over. See [[queriesLiveWhileReplaying]].
    */
  private[this] def noteQueriesLiveAt(event: DomainIndexEvent): Unit = {
    import DomainIndexEvent._
    event match {
      case CreateDomainStandingQuerySubscription(_, sqId, relatedQueries) =>
        queriesLiveWhileReplaying ++= relatedQueries + sqId
      case CreateDomainNodeSubscription(_, _, relatedQueries) =>
        queriesLiveWhileReplaying ++= relatedQueries
      case CancelDomainStandingQuerySubscription(_, sqId) =>
        queriesLiveWhileReplaying -= sqId
      case CancelDomainNodeSubscription(_, _) => ()
      case DomainNodeSubscriptionResult(_, _, _) => ()
    }
  }

  protected[this] def applyDomainIndexEffect(
    event: DomainIndexEvent,
    shouldCauseSideEffects: Boolean,
  ): Unit = {
    import DomainIndexEvent._
    event match {
      /** Outer-most subscriber for a Standing Query (no dual) */
      case CreateDomainStandingQuerySubscription(dgnId, sqId, forQuery) =>
        receiveDomainNodeSubscription(Right(sqId), dgnId, forQuery, shouldSendReplies = shouldCauseSideEffects)

      /** Internal node-to-node subscription. Dual of: CancelDomainNodeSubscription */
      case CreateDomainNodeSubscription(dgnId, nodeId, forQuery) =>
        receiveDomainNodeSubscription(Left(nodeId), dgnId, forQuery, shouldSendReplies = shouldCauseSideEffects)

      /** Cancels internal subscriptions. Dual of: CreateDomainNodeSubscription */
      case CancelDomainNodeSubscription(dgnId, fromSubscriber) =>
        retireSubscription(
          dgnId,
          Left(fromSubscriber),
          isRunningWhen(live = shouldCauseSideEffects),
          shouldSendReplies = shouldCauseSideEffects,
        )

      /** Retires a standing query's subscription. Dual of: CreateDomainStandingQuerySubscription */
      case CancelDomainStandingQuerySubscription(dgnId, fromSubscriber) =>
        retireSubscription(
          dgnId,
          Right(fromSubscriber),
          isRunningWhen(live = shouldCauseSideEffects),
          shouldSendReplies = shouldCauseSideEffects,
        )

      /** Record of this node matching or not. */
      case DomainNodeSubscriptionResult(from, dgnId, result) =>
        receiveIndexUpdate(from, dgnId, result, shouldSendReplies = shouldCauseSideEffects)
    }
  }

  /** Record that this node was written to, without claiming its snapshot is now out of date.
    *
    * The two come apart whenever a write goes somewhere a snapshot does not reach. Such a write is still a write:
    * a node in the middle of one should no more be put to sleep than any other. But the snapshot would say nothing
    * new, so marking it stale only costs a rewrite. An [[com.thatdot.quine.graph.edges.EdgeProcessor]] whose writes
    * are not carried by the snapshot is expected to call this instead of [[updateLastWriteAfterSnapshot]].
    */
  protected[this] def recordWriteOccurred(): Unit =
    lastWriteMillis = previousMessageMillis()

  protected[this] def updateLastWriteAfterSnapshot(): Unit = {
    latestUpdateAfterSnapshot = Some(peekEventSequence())
    lastWriteMillis = previousMessageMillis()
    if (persistenceConfig.snapshotOnUpdate) persistSnapshot()
  }

  /** Call this if effects were applied to the node state (it was modified)
    * to update the "last update" timestamp, save a snapshot (if configured to),
    * and notify any subscribers of the applied [[NodeChangeEvent]]s
    * @param events
    */
  protected[this] def notifyNodeUpdate(events: List[NodeChangeEvent]): Unit = {
    updateLastWriteAfterSnapshot()
    runPostActions(events)
  }

  /** Fold the journal accumulated since the snapshot back into this node.
    *
    * A property or edge event re-runs the DistinctId evaluation it triggered when it was first
    * applied, with replies suppressed. `latestAnswer` is not journaled: it is rebuilt by
    * evaluating at the same points the live node evaluated. Skip those and the node wakes
    * remembering an answer from partway through its own history, and re-reports on the next write.
    *
    * @return The number of events applied while replaying this journal
    */
  protected[this] def replayJournal(journal: NodeActor.Journal): Int = {
    // A historical node serves reads at a past time and no standing query, and refuses to be marked
    // updated, which evaluating would do. It applies property and edge events and skips DistinctId
    // bookkeeping. The wake read in StaticNodeSupport already gives it no domain index events; the
    // guard below is so that holds here too.
    val isPresentNode = atTime.isEmpty
    var journalEventsApplied = 0
    // Whatever a snapshot restored was written while its queries were live, so those start the tally.
    queriesLiveWhileReplaying ++= domainGraphSubscribers.subscribersToThisNode.valuesIterator.flatMap(_.relatedQueries)
    queriesLiveWhileReplaying ++= domainNodeIndex.index.valuesIterator.flatMap(_.valuesIterator.flatMap(_.forQueries))
    journal.foreach {
      case event: PropertyEvent =>
        applyPropertyEffect(event)
        journalEventsApplied += 1
        if (isPresentNode) reevaluateDomainNodesWatching(event, shouldSendReplies = false)
      case event: EdgeEvent =>
        edges.updateEdgeCollection(event)
        journalEventsApplied += 1
        if (isPresentNode) {
          withdrawSubscriptionsUnreachableAfter(event, shouldSendReplies = false)
          reevaluateDomainNodesWatching(event, shouldSendReplies = false)
        }
      case event: DomainIndexEvent =>
        if (isPresentNode) {
          noteQueriesLiveAt(event)
          applyDomainIndexEffect(event, shouldCauseSideEffects = false)
          journalEventsApplied += 1
        }
    }
    journalEventsApplied
  }

  /** Re-run the DistinctId evaluations rooted here that watch `event`. */
  private[this] def reevaluateDomainNodesWatching(event: NodeChangeEvent, shouldSendReplies: Boolean): Unit =
    watchableEventIndex.standingQueriesWatchingNodeEvent(
      event,
      {
        case _: StandingQueryWatchableEventIndex.StandingQueryWithId => false
        case StandingQueryWatchableEventIndex.DomainNodeIndexSubscription(dgnId) =>
          reevaluateDomainNode(dgnId, shouldSendReplies)
      },
    )

  /** Returns true when the DGN no longer exists, which tells the watch index to drop its record. */
  private[this] def reevaluateDomainNode(dgnId: DomainGraphNodeId, shouldSendReplies: Boolean): Boolean =
    dgnRegistry.getIdentifiedDomainGraphNode(dgnId) match {
      case Some(dgn) =>
        // ensure that this node is subscribed to all other necessary nodes to continue processing the DGN
        ensureSubscriptionToDomainEdges(dgn, domainGraphSubscribers.getRelatedQueries(dgnId), shouldSendReplies)
        // inform all subscribers to this node about any relevant changes caused by the recent event
        domainGraphSubscribers.updateAnswerAndNotifySubscribers(dgn, shouldSendReplies)
        false
      case None => true
    }

  /** Hook for registering some arbitrary action after processing a node event. Right now, all this
    * does is advance standing queries
    *
    * @param events ordered sequence of node events produced from a single message.
    */
  protected[this] def runPostActions(events: List[NodeChangeEvent]): Unit = {

    var eventsForMvsqs: Map[StandingQueryWatchableEventIndex.StandingQueryWithId, Seq[NodeChangeEvent]] = Map.empty

    events.foreach { event =>
      watchableEventIndex.standingQueriesWatchingNodeEvent(
        event,
        {
          case cypherSubscriber: StandingQueryWatchableEventIndex.StandingQueryWithId =>
            eventsForMvsqs += cypherSubscriber -> (event +: eventsForMvsqs.getOrElse(cypherSubscriber, Seq.empty))
            false
          case StandingQueryWatchableEventIndex.DomainNodeIndexSubscription(dgnId) =>
            reevaluateDomainNode(dgnId, shouldSendReplies = true)
        },
      )
    }
    eventsForMvsqs.foreach { case (sq, events) =>
      updateMultipleValuesSqs(events, sq)(logConfig)
    }

    // State notification for edge events
    events.foreach {
      case EdgeEvent.EdgeAdded(edge) =>
        handleGraphEvent(GraphEvent.EdgeAdded(edge))
      case removal @ EdgeEvent.EdgeRemoved(edge) =>
        withdrawSubscriptionsUnreachableAfter(removal, shouldSendReplies = true)
        handleGraphEvent(GraphEvent.EdgeRemoved(edge))
      case _ => () // Property events are handled in applyPropertyEffect
    }
  }

  /** Run a snapshot codec, with the bookkeeping every snapshot owes whichever codec writes it.
    *
    * Subclasses override `toSnapshotBytes` to supply their own codec, so anything done alongside
    * the write lives here or it exists in one copy and not the other. Timed apart from the persist
    * that follows because this runs on the actor thread and that does not.
    */
  protected[this] def serializingSnapshot(write: => Array[Byte]): Array[Byte] = {
    latestUpdateAfterSnapshot = None // TODO: reconsider what to do if saving the snapshot fails!
    journaledEventsAppliedSinceSnapshot = 0
    metrics.snapshotSerializeTimer.time(() => write)
  }

  /** Serialize node state into a binary node snapshot
    *
    * @note returning just bytes instead of [[NodeSnapshot]] means that we don't need to worry
    * about accidentally leaking references to (potentially thread-unsafe) internal actor state
    *
    * @return Snapshot bytes, as managed by [[SnapshotCodec]]
    */
  def toSnapshotBytes(time: EventTime): Array[Byte] = serializingSnapshot {
    NodeSnapshot.snapshotCodec.format.write(
      NodeSnapshot(
        time,
        properties,
        edges.toSerialize,
        domainGraphSubscribers.subscribersToThisNode,
        domainNodeIndex.index,
      ),
    )
  }

  def debugNodeInternalState(): Future[NodeInternalState] = {
    // Return a string that (if possible) shows the deserialized representation
    def propertyValue2String(propertyValue: PropertyValue): String =
      propertyValue.deserialized.fold(
        _ => ByteConversions.formatHexBinary(propertyValue.serialized),
        _.toString,
      )

    val subscribersStrings = domainGraphSubscribers.subscribersToThisNode.toList
      .map { case (a, c) =>
        a -> c.subscribers.map {
          case Left(q) => q.pretty
          case Right(x) => x
        } -> c.latestAnswer -> c.relatedQueries
      }
      .map(_.toString)

    val domainNodeIndexStrings = domainNodeIndex.index.toList
      .map(t => t._1.pretty -> t._2.map { case (a, c) => a -> c })
      .map(_.toString)

    val dgnWatchableEventIndexSummary = {
      val propsIdx = watchableEventIndex.watchingForProperty.toMap.map { case (propertyName, notifiables) =>
        propertyName.name -> notifiables.toList.collect {
          case StandingQueryWatchableEventIndex.DomainNodeIndexSubscription(dgnId) =>
            dgnId
        }
      }
      val edgesIdx = watchableEventIndex.watchingForEdge.toMap.map { case (edgeLabel, notifiables) =>
        edgeLabel.name -> notifiables.toList.collect {
          case StandingQueryWatchableEventIndex.DomainNodeIndexSubscription(dgnId) =>
            dgnId
        }
      }
      val anyEdgesIdx = watchableEventIndex.watchingForAnyEdge.collect {
        case StandingQueryWatchableEventIndex.DomainNodeIndexSubscription(dgnId) =>
          dgnId
      }

      DgnWatchableEventIndexSummary(
        propsIdx,
        edgesIdx,
        anyEdgesIdx.toList,
      )
    }

    persistor
      .getJournalWithTime(
        qid,
        startingAt = EventTime.MinValue,
        endingAt =
          atTime.map(EventTime.fromMillis).map(_.largestEventTimeInThisMillisecond).getOrElse(EventTime.MaxValue),
        includeDomainIndexEvents = false,
      )
      // [[NodeInternalState]] carries the journal as a `Set`, so the report cannot be assembled
      // without it in hand. Collected straight into that `Set` rather than into a `Seq` that is
      // then converted, which would build the whole journal twice.
      .runWith(Sink.collection[NodeEvent.WithTime[NodeEvent], Set[NodeEvent.WithTime[NodeEvent]]])(
        graph.materializer,
      )
      // Reported as a failure rather than as an empty journal, which a node that genuinely never
      // changed would also produce. The log adds the node's identity, which the caller lacks.
      .recoverWith { case err =>
        log.error(log"failed to get journal for node: $qidAtTime" withException err)
        Future.failed(err)
      }(context.dispatcher)
      .map { journal =>
        NodeInternalState(
          atTime,
          properties.fmap(propertyValue2String),
          edges.toSet,
          latestUpdateAfterSnapshot,
          subscribersStrings,
          domainNodeIndexStrings,
          getSqState(),
          dgnWatchableEventIndexSummary,
          multipleValuesStandingQueries.toVector.map {
            case ((globalId, sqId), (MultipleValuesStandingQueryPartSubscription(_, _, subs), st)) =>
              LocallyRegisteredStandingQuery(
                sqId.toString,
                globalId.toString,
                subs.map(_.pretty).toSet,
                s"${st.toString}{${st.readResults(properties, graph.labelsProperty).map(_.toList)}}",
              )
          },
          journal,
          getNodeHashCode().value,
        )
      }(context.dispatcher)
  }

  def getNodeHashCode(): GraphNodeHashCode =
    GraphNodeHashCode(qid, properties, edges.toSet)

  /** Retrieve the journal for a node, as of an optional `atTime`
    *
    * @param startingAt optional earliest millisecond to report, inclusive; `None` reads from the
    *                   beginning of the node's history
    * @param endingAt optional latest millisecond to report, inclusive; `None` reports everything
    *                 recorded. This bounds the slice of journal returned, not the moment the node is
    *                 read at, so it may name a moment that has not arrived yet
    * @return the node's events in ascending timestamp order, streamed rather than collected
    */
  def getJournal(
    startingAt: Option[Milliseconds],
    endingAt: Option[Milliseconds],
  ): Source[JournalEntry, NotUsed] = {
    // Both bounds are inclusive of the whole millisecond they name: the lower bound starts at the
    // first event that millisecond could hold, the upper bound ends at the last. Neither says
    // anything about which moment the node is being read at, so an upper bound may name a moment
    // that has not arrived; the journal simply has nothing recorded past the present to return.
    val from = startingAt.map(EventTime.fromMillis).getOrElse(EventTime.MinValue)
    val to =
      endingAt.map(EventTime.fromMillis(_).largestEventTimeInThisMillisecond).getOrElse(EventTime.MaxValue)
    // Standing query events are excluded, so only the node-change journal is read. Streaming it
    // keeps a node with a very long history from being assembled in memory all at once.
    persistor
      .getNodeChangeEventsWithTime(
        qid,
        startingAt = from,
        endingAt = to,
      )
      .map(JournalEntry(_))
  }

  /** Every piece of DistinctId standing query state this node holds.
    *
    * Reports each subscriber individually, including the standing queries that subscribe directly rather than
    * only the nodes: a pattern whose one subscriber is its own query used to appear here as nothing at all. The
    * per-subscriber and per-answer query sets are reported too, since they are what decides whether either end of
    * a subscription still wants it.
    */
  def getSqState(): SqStateResults =
    SqStateResults(
      subscribers = domainGraphSubscribers.subscribersToThisNode.toList.flatMap { case (dgnId, subscription) =>
        subscription.queriesPerSubscriber.toList.map { case (subscriber, queries) =>
          DistinctIdSubscriberState(
            dgnId = dgnId,
            subscriberNode = subscriber.left.toOption,
            subscriberQuery = subscriber.toOption,
            forQueries = queries.toList,
            lastResult = subscription.latestAnswer,
          )
        }
      },
      subscriptions = domainNodeIndex.index.toList.flatMap { case (peer, byDgn) =>
        byDgn.toList.map { case (dgnId, result) =>
          DistinctIdIndexState(
            dgnId = dgnId,
            peer = peer,
            forQueries = result.forQueries.toList,
            answer = result.answer,
          )
        }
      },
      parentIndex = domainGraphNodeParentIndex.knownParents.toList.flatMap { case (child, parents) =>
        parents.toList.map(DistinctIdParentLink(child, _))
      },
    )
}

object AbstractNodeActor {
  private[graph] def internallyDeduplicatePropertyEvents(events: List[PropertyEvent]): List[PropertyEvent] =
    // Use only the last event for each property key. This form of "internal deduplication" is only applied to
    // a) batches of b) property events.
    events
      .groupMapReduce(_.key)(identity)(Keep.right)
      .values
      .toList

}
