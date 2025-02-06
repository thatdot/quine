package com.thatdot.quine.graph

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.locks.StampedLock

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

import org.apache.pekko.actor.Actor
import org.apache.pekko.stream.scaladsl.Keep

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
import com.thatdot.quine.graph.edges.{EdgeProcessor, MemoryFirstEdgeProcessor, PersistorFirstEdgeProcessor}
import com.thatdot.quine.graph.messaging.BaseMessage.Done
import com.thatdot.quine.graph.messaging.LiteralMessage.{
  DgnWatchableEventIndexSummary,
  LocallyRegisteredStandingQuery,
  NodeInternalState,
  SqStateResult,
  SqStateResults,
}
import com.thatdot.quine.graph.messaging.{QuineIdOps, QuineRefOps, SpaceTimeQuineId}
import com.thatdot.quine.graph.metrics.HostQuineMetrics
import com.thatdot.quine.graph.metrics.implicits.TimeFuture
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
          updateSnapshotTimestamp = () => updateLastWriteAfterSnapshot(),
          runPostActions = runPostActions,
          qid = qid,
          costToSleep = costToSleep,
          nodeEdgesCounter = metrics.nodeEdgesCounter(namespace),
        )
      case EventEffectOrder.MemoryFirst =>
        new MemoryFirstEdgeProcessor(
          edges = edgeCollection,
          persistToJournal = persistEventsToJournal,
          updateSnapshotTimestamp = () => updateLastWriteAfterSnapshot(),
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

  protected def processDomainIndexEvent(
    event: DomainIndexEvent,
  ): Future[Done.type] =
    refuseHistoricalUpdates(event :: Nil)(
      persistAndApplyEventsEffectsInMemory[DomainIndexEvent](
        NonEmptyList.one(NodeEvent.WithTime(event, tickEventSequence())),
        persistor.persistDomainIndexEvents(qid, _),
        // We know there is only one event here, because we're only passing one above.
        // So just calling .head works as well as .foreach
        events => applyDomainIndexEffect(events.head, shouldCauseSideEffects = true),
      ),
    )

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

    graph.effectOrder match {
      case EventEffectOrder.MemoryFirst =>
        val events = effectingEvents.map(_.event)
        applyEventsEffectsInMemory(events)
        notifyNodeUpdate(events collect { case e: NodeChangeEvent => e })
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
              val events = effectingEvents.map(_.event)
              applyEventsEffectsInMemory(events)
              notifyNodeUpdate(events collect { case e: NodeChangeEvent => e })
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

  private[this] def persistSnapshot(): Unit = if (atTime.isEmpty) {
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
      metrics.nodePropertyCounter(namespace).increment(previousCount = properties.size)
      properties = properties + (key -> value)
    case PropertyRemoved(key, _) =>
      metrics.nodePropertyCounter(namespace).decrement(previousCount = properties.size)
      properties = properties - key
  }

  /** Apply a [[DomainIndexEvent]] to the node state, updating its DGB bookkeeping and potentially (only if
    * shouldCauseSideEffects) messaging other nodes with any relevant updates.
    * @param event                  the event to apply
    * @param shouldCauseSideEffects whether the application of this event should cause off-node side effects, such
    *                               as Standing Query results. This value should be false when restoring
    *                               events from a journal.
    */
  protected[this] def applyDomainIndexEffect(event: DomainIndexEvent, shouldCauseSideEffects: Boolean): Unit = {
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
            dgnRegistry.getIdentifiedDomainGraphNode(dgnId) match {
              case Some(dgn) =>
                // ensure that this node is subscribed to all other necessary nodes to continue processing the DGN
                ensureSubscriptionToDomainEdges(
                  dgn,
                  domainGraphSubscribers.getRelatedQueries(dgnId),
                  shouldSendReplies = true,
                )
                // inform all subscribers to this node about any relevant changes caused by the recent event
                domainGraphSubscribers.updateAnswerAndNotifySubscribers(dgn, shouldSendReplies = true)
                false
              case None =>
                true // true returned to standingQueriesWatchingNodeEvent indicates record should be removed
            }
        },
      )
    }
    eventsForMvsqs.foreach { case (sq, events) =>
      updateMultipleValuesSqs(events, sq)(logConfig)
    }
  }

  /** Serialize node state into a binary node snapshot
    *
    * @note returning just bytes instead of [[NodeSnapshot]] means that we don't need to worry
    * about accidentally leaking references to (potentially thread-unsafe) internal actor state
    *
    * @return Snapshot bytes, as managed by [[SnapshotCodec]]
    */
  def toSnapshotBytes(time: EventTime): Array[Byte] = {
    latestUpdateAfterSnapshot = None // TODO: reconsider what to do if saving the snapshot fails!
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
        } -> c.lastNotification -> c.relatedQueries
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
      .recover { case err =>
        log.error(log"failed to get journal for node: $qidAtTime" withException err)
        Iterable.empty
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
          journal.toSet,
          getNodeHashCode().value,
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
