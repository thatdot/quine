package com.thatdot.quine.graph

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.locks.StampedLock

import scala.collection.compat._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

import akka.actor.{Actor, ActorLogging}

import com.thatdot.quine.graph.NodeChangeEvent._
import com.thatdot.quine.graph.behavior.DomainNodeIndexBehavior.SubscribersToThisNodeUtil
import com.thatdot.quine.graph.behavior._
import com.thatdot.quine.graph.cypher.StandingQueryLookupInfo
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
import com.thatdot.quine.graph.messaging.{QuineIdAtTime, QuineIdOps, QuineRefOps}
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.model.{HalfEdge, Milliseconds, PropertyValue, QuineId, QuineIdProvider}
import com.thatdot.quine.persistor.codecs.{SnapshotCodec, StandingQueryStateCodec}
import com.thatdot.quine.persistor.{EventEffectOrder, PersistenceAgent, PersistenceConfig}
import com.thatdot.quine.util.HexConversions

// This is just to pass the initial state to construct NodeActor with
case class NodeActorConstructorArgs(
  properties: Map[Symbol, PropertyValue],
  edges: Iterable[HalfEdge],
  subscribersToThisNode: mutable.Map[
    DomainGraphNodeId,
    SubscribersToThisNodeUtil.Subscription
  ],
  domainNodeIndex: DomainNodeIndexBehavior.DomainNodeIndex,
  standingQueries: mutable.Map[
    (StandingQueryId, StandingQueryPartId),
    (StandingQuerySubscribers, cypher.StandingQueryState)
  ],
  initialJournal: NodeActor.Journal
)
object NodeActorConstructorArgs {
  def empty: NodeActorConstructorArgs = NodeActorConstructorArgs(
    properties = Map.empty,
    edges = Iterable.empty,
    subscribersToThisNode = mutable.Map.empty,
    domainNodeIndex = DomainNodeIndexBehavior.DomainNodeIndex(mutable.Map.empty),
    standingQueries = mutable.Map.empty,
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
  protected val persistor: PersistenceAgent,
  costToSleep: CostToSleep,
  protected val wakefulState: AtomicReference[WakefulState],
  protected val actorRefLock: StampedLock,
  protected var properties: Map[Symbol, PropertyValue],
  initialEdges: Iterable[HalfEdge],
  subscribersToThisNode: mutable.Map[
    DomainGraphNodeId,
    SubscribersToThisNodeUtil.Subscription
  ],
  protected val domainNodeIndex: DomainNodeIndexBehavior.DomainNodeIndex,
  protected val standingQueries: mutable.Map[
    (StandingQueryId, StandingQueryPartId),
    (StandingQuerySubscribers, cypher.StandingQueryState)
  ],
  initialJournal: NodeActor.Journal
) extends Actor
    with ActorLogging
    with BaseNodeActor
    with QuineRefOps
    with QuineIdOps
    with LiteralCommandBehavior
    with DomainNodeIndexBehavior
    with GoToSleepBehavior
    with PriorityStashingBehavior
    with CypherBehavior
    with CypherStandingBehavior
    with ActorClock {

  def receive: Receive = actorClockBehavior {
    case control: NodeControlMessage => goToSleepBehavior(control)
    case StashedMessage(message) => receive(message)
    case query: CypherQueryInstruction => cypherBehavior(query)
    case command: LiteralCommand => literalCommandBehavior(command)
    case command: DomainNodeSubscriptionCommand => domainNodeIndexBehavior(command)
    case command: CypherStandingQueryCommand => cypherStandingQueryBehavior(command)
    case command: UpdateStandingQueriesCommand => updateStandingQueriesBehavior(command)
    case msg => log.error("Node received an unknown message (from {}): {}", sender(), msg)
  }

  val qid: QuineId = qidAtTime.id
  val atTime: Option[Milliseconds] = qidAtTime.atTime
  implicit val idProvider: QuineIdProvider = graph.idProvider
  protected val persistenceConfig: PersistenceConfig = persistor.persistenceConfig
  protected val metrics: HostQuineMetrics = graph.metrics
  protected val edges: EdgeCollection = graph.edgeCollectionFactory.get
  initialEdges.foreach(edges +=)
  protected val dgnRegistry: DomainGraphNodeRegistry = graph.dgnRegistry

  protected val subscribers: SubscribersToThisNode = SubscribersToThisNode(subscribersToThisNode)

  protected var latestUpdateAfterSnapshot: Option[EventTime] = None
  protected var lastWriteMillis: Long = 0

  protected def updateRelevantToSnapshotOccurred(): Unit =
    // TODO: should this update `lastWriteMillis` too?
    latestUpdateAfterSnapshot = Some(latestEventTime())

  // here be the side-effects performed by the constructor

  protected var nodeParentIndex: DomainNodeIndexBehavior.NodeParentIndex = {
    val (nodeIndex, removed) = DomainNodeIndexBehavior.NodeParentIndex.reconstruct(
      domainNodeIndex,
      subscribers.subscribersToThisNode.keys,
      dgnRegistry
    )
    subscribers.subscribersToThisNode --= removed
    nodeIndex
  }

  applyEventsEffectsInMemory(initialJournal, shouldCauseSideEffects = false)

  {
    val lookupInfo = StandingQueryLookupInfo(
      graph.getStandingQueryPart,
      qid,
      idProvider
    )
    standingQueries.values foreach { case (_, sqState) => sqState.preStart(lookupInfo) }
  }

  /** Index for efficiently determining which subscribers to standing queries (be they other standing queries or sets
    * of Notifiables) should be notified for node events
    */
  protected val localEventIndex: StandingQueryLocalEventIndex = {
    val (localIndex, removed) = StandingQueryLocalEventIndex.from(
      dgnRegistry,
      subscribers.subscribersToThisNode.keysIterator,
      Iterator.empty //standingQueries.iterator.map { case (handler, (_, sqState)) => handler -> sqState }
    )
    subscribers.removeSubscribers(removed)
    localIndex
  }
  syncStandingQueries()

  // Once edge collection is updated, compute cost to sleep
  costToSleep.set(Math.round(Math.round(edges.size.toDouble) / Math.log(2) - 2))

  /** Synchronizes this node's standing queries with those of the current graph
    * - Registers and emits initial results for any standing queries not yet registered on this node
    * - Removes any standing queries defined on this node but no longer known to the graph
    */
  protected def syncStandingQueries(): Unit =
    if (atTime.isEmpty) {
      updateUniversalQueriesOnWake(shouldSendReplies = true)
      updateUniversalCypherQueriesOnWake()
    }

  /** Fast check for if a number is a power of 2 */
  private[this] def isPowerOfTwo(n: Int): Boolean = (n & (n - 1)) == 0

  /* Determine if this event causes a change to the respective state (defaults to this node's state) */
  protected def hasEffect(event: NodeChangeEvent): Boolean = event match {
    case PropertySet(key, value) => !properties.get(key).contains(value)
    case PropertyRemoved(key, _) => properties.contains(key)
    case EdgeAdded(edge) => !edges.contains(edge)
    case EdgeRemoved(edge) => edges.contains(edge)
  }

  /** Process multiple node events as a single unit, so their effects are applied in memory together, and also persisted
    * together. Will check the incoming sequence for conflicting events (modifying the same value more than once), and
    * keep only the last event, ensuring the provided collection is internally coherent.
    */
  protected def processEvents(
    events: Seq[NodeChangeEvent],
    atTimeOverride: Option[EventTime] = None
  ): Future[Done.type] =
    if (atTime.isDefined) Future.failed(IllegalHistoricalUpdate(events, qid, atTime.get))
    else if (atTimeOverride.isDefined && events.size > 1)
      Future.failed(IllegalTimeOverride(events, qid, atTimeOverride.get))
    else
      persistAndApplyEventsEffectsInMemory {
        if (events.isEmpty) Seq.empty
        else if (events.size == 1 && hasEffect(events.head))
          Seq(NodeEvent.WithTime(events.head, atTimeOverride.getOrElse(nextEventTime())))
        else {
          /* This process reverses the events, considering only the last event per property/edge/etc. and keeps the
           * event if it has an effect. If multiple events would affect the same value (e.g. have the same property key),
           * but would result in no change when applied in order, then no change at all will be applied. e.g. if a
           * property exists, and these events would remove it and set it back to its same value, then no change to the
           * property will be recorded at all. Original event order is maintained. */
          var es: Set[HalfEdge] = Set.empty
          var ps: Set[Symbol] = Set.empty
          events.reverse
            .filter {
              case e @ EdgeAdded(ha) => if (es.contains(ha)) false else { es += ha; hasEffect(e) }
              case e @ EdgeRemoved(ha) => if (es.contains(ha)) false else { es += ha; hasEffect(e) }
              case e @ PropertySet(k, _) => if (ps.contains(k)) false else { ps += k; hasEffect(e) }
              case e @ PropertyRemoved(k, _) => if (ps.contains(k)) false else { ps += k; hasEffect(e) }
            }
            .reverse
            .map(e => NodeEvent.WithTime(e, atTimeOverride.getOrElse(nextEventTime())))
          // TODO: It should be possible to do all this in only two passes over the collection with no reverses.
        }
      }

  protected def processDomainIndexEvent(
    event: DomainIndexEvent
  ): Future[Done.type] =
    persistAndApplyEventsEffectsInMemory(Seq(NodeEvent.WithTime(event, nextEventTime())))

  protected def persistAndApplyEventsEffectsInMemory(
    dedupedEffectingEvents: Seq[NodeEvent.WithTime]
  ): Future[Done.type] = {

    val persistAttempts = new AtomicInteger(1)
    def persistEventsToJournal(): Future[Done.type] =
      if (persistenceConfig.journalEnabled) {
        metrics.persistorPersistEventTimer
          .time(persistor.persistEvents(qid, dedupedEffectingEvents))
          .transform(
            _ =>
              // TODO: add a metric to count `persistAttempts`
              Done,
            (e: Throwable) => {
              val attemptCount = persistAttempts.getAndIncrement()
              log.info(
                s"Retrying persistence from node: ${qid.pretty} with events: $dedupedEffectingEvents after: " +
                s"$attemptCount attempts, with error: $e"
              )
              e
            }
          )(cypherEc)
      } else Future.successful(Done)

    (dedupedEffectingEvents.nonEmpty, graph.effectOrder) match {
      case (false, _) => Future.successful(Done)
      case (true, EventEffectOrder.MemoryFirst) =>
        applyEventsEffectsInMemory(dedupedEffectingEvents.map(_.event), shouldCauseSideEffects = true)
        akka.pattern.retry(
          () => persistEventsToJournal(),
          Int.MaxValue,
          1.millisecond,
          10.seconds,
          randomFactor = 0.1d
        )(cypherEc, context.system.scheduler)
      case (true, EventEffectOrder.PersistorFirst) =>
        pauseMessageProcessingUntil[Done.type](
          persistEventsToJournal(),
          {
            case Success(_) =>
              // Executed by this actor (which is not slept), in order before any other messages are processed.
              applyEventsEffectsInMemory(dedupedEffectingEvents.map(_.event), shouldCauseSideEffects = true)
            case Failure(e) =>
              log.info(
                s"Persistor error occurred when writing events to journal on node: ${qid.pretty} Will not apply " +
                s"events: $dedupedEffectingEvents to in-memory state. Returning failed result. Error: $e"
              )
          }
        ).map(_ => Done)(cypherEc)
    }
  }

  private[this] def snapshotOnUpdate(): Unit = if (persistenceConfig.snapshotOnUpdate) {
    val occurredAt: EventTime = nextEventTime()
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
    def infinitePersisting(logFunc: String => Unit, f: Future[Unit] = persistSnapshot()): Future[Done.type] =
      f.recoverWith { case NonFatal(e) =>
        logFunc(s"Persisting snapshot for: $occurredAt is being retried after the error: $e")
        infinitePersisting(logFunc, persistSnapshot())
      }(cypherEc)
        .map(_ => Done)(cypherEc)
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
  }

  /** Apply the in-memory effects of the provided events.
    *
    * @param events a sequence of already-deduplicated events to apply in order
    * @param shouldCauseSideEffects whether the application of these effects should cause additional side effects, such
    *                               as Standing Query results and creation of a new snapshot (if applicable based on
    *                               `quine.persistence` configuration). This value should be false when restoring
    *                               events from a journal.
    */
  private[this] def applyEventsEffectsInMemory(
    events: Iterable[NodeEvent],
    shouldCauseSideEffects: Boolean
  ): Unit = {
    import DomainIndexEvent._
    events.foreach {
      case PropertySet(propKey, propValue) =>
        properties = properties + (propKey -> propValue)
        metrics.nodePropertyCounter.increment(properties.size)

      case PropertyRemoved(propKey, _) =>
        properties = properties - propKey
        metrics.nodePropertyCounter.decrement(properties.size)

      case EdgeAdded(edge) =>
        // The more edges you get, the worse it is to sleep
        val len = edges.size
        if (len > 7 && isPowerOfTwo(len)) costToSleep.incrementAndGet()

        val edgeCollectionSizeWarningInterval = 10000
        if (log.isWarningEnabled && (len + 1) % edgeCollectionSizeWarningInterval == 0)
          log.warning(s"Node: ${qid.pretty} has: ${len + 1} edges")

        edges += edge
        metrics.nodeEdgesCounter.increment(edges.size)

      case EdgeRemoved(edge) =>
        edges -= edge
        metrics.nodeEdgesCounter.decrement(edges.size)

      case CreateDomainNodeSubscription(dgnId, nodeId, forQuery) =>
        receiveDomainNodeSubscription(Left(nodeId), dgnId, forQuery, shouldSendReplies = shouldCauseSideEffects)

      case CreateDomainStandingQuerySubscription(dgnId, sqId, forQuery) =>
        receiveDomainNodeSubscription(Right(sqId), dgnId, forQuery, shouldSendReplies = shouldCauseSideEffects)

      case DomainNodeSubscriptionResult(from, dgnId, result) =>
        receiveIndexUpdate(from, dgnId, result, shouldSendReplies = shouldCauseSideEffects)

      case CancelDomainNodeSubscription(dgnId, fromSubscriber) =>
        cancelSubscription(dgnId, Left(fromSubscriber), shouldSendReplies = shouldCauseSideEffects)
    }

    if (shouldCauseSideEffects) { // `false` when restoring from journals
      latestUpdateAfterSnapshot = Some(latestEventTime())
      lastWriteMillis = latestEventTime().millis
      snapshotOnUpdate()
      val nodeChangeEvents = events.collect { case e: NodeChangeEvent => e }
      runPostActions(nodeChangeEvents)
    }
  }

  /** Hook for registering some arbitrary action after processing a node event. Right now, all this
    * does is advance standing queries
    *
    * @param events ordered sequence of node events produced from a single message.
    */
  private[this] def runPostActions(events: Iterable[NodeChangeEvent]): Unit = events.foreach { event =>
    localEventIndex.standingQueriesWatchingNodeEvent(
      event,
      {
        case cypherSubscriber: StandingQueryLocalEventIndex.StandingQueryWithId =>
          updateCypherSq(event, cypherSubscriber)
          false
        case StandingQueryLocalEventIndex.DomainNodeIndexSubscription(dgnId) =>
          dgnRegistry.getIdentifiedDomainGraphNode(dgnId) match {
            case Some(dgn) =>
              // ensure that this node is subscribed to all other necessary nodes to continue processing the DGN
              ensureSubscriptionToDomainEdges(dgn, subscribers.getRelatedQueries(dgnId), shouldSendReplies = true)
              // inform all subscribers to this node about any relevant changes caused by the recent event
              subscribers.updateAnswerAndNotifySubscribers(dgn, shouldSendReplies = true)
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
        subscribers.subscribersToThisNode,
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

    val subscribersStrings = subscribers.subscribersToThisNode.toList
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
      val propsIdx = localEventIndex.watchingForProperty.toList.flatMap { case (k, v) =>
        v.toList.collect { case StandingQueryLocalEventIndex.DomainNodeIndexSubscription(dgnId) =>
          k.name -> dgnId
        }
      }
      val edgesIdx = localEventIndex.watchingForEdge.toList.flatMap { case (k, v) =>
        v.toList.collect { case StandingQueryLocalEventIndex.DomainNodeIndexSubscription(dgnId) =>
          k.name -> dgnId
        }
      }
      val anyEdgesIdx = localEventIndex.watchingForAnyEdge.toList.collect {
        case StandingQueryLocalEventIndex.DomainNodeIndexSubscription(dgnId) =>
          dgnId
      }

      DgnLocalEventIndexSummary(
        propsIdx.toMap,
        edgesIdx.toMap,
        anyEdgesIdx.distinct
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
        log.error(err, "failed to get journal for node {}", qid)
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
          standingQueries.view.map { case ((globalId, sqId), (StandingQuerySubscribers(_, _, subs), st)) =>
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
      subscribers.subscribersToThisNode.toList.flatMap { case (dgnId, subs) =>
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

  type Snapshot = Option[Array[Byte]]
  type Journal = Iterable[NodeEvent]
  type StandingQueryStates = Map[(StandingQueryId, StandingQueryPartId), Array[Byte]]

  def create(
    quineIdAtTime: QuineIdAtTime,
    snapshotBytesOpt: Option[Array[Byte]],
    persistor: PersistenceAgent,
    metrics: HostQuineMetrics
  )(implicit ec: ExecutionContext): Future[NodeActorConstructorArgs] =
    snapshotBytesOpt match {
      case Some(bytes) => Future.successful(restoreFromSnapshotBytes(bytes)._2)
      case None => restoreFromSnapshotAndJournal(quineIdAtTime, persistor, metrics)
    }

  /** Deserialize a binary node snapshot and use it to initialize node state
    *
    * @param snapshotBytes binary node snapshot
    */
  @throws("if snapshot bytes cannot be deserialized")
  private def restoreFromSnapshotBytes(snapshotBytes: Array[Byte]): (EventTime, NodeActorConstructorArgs) = {
    val restored: NodeSnapshot =
      SnapshotCodec.format
        .read(snapshotBytes)
        .get // Throws! ...because there's nothing better to do...

    restored.time -> NodeActorConstructorArgs(
      properties = restored.properties,
      edges = restored.edges,
      subscribersToThisNode = restored.subscribersToThisNode,
      domainNodeIndex = DomainNodeIndexBehavior.DomainNodeIndex(restored.domainNodeIndex),
      standingQueries = mutable.Map.empty,
      initialJournal = Iterable.empty
    )

  }

  /** Read the NodeActor state at the specified time
    */
  private[this] def restoreFromSnapshotAndJournal(
    quineIdAtTime: QuineIdAtTime,
    persistor: PersistenceAgent,
    metrics: HostQuineMetrics
  )(implicit ec: ExecutionContext): Future[NodeActorConstructorArgs] = {

    val persistenceConfig = persistor.persistenceConfig
    val QuineIdAtTime(qid, atTime) = quineIdAtTime

    def getSnapshot(): Future[Option[Array[Byte]]] = {
      val upToTime = atTime match {
        case Some(historicalTime) if !persistenceConfig.snapshotSingleton =>
          EventTime.fromMillis(historicalTime)
        case _ =>
          EventTime.MaxValue
      }
      metrics.persistorGetLatestSnapshotTimer.time {
        persistor.getLatestSnapshot(qid, upToTime)
      }
    }

    def getJournalAfter(after: Option[EventTime], includeDomainIndexEvents: Boolean): Future[Iterable[NodeEvent]] = {
      val startingAt = after.fold(EventTime.MinValue)(_.nextEventTime(None))
      val endingAt = atTime match {
        case Some(until) => EventTime.fromMillis(until).largestEventTimeInThisMillisecond
        case None => EventTime.MaxValue
      }
      metrics.persistorGetJournalTimer.time {
        persistor.getJournal(qid, startingAt, endingAt, includeDomainIndexEvents)
      }
    }

    val snapshotAndJournal: Future[(Option[NodeActorConstructorArgs], Journal)] = for {
      latestSnapshotBytesOpt <- if (persistenceConfig.snapshotEnabled) getSnapshot() else Future.successful(None)
      //(snapshotTime, snapshotArgs) = latestSnapshotOpt.map(restoreFromSnapshotBytes)
      latestSnapshotOpt = latestSnapshotBytesOpt map restoreFromSnapshotBytes
      journalAfterSnapshot <-
        if (persistenceConfig.journalEnabled) getJournalAfter(latestSnapshotOpt.map(_._1), atTime.isEmpty)
        else Future.successful(Iterable.empty)
    } yield (latestSnapshotOpt.map(_._2), journalAfterSnapshot)
    val standingQueryStates: Future[StandingQueryStates] = atTime match {
      case Some(_) => Future.successful(Map.empty)
      case None =>
        metrics.persistorGetStandingQueryStatesTimer.time {
          persistor.getStandingQueryStates(qid)
        }
    }
    snapshotAndJournal.zipWith(standingQueryStates) {
      case (
            (latestSnapshotOpt: Option[NodeActorConstructorArgs], journalAfterSnapshot: Journal),
            standingQueryStates: StandingQueryStates
          ) =>
        val snapshotArgs = latestSnapshotOpt getOrElse NodeActorConstructorArgs.empty
        snapshotArgs.copy(
          initialJournal = journalAfterSnapshot,
          standingQueries = mutable.Map.from(
            standingQueryStates.view.mapValues(StandingQueryStateCodec.format.read(_).get)
          )
        )
    }

  }
}
