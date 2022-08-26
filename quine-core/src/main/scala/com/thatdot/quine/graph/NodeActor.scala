package com.thatdot.quine.graph

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.locks.StampedLock

import scala.collection.compat._
import scala.collection.mutable.{Map => MutableMap}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

import akka.actor.{Actor, ActorLogging}

import com.thatdot.quine.graph.NodeChangeEvent._
import com.thatdot.quine.graph.behavior._
import com.thatdot.quine.graph.edgecollection.EdgeCollection
import com.thatdot.quine.graph.messaging.BaseMessage.Done
import com.thatdot.quine.graph.messaging.CypherMessage._
import com.thatdot.quine.graph.messaging.LiteralMessage.{
  DgbLocalEventIndexSummary,
  LiteralCommand,
  LocallyRegisteredStandingQuery,
  NodeInternalState,
  SqStateResult,
  SqStateResults
}
import com.thatdot.quine.graph.messaging.StandingQueryMessage._
import com.thatdot.quine.graph.messaging.{QuineIdAtTime, QuineIdOps, QuineRefOps}
import com.thatdot.quine.model.{HalfEdge, Milliseconds, PropertyValue, QuineId, QuineIdProvider}
import com.thatdot.quine.persistor.{EventEffectOrder, PersistenceAgent, PersistenceCodecs, PersistenceConfig}
import com.thatdot.quine.util.HexConversions

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
  * @param restoreAfterFailureSnapshotOpt If a node snapshot failed to save when going to sleep, it is delivered back to
  *                                       the node and used to restore the node in a wakeful state. This is defined as a
  *                                       `var` so that it can be reassigned/released for garbage collection after use.
  */
private[graph] class NodeActor(
  val qidAtTime: QuineIdAtTime,
  val graph: StandingQueryOpsGraph with CypherOpsGraph,
  costToSleep: CostToSleep,
  val wakefulState: AtomicReference[WakefulState],
  val actorRefLock: StampedLock,
  private var restoreAfterFailureSnapshotOpt: Option[Array[Byte]]
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
  protected val persistor: PersistenceAgent = graph.persistor
  protected val persistenceConfig: PersistenceConfig = persistor.persistenceConfig
  protected val metrics: HostQuineMetrics = graph.metrics
  protected var properties: Map[Symbol, PropertyValue] = Map.empty
  protected val edges: EdgeCollection = graph.edgeCollectionFactory.get()

  private var forwardTo: Option[QuineId] = None
  private var mergedIntoHere: Set[QuineId] = Set.empty[QuineId]

  protected var latestUpdateAfterSnapshot: Option[EventTime] = None
  protected var lastWriteMillis: Long = 0

  protected def updateRelevantToSnapshotOccurred(): Unit =
    // TODO: should this update `lastWriteMillis` too?
    latestUpdateAfterSnapshot = Some(latestEventTime())

  /** Index for efficiently determining which subscribers to standing queries (be they other standing queries or sets
    * of Notifiables) should be notified for node events
    */
  protected var localEventIndex: StandingQueryLocalEventIndex =
    StandingQueryLocalEventIndex.empty

  { // here be the side-effects performed by the constructor
    restoreAfterFailureSnapshotOpt match {
      case None =>
        restoreFromSnapshotAndJournal(atTime)
      case Some(snapshotBytes) =>
        restoreFromSnapshotBytes(snapshotBytes)
        // Register/unregister universal standing queries
        syncStandingQueries()
        // TODO: QU-430 restoreFromSnapshotBytes doesn't account for standing query states!
        // However, DGB subscribers are available with just restoreFromSnapshotBytes
        localEventIndex =
          StandingQueryLocalEventIndex.from(subscribers.subscribersToThisNode.keysIterator, Iterator.empty)
    }

    // Allow the GC to collect the snapshot now
    restoreAfterFailureSnapshotOpt = None
  }

  /** Synchronizes this node's standing queries with those of the current graph
    * - Registers and emits initial results for any standing queries not yet registered on this node
    * - Removes any standing queries defined on this node but no longer known to the graph
    */
  protected def syncStandingQueries(): Unit =
    if (atTime.isEmpty) {
      updateUniversalQueriesOnWake()
      updateUniversalCypherQueriesOnWake()
    }

  /* Determine if this event causes a change to the respective state (defaults to this node's state) */
  protected def hasEffect(event: NodeChangeEvent): Boolean = event match {
    case PropertySet(key, value) => !properties.get(key).contains(value)
    case PropertyRemoved(key, _) => properties.contains(key)
    case EdgeAdded(edge) => !edges.contains(edge)
    case EdgeRemoved(edge) => edges.contains(edge)
    case MergedIntoOther(_) => forwardTo.isEmpty
    case MergedHere(other) => !mergedIntoHere.contains(other)
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
    else {
      val dedupedEffectingEvents = {
        if (events.isEmpty) Seq.empty
        else if (events.size == 1 && hasEffect(events.head))
          Seq(NodeChangeEvent.WithTime(events.head, atTimeOverride.getOrElse(nextEventTime())))
        else {
          /* This process reverses the events, considering only the last event per property/edge/etc. and keeps the
           * event if it has an effect. If multiple events would affect the same value (e.g. have the same property key),
           * but would result in no change when applied in order, then no change at all will be applied. e.g. if a
           * property exists, and these events would remove it and set it back to its same value, then no change to the
           * property will be recorded at all. Original event order is maintained. */
          var es: Set[HalfEdge] = Set.empty
          var ps: Set[Symbol] = Set.empty
          var ft: Option[QuineId] = None
          var mih: Set[QuineId] = Set.empty
          events.reverse
            .filter {
              case e @ EdgeAdded(ha) => if (es.contains(ha)) false else { es += ha; hasEffect(e) }
              case e @ EdgeRemoved(ha) => if (es.contains(ha)) false else { es += ha; hasEffect(e) }
              case e @ PropertySet(k, _) => if (ps.contains(k)) false else { ps += k; hasEffect(e) }
              case e @ PropertyRemoved(k, _) => if (ps.contains(k)) false else { ps += k; hasEffect(e) }
              case e @ MergedIntoOther(id) => if (ft.isDefined) false else { ft = Some(id); hasEffect(e) }
              case e @ MergedHere(o) => if (mih.contains(o)) false else { mih += o; hasEffect(e) }
            }
            .reverse
            .map(e => WithTime(e, atTimeOverride.getOrElse(nextEventTime())))
          // TODO: It should be possible to do all this in only two passes over the collection with no reverses.
        }
      }

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
          applyEventsEffectsInMemory(dedupedEffectingEvents.map(_.event))
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
                applyEventsEffectsInMemory(dedupedEffectingEvents.map(_.event))
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
    val snapshot = toSnapshotBytes()
    val occurredAt: EventTime = nextEventTime()
    metrics.snapshotSize.update(snapshot.length)
    def persistSnapshot(): Future[Unit] =
      metrics.persistorPersistSnapshotTimer.time(persistor.persistSnapshot(qid, occurredAt, snapshot))
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
    events: Iterable[NodeChangeEvent],
    shouldCauseSideEffects: Boolean = true
  ): Unit = {
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

      case MergedIntoOther(otherNode) =>
        forwardTo = Some(otherNode)
        edges.clear()
        properties = Map.empty
        context.become(
          MergeNodeBehavior.mergedMessageHandling(this, graph, otherNode),
          discardOld = false
        )

      case MergedHere(otherNode) =>
        mergedIntoHere = mergedIntoHere + otherNode
    }

    if (shouldCauseSideEffects) { // `false` when restoring from journals
      latestUpdateAfterSnapshot = Some(latestEventTime())
      lastWriteMillis = latestEventTime().millis
      snapshotOnUpdate()
      runPostActions(events)
    }
  }

  /** Hook for registering some arbitrary action after processing a node event. Right now, all this
    * does is advance standing queries
    *
    * @param events ordered sequence of node events produced from a single message.
    */
  private[this] def runPostActions(events: Iterable[NodeChangeEvent]): Unit = events foreach { e =>
    e match {
      case _: EdgeAdded | _: EdgeRemoved | _: PropertySet | _: PropertyRemoved =>
        // update standing queries
        localEventIndex.standingQueriesWatchingNodeEvent(e).foreach {
          case cypherSubscriber: StandingQueryLocalEventIndex.StandingQueryWithId =>
            updateCypherSq(e, cypherSubscriber)
          case StandingQueryLocalEventIndex.DomainNodeIndexSubscription(branch, assumedEdge) =>
            // ensure that this node is subscribed to all other necessary nodes to continue processing the DGB
            ensureSubscriptionToDomainEdges(branch, assumedEdge, subscribers.getRelatedQueries(branch, assumedEdge))
            // ensure that all subscribers to this node are informed about any relevant changes caused by the recent
            // event
            subscribers.updateAnswerAndNotifySubscribers(branch, assumedEdge)
        }
      case _ => ()
    }
  }

  /** Fast check for if a number is a power of 2 */
  private[this] def isPowerOfTwo(n: Int): Boolean = (n & (n - 1)) == 0

  /** Load into the actor the state of the node at the specified time
    *
    * @note Since persistor access is asynchronous, this method will return
    * before restoration is done. However, it will block the actor from
    * processing messages until done.
    *
    * @param untilOpt load changes made up to and including this time
    */
  @throws[NodeWakeupFailedException]("When node wakeup fails irrecoverably")
  private[this] def restoreFromSnapshotAndJournal(untilOpt: Option[Milliseconds]): Unit = {

    type Snapshot = Option[Array[Byte]]
    type Journal = Iterable[NodeChangeEvent]
    type StandingQueryStates = Map[(StandingQueryId, StandingQueryPartId), Array[Byte]]

    // Get the snapshot and journal events
    val snapshotAndJournal: Future[(Snapshot, Journal)] = {
      implicit val ec: ExecutionContext = cypherEc
      for {
        // Find the snapshot
        latestSnapshotOpt <-
          if (persistenceConfig.snapshotEnabled) {
            // TODO: should we warn about snapshot singleton with a historical time?
            val upToTime = atTime match {
              case Some(historicalTime) if !persistenceConfig.snapshotSingleton =>
                EventTime.fromMillis(historicalTime)
              case _ =>
                EventTime.MaxValue
            }
            metrics.persistorGetLatestSnapshotTimer.time {
              persistor.getLatestSnapshot(qid, upToTime)
            }
          } else
            Future.successful(None)

        // Query the journal for any events that come after the snapshot
        journalAfterSnapshot <-
          if (persistenceConfig.journalEnabled) {
            val startingAt = latestSnapshotOpt match {
              case Some((snapshotTime, _)) => snapshotTime.nextEventTime(Some(log))
              case None => EventTime.MinValue
            }
            val endingAt = untilOpt match {
              case Some(until) => EventTime.fromMillis(until).largestEventTimeInThisMillisecond
              case None => EventTime.MaxValue
            }
            metrics.persistorGetJournalTimer.time {
              persistor.getJournal(qid, startingAt, endingAt)
            }
            // QU-429 to avoid extra retries, consider unifying the Failure types of `persistor.getJournal`, and adding a
            // recoverWith here to map any that represent irrecoverable failures to a [[NodeWakeupFailedException]]
          } else
            Future.successful(Vector.empty)
      } yield (latestSnapshotOpt.map(_._2), journalAfterSnapshot)
    }

    // Get the standing query states
    val standingQueryStates: Future[StandingQueryStates] =
      if (untilOpt.isEmpty)
        metrics.persistorGetStandingQueryStatesTimer.time {
          persistor.getStandingQueryStates(qid)
        }
      else
        Future.successful(Map.empty)

    // Will defer all other message processing until the future is complete
    // It is OK to ignore the returned future from `pauseMessageProcessingUntil` because nothing else happens during
    // initialization of this actor. Future processing is deferred by `pauseMessageProcessingUntil`'s message stashing.
    val _ = pauseMessageProcessingUntil[((Snapshot, Journal), StandingQueryStates)](
      snapshotAndJournal.zip(standingQueryStates),
      {
        case Success(((latestSnapshotOpt, journalAfterSnapshot), standingQueryStates)) =>
          // Update node state
          latestSnapshotOpt match {
            case None =>
              edges.clear()
              properties = Map.empty

            case Some(snapshotBytes) =>
              restoreFromSnapshotBytes(snapshotBytes)
          }
          applyEventsEffectsInMemory(journalAfterSnapshot, shouldCauseSideEffects = false)

          // Update standing query state
          standingQueries = MutableMap.empty
          val idProv = idProvider
          val lookupInfo = new cypher.StandingQueryLookupInfo {
            def lookupQuery(queryPartId: StandingQueryPartId): cypher.StandingQuery =
              graph.getStandingQueryPart(queryPartId)
            val node = qid
            val idProvider = idProv
          }
          for ((handler, bytes) <- standingQueryStates) {
            val sqState = PersistenceCodecs.standingQueryStateFormat
              .read(bytes)
              .getOrElse(
                throw new NodeWakeupFailedException(
                  s"NodeActor state (Standing Query States) for node: ${qid.debug} could not be loaded"
                )
              )
            sqState._2.preStart(lookupInfo)
            standingQueries += handler -> sqState
          }
          localEventIndex = StandingQueryLocalEventIndex.from(
            subscribers.subscribersToThisNode.keysIterator,
            standingQueries.iterator.map { case (handler, (_, state)) => handler -> state }
          )
          syncStandingQueries()

          // Once edge map is updated, recompute cost to sleep
          costToSleep.set(Math.round(Math.round(edges.size.toDouble) / Math.log(2) - 2))
        case Failure(err) =>
          // See QU-429
          throw new NodeWakeupFailedException(
            s"""NodeActor state for node: ${qid.debug} could not be loaded - this was most
               |likely due to a problem deserializing journal events""".stripMargin.replace('\n', ' '),
            err
          )
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
  def toSnapshotBytes(): Array[Byte] = {
    latestUpdateAfterSnapshot = None // TODO: reconsider what to do if saving the snapshot fails!
    PersistenceCodecs.nodeSnapshotFormat.write(
      NodeSnapshot(
        properties,
        edges.toSerialize,
        forwardTo,
        mergedIntoHere,
        subscribers.subscribersToThisNode,
        domainNodeIndex.index
      )
    )
  }

  /** During wake-up, deserialize a binary node snapshot and use it to initialize node state
    *
    * @note must be called on the actor thread
    * @param snapshotBytes binary node snapshot
    */
  @throws[NodeWakeupFailedException]("if snapshot bytes cannot be deserialized")
  private def restoreFromSnapshotBytes(snapshotBytes: Array[Byte]): Unit = {
    val restored: NodeSnapshot =
      PersistenceCodecs.nodeSnapshotFormat
        .read(snapshotBytes)
        .fold(
          err =>
            throw new NodeWakeupFailedException(
              s"NodeActor state (snapshot) for node: ${qid.debug} could not be loaded",
              err
            ),
          identity
        )
    properties = restored.properties
    restored.edges.foreach(edges +=)
    forwardTo = restored.forwardTo
    mergedIntoHere = restored.mergedIntoHere
    subscribers = SubscribersToThisNode(restored.subscribersToThisNode)
    domainNodeIndex = DomainNodeIndexBehavior.DomainNodeIndex(restored.domainNodeIndex)
    branchParentIndex =
      DomainNodeIndexBehavior.BranchParentIndex.reconstruct(domainNodeIndex, subscribers.subscribersToThisNode.keys)

    forwardTo.foreach(other =>
      context.become(MergeNodeBehavior.mergedMessageHandling(this, graph, other), discardOld = false)
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
      .map { case ((a, _), c) =>
        a -> c.subscribers.map {
          case Left(q) => q.pretty
          case Right(x) => x
        } -> c.lastNotification -> c.relatedQueries
      }
      .map(_.toString)

    val domainNodeIndexStrings = domainNodeIndex.index.toList
      .map(t => t._1.pretty -> t._2.map { case ((a, _), c) => a -> c })
      .map(_.toString)

    val dgbLocalEventIndexSummary = {
      val propsIdx = localEventIndex.watchingForProperty.toList.flatMap { case (k, v) =>
        v.toList.collect { case StandingQueryLocalEventIndex.DomainNodeIndexSubscription(branch, _) =>
          k.name -> branch.hashCode()
        }
      }
      val edgesIdx = localEventIndex.watchingForEdge.toList.flatMap { case (k, v) =>
        v.toList.collect { case StandingQueryLocalEventIndex.DomainNodeIndexSubscription(branch, _) =>
          k.name -> branch.hashCode()
        }
      }
      val anyEdgesIdx = localEventIndex.watchingForAnyEdge.toList.collect {
        case StandingQueryLocalEventIndex.DomainNodeIndexSubscription(branch, _) =>
          branch.hashCode()
      }

      DgbLocalEventIndexSummary(
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
          atTime.map(EventTime.fromMillis).map(_.largestEventTimeInThisMillisecond).getOrElse(EventTime.MaxValue)
      )
      .recover { case err =>
        log.error(err, "failed to get journal for node {}", qid)
        Vector.empty
      }(context.dispatcher)
      .map { journal =>
        NodeInternalState(
          atTime,
          properties.view.mapValues(propertyValue2String).toMap,
          edges.toSet,
          forwardTo,
          mergedIntoHere,
          latestUpdateAfterSnapshot,
          subscribersStrings,
          domainNodeIndexStrings,
          getSqState(),
          dgbLocalEventIndexSummary,
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
      subscribers.subscribersToThisNode.toList.flatMap { case ((dgb, _), subs) =>
        subs.subscribers.toList.collect { case Left(q) => // filters out receivers outside the graph
          SqStateResult(dgb.hashCode, dgb.size, q, subs.lastNotification)
        }
      },
      domainNodeIndex.index.toList.flatMap { case (q, m) =>
        m.toList.map { case ((dgb, _), lastN) =>
          SqStateResult(dgb.hashCode, dgb.size, q, lastN)
        }
      }
    )
}
