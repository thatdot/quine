package com.thatdot.quine.graph

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.StampedLock

import scala.collection.compat._
import scala.collection.mutable.{Map => MutableMap}
import scala.compat.ExecutionContexts
import scala.concurrent.Future
import scala.util.{Failure, Success}

import akka.actor.{Actor, ActorLogging}

import com.thatdot.quine.graph.NodeChangeEvent._
import com.thatdot.quine.graph.behavior._
import com.thatdot.quine.graph.edgecollection.EdgeCollection
import com.thatdot.quine.graph.messaging.BaseMessage.Done
import com.thatdot.quine.graph.messaging.CypherMessage._
import com.thatdot.quine.graph.messaging.LiteralMessage.{
  LiteralCommand,
  LocallyRegisteredStandingQuery,
  NodeInternalState
}
import com.thatdot.quine.graph.messaging.StandingQueryMessage._
import com.thatdot.quine.graph.messaging.{QuineIdAtTime, QuineIdOps, QuineRefOps}
import com.thatdot.quine.model.{Milliseconds, PropertyValue, QuineId, QuineIdProvider}
import com.thatdot.quine.persistor.{PersistenceAgent, PersistenceCodecs, PersistenceConfig}
import com.thatdot.quine.util.HexConversions

/** The fundamental graph unit for both data storage (eg [[com.thatdot.quine.graph.NodeActor#properties()]]) and
  * computation (as an Akka actor).
  * At most one [[NodeActor]] exists in the actor system ([[graph.system]]) per node per moment in
  * time (see [[atTime]]).
  *
  * @param qid the ID that comprises this node's notion of nominal identity -- analogous to akka's ActorRef
  * @param graph a reference to the graph in which this node exists
  * @param costToSleep @see [[CostToSleep]]
  * @param wakefulState an atomic reference used like a variable to track the current lifecycle state of this node.
  *                     This is (and may be expected to be) threadsafe, so that [[GraphShardActor]]s can access it
  * @param actorRefLock a lock on this node's [[ActorRef]] used to hard-stop messages when sleeping the node (relayTell uses
  *                     tryReadLock during its tell, so if a write lock is held for a node's actor, no messages can be
  *                     sent to it)
  * @param restoreAfterFailureSnapshotOpt
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

  protected def processEvent(
    event: NodeChangeEvent,
    atTimeOverride: Option[EventTime] = None
  ): Future[Done.type] = {

    // Prevent updates to historical times states
    atTime match {
      case None =>
      case Some(t) => return Future.failed(IllegalHistoricalUpdate(event, qid, t))
    }

    // Does this event change anything in node state
    val hasEffect = event match {
      case PropertySet(propKey, propValue) =>
        properties.get(propKey) match {
          case None =>
            metrics.nodePropertyCounter.increment(properties.size)
            true
          case Some(fp) => !fp.equals(propValue)
        }

      case PropertyRemoved(propKey, _) =>
        if (properties.contains(propKey)) {
          metrics.nodePropertyCounter.decrement(properties.size)
          true
        } else {
          false
        }

      case EdgeAdded(edge) =>
        if (!edges.contains(edge)) {
          metrics.nodeEdgesCounter.increment(edges.size)
          true
        } else {
          false
        }

      case EdgeRemoved(edge) =>
        if (edges.contains(edge)) {
          metrics.nodeEdgesCounter.decrement(edges.size)
          true
        } else {
          false
        }

      case MergedIntoOther(_) => forwardTo.isEmpty
      case MergedHere(other) => !mergedIntoHere.contains(other)
    }
    if (hasEffect) {
      applyEvent(event)
      // Pick the time at which this event occurred at
      val occurredAt = atTimeOverride.getOrElse(nextEventTime())
      latestUpdateAfterSnapshot = Some(occurredAt)
      lastWriteMillis = latestEventTime().millis
      val journalFuture = if (persistenceConfig.journalEnabled) {
        metrics.persistorPersistEventTimer.time(persistor.persistEvent(qid, occurredAt, event))
      } else Future.unit
      val snapshotFuture = if (persistenceConfig.snapshotOnUpdate) {
        val snapshot = toSnapshotBytes()
        latestUpdateAfterSnapshot = None
        metrics.snapshotSize.update(snapshot.length)
        metrics.persistorPersistSnapshotTimer.time(persistor.persistSnapshot(qid, occurredAt, snapshot))
      } else Future.unit
      val persistFuture = journalFuture.zip(snapshotFuture)
      runPostActions(event)
      persistFuture.map(_ => Done)(ExecutionContexts.parasitic)
    } else Future.successful(Done)
  }

  private[this] def applyEvent(event: NodeChangeEvent): Unit = {
    event match {
      case PropertySet(propKey, propValue) =>
        properties = properties + (propKey -> propValue)

      case PropertyRemoved(propKey, _) =>
        properties = properties - propKey

      case EdgeAdded(edge) =>
        // The more edges you get, the worse it is to sleep
        val len = edges.size
        if (len > 7 && isPowerOfTwo(len)) costToSleep.incrementAndGet()

        val edgeCollectionSizeWarningInterval = 10000
        if (log.isWarningEnabled && (len + 1) % edgeCollectionSizeWarningInterval == 0)
          log.warning(s"Node: ${qid.pretty} has: ${len + 1} edges")

        edges += edge

      case EdgeRemoved(edge) =>
        edges -= edge

      case MergedIntoOther(otherNode) =>
        forwardTo = Some(otherNode)
        edges.clear()
        properties = Map.empty
        context.become(MergeNodeBehavior.mergedMessageHandling(this, graph, otherNode), discardOld = false)

      case MergedHere(otherNode) =>
        mergedIntoHere = mergedIntoHere + otherNode
    }
    ()
  }

  /** Fast check for if a number is a power of 2 */
  private[this] def isPowerOfTwo(n: Int): Boolean = (n & (n - 1)) == 0

  /** Hook for registering some arbitrary action after processing a node event. Right now, all this
    * does is advance standing queries
    *
    * @param event node event
    */
  private[this] def runPostActions(event: NodeChangeEvent): Unit =
    event match {
      case _: EdgeAdded | _: EdgeRemoved | _: PropertySet | _: PropertyRemoved =>
        // update standing queries
        localEventIndex.standingQueriesWatchingNodeEvent(event).foreach {
          case cypherSubscriber: StandingQueryLocalEventIndex.StandingQueryWithId =>
            updateCypherSq(event, cypherSubscriber)
          case StandingQueryLocalEventIndex.DomainNodeIndexSubscription(branch, assumedEdge) =>
            // ensure that this node is subscribed to all other necessary nodes to continue processing the DGB
            ensureSubscriptionToDomainEdges(branch, assumedEdge, subscribers.getRelatedQueries(branch, assumedEdge))
            // ensure that all subscribers to this node are informed about any relevant changes caused by the recent
            // event
            subscribers.updateAnswerAndNotifySubscribers(branch, assumedEdge)
        }
      case _ =>
    }

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
    import context.dispatcher

    type Snapshot = Option[Array[Byte]]
    type Journal = Vector[NodeChangeEvent]
    type StandingQueryStates = Map[(StandingQueryId, StandingQueryPartId), Array[Byte]]

    // Get the snapshot and journal events
    val snapshotAndJournal: Future[(Snapshot, Journal)] = for {
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
            case Some((snapshotTime, _)) => snapshotTime.nextEventTime
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

    // Get the standing query states
    val standingQueryStates: Future[StandingQueryStates] =
      if (untilOpt.isEmpty)
        metrics.persistorGetStandingQueryStatesTimer.time {
          persistor.getStandingQueryStates(qid)
        }
      else
        Future.successful(Map.empty)

    // Will defer all other message processing until the future is complete
    pauseMessageProcessingUntil[((Snapshot, Journal), StandingQueryStates)](
      snapshotAndJournal.zip(standingQueryStates),
      {
        case Success(
              (
                (latestSnapshotOpt: Snapshot, journalAfterSnapshot: Journal),
                standingQueryStates: StandingQueryStates
              )
            ) =>
          // Update node state
          latestSnapshotOpt match {
            case None =>
              edges.clear()
              properties = Map.empty

            case Some(snapshotBytes) =>
              restoreFromSnapshotBytes(snapshotBytes)
          }
          journalAfterSnapshot.foreach(applyEvent)

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
    implicit val ec = context.dispatcher

    // Return a string that (if possible) shows the deserialized representation
    def propertyValue2String(propertyValue: PropertyValue): String =
      propertyValue.deserialized.fold(
        _ => HexConversions.formatHexBinary(propertyValue.serialized),
        _.toString
      )

    val subscribersString = subscribers.subscribersToThisNode
      .map { case ((a, b), c) => a -> b -> c }
      .mkString("\n  ", "\n  ", "\n")

    val domainNodeIndexString = domainNodeIndex.index
      .map(t => t._1 -> t._2.map { case ((a, b), c) => a -> b -> c })
      .mkString("\n  ", "\n  ", "\n")

    persistor
      .getJournal(qid, startingAt = EventTime.MinValue, endingAt = EventTime.MaxValue)
      .recover { case err =>
        log.error(err, "failed to get journal for node {}", qid)
        Vector.empty
      }
      .map { (journal: Vector[NodeChangeEvent]) =>
        NodeInternalState(
          properties.view.mapValues(propertyValue2String).toMap,
          edges.toSet,
          forwardTo,
          mergedIntoHere,
          latestUpdateAfterSnapshot,
          if (subscribers.subscribersToThisNode.nonEmpty) Some(subscribersString) else None,
          if (domainNodeIndex.index.nonEmpty) Some(domainNodeIndexString) else None,
          standingQueries.view.map { case ((globalId, sqId), (StandingQuerySubscribers(_, _, subs), st)) =>
            LocallyRegisteredStandingQuery(
              sqId.toString,
              globalId.toString,
              subs.map(_.toString).toSet,
              st.toString
            )
          }.toVector,
          journal
        )
      }
  }
}
