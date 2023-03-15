package com.thatdot.quine.graph

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.StampedLock

import scala.collection.compat._
import scala.collection.mutable
import scala.compat.ExecutionContexts
import scala.concurrent.Future

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
import com.thatdot.quine.graph.messaging.CypherMessage._
import com.thatdot.quine.graph.messaging.LiteralMessage.LiteralCommand
import com.thatdot.quine.graph.messaging.StandingQueryMessage._
import com.thatdot.quine.graph.messaging.{AlgorithmCommand, QuineIdAtTime}
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.model.{HalfEdge, PropertyValue, QuineId, QuineIdProvider}
import com.thatdot.quine.persistor.codecs.{MultipleValuesStandingQueryStateCodec, SnapshotCodec}

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
  qidAtTime: QuineIdAtTime,
  graph: StandingQueryOpsGraph with CypherOpsGraph,
  costToSleep: CostToSleep,
  wakefulState: AtomicReference[WakefulState],
  actorRefLock: StampedLock,
  initialProperties: Map[Symbol, PropertyValue],
  initialEdges: Iterable[HalfEdge],
  distinctIdSubscribers: mutable.Map[
    DomainGraphNodeId,
    SubscribersToThisNodeUtil.DistinctIdSubscription
  ],
  domainNodeIndex: DomainNodeIndexBehavior.DomainNodeIndex,
  multipleValuesStandingQueries: NodeActor.MultipleValuesStandingQueries,
  initialJournal: NodeActor.Journal
) extends AbstractNodeActor(
      qidAtTime,
      graph,
      costToSleep,
      wakefulState,
      actorRefLock,
      initialProperties,
      initialEdges,
      distinctIdSubscribers,
      domainNodeIndex,
      multipleValuesStandingQueries,
      initialJournal
    ) {
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

  val edges = defaultSynchronousEdgeProcessor

  { // here be the side-effects performed by the constructor

    initialJournal foreach {
      case event: PropertyEvent => applyPropertyEffect(event)
      case event: EdgeEvent => edges.updateEdgeCollection(event)
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
