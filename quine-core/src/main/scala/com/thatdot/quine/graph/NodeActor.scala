package com.thatdot.quine.graph

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.StampedLock

import scala.collection.mutable

import com.thatdot.common.logging.Log.{LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.common.logging.Pretty.PrettyHelper
import com.thatdot.quine.graph.behavior.DomainNodeIndexBehavior.{NodeParentIndex, SubscribersToThisNodeUtil}
import com.thatdot.quine.graph.behavior._
import com.thatdot.quine.graph.cypher.MultipleValuesStandingQueryState
import com.thatdot.quine.graph.messaging.CypherMessage._
import com.thatdot.quine.graph.messaging.LiteralMessage.LiteralCommand
import com.thatdot.quine.graph.messaging.StandingQueryMessage._
import com.thatdot.quine.graph.messaging.{AlgorithmCommand, SpaceTimeQuineId}
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.model.{HalfEdge, PropertyValue}
import com.thatdot.quine.util.Log.implicits._

case class NodeConstructorArgs(
  properties: Map[Symbol, PropertyValue],
  edges: Iterable[HalfEdge],
  distinctIdSubscribers: mutable.Map[
    DomainGraphNodeId,
    SubscribersToThisNodeUtil.DistinctIdSubscription,
  ],
  domainNodeIndex: DomainNodeIndexBehavior.DomainNodeIndex,
  multipleValuesStandingQueryStates: NodeActor.MultipleValuesStandingQueries,
  initialJournal: NodeActor.Journal,
)

/** The fundamental graph unit for both data storage (eg [[com.thatdot.quine.graph.NodeActor#properties()]]) and
  * computation (as a Pekko actor).
  * At most one [[NodeActor]] exists in the actor system ([[graph.system]]) per node per moment in
  * time (see [[atTime]]).
  *
  * @param qidAtTime the ID that comprises this node's notion of nominal identity -- analogous to pekko's ActorRef
  * @param graph a reference to the graph in which this node exists
  * @param costToSleep @see [[CostToSleep]]
  * @param wakefulState an atomic reference used like a variable to track the current lifecycle state of this node.
  *                     This is (and may be expected to be) threadsafe, so that [[GraphShardActor]]s can access it
  * @param actorRefLock a lock on this node's [[ActorRef]] used to hard-stop messages when sleeping the node (relayTell uses
  *                     tryReadLock during its tell, so if a write lock is held for a node's actor, no messages can be
  *                     sent to it)
  */
private[graph] class NodeActor(
  qidAtTime: SpaceTimeQuineId,
  graph: QuinePatternOpsGraph with StandingQueryOpsGraph with CypherOpsGraph,
  costToSleep: CostToSleep,
  wakefulState: AtomicReference[WakefulState],
  actorRefLock: StampedLock,
  initialProperties: Map[Symbol, PropertyValue],
  initialEdges: Iterable[HalfEdge],
  distinctIdSubscribers: mutable.Map[
    DomainGraphNodeId,
    SubscribersToThisNodeUtil.DistinctIdSubscription,
  ],
  domainNodeIndex: DomainNodeIndexBehavior.DomainNodeIndex,
  multipleValuesStandingQueries: NodeActor.MultipleValuesStandingQueries,
  initialJournal: NodeActor.Journal,
  logConfig: LogConfig,
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
    )(logConfig) {
  implicit def logConfig_ : LogConfig = logConfig
  def receive: Receive = actorClockBehavior {
    case control: NodeControlMessage => goToSleepBehavior(control)
    case StashedMessage(message) => receive(message)
    case query: CypherQueryInstruction => cypherBehavior(query)
    case command: LiteralCommand => literalCommandBehavior(command)
    case command: AlgorithmCommand => algorithmBehavior(command)
    case command: DomainNodeSubscriptionCommand => domainNodeIndexBehavior(command)
    case command: MultipleValuesStandingQueryCommand => multipleValuesStandingQueryBehavior(command)
    case command: UpdateStandingQueriesCommand => updateStandingQueriesBehavior(command)
    case command: QuinePatternCommand => quinePatternQueryBehavior(command)
    case msg => log.error(log"Node received an unknown message (from ${sender()}): ${msg.toString}")
  }

  val edges = defaultSynchronousEdgeProcessor

  { // here be the side-effects performed by the constructor

    // initialize relevant histograms
    metrics.nodeEdgesCounter(namespace).bucketContaining(edges.size).inc()
    metrics.nodePropertyCounter(namespace).bucketContaining(properties.size).inc()

    // replay journal
    initialJournal foreach {
      case event: PropertyEvent => applyPropertyEffect(event)
      case event: EdgeEvent => edges.updateEdgeCollection(event)
      case event: DomainIndexEvent => applyDomainIndexEffect(event, shouldCauseSideEffects = false)
    }

    // Once edge map is updated, recompute cost to sleep:
    costToSleep.set(Math.round(Math.round(edges.size.toDouble) / Math.log(2) - 2))

    // Make a best-effort attempt at restoring the watchableEventIndex: This will fail for DGNs that no longer exist,
    // so also make note of which those are for further cleanup. Now that the journal and snapshot have both been
    // applied, we know that this reconstruction + removal detection will be as complete as possible
    val (watchableEventIndexRestored, locallyWatchedDgnsToRemove) = StandingQueryWatchableEventIndex.from(
      dgnRegistry,
      domainGraphSubscribers.subscribersToThisNode.keysIterator,
      multipleValuesStandingQueries.iterator.map { case (sqIdAndPartId, (_, state)) => sqIdAndPartId -> state },
      graph.labelsProperty,
    )
    this.watchableEventIndex = watchableEventIndexRestored

    // Phase: The node has caught up to the target time, but some actions locally on the node need to catch up
    // with what happened with the graph while this node was asleep.

    // stop tracking subscribers of deleted DGNs that were previously watching for local events
    domainGraphSubscribers.removeSubscribersOf(locallyWatchedDgnsToRemove)

    // determine newly-registered DistinctId SQs and the DGN IDs they track (returns only those DGN IDs that are
    // potentially-rooted on this node)
    // see: [[updateDistinctIdStandingQueriesOnNode]]
    val newDistinctIdSqDgns = for {
      (sqId, runningSq) <- graph
        .standingQueries(namespace) // Silently ignore absent namespace.
        .fold(Map.empty[StandingQueryId, RunningStandingQuery])(_.runningStandingQueries)
      dgnId <- runningSq.query.queryPattern match {
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
    log.whenDebugEnabled {
      if (
        ((propogationDgnsToRemove: Iterable[_]) ++
        (locallyWatchedDgnsToRemove: Iterable[_]) ++
        (newDistinctIdSqDgns: Iterable[_])).nonEmpty
      ) {
        // serializing DGN collections is potentially nontrivial work, so only do it when the target log level is enabled
        log.trace(
          safe"""Detected Standing Query changes while asleep. Removed DGN IDs:
                |${Safe((propogationDgnsToRemove ++ locallyWatchedDgnsToRemove).toList.distinct.toString)}.
                |Added DGN IDs: ${Safe(newDistinctIdSqDgns.toString)}. Catching up now.""".cleanLines,
        )
      }
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

    // Node is done waking up, stop the wakeup timer (if it's running)
    wakefulState.get match {
      case WakefulState.Awake(wakeTimer) => wakeTimer.stop()
      case WakefulState.ConsideringSleep(_, _, wakeTimer) => wakeTimer.stop()
      case _: WakefulState.GoingToSleep =>
        // This is impossible, because only the node itself (GoToSleepBehavior) can update its wakeful state to
        // `GoingToSleep`, and only in response to a message -- this node hasn't had a chance to receive any messages
        // yet, it's still being constructed!
        throw new IllegalStateException(
          s"The node: ${qid.pretty} is going to sleep before it has woken up enough to decide to go back to sleep",
        )
    }
  }
}

object NodeActor {
  type Journal = Iterable[NodeEvent]
  type MultipleValuesStandingQueries = mutable.Map[
    (StandingQueryId, MultipleValuesStandingQueryPartId),
    (MultipleValuesStandingQueryPartSubscription, MultipleValuesStandingQueryState),
  ]
}
