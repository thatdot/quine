package com.thatdot.quine.graph

import java.util.LinkedHashMap
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import java.util.concurrent.locks.StampedLock

import scala.collection.concurrent
import scala.concurrent.duration.{Deadline, DurationDouble, DurationInt, FiniteDuration}
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

import akka.actor.{Actor, ActorLogging, ActorRef, InvalidActorNameException, Props, Timers}
import akka.dispatch.Envelope
import akka.stream.scaladsl.Source

import com.thatdot.quine.graph.GraphShardActor.LivenessStatus
import com.thatdot.quine.graph.messaging.BaseMessage.{Ack, DeliveryRelay, Done, LocalMessageDelivery}
import com.thatdot.quine.graph.messaging.ShardMessage.{
  AwakeNode,
  CurrentInMemoryLimits,
  GetInMemoryLimits,
  GetShardStats,
  InitiateShardShutdown,
  LocalPredicate,
  RemoveNodesIf,
  RequestNodeSleep,
  SampleAwakeNodes,
  ShardShutdownProgress,
  ShardStats,
  UpdateInMemoryLimits
}
import com.thatdot.quine.graph.messaging.{NodeActorMailboxExtension, QuineIdAtTime, QuineMessage, QuineRefOps}
import com.thatdot.quine.model.QuineIdProvider
import com.thatdot.quine.util.{ExpiringLruSet, QuineDispatchers}

/** Shard in the Quine graph
  *
  * Each node in the Quine graph is managed by exactly one shard (and which
  * shard that is can be computed from [[ClusterOperationConfig.whichGlobalShardId]]).
  * Shards are responsible for:
  *
  *   - waking up nodes (eg. when there is a message waiting for them) and
  *     sleeping them (when they've been inactive and the in-memory limit is
  *     reached)
  *
  *   - relaying messages from cross-host destinations
  *
  * @param graph         graph of which this shard is a part of
  * @param shardId       shard index (unique within the entire logical graph)
  * @param nodes         nodes which have a shard-spawned node actor running (or just stopped)
  * @param inMemoryLimit bounds on how many node actors the shard may create
  */
final private[quine] class GraphShardActor(
  val graph: BaseGraph,
  shardId: Int,
  nodes: concurrent.Map[QuineIdAtTime, GraphShardActor.NodeState],
  private var inMemoryLimit: Option[InMemoryNodeLimit]
) extends Actor
    with ActorLogging
    with QuineRefOps
    with Timers {

  import context.system

  implicit def idProvider: QuineIdProvider = graph.idProvider

  // Periodic signal sent once the shard has begun to shutdown
  private case object ShuttingDownShard

  // Periodic signal to clean up old nodes
  private case object CheckForInactiveNodes
  timers.startTimerWithFixedDelay(CheckForInactiveNodes, CheckForInactiveNodes, 10.seconds)

  /** If it isn't already, start shutting down the shard and report on progress
    *
    * @note new nodes can still be started (to finish work in-process)
    * @return how many nodes are still awake
    */
  def requestShutdown(): ShardShutdownProgress = {
    if (!timers.isTimerActive(ShuttingDownShard)) {
      this.receive(ShuttingDownShard)
      timers.startTimerWithFixedDelay(ShuttingDownShard, ShuttingDownShard, 200 milliseconds)
    }
    ShardShutdownProgress(nodes.size)
  }

  /** == Metrics Counters ==
    */
  private[this] val name = self.path.name

  private[this] val messagesDeduplicatedCounter = graph.metrics.shardMessagesDeduplicatedCounter(name)

  // Counters that track the sleep cycle (in aggregate) of nodes on the shard
  private[this] val nodesWokenUpCounter = graph.metrics.shardNodesWokenUpCounter(name)
  private[this] val nodesSleptSuccessCounter = graph.metrics.shardNodesSleptSuccessCounter(name)
  private[this] val nodesSleptFailureCounter = graph.metrics.shardNodesSleptFailureCounter(name)
  private[this] val nodesRemovedCounter = graph.metrics.shardNodesRemovedCounter(name)

  // Counters that track occurrences of supposedly unlikely (and generally bad) code paths
  private[this] val unlikelyWakeupFailed = graph.metrics.shardUnlikelyWakeupFailed(name)
  private[this] val unlikelyIncompleteShdnCounter = graph.metrics.shardUnlikelyIncompleteShdnCounter(name)
  private[this] val unlikelyActorNameRsvdCounter = graph.metrics.shardUnlikelyActorNameRsvdCounter(name)
  private[this] val unlikelyHardLimitReachedCounter = graph.metrics.shardUnlikelyHardLimitReachedCounter(name)
  private[this] val unlikelyUnexpectedWakeUpErrCounter = graph.metrics.shardUnlikelyUnexpectedWakeUpErrCounter(name)

  override def postStop(): Unit = graph.metrics.removeShardMetrics(name)

  import GraphShardActor.NodeState

  /** An LRU cache of nodes. Used to decide which node to sleep next.
    *
    * @note this is only populated if [[inMemoryLimit]] is set!
    *
    * Invariant: if [[inMemoryLimit]] is set, the following holds before and
    * after calling `receive`:
    *
    *   - if a node is in [[inMemoryActorList]], the node is also in [[nodes]]
    *     with wakeful state [[Awake]]
    *
    *   - if a node is in [[nodes]] with wakeful state [[Awake]], it is either
    *     in [[inMemoryActorList]] or there is a [[StillAwake]] message for that
    *     node waiting to be processed by this shard
    */
  private val inMemoryActorList: ExpiringLruSet[QuineIdAtTime] = inMemoryLimit match {
    case Some(InMemoryNodeLimit(softLimit, _)) if softLimit > 0 =>
      new ExpiringLruSet.SizeAndTimeBounded[QuineIdAtTime](
        initialCapacity = softLimit + 1,
        initialMaximumSize = softLimit,
        initialNanosExpiry = Long.MaxValue
      ) {
        def shouldExpire(id: QuineIdAtTime): ExpiringLruSet.ExpiryDecision = nodes(id) match {
          case NodeState.LiveNode(costToSleep, _, _, _) =>
            if (costToSleep.decrementAndGet() > 0)
              ExpiringLruSet.ExpiryDecision.RejectRemoval(true) // too costly to sleep
            else
              ExpiringLruSet.ExpiryDecision.ShouldRemove

          // WakingNodes shouldn't be in this inMemoryActorList to begin with.
          case NodeState.WakingNode =>
            throw new IllegalStateException(s"Programmer error: $id refers to a non-awake node")
        }

        def expiryListener(cause: ExpiringLruSet.RemovalCause, id: QuineIdAtTime): Unit = sleepActor(id)
      }

    case _ => new ExpiringLruSet.Noop[QuineIdAtTime]
  }

  /** Instruct a node to go to sleep.
    *
    * @note this can fail, see [[WakefulState]] for transitions out of [[ConsideringSleep]]
    * @param target the node/edge being told to sleep
    */
  private def sleepActor(target: QuineIdAtTime): Unit =
    nodes.get(target) match {
      case Some(NodeState.LiveNode(_, actorRef, _, state)) =>
        // Start/extend a deadline if the node isn't already going to sleep
        val previous = state.getAndUpdate {
          case WakefulState.Awake | _: WakefulState.ConsideringSleep =>
            WakefulState.ConsideringSleep(GraphShardActor.SleepDeadlineDelay.fromNow)
          case goingToSleep: WakefulState.GoingToSleep => goingToSleep
        }

        // If the node was not already considering sleep, tell it to
        if (previous == WakefulState.Awake) {
          log.debug("sleepActor: sent GoToSleep request to: {}", target)
          actorRef ! GoToSleep
        } else {
          log.debug("sleepActor: {} is already: {}", target, previous)
        }

      case Some(NodeState.WakingNode) =>
        log.info("Ignoring instruction to sleep a node not yet awake: {}", target)

      case None =>
        log.warning("sleepActor: cannot find actor for: {}", target)
    }

  /** Basic LRU cache of the dedup IDs of the last 10000 delivery relays
    *
    * Implementation is inspired by the documentation of [[LinkedHashMap.removeEldestEntry]]
    */
  private val msgDedupCache: LinkedHashMap[Long, None.type] = {
    val capacity = 10000
    val loadFactor = 0.75F // the default
    val accessOrder = true // "eldest" tracks according to accesses as well as inserts
    new java.util.LinkedHashMap[Long, None.type](capacity, loadFactor, accessOrder) {
      override def removeEldestEntry(eldest: java.util.Map.Entry[Long, None.type]) =
        this.size() >= capacity
    }
  }

  /** This should be used mostly for debugging.
    *
    * @return statistics about the nodes managed by the shard
    */
  private def shardStats(): ShardStats = {
    var nodesAwake = 0
    var nodesAskedToSleep = 0
    var nodesSleeping = 0

    for (entry <- nodes.values)
      entry match {
        case NodeState.WakingNode =>
          nodesAwake += 1 // Count these separately? This would've formerly been counted as awake nodes.
        case NodeState.LiveNode(_, _, _, wakefulState) =>
          wakefulState.get match {
            case WakefulState.Awake => nodesAwake += 1
            case _: WakefulState.ConsideringSleep => nodesAskedToSleep += 1
            case _: WakefulState.GoingToSleep => nodesSleeping += 1
          }
      }

    ShardStats(nodesAwake, nodesAskedToSleep, nodesSleeping)
  }

  def getAwakeNode(qid: QuineIdAtTime): LivenessStatus = nodes.get(qid) match {
    case Some(value) =>
      value match {
        case NodeState.WakingNode => LivenessStatus.WakingUp
        case NodeState.LiveNode(_, actorRef, _, wakefulState) =>
          // Re-awake nodes in the process of going to sleep
          val newState =
            wakefulState.updateAndGet {
              case WakefulState.ConsideringSleep(_) => WakefulState.Awake
              case other => other
            }
          newState match {
            case WakefulState.Awake =>
              inMemoryActorList.update(qid)
              // No lock needed because the actor cannot be shutting down
              LivenessStatus.AlreadyAwake(actorRef)
            case WakefulState.GoingToSleep(shardPromise) =>
              unlikelyIncompleteShdnCounter.inc()
              // Keep track of the side effects as a result of shutting down the node
              LivenessStatus.IncompleteActorShutdown(shardPromise.future)
            // Impossible - the `updateAndGet` above rules this case out
            case WakefulState.ConsideringSleep(_) =>
              throw new IllegalStateException("wakeUpActor: unexpectedly still in ConsideringSleep state")
          }
      }
    case None => LivenessStatus.Nonexistent
  }

  /** Deliver a message to a node this shard is responsible for, possibly
    * waking/creating the actor along the way.
    *
    * @param message message to deliver
    * @param qid node (and time)
    * @param originalSender original sender of the message - used for debug only
    */
  def deliverLocalMessage(
    message: QuineMessage,
    qid: QuineIdAtTime,
    originalSender: ActorRef
  ): Unit = getAwakeNode(qid) match {
    case LivenessStatus.AlreadyAwake(nodeActor) => nodeActor.tell(message, originalSender)
    case LivenessStatus.WakingUp =>
      val envelope = Envelope(message, originalSender, system)
      // No need for another WakeUp message to the shard, is this node is already waking up
      NodeActorMailboxExtension(system).enqueueIntoMessageQueue(qid, envelope)
      ()
    case LivenessStatus.IncompleteActorShutdown(persistingFuture) =>
      val envelope = Envelope(message, originalSender, system)
      if (NodeActorMailboxExtension(system).enqueueIntoMessageQueue(qid, envelope))
        persistingFuture.onComplete(_ => self.tell(WakeUp(qid), ActorRef.noSender))(context.dispatcher)
    case LivenessStatus.Nonexistent =>
      val envelope = Envelope(message, originalSender, system)
      if (NodeActorMailboxExtension(system).enqueueIntoMessageQueue(qid, envelope))
        self.tell(WakeUp(qid), ActorRef.noSender)
  }

  def receive: Receive = {

    case s @ SampleAwakeNodes(limitOpt, atTime, _) =>
      val toTake = limitOpt.getOrElse(Int.MaxValue)
      val sampled =
        if (toTake <= 0)
          Nil
        else if (inMemoryLimit.isEmpty)
          nodes.keys.iterator
            .collect { case QuineIdAtTime(qid, t) if t == atTime => AwakeNode(qid) }
            .take(toTake)
            .toVector
        else {
          val lastN = collection.mutable.Queue.empty[AwakeNode]
          for (qidAtTime <- inMemoryActorList.iterator) {
            val QuineIdAtTime(qid, t) = qidAtTime
            if (t == atTime) {
              lastN.enqueue(AwakeNode(qid))
              if (lastN.size > toTake)
                lastN.dequeue()
            }
          }
          lastN.toList
        }
      s ?! Source(sampled)

    case DeliveryRelay(msg, dedupId, needsAck) =>
      if (needsAck) sender() ! Ack
      Option(msgDedupCache.put(dedupId, None)) match { // `.put` returns `null` if key is not present
        case None => this.receive(msg) // Not a duplicate
        case Some(_) => messagesDeduplicatedCounter.inc() // It is a duplicate. Ignore.
      }

    case LocalMessageDelivery(msg, target, originalSender) =>
      // Note: This does nothing with the sender of this `LocalMessageDelivery`
      deliverLocalMessage(msg, target, originalSender)

    case NodeStateRehydrated(id, nodeArgs, remaining, errorCount) =>
      // Will be re-calculated from edge count later.
      val costToSleep = new CostToSleep(0L)
      val wakefulState = new AtomicReference[WakefulState](WakefulState.Awake)
      val actorRefLock = new StampedLock()
      val props = Props(
        graph.nodeClass,
        id :: graph :: costToSleep :: wakefulState :: actorRefLock :: nodeArgs.productIterator.toList: _*
      ).withMailbox("akka.quine.node-mailbox")
        .withDispatcher(QuineDispatchers.nodeDispatcherName)
      try {
        val actorRef: ActorRef = context.actorOf(props, name = id.toInternalString)
        nodes(id) = NodeState.LiveNode(costToSleep, actorRef, actorRefLock, wakefulState)
        inMemoryActorList.update(id)
        nodesWokenUpCounter.inc()
      } catch {
        // Akka may not have finished freeing the name even if the actor is shut down.
        // InvalidActorNameException is thrown for a variety of different reasons, see
        // https://github.com/akka/akka/search?q=%22throw+InvalidActorNameException%22
        // Here we're only interested in catching the case where the actor name is syntactically
        // valid, but at runtime Akka still thinks there's another Actor with that same name.
        // e.g. specifically:
        // https://github.com/akka/akka/blob/7abc41cf4e7e8827393b181cd06c5f8ea684e696/akka-actor/src/main/scala/akka/actor/dungeon/ChildrenContainer.scala#L134
        case InvalidActorNameException(msg) if msg endsWith "is not unique!" =>
          nodes.remove(id)
          unlikelyActorNameRsvdCounter.inc()
          val eKey = WakeUpErrorStates.ActorNameStillReserved
          val newErrorCount = errorCount.updated(eKey, errorCount.getOrElse(eKey, 0) + 1)
          val msgToDeliver = WakeUp(id, None, remaining - 1, newErrorCount)
          LocalMessageDelivery.slidingDelay(remaining) match {
            case None => self ! msgToDeliver
            case Some(delay) =>
              context.system.scheduler.scheduleOnce(delay)(self ! msgToDeliver)(context.dispatcher)
              ()
          }
      }

    case msg: GetShardStats => msg ?! shardStats()

    case msg: GetInMemoryLimits =>
      msg ?! CurrentInMemoryLimits(inMemoryLimit)

    case msg: UpdateInMemoryLimits =>
      inMemoryActorList match {
        case list: ExpiringLruSet.SizeAndTimeBounded[QuineIdAtTime @unchecked] if inMemoryLimit.nonEmpty =>
          inMemoryLimit = Some(msg.newLimits)
          list.maximumSize = msg.newLimits.softLimit

        // TODO: implement this case (see scaladoc on [[UpdateInMemoryLimits]])
        case _ =>
      }
      msg ?! CurrentInMemoryLimits(inMemoryLimit)

    // This is a ping sent from a node to ensure it is still in the LRU
    case StillAwake(id) =>
      // Should a waking node be counted, too?
      if (
        nodes.get(id).collect { case NodeState.LiveNode(_, _, _, wakefulState) =>
          wakefulState.get == WakefulState.Awake
        } getOrElse false
      )
        inMemoryActorList.update(id)

    // Actor shut down completely
    case SleepOutcome.SleepSuccess(id, shardPromise) =>
      log.debug("Sleep succeeded for {}", id.debug)
      nodes -= id
      inMemoryActorList.remove(id)
      nodesSleptSuccessCounter.inc()
      val promiseCompletedUniquely = shardPromise.trySuccess(())
      if (!promiseCompletedUniquely) { // Promise was already completed -- log an appropriate message
        shardPromise.future.value.get match {
          case Success(_) =>
            log.info("Received redundant notification about successfully slept node: {}", id.debug)
          case Failure(_) =>
            log.error(
              """Received notification that node: {} slept, but that node already reported a failure for the same sleep request""",
              id.debug
            )
        }
      }

      // Remove the message queue if empty, or else wake up the node
      val removed = NodeActorMailboxExtension(system).removeMessageQueueIfEmpty(id)
      if (!removed) self ! WakeUp(id, errorCount = Map(WakeUpErrorStates.SleepSucceededButMessageQueueNonEmpty -> 1))

    /** The failure here is not that the actor couldn't be shut down, but that
      * the persistor couldn't successfully persist the data. Try to wake the
      * node back up.
      */
    case SleepOutcome.SleepFailed(id, snapshot, numEdges, propertySizes, exception, shardPromise) =>
      log.error(
        exception,
        "Failed to store: {} bytes on: {}, composed of: {} edges and: {} properties. Restoring the node.",
        snapshot.length,
        id.debug,
        numEdges,
        propertySizes.size
      )
      if (log.isInfoEnabled)
        log.info(
          "Property sizes on failed store: {}: {}",
          id.debug,
          propertySizes.map { case (k, v) => k.name + ":" + v }.mkString("{", ", ", "}")
        )
      nodes -= id
      inMemoryActorList.remove(id)
      nodesSleptFailureCounter.inc()
      val promiseCompletedUniquely = shardPromise.tryFailure(exception)
      if (!promiseCompletedUniquely) { // Promise was already completed -- log an appropriate message
        shardPromise.future.value.get match {
          case Success(_) =>
            log.error(
              """A node failed to sleep: {}, but that node already reported a success for the same sleep request""",
              id.debug
            )
          case Failure(e) =>
            log.warning(
              s"A node failed to sleep, and reported that failure multiple times: {}. Latest error was: {}",
              id.debug,
              e
            )
        }
      }

      // wake the node back up
      self ! WakeUp(
        id,
        Some(snapshot),
        errorCount = Map(WakeUpErrorStates.SleepOutcomeSleepFailed -> 1)
      )

    case WakeUp(id, snapshotOpt, remaining, errorCount) =>
      getAwakeNode(id) match {
        case LivenessStatus.AlreadyAwake(nodeActor) => nodeActor.tell(ProcessMessages, ActorRef.noSender)
        case LivenessStatus.WakingUp => ()
        case badOutcome if remaining <= 0 =>
          unlikelyWakeupFailed.inc()
          val stats = shardStats()
          if (log.isErrorEnabled)
            log.error(
              s"No more retries waking up: ${id.debug} " +
              s"with sleep status: ${nodes.get(id)} " +
              s"with nodes-on-shard: ${stats.awake} awake, ${stats.goingToSleep} going to sleep " +
              s"Outcome: $badOutcome " +
              s"Errors: " + errorCount.toList.map { case (k, v) => s"$k: $v" }.mkString(", ")
            )
        case LivenessStatus.IncompleteActorShutdown(nodeRemovedFromMaps) =>
          nodeRemovedFromMaps.onComplete { _ =>
            val eKey = WakeUpErrorStates.IncompleteActorShutdown
            val newErrorCount = errorCount.updated(eKey, errorCount.getOrElse(eKey, 0) + 1)
            val msgToDeliver = WakeUp(id, snapshotOpt, remaining - 1, newErrorCount)
            self ! msgToDeliver
          }(context.dispatcher)
        // The node is not awake at all
        case LivenessStatus.Nonexistent =>
          val canCreateNewNodes = inMemoryLimit.forall(_.hardLimit > nodes.size)
          if (canCreateNewNodes) {
            val ec = graph.nodeDispatcherEC
            nodes(id) = NodeState.WakingNode
            NodeActor
              .create(id, snapshotOpt, graph)
              .onComplete {
                case Success(nodeArgs) =>
                  self.tell(NodeStateRehydrated(id, nodeArgs, remaining, errorCount), self)
                case Failure(error) => // Some persistor error, likely
                  nodes.remove(id)
                  unlikelyUnexpectedWakeUpErrCounter.inc()
                  if (log.isInfoEnabled) log.info(s"$remaining retries remaining. Retrying because of a $error")
                  val eKey = WakeUpErrorStates.UnexpectedWakeUpError
                  val newErrorCount = errorCount.updated(eKey, errorCount.getOrElse(eKey, 0) + 1)
                  val msgToDeliver = WakeUp(id, snapshotOpt, remaining - 1, newErrorCount)
                  LocalMessageDelivery.slidingDelay(remaining) match {
                    case None => self ! msgToDeliver
                    case Some(delay) =>
                      context.system.scheduler.scheduleOnce(delay)(self ! msgToDeliver)(context.dispatcher)
                      ()
                  }
              }(ec)
          } else {
            unlikelyHardLimitReachedCounter.inc()
            val eKey = WakeUpErrorStates.InMemoryNodeCountHardLimitReached
            val newErrorCount = errorCount.updated(eKey, errorCount.getOrElse(eKey, 0) + 1)
            val msgToDeliver = WakeUp(id, snapshotOpt, remaining - 1, newErrorCount)
            // TODO: don't hardcode the time until retry
            log.warning("Failed to wake up {} due to hard in-memory limit: {} (retrying)", id, inMemoryLimit)
            context.system.scheduler.scheduleOnce(0.01 second)(self ! msgToDeliver)(context.dispatcher)
            // TODO: This will cause _more_ memory usage because the mailbox will fill up with all these undelivered messages.
            ()
          }
      }

    case msg @ RemoveNodesIf(LocalPredicate(predicate), _) =>
      var wakingNodesExist = false
      for ((nodeId, nodeState) <- nodes; if predicate(nodeId))
        nodeState match {
          case NodeState.WakingNode =>
            if (log.isInfoEnabled) log.info(s"Got message to remove node $nodeId that's not yet awake, will recurse")
            wakingNodesExist = true
          case NodeState.LiveNode(_, actorRef, _, _) =>
            nodes.remove(nodeId)
            nodesRemovedCounter.inc()
            context.stop(actorRef)
            inMemoryActorList.remove(nodeId)
        }
      if (wakingNodesExist) {
        val _ = context.system.scheduler.scheduleOnce(8.millis, self, msg)(context.dispatcher, sender())
      } else {
        msg ?! Done
      }

    case msg @ RequestNodeSleep(idToSleep, _) =>
      sleepActor(idToSleep)
      msg ?! Done

    case msg @ InitiateShardShutdown(_) =>
      val remaining = requestShutdown() // Reports the count of live actors remaining
      if (remaining.remainingNodeActorCount < 10 && remaining.remainingNodeActorCount > 0)
        log.info(s"Shard #${shardId} has ${remaining.remainingNodeActorCount} node(s) awake: ${nodes.mkString(", ")}")
      msg ?! remaining

    case ShuttingDownShard =>
      nodes.keys.foreach(sleepActor)
      inMemoryActorList.clear()

    case CheckForInactiveNodes =>
      inMemoryActorList.doExpiration()

    case m => log.error(s"Message unhandled by GraphShardActor: $m")
  }
}
object GraphShardActor {

  /** Actor name used for shard actors
    *
    * @note deterministic names allow resolution of remote shards using actor selections
    */
  def name(shardId: Int): String = "shard-" + shardId

  /** How long the node has to process the GoToSleep message before it refuses sleep
    * (starting from when that message was sent).
    */
  val SleepDeadlineDelay: FiniteDuration = 3.seconds

  sealed abstract private[graph] class LivenessStatus
  private[graph] object LivenessStatus {
    final case class AlreadyAwake(nodeActor: ActorRef) extends LivenessStatus
    case object WakingUp extends LivenessStatus

    /** @param shardNodesUpdated Future tracking when the shard has removed the node from its nodes map
      */
    final case class IncompleteActorShutdown(shardNodesUpdated: Future[Unit]) extends LivenessStatus
    case object Nonexistent extends LivenessStatus

  }

  sealed abstract private[quine] class NodeState
  private[quine] object NodeState {

    case object WakingNode extends NodeState

    /** This is what the shard tracks for each node it manages
      *
      * == Locking `actorRef` ==
      *
      * Whenever using the `actorRef`, acquire a read lock (in a non-blocking way)
      * and release it once done with the `actorRef`. This lock ensures that the
      * actor behind the `ActorRef` is still alive. It is important not to block
      * when trying to get the read lock because when the actor terminates itself,
      * it will acquire a write lock and never release it!
      *
      * == State transitions ==
      *
      * The actor advances through state transitions when `state` is updated. The
      * use of an atomic reference means that the shard and node can both try to
      * update the state and they will always have one source of truth for the
      * current state (and that source of truth can be atomically updated, so we
      * can be sure that the transition is valid).
      *
      * @param costToSleep  measure of how costly it is to sleep the node
      * @param actorRef     Akka reference for sending to the actor
      * @param actorRefLock lock to ensure the liveness of the actor behind `actorRef`
      * @param wakefulState where is the node at in the sleep cycle?
      */
    final case class LiveNode(
      costToSleep: AtomicLong,
      actorRef: ActorRef,
      actorRefLock: StampedLock,
      wakefulState: AtomicReference[WakefulState]
    ) extends NodeState
  }
}

final case class InMemoryNodeLimit(softLimit: Int, hardLimit: Int)
object InMemoryNodeLimit {

  def fromOptions(softLimitOpt: Option[Int], hardLimitOpt: Option[Int]): Option[InMemoryNodeLimit] =
    (softLimitOpt, hardLimitOpt) match {
      case (Some(s), Some(h)) =>
        if (h > s) {
          Some(InMemoryNodeLimit(s, h))
        } else {
          throw new IllegalArgumentException("In memory node limits require a hard limit greater than the soft limit")
        }
      case (Some(s), None) => Some(InMemoryNodeLimit(s, Int.MaxValue))
      case (None, Some(h)) => Some(InMemoryNodeLimit(h, h))
      case (None, None) => None
    }
}

/* State in a node actor's lifecycle
 *
 * == Valid transitions ==
 *
 * {{{
 *    _----[0]- Asleep (not in map) <--_
 *   /                                  \
 *   |   _--[1]-_                       |
 *   |  /        \                     [5]
 *   v |         v                      |
 *  Awake    ConsideringSleep -[4]-> GoingToSleep
 *     ^         ||      ^
 *      \       / |      |
 *       `-[2]-'   `-[3]-'
 * }}}
 *
 * 0 (shard): when a shard receives a `WakeUp` message for a node (sometimes this involves retries)
 * 1 (shard): when `sleepActor` is called (probably due to the in-memory limit being hit)
 * 2 (shard): when the shard receives a delivery relay meant for a node the shard told to sleep
 * 2 (node): when a node refuses sleep because the sleep deadline expired or it has recent activity
 * 3 (shard): when `sleepActor` is called and the previous deadline expired
 * 4 (node): when a node accepts sleep because the sleep deadline has not expired
 * 5 (shard): when the shard get confirmation from the node that the node finished sleeping
 *
 * Other invariants:
 *
 *  - whenever the shard goes through [1], it sends the node a [[GoingToSleep]] message
 *
 *  - whenever the node goes through [2], it sends the shard a [[StillAwake]] message
 *
 *  - when the shard Promise in [[GoingToSleep]] completes, a [[SleepOutcome]] message is sent to the shard carrying
 *    the shard promise
 *
 *  - when the shard receives a [[SleepOutcome]] message, it will complete the included Promise
 *
 *  - `actorRefLock: StampedLock` is write-acquired in a blocking fashion (and never released)
 *    right after the node enters `GoingToSleep` (since the actor ref is no longer valid as soon
 *    as the actor is terminated)
 */
sealed abstract private[quine] class WakefulState
private[quine] object WakefulState {
  case object Awake extends WakefulState
  final case class ConsideringSleep(deadline: Deadline) extends WakefulState
  final case class GoingToSleep(shard: Promise[Unit]) extends WakefulState
}

sealed abstract class ControlMessages
sealed abstract class NodeControlMessage extends ControlMessages
sealed abstract class ShardControlMessage extends ControlMessages

/** Sent by a shard to a node to request the node check its wakeful state and
  * possibly go to sleep. This will result in at most 1 [[SleepOutcome]] sent
  * from the node back to the shard.
  *
  * @note if the node wakeful state no longer makes sense by the time the node
  * gets this message, that's fine, it'll be ignored!
  */
private[quine] case object GoToSleep extends NodeControlMessage

/** Sent by a shard to a node to ensure that it is going to process a message
  * in its mailbox. By sending this message to the node actor, we are ensuring
  * that the dispatcher knows that the actor has messages to process.
  */
private[quine] case object ProcessMessages extends NodeControlMessage

/** Sent by the node to the shard right before the node's actor is stopped. This
  * allows the shard to remove the node from the map and possibly also take
  * mitigating actions for a failed snapshot. This is always sent within a JVM, and
  * at most 1 [[SleepOutcome]] message will be sent as a result of a [[GoToSleep]] message
  */
sealed abstract private[quine] class SleepOutcome extends ShardControlMessage {

  /** Promise that the shard will complete once the shard's in-memory tracking of nodes has been updated
    * to account for this message. Because the shard receives a [[SleepOutcome]] at most once, this promise
    * will be completed exactly once, up to the JVM crashing: when the shard processes the [[SleepOutcome]] message.
    */
  val nodeMapUpdatedPromise: Promise[Unit]
}
object SleepOutcome {

  /** Node is asleep and fine
    *
    * @param id node that slept
    * @param nodeMapUpdatedPromise [[SleepOutcome.nodeMapUpdatedPromise]]
    */
  final private[quine] case class SleepSuccess(id: QuineIdAtTime, nodeMapUpdatedPromise: Promise[Unit])
      extends SleepOutcome

  /** Node is stopped, but the saving of data failed
    *
    * This gets returned by the node to the shard right before it terminates
    * itself to indicate to that the persistor couldn't save the final
    * snapshot. Since this contains the snapshot, it is a final opportunity to
    * spin up a new actor to hold this state.
    *
    * @param id node that stopped
    * @param snapshotBytes data bytes of the node snapshot that could not be saved
    * @param numEdges number of half edges on this node
    * @param propertySizes exact serialized size of each property on this node
    * @param error the error from the persistence layer
    * @param nodeMapUpdatedPromise [[SleepOutcome.nodeMapUpdatedPromise]]
    */
  final private[quine] case class SleepFailed(
    id: QuineIdAtTime,
    snapshotBytes: Array[Byte],
    numEdges: Int,
    propertySizes: Map[Symbol, Int],
    error: Throwable,
    nodeMapUpdatedPromise: Promise[Unit]
  ) extends SleepOutcome
}

/** Sent by a node to a shard to request the shard consider adding the node back
  * into the `inMemoryActorList` (the shard ultimately makes that decision by
  * checking the nodes sleep status)
  *
  * @param id node which claims to be still awake
  */
final private[quine] case class StillAwake(id: QuineIdAtTime) extends ShardControlMessage

/** Sent to a shard to request that a node be woken up
  *
  * @param id which node to wake up
  * @param snapshotOpt snapshot with which to restore the node
  * @param remainingRetries how many retries left (waiting for Akka to free up the name)
  */
final private[quine] case class WakeUp(
  id: QuineIdAtTime,
  snapshotOpt: Option[Array[Byte]] = None,
  remainingRetries: Int = LocalMessageDelivery.remainingRetriesMax,
  errorCount: Map[WakeUpErrorStates, Int] = Map.empty
) extends ShardControlMessage

/** Sent to a shard to tell it the state for a waking Node has been read from persistence */
final private[quine] case class NodeStateRehydrated(
  id: QuineIdAtTime,
  nodeArgs: NodeActorConstructorArgs,
  remainingRetries: Int,
  errorCount: Map[WakeUpErrorStates, Int]
) extends ShardControlMessage

/** Possible failures encountered when waking up nodes. Tracking how often these errors occur can aid understanding
  * of some protocol failure conditions.
  */
sealed trait WakeUpErrorStates
object WakeUpErrorStates {
  case object SleepOutcomeSleepFailed extends WakeUpErrorStates
  case object SleepSucceededButMessageQueueNonEmpty extends WakeUpErrorStates
  case object ActorNameStillReserved extends WakeUpErrorStates
  case object UnexpectedWakeUpError extends WakeUpErrorStates
  case object IncompleteActorShutdown extends WakeUpErrorStates
  case object InMemoryNodeCountHardLimitReached extends WakeUpErrorStates
}
