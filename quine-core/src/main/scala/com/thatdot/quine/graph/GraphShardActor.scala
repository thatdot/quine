package com.thatdot.quine.graph

import java.util.LinkedHashMap
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import java.util.concurrent.locks.StampedLock

import scala.collection.concurrent
import scala.concurrent.duration.{Deadline, DurationDouble, DurationInt, FiniteDuration}
import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

import akka.actor.{Actor, ActorLogging, ActorRef, InvalidActorNameException, Props, SupervisorStrategy, Timers}
import akka.dispatch.Envelope
import akka.pattern.{AskTimeoutException, ask}
import akka.stream.scaladsl.Source
import akka.util.Timeout

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
  SnapshotFailed,
  SnapshotInMemoryNodes,
  SnapshotSucceeded,
  UpdateInMemoryLimits
}
import com.thatdot.quine.graph.messaging.{NodeActorMailboxExtension, QuineIdAtTime, QuineMessage, QuineRefOps}
import com.thatdot.quine.model.{QuineId, QuineIdProvider}
import com.thatdot.quine.util.ExpiringLruSet

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

  import GraphShardActor.{NodeState, WakeUpOutcome}
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

  // Counters that track the sleep cycle (in aggregate) of nodes on the shard
  private[this] val nodesWokenUpCounter = graph.metrics.shardNodesWokenUpCounter(name)
  private[this] val nodesSleptSuccessCounter = graph.metrics.shardNodesSleptSuccessCounter(name)
  private[this] val nodesSleptFailureCounter = graph.metrics.shardNodesSleptFailureCounter(name)
  private[this] val nodesRemovedCounter = graph.metrics.shardNodesRemovedCounter(name)

  // Counters that track occurences of supposedly unlikely (and generally bad) code paths
  private[this] val unlikelyWakeupFailed = graph.metrics.shardUnlikelyWakeupFailed(name)
  private[this] val unlikelyIncompleteShdnCounter = graph.metrics.shardUnlikelyIncompleteShdnCounter(name)
  private[this] val unlikelyActorNameRsvdCounter = graph.metrics.shardUnlikelyActorNameRsvdCounter(name)
  private[this] val unlikelyHardLimitReachedCounter = graph.metrics.shardUnlikelyHardLimitReachedCounter(name)
  private[this] val unlikelyUnexpectedWakeUpErrCounter = graph.metrics.shardUnlikelyUnexpectedWakeUpErrCounter(name)

  override def postStop(): Unit = graph.metrics.removeShardMetrics(name)

  /** An LRU cache of nodes. Used to decide which node to sleep next.
    *
    * @note this is only populated if [[inMemoryLimit]] is set!
    *
    * Invariant: if [[inMemoryLimit]] is set, the following holds before and
    * after calling `recieve`:
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
        def shouldExpire(id: QuineIdAtTime): ExpiringLruSet.ExpiryDecision =
          if (nodes(id).costToSleep.decrementAndGet() > 0) {
            ExpiringLruSet.ExpiryDecision.RejectRemoval(true) // too costly to sleep
          } else {
            ExpiringLruSet.ExpiryDecision.ShouldRemove
          }

        def expiryListener(cause: ExpiringLruSet.RemovalCause, id: QuineIdAtTime): Unit = sleepActor(id)
      }

    case _ => new ExpiringLruSet.Noop[QuineIdAtTime]
  }

  override val supervisorStrategy: SupervisorStrategy = new NodeAndShardSupervisorStrategy().create()

  /** Try to ensure an actor is awake for a node, and if successful, do
    * something with the associated [[ActorRef]]. This is the only place where
    * node actors are created.
    *
    * @note this can fail, see [[WakeUpOutcome]] for the possible outcomes
    * @param id the ID of the node for which we want an actor
    * @param withLiveActorRef what to do with the restored actor reference?
    * @param snapshotBytesOpt snapshot to restore from a failed attempt to persist the node's snapshot
    * @return outcome of trying to wake up the actor
    */
  private def wakeUpActor(
    id: QuineIdAtTime,
    withLiveActorRef: ActorRef => Unit = _.tell(ProcessMessages, ActorRef.noSender),
    snapshotBytesOpt: Option[Array[Byte]] = None
  ): WakeUpOutcome = {
    val canCreateNewNodes = inMemoryLimit.forall(_.hardLimit > nodes.size)
    nodes.get(id) match {
      // The node is awake and ready
      case Some(NodeState(_, actorRef, _, wakefulState)) =>
        // Re-awake nodes in the process of going to sleep
        val newState = wakefulState.updateAndGet {
          case _: WakefulState.ConsideringSleep => WakefulState.Awake
          case other => other
        }

        newState match {
          // Keep track of the side effects as a result of shutting down the node
          case WakefulState.GoingToSleep(persistenceFut @ _, shardPromise) =>
            unlikelyIncompleteShdnCounter.inc()
            WakeUpOutcome.IncompleteActorShutdown(shardPromise.future)

          // Try to perform the action
          case WakefulState.Awake =>
            inMemoryActorList.update(id)
            withLiveActorRef(actorRef) // No lock needed because the actor cannot be shutting down
            WakeUpOutcome.AlreadyAwake

          // Impossible - the `updateAndGet` above rules this case out
          case _: WakefulState.ConsideringSleep =>
            log.error("wakeUpActor: unexpectedly still in ConsideringSleep state")
            WakeUpOutcome.IncompleteActorShutdown(Future.unit)
        }

      // The node is not awake at all
      case None if canCreateNewNodes =>
        val costToSleep = new AtomicLong(0L)
        val wakefulState = new AtomicReference[WakefulState](WakefulState.Awake)
        val actorRefLock = new StampedLock()
        val props = Props(
          graph.nodeClass,
          id,
          graph,
          costToSleep,
          wakefulState,
          actorRefLock,
          snapshotBytesOpt
        ).withMailbox("akka.quine.node-mailbox")
          .withDispatcher("akka.quine.node-dispatcher")

        // Must be in a try because Akka may not have finished freeing the name even if the actor is shut down.
        try {
          val actorRef = context.actorOf(props, name = id.toInternalString)
          withLiveActorRef(actorRef) // No lock needed because the actor cannot be shutting down
          nodes(id) = NodeState(costToSleep, actorRef, actorRefLock, wakefulState)
          inMemoryActorList.update(id)
          nodesWokenUpCounter.inc()
          WakeUpOutcome.Awoken
        } catch {
          case error: InvalidActorNameException =>
            unlikelyActorNameRsvdCounter.inc()
            WakeUpOutcome.ActorNameStillReserved(error)

          case NonFatal(error) =>
            unlikelyUnexpectedWakeUpErrCounter.inc()
            WakeUpOutcome.UnexpectedWakeUpError(error)
        }

      // The node is not awake, and we cannot make new actors!
      case None /* if !canCreateNewNodes */ =>
        unlikelyHardLimitReachedCounter.inc()
        WakeUpOutcome.InMemoryNodeCountHardLimitReached
    }
  }

  /** Instruct a node to go to sleep.
    *
    * @note this can fail, see [[WakefulState]] for transitions out of [[ConsideringSleep]]
    * @param target the node/edge being told to sleep
    */
  private def sleepActor(target: QuineIdAtTime): Unit =
    nodes.get(target) match {
      case Some(NodeState(_, actorRef, _, state)) =>
        // Start/extend a deadline if the node isn't already going to sleep
        val previous = state.getAndUpdate {
          case WakefulState.Awake | _: WakefulState.ConsideringSleep =>
            WakefulState.ConsideringSleep(GraphShardActor.SleepDeadlineDelay.fromNow)
          case goingToSleep: WakefulState.GoingToSleep => goingToSleep
        }

        // If the node was not already considering sleep, tell it to
        if (previous == WakefulState.Awake) {
          log.debug("sleepActor: sent GoToSleep request to {}", target)
          actorRef ! GoToSleep
        } else {
          log.debug("sleepActor: {} is already {}", target, previous)
        }

      case None =>
        log.warning("sleepActor: cannot find actor for {}", target)
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
      entry.wakefulState.get() match {
        case WakefulState.Awake => nodesAwake += 1
        case _: WakefulState.ConsideringSleep => nodesAskedToSleep += 1
        case _: WakefulState.GoingToSleep => nodesSleeping += 1
      }

    ShardStats(nodesAwake, nodesAskedToSleep, nodesSleeping)
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
  ): Unit =
    wakeUpActor(qid, withLiveActorRef = _.tell(message, originalSender)) match {
      case WakeUpOutcome.AlreadyAwake | WakeUpOutcome.Awoken => ()

      // For some reason, the node was not instantly available
      case _ =>
        val envelope = Envelope(message, originalSender, system)
        NodeActorMailboxExtension(system).enqueueIntoMessageQueue(qid, self, envelope)
    }

  def receive: Receive = {

    case s @ SampleAwakeNodes(limitOpt, atTime, _) =>
      val toTake = limitOpt.getOrElse(Int.MaxValue)
      val sampled = if (toTake <= 0) {
        Nil
      } else if (inMemoryLimit.isEmpty) {
        nodes.keys.iterator
          .collect { case QuineIdAtTime(qid, t) if t == atTime => AwakeNode(qid) }
          .take(toTake)
          .toVector
      } else {
        val lastN = collection.mutable.Queue.empty[AwakeNode]
        for (qidAtTime <- inMemoryActorList.iterator) {
          val QuineIdAtTime(qid, t) = qidAtTime
          if (t == atTime) {
            lastN.enqueue(AwakeNode(qid))
            if (lastN.size > toTake) {
              lastN.dequeue()
            }
          }
        }
        lastN.toList
      }
      s ?! Source(sampled)

    case DeliveryRelay(msg, dedupId, needsAck) =>
      if (needsAck) sender() ! Ack
      Option(msgDedupCache.put(dedupId, None)) match { // `.put` returns `null` if key is not present
        case None => this.receive(msg) // Not a duplicate
        case Some(_) => () // It is a duplicate. Ignore.
      }

    case LocalMessageDelivery(msg, target, originalSender) =>
      // Note: This does nothing with the sender of this `LocalMessageDelivery`
      deliverLocalMessage(msg, target, originalSender)

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
      if (nodes.get(id).exists(_.wakefulState.get() == WakefulState.Awake)) {
        inMemoryActorList.update(id)
      }

    // Actor shut down completely
    case SleepOutcome.SleepSuccess(id, shardPromise) =>
      log.debug("Sleep succeeded for {}", id)
      nodes -= id
      inMemoryActorList.remove(id)
      nodesSleptSuccessCounter.inc()
      val promiseCompletedUniquely = shardPromise.trySuccess(())
      if (!promiseCompletedUniquely) { // Promise was already completed -- log an appropriate message
        shardPromise.future.value.get match {
          case Success(_) =>
            log.info("Received redundant notification about successfully slept node: {}", id.id)
          case Failure(_) =>
            log.error(
              """Received notification that node: {} slept, but that node already reported a failure for the same sleep request""",
              id.id
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
        "Failed to store {}'s {} bytes, composed of {} edges and {} properties. Restoring the node.",
        id,
        snapshot.length,
        numEdges,
        propertySizes.size
      )
      if (log.isWarningEnabled)
        log.warning(
          "Property sizes on failed store {}: {}",
          id,
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
              id.id
            )
          case Failure(e) =>
            log.warning(
              s"A node failed to sleep, and reported that failure multiple times: {}. Latest error was {}",
              id.id,
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
      wakeUpActor(id, snapshotBytesOpt = snapshotOpt) match {
        // Success
        case WakeUpOutcome.AlreadyAwake | WakeUpOutcome.Awoken => ()
        // Failure and out of retry attempts
        case badOutcome if remaining <= 0 =>
          unlikelyWakeupFailed.inc()
          val stats = shardStats()
          if (log.isWarningEnabled) {
            log.warning(
              s"No more retries waking up $id " +
              s"with sleep status ${nodes.get(id)} " +
              s"with nodes-on-shard: ${stats.awake} awake, ${stats.goingToSleep} going to sleep " +
              s"Outcome: $badOutcome " +
              s"Errors: " + errorCount.toList.map { case (k, v) => s"$k: $v" }.mkString(", ")
            )
          }
          throw new NodeWakeupFailedException(s"Failed to wake node: ${id.debug} after exhausting retries.")

        // Retry because the actor name should (hopefully) be freed by then.
        case _: WakeUpOutcome.ActorNameStillReserved =>
          implicit val ec = context.dispatcher
          val eKey = WakeUpErrorStates.ActorNameStillReserved
          val newErrorCount = errorCount.updated(eKey, errorCount.getOrElse(eKey, 0) + 1)
          val msgToDeliver = WakeUp(id, snapshotOpt, remaining - 1, newErrorCount)
          LocalMessageDelivery.slidingDelay(remaining) match {
            case None => self ! msgToDeliver
            case Some(delay) =>
              context.system.scheduler.scheduleOnce(delay)(self ! msgToDeliver)
              ()
          }

        // Retry because the actor name should (hopefully) be freed by then.
        case _: WakeUpOutcome.UnexpectedWakeUpError =>
          implicit val ec = context.dispatcher
          val eKey = WakeUpErrorStates.UnexpectedWakeUpError
          val newErrorCount = errorCount.updated(eKey, errorCount.getOrElse(eKey, 0) + 1)
          val msgToDeliver = WakeUp(id, snapshotOpt, remaining - 1, newErrorCount)
          LocalMessageDelivery.slidingDelay(remaining) match {
            case None => self ! msgToDeliver
            case Some(delay) =>
              context.system.scheduler.scheduleOnce(delay)(self ! msgToDeliver)
              ()
          }

        // Wait until the node is done shutting down before we retry
        case WakeUpOutcome.IncompleteActorShutdown(nodeRemovedFromMaps) =>
          implicit val ec = context.dispatcher
          val eKey = WakeUpErrorStates.IncompleteActorShutdown
          val newErrorCount = errorCount.updated(eKey, errorCount.getOrElse(eKey, 0) + 1)
          val msgToDeliver = WakeUp(id, snapshotOpt, remaining - 1, newErrorCount)

          nodeRemovedFromMaps.onComplete { _ =>
            self ! msgToDeliver
          }
          ()

        // Retry in some fixed time
        case WakeUpOutcome.InMemoryNodeCountHardLimitReached =>
          implicit val ec = context.dispatcher
          val eKey = WakeUpErrorStates.InMemoryNodeCountHardLimitReached
          val newErrorCount = errorCount.updated(eKey, errorCount.getOrElse(eKey, 0) + 1)
          val msgToDeliver = WakeUp(id, snapshotOpt, remaining - 1, newErrorCount)
          // TODO: don't hardcode the time until retry
          log.warning("Failed to wake up {} due to hard in-memory limit {} (retrying)", id, inMemoryLimit)
          context.system.scheduler.scheduleOnce(0.01 second)(self ! msgToDeliver)
          // TODO: This will cause _more_ memory usage because the mailbox will fill up with all these undelivered messages.
          ()
      }

    case s @ SnapshotInMemoryNodes(_) =>
      implicit val timeout = Timeout(10 minutes)
      implicit val ec = context.dispatcher

      /* TODO: Since `SaveSnapshot` is one of the message types we discard when
       * cleaning up a node mailbox, we can't differentiate an ask timeout (due
       * to the node being too busy or crashing) from a timeout due to the node
       * sleeping first. For now, we assume a timeout can be ignored.
       */
      val toSnapshotFutures = nodes.iterator
        .collect[Future[Option[(QuineId, Throwable)]]] { case (QuineIdAtTime(id, None), ns) =>
          (ns.actorRef ? SaveSnapshot)
            .mapTo[Future[Unit]]
            .recover { case _: AskTimeoutException =>
              Future.unit
            } // this timeout is most likel the node going to sleep
            .flatten
            .transform {
              case Success(()) => Success(None)
              case Failure(err) => Success(Some(id -> err))
            }
        }
        .toVector

      Future
        .foldLeft(toSnapshotFutures)(Map.empty[QuineId, Throwable]) { (map, outcome) =>
          outcome match {
            case None => map
            case Some((id, err)) => map + (id -> err)
          }
        }
        .onComplete {
          case Success(failureMap) =>
            s ?! SnapshotSucceeded(failureMap)

          case Failure(err) =>
            s ?! SnapshotFailed(shardId, err)
        }

    case msg @ RemoveNodesIf(LocalPredicate(predicate), _) =>
      for ((nodeId, nodeState) <- nodes; if predicate(nodeId)) {
        nodes.remove(nodeId)
        nodesRemovedCounter.inc()
        context.stop(nodeState.actorRef)
        inMemoryActorList.remove(nodeId)
      }
      msg ?! Done

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
  def name(shardId: Int): String = s"shard-$shardId"

  /** How long is the delay before a node accepts sleep? */
  val SleepDeadlineDelay: FiniteDuration = 3.seconds

  /** Possible outcomes when trying to wake up a node */
  sealed abstract private class WakeUpOutcome
  private object WakeUpOutcome {
    case object AlreadyAwake extends WakeUpOutcome
    case object Awoken extends WakeUpOutcome

    /** @param shardNodesUpdated Future tracking when the shard has removed the node from its nodes map
      */
    final case class IncompleteActorShutdown(shardNodesUpdated: Future[Unit]) extends WakeUpOutcome
    case object InMemoryNodeCountHardLimitReached extends WakeUpOutcome
    final case class ActorNameStillReserved(underlying: InvalidActorNameException) extends WakeUpOutcome
    final case class UnexpectedWakeUpError(error: Throwable) extends WakeUpOutcome
  }

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
    * @param costToSleep measure of how costly it is to sleep the node
    * @param actorRef Akka reference for sending to the actor
    * @param actorRefLock lock to ensure the liveness of the actor behind `actorRef`
    * @param wakefulState where is the node at in the sleep cycle?
    */
  final private[quine] case class NodeState(
    costToSleep: AtomicLong,
    actorRef: ActorRef,
    actorRefLock: StampedLock,
    wakefulState: AtomicReference[WakefulState]
  )
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
 *  - when the persistor future in [[GoingToSleep]] completes, a [[SleepOutcome]] message is sent to the shard carrying
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
  final case class GoingToSleep(persistor: Future[Unit], shard: Promise[Unit]) extends WakefulState
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

/** Message sent by a shard to one of the nodes it owns telling it to save a
  * snapshot of its state to disk. Response should be a `Future[Unit]`
  */
private[quine] case object SaveSnapshot extends NodeControlMessage

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
