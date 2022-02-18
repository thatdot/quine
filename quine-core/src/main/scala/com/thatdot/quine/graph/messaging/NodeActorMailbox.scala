package com.thatdot.quine.graph.messaging

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Comparator, Queue}

import akka.actor._
import akka.dispatch._
import akka.util.StablePriorityBlockingQueue

import com.codahale.metrics.{MetricRegistry, SharedMetricRegistries}
import com.typesafe.config.Config

import com.thatdot.quine.graph.behavior.StashedMessage
import com.thatdot.quine.graph.messaging.StandingQueryMessage.CancelDomainNodeSubscription
import com.thatdot.quine.graph.{
  BinaryHistogramCounter,
  GoToSleep,
  HostQuineMetrics,
  ProcessMessages,
  SaveSnapshot,
  WakeUp
}

/** Mailbox used for node actors
  *
  * This mailbox is like `UnboundedStablePriorityMailbox`, but:
  *
  *   - the underlying message queue is [[MessageQueue]]
  *   - the comparator for the priority is hard-coded to what a node expects
  */
final class NodeActorMailbox(settings: ActorSystem.Settings, config: Config)
    extends MailboxType
    with ProducesMessageQueue[NodeActorMailbox.NodeMessageQueue] {

  // Note: as long as we do _not_ use a balancing dispatcher, `owner` and `system` will be defined
  override def create(owner: Option[ActorRef], system: Option[ActorSystem]): MessageQueue = {
    // Turn the actor name back into a node ID (we always create nodes with their ID in the name)
    val path: ActorPath = owner.get.path
    val qidAtTime = QuineIdAtTime.fromInternalString(path.name)

    // Fetch back out an existing message queue or else create a new one
    NodeActorMailboxExtension(system.get).getOrCreateMessageQueue(qidAtTime)
  }
}
object NodeActorMailbox {

  // Priority for node messages
  private val cmp: Comparator[Envelope] = PriorityGenerator(
    StashedMessage.priority { // Lower priority is handled first
      case GoToSleep => 1
      case _ => 0
    }
  )

  /** Which messages should be discarded if the node is sleeping or going to
    * sleep (instead of waking it back up)
    *
    * @param msg message received
    * @return whether the message should be ignored
    */
  def shouldIgnoreWhenSleeping(msg: Any): Boolean = msg match {
    case GoToSleep | SaveSnapshot =>
      // redundant messages when already asleep
      true
    case ProcessMessages | BaseMessage.Ack | StandingQueryMessage.UpdateStandingQueriesNoWake =>
      // messages whose semantics define that waking is undesirable
      true
    case CancelDomainNodeSubscription(_, _, _) =>
      // optimization: nodes will proactively check for cancellations they need to perform on wakeup anyway
      // note that MultipleValues standing queries will still cause node wakes via CancelCypherSubscription()
      // but the logic that *sends* CancelCypherSubscription has a similar optimization
      true
    case _ => false
  }

  /** Message queue for node actors
    *
    * This queue is like the `UnboundedStablePriorityMailbox.MessageQueue`, but
    * doesn't drain letters on cleanup.
    */
  final class NodeMessageQueue(mailboxSizeHistogram: BinaryHistogramCounter)
      extends StablePriorityBlockingQueue[Envelope](capacity = 11, cmp)
      with QueueBasedMessageQueue
      with UnboundedMessageQueueSemantics {
    private[this] val sizeCounter = new AtomicInteger()

    def queue: Queue[Envelope] = this

    // Normally, this just adds to the queue. We track mailbox size along the way.
    def enqueue(receiver: ActorRef, handle: Envelope): Unit = {
      if (handle != null) mailboxSizeHistogram.increment(sizeCounter.getAndIncrement)
      queue.add(handle)
      ()
    }

    // Normally, this just removes from the queue. We track mailbox size along the way.
    def dequeue(): Envelope = {
      val handle = queue.poll()
      if (handle != null) mailboxSizeHistogram.decrement(sizeCounter.getAndDecrement)
      handle
    }

    // Normally, `cleanUp` would drain remaining messages to dead letters - we don't want that
    override def cleanUp(owner: ActorRef, deadLetters: MessageQueue): Unit = {
      queue.removeIf(shouldIgnoreWhenSleeping(_))
      ()
    }
  }
}

object NodeActorMailboxExtension extends ExtensionId[NodeActorMailboxExtensionImpl] with ExtensionIdProvider {
  override def lookup = NodeActorMailboxExtension
  override def createExtension(system: ExtendedActorSystem) = new NodeActorMailboxExtensionImpl
}

/** This actor system extension stores the actorsystem-global concurrent map of
  * node mailboxes
  *
  * @see NodeActorMailboxExtension
  */
final class NodeActorMailboxExtensionImpl extends Extension {

  /** Map of all of the message queues for nodes that are awake (or about to be
    * awake, or just stopped being awake).
    *
    * We almost always want to guard access to values behind a concurrent hash
    * map `compute` lock. We want to avoid racing `removeMessageQueueIfEmpty`
    * and `enqueueIntoMessageQueue` and we do this by putting both of those
    * behind the CHM's `compute` write lock.
    */
  val messageQueues =
    new ConcurrentHashMap[QuineIdAtTime, NodeActorMailbox.NodeMessageQueue]()

  /** Histogram of the mailbox sizes */
  val mailboxSizes: BinaryHistogramCounter = BinaryHistogramCounter(
    SharedMetricRegistries.getOrCreate(HostQuineMetrics.MetricsRegistryName),
    MetricRegistry.name("node", "mailbox-sizes")
  )

  /** Find the message queue for a node. If that queue doesn't exist, create a
    * fresh queue
    *
    * @param qidAtTime node for which we want the queue
    * @return message queue for the node
    */
  def getOrCreateMessageQueue(qidAtTime: QuineIdAtTime): NodeActorMailbox.NodeMessageQueue =
    messageQueues.computeIfAbsent(
      qidAtTime,
      (_: QuineIdAtTime) => new NodeActorMailbox.NodeMessageQueue(mailboxSizes)
    )

  /** Removes the message queue for a node if that queue is empty
    *
    * @note does nothing if the message queue is already absent and returns success
    * @param qid node for which we should remove the queue
    * @return if the removal succeeded (failure indicates a non-empty queue)
    */
  def removeMessageQueueIfEmpty(qidAtTime: QuineIdAtTime): Boolean = {
    // `compute` prevents concurrent lookups of the queue while we check if it is empty
    val updatedQueue = messageQueues.compute(
      qidAtTime,
      (_: QuineIdAtTime, queue: NodeActorMailbox.NodeMessageQueue) =>
        if ((queue eq null) || queue.hasMessages) queue else null
    )
    updatedQueue eq null
  }

  /** Gets or creates a message queue for a node and inserts the given message
    * into it.
    *
    * @note this ignores some messages, see [[NodeActorMailbox.shouldIgnoreWhenSleeping]]
    * @param qid node for which we should enqueue a message
    * @param envelope message (and sender) to enqueue
    * @return whether the message was enqueued (else it was ignored)
    */
  @inline
  private def enqueueIntoMessageQueue(qidAtTime: QuineIdAtTime, envelope: Envelope): Boolean =
    if (NodeActorMailbox.shouldIgnoreWhenSleeping(envelope.message)) {
      false
    } else {

      // `compute` prevents concurrent removal of the queue while we insert into it
      messageQueues.compute(
        qidAtTime,
        (_: QuineIdAtTime, queue: NodeActorMailbox.NodeMessageQueue) => {
          val newQueue = if (queue eq null) {
            new NodeActorMailbox.NodeMessageQueue(mailboxSizes)
          } else {
            queue
          }
          newQueue.enqueue(null, envelope)
          newQueue
        }
      )
      true
    }

  /** Gets or creates a message queue for a node and inserts the given message
    * into it, then sends a message to a shard to wake up the node.
    *
    * @note this ignores some messages, see [[NodeActorMailbox.shouldIgnoreWhenSleeping]]
    * @param qid node for which we should enqueue a message
    * @param shardRef address of the shard to which the node belongs
    * @param envelope message (and sender) to enqueue
    */
  def enqueueIntoMessageQueue(qidAtTime: QuineIdAtTime, shardRef: ActorRef, envelope: Envelope): Unit =
    if (enqueueIntoMessageQueue(qidAtTime, envelope)) {
      // Only wake up the node if a message was enqueued
      shardRef.tell(WakeUp(qidAtTime), ActorRef.noSender)
    }
}
