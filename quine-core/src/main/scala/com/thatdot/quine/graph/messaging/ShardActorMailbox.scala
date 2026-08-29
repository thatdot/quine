package com.thatdot.quine.graph.messaging

import java.lang.ref.WeakReference
import java.util.concurrent.ConcurrentHashMap

import org.apache.pekko.actor.{ActorRef, ActorSystem}
import org.apache.pekko.dispatch.{
  MailboxType,
  MessageQueue,
  PriorityGenerator,
  ProducesMessageQueue,
  UnboundedStablePriorityMailbox,
}

import com.codahale.metrics.{Gauge, MetricRegistry, SharedMetricRegistries}
import com.typesafe.config.Config

import com.thatdot.quine.graph.SleepOutcome
import com.thatdot.quine.graph.metrics.HostQuineMetrics

/** Mailbox used for shard actors
  *
  * The queue handed back is exactly the one [[UnboundedStablePriorityMailbox]] builds, unwrapped,
  * so its priority, stability, unbounded and multiple-consumer semantics are Pekko's own and the
  * enqueue/dequeue path has nothing added to it. All this class does beyond choosing the comparator
  * is record the queue so its depth can be read later; see [[ShardActorMailbox.queuedMessages]].
  *
  * It cannot extend [[UnboundedStablePriorityMailbox]] directly, because that class's `create` is
  * final and the queue has to be recorded as it is created.
  */
class ShardActorMailbox(settings: ActorSystem.Settings, config: Config)
    extends MailboxType
    with ProducesMessageQueue[UnboundedStablePriorityMailbox.MessageQueue] {

  private val underlying = new UnboundedStablePriorityMailbox(
    PriorityGenerator { // Lower priority is handled first
      case _: SleepOutcome => 0
      case BaseMessage.DeliveryRelay(_, _, true) => 1 // needsAck == true
      case _ => 2
    },
  )

  def create(owner: Option[ActorRef], system: Option[ActorSystem]): MessageQueue = {
    val queue = underlying.create(owner, system)
    ShardActorMailbox.register(queue)
    queue
  }
}

object ShardActorMailbox {

  /** Every shard mailbox queue built in this JVM.
    *
    * Weakly held, so a queue whose shard is gone can still be collected; cleared entries are pruned
    * the next time the depth is read. There are only a few dozen shards per host, so this stays small.
    */
  private val liveQueues = ConcurrentHashMap.newKeySet[WeakReference[MessageQueue]]()

  private def register(queue: MessageQueue): Unit = {
    liveQueues.add(new WeakReference(queue))
    ()
  }

  /** Messages sitting in shard mailboxes on this host, summed across shards.
    *
    * Shard mailboxes are the one queue in the messaging path with no visibility: `node.mailbox-sizes`
    * measures NODE actor mailboxes, which are a different queue entirely, so a shard backlog reads as
    * "no mailbox pressure" there. It matters because a shard actor is a single actor that both routes
    * per-record ingest traffic and services the wake requests a query needs, and because this mailbox
    * is a PRIORITY queue: cross-member `DeliveryRelay(needsAck = true)` outranks everything except
    * sleep outcomes, so sustained relay volume can in principle hold lower-priority work behind it.
    *
    * Summed on read rather than counted per message, so that measuring this costs the messaging path
    * nothing at all. Each `numberOfMessages` is a queue `size`, and the gauge below is pull-based, so
    * the whole cost is a few dozen `size` calls per scrape.
    */
  def queuedMessages: Long = {
    var total = 0L
    val queues = liveQueues.iterator()
    while (queues.hasNext) {
      val queue = queues.next().get()
      if (queue eq null) queues.remove() else total += queue.numberOfMessages.toLong
    }
    total
  }

  /** A gauge rather than a histogram because the question is "is there a standing backlog", not "how
    * are individual queues distributed": there are only a few dozen shards per host.
    */
  SharedMetricRegistries
    .getOrCreate(HostQuineMetrics.MetricsRegistryName)
    .gauge[Gauge[Long]](
      MetricRegistry.name("shard", "mailbox-queued"),
      () => new Gauge[Long] { def getValue: Long = queuedMessages },
    )
}
