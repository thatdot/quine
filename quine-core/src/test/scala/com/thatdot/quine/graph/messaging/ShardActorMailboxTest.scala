package com.thatdot.quine.graph.messaging

import scala.concurrent.Promise

import org.apache.pekko.actor.{ActorRef, ActorSystem}
import org.apache.pekko.dispatch.{
  Envelope,
  MessageQueue,
  MultipleConsumerSemantics,
  UnboundedMessageQueueSemantics,
  UnboundedStablePriorityMailbox,
}

import com.codahale.metrics.Timer
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.{SleepOutcome, defaultNamespaceId}

/** The shard mailbox is measured by summing queue depth when the gauge is read, rather than by
  * counting on the messaging path, which means the queue it hands out is Pekko's own object. These
  * tests pin that: the queue's identity, and the priority contract the shard depends on.
  */
class ShardActorMailboxTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val system: ActorSystem = ActorSystem("shard-actor-mailbox-test")

  override def afterAll(): Unit = {
    system.terminate()
    ()
  }

  private def newQueue(): MessageQueue =
    new ShardActorMailbox(system.settings, ConfigFactory.empty()).create(None, Some(system))

  private def envelope(message: Any): Envelope = Envelope(message, ActorRef.noSender, system)

  private def drain(queue: MessageQueue): List[Any] = {
    val drained = List.newBuilder[Any]
    var next = queue.dequeue()
    while (next ne null) {
      drained += next.message
      next = queue.dequeue()
    }
    drained.result()
  }

  private def sleepOutcome: SleepOutcome = SleepOutcome.SleepSuccess(
    SpaceTimeQuineId(QuineId(Array[Byte](1)), defaultNamespaceId, None),
    Promise[Unit](),
    new Timer().time(),
  )

  private def relay(needsAck: Boolean): BaseMessage.DeliveryRelay =
    BaseMessage.DeliveryRelay(BaseMessage.Ack, 1L, needsAck)

  "the shard mailbox" should "hand out the queue Pekko builds, not a wrapper" in {
    // If this ever fails, something has been interposed on the messaging hot path.
    newQueue() shouldBe a[UnboundedStablePriorityMailbox.MessageQueue]
  }

  it should "produce a queue carrying unbounded, multiple-consumer semantics" in {
    // Pekko validates these markers against any `RequiresMessageQueue` actor or dispatcher
    // `mailbox-requirement`. Losing them fails actor creation at startup, far from the cause.
    newQueue() shouldBe a[UnboundedMessageQueueSemantics]
    newQueue() shouldBe a[MultipleConsumerSemantics]
  }

  it should "hand back sleep outcomes first, then acked relays, then everything else" in {
    val queue = newQueue()
    val sleep = sleepOutcome
    val ackedRelay = relay(needsAck = true)
    val unackedRelay = relay(needsAck = false)

    // Enqueued worst-priority-first, so ordering cannot pass by accident of insertion order
    List[Any]("ordinary", unackedRelay, ackedRelay, sleep).foreach(m => queue.enqueue(ActorRef.noSender, envelope(m)))

    drain(queue) shouldBe List[Any](sleep, ackedRelay, "ordinary", unackedRelay)
  }

  it should "preserve insertion order among messages of equal priority" in {
    val queue = newQueue()
    val ordinary = (0 until 32).map(i => s"message-$i").toList
    ordinary.foreach(m => queue.enqueue(ActorRef.noSender, envelope(m)))

    drain(queue) shouldBe ordinary
  }

  it should "not let an acked relay overtake an earlier acked relay" in {
    val queue = newQueue()
    val relays = (0 until 16).map(i => BaseMessage.DeliveryRelay(BaseMessage.Ack, i.toLong, true)).toList
    relays.foreach(m => queue.enqueue(ActorRef.noSender, envelope(m)))

    drain(queue) shouldBe relays
  }

  "the depth gauge" should "follow what is actually queued" in {
    val queue = newQueue()
    // Relative to a baseline: the registry of live queues is JVM-wide, so other tests may hold some
    val baseline = ShardActorMailbox.queuedMessages

    (0 until 3).foreach(i => queue.enqueue(ActorRef.noSender, envelope(s"message-$i")))
    ShardActorMailbox.queuedMessages shouldBe baseline + 3

    drain(queue)
    ShardActorMailbox.queuedMessages shouldBe baseline
  }
}
