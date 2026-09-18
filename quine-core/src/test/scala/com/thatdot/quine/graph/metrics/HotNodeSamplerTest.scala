package com.thatdot.quine.graph.metrics

import scala.jdk.CollectionConverters._
import scala.reflect.{ClassTag, classTag}
import scala.util.Try

import org.apache.pekko.actor.{ActorRef, ActorSystem}
import org.apache.pekko.dispatch.Envelope

import com.codahale.metrics.{Gauge, MetricRegistry, SharedMetricRegistries}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import com.thatdot.quine.graph.behavior.StashedMessage
import com.thatdot.quine.graph.messaging.{NodeActorMailboxExtension, NodeActorMailboxExtensionImpl, SpaceTimeQuineId}
import com.thatdot.quine.graph.{QuineIdLongProvider, defaultNamespaceId}
import com.thatdot.quine.model.{Milliseconds, QuineIdProvider}
import com.thatdot.quine.util.TestLogging._

/** The sampler ranks node mailboxes three ways over the sample window, by peak depth, by message rate, and
  * by estimated wait (peak depth divided by rate), and publishes a backlog gauge and a rate gauge for every
  * node in any ranking, named by the node's pretty ID. It keeps the set bounded by removing the gauges of
  * nodes that are no longer ranked. These tests pin the naming, the three rankings, the bound, the removal,
  * the peak semantics, and that stash re-deliveries are not counted as messages.
  */
class HotNodeSamplerTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  private val system: ActorSystem = ActorSystem("hot-node-sampler-test")
  private val idProvider = QuineIdLongProvider()

  /** A generous window, so a handful of messages never crosses a rate threshold by accident */
  private val Window = 10.0

  override def afterAll(): Unit = {
    system.terminate()
    ()
  }

  // The mailbox map is per actor system, so start every test from an empty one
  override def beforeEach(): Unit = extension.messageQueues.keySet.forEach(drainAndRemove(_))

  private def extension: NodeActorMailboxExtensionImpl = NodeActorMailboxExtension(system)

  private def qid(n: Long, atTime: Option[Milliseconds] = None): SpaceTimeQuineId =
    SpaceTimeQuineId(idProvider.customIdToQid(n), defaultNamespaceId, atTime)

  private def enqueue(id: SpaceTimeQuineId, count: Int): Unit =
    (0 until count).foreach { i =>
      extension.enqueueIntoMessageQueue(id, Envelope(s"message-$i", ActorRef.noSender, system))
    }

  private def dequeue(id: SpaceTimeQuineId, count: Int): Unit = {
    val queue = extension.getOrCreateMessageQueue(id)
    (0 until count).foreach(_ => queue.dequeue())
  }

  /** Empty a node's queue and drop it from the extension, as a slept node does. */
  private def drainAndRemove(id: SpaceTimeQuineId): Unit = {
    val queue = extension.getOrCreateMessageQueue(id)
    while (queue.dequeue() ne null) ()
    extension.removeMessageQueueIfEmpty(id) shouldBe true
    ()
  }

  private def newSampler(
    topN: Int = 10,
    minBacklog: Int = 1,
    minMessageRate: Double = 1000.0,
    minWaitSeconds: Double = 1e9,
    enabled: Boolean = true,
    provider: QuineIdProvider = idProvider,
  ): (HotNodeSampler, MetricRegistry) = {
    val registry = new MetricRegistry
    val metrics = HostQuineMetrics(enableDebugMetrics = false, registry, omitDefaultNamespace = false)
    val config = HotNodeMetricsConfig(
      enabled = enabled,
      topN = topN,
      minBacklog = minBacklog,
      minMessageRate = minMessageRate,
      minWaitSeconds = minWaitSeconds,
    )
    (new HotNodeSampler(extension.messageQueues, metrics, provider, config), registry)
  }

  private def gauges[T](registry: MetricRegistry, family: String): Map[String, T] =
    registry.getGauges.asScala.collect {
      case (name, gauge: Gauge[_]) if name.contains(family) => name -> gauge.getValue.asInstanceOf[T]
    }.toMap

  private def backlogGauges(registry: MetricRegistry): Map[String, Long] = gauges[Long](registry, "mailbox-backlog")
  private def rateGauges(registry: MetricRegistry): Map[String, Double] = gauges[Double](registry, "message-rate")

  private def backlogName(id: Long): String = s"quine.node.mailbox-backlog.$id"
  private def rateName(id: Long): String = s"quine.node.message-rate.$id"

  "the backlog ranking" should "register a gauge for each of the deepest queues, up to top-n" in {
    enqueue(qid(11), 5)
    enqueue(qid(12), 3)
    enqueue(qid(13), 1)
    enqueue(qid(14), 0)
    val (sampler, registry) = newSampler(topN = 2)

    sampler.sample(Window)

    backlogGauges(registry) shouldBe Map(backlogName(11) -> 5L, backlogName(12) -> 3L)
  }

  it should "ignore queues shallower than min-backlog" in {
    enqueue(qid(21), 5)
    enqueue(qid(22), 3)
    val (sampler, registry) = newSampler(minBacklog = 4)

    sampler.sample(Window)

    backlogGauges(registry) shouldBe Map(backlogName(21) -> 5L)
  }

  it should "remove gauges for queues that fell out of the top-n and add new entrants" in {
    enqueue(qid(31), 5)
    enqueue(qid(32), 3)
    enqueue(qid(33), 1)
    val (sampler, registry) = newSampler(topN = 2)
    sampler.sample(Window)
    backlogGauges(registry).keySet shouldBe Set(backlogName(31), backlogName(32))

    drainAndRemove(qid(31))
    enqueue(qid(34), 4)
    sampler.sample(Window)

    backlogGauges(registry) shouldBe Map(backlogName(34) -> 4L, backlogName(32) -> 3L)
    rateGauges(registry).keySet shouldBe Set(rateName(34), rateName(32))
  }

  it should "report the peak depth reached since the previous sample, not the depth at sampling time" in {
    // A hot node's queue is a sawtooth: it fills in a burst and drains in milliseconds. The peak is what
    // makes it hot; the instantaneous depth at sampling time is a coin toss.
    enqueue(qid(41), 6)
    dequeue(qid(41), 5)
    extension.getOrCreateMessageQueue(qid(41)).currentSize shouldBe 1
    val (sampler, registry) = newSampler()

    sampler.sample(Window)
    backlogGauges(registry)(backlogName(41)) shouldBe 6L

    // Between samples the value stays what the ranking saw, so a row and its number always agree
    enqueue(qid(41), 2)
    backlogGauges(registry)(backlogName(41)) shouldBe 6L

    // The next window starts from the depth at the previous sample, so the peak resets
    sampler.sample(Window)
    backlogGauges(registry)(backlogName(41)) shouldBe 3L
  }

  "the rate ranking" should "rank nodes by messages received per second over the window" in {
    enqueue(qid(51), 300)
    enqueue(qid(52), 30)
    enqueue(qid(53), 3)
    val (sampler, registry) = newSampler(topN = 2, minBacklog = 1000, minMessageRate = 1.0)

    sampler.sample(Window)

    rateGauges(registry) shouldBe Map(rateName(51) -> 30.0, rateName(52) -> 3.0)
  }

  it should "count messages received in the window, not the depth, and reset each sample" in {
    enqueue(qid(61), 40)
    dequeue(qid(61), 40)
    val (sampler, registry) = newSampler(minBacklog = 1000, minMessageRate = 1.0)

    sampler.sample(Window)
    rateGauges(registry)(rateName(61)) shouldBe 4.0

    sampler.sample(Window)
    rateGauges(registry) shouldBe empty
  }

  it should "not count stash re-deliveries as messages" in {
    val shared = SharedMetricRegistries.getOrCreate(HostQuineMetrics.MetricsRegistryName)
    val before = shared.counter("node.messages-received").getCount
    enqueue(qid(71), 3)
    (0 until 2).foreach { i =>
      extension.enqueueIntoMessageQueue(qid(71), Envelope(StashedMessage(s"again-$i"), ActorRef.noSender, system))
    }
    val (sampler, registry) = newSampler(minBacklog = 1000, minMessageRate = 0.1)

    sampler.sample(Window)

    rateGauges(registry)(rateName(71)) shouldBe 0.3
    backlogGauges(registry)(backlogName(71)) shouldBe 5L
    shared.counter("node.messages-received").getCount - before shouldBe 3L
  }

  "the wait ranking" should "surface a node falling behind at a modest rate that the other rankings miss" in {
    // Little's law: wait = backlog / arrival rate. Over a 10s window the hub takes 4,500 arrivals and peaks at
    // 5,000 queued (11s of wait); the slow node takes 10 arrivals and peaks at 110 queued (110s of wait).
    // With top-n = 1 the backlog ranking can only pick the hub and the rate threshold excludes both, so the
    // slow node is published only because the wait ranking chose it over the hub.
    enqueue(qid(121), 500)
    enqueue(qid(122), 100)
    val (sampler, registry) = newSampler(topN = 1, minBacklog = 10, minMessageRate = 1000.0, minWaitSeconds = 1.0)
    sampler.sample(Window) // opens the window with 500 and 100 already queued
    enqueue(qid(121), 4500)
    dequeue(qid(121), 4500)
    enqueue(qid(122), 10)
    dequeue(qid(122), 10)

    sampler.sample(Window)

    backlogGauges(registry) shouldBe Map(backlogName(121) -> 5000L, backlogName(122) -> 110L)
    rateGauges(registry)(rateName(122)) shouldBe 1.0
  }

  it should "treat a node holding messages it received before the window as stalled and rank it first" in {
    // No arrivals in the window floors the rate at one per window, so 20 queued reads as 200s of wait,
    // ahead of a busy node that peaks at 500 while taking 500 arrivals (10s)
    enqueue(qid(131), 20)
    enqueue(qid(132), 20)
    dequeue(qid(132), 20)
    val (sampler, registry) = newSampler(topN = 1, minBacklog = 10, minMessageRate = 1000.0, minWaitSeconds = 1.0)
    sampler.sample(Window)
    enqueue(qid(132), 500)
    dequeue(qid(132), 500)

    sampler.sample(Window)

    backlogGauges(registry) shouldBe Map(backlogName(132) -> 500L, backlogName(131) -> 20L)
    rateGauges(registry)(rateName(131)) shouldBe 0.0
  }

  it should "ignore a stray leftover message below min-backlog even though its wait is long" in {
    enqueue(qid(141), 1)
    val (sampler, registry) = newSampler(topN = 1, minBacklog = 10, minMessageRate = 1000.0, minWaitSeconds = 1.0)
    sampler.sample(Window)

    sampler.sample(Window)

    backlogGauges(registry) shouldBe empty
  }

  "the published set" should "carry both gauges for a node selected by only one ranking" in {
    // 81 is deep but slow, 82 is fast but never deep
    enqueue(qid(81), 150)
    (0 until 5).foreach { _ =>
      enqueue(qid(82), 50)
      dequeue(qid(82), 50)
    }
    val (sampler, registry) = newSampler(minBacklog = 100, minMessageRate = 1.0)

    sampler.sample(windowSeconds = 200.0)

    backlogGauges(registry) shouldBe Map(backlogName(81) -> 150L, backlogName(82) -> 50L)
    rateGauges(registry) shouldBe Map(rateName(81) -> 0.75, rateName(82) -> 1.25)
  }

  it should "name historical nodes with their at-time" in {
    enqueue(qid(91, Some(Milliseconds(123L))), 2)
    val (sampler, registry) = newSampler()

    sampler.sample(Window)

    backlogGauges(registry).keySet shouldBe Set("quine.node.mailbox-backlog.91-at-123")
    rateGauges(registry).keySet shouldBe Set("quine.node.message-rate.91-at-123")
  }

  it should "replace ID characters that would split or break the metric name" in {
    // A provider whose pretty IDs carry every character class the name must not contain
    object AwkwardIdProvider extends QuineIdProvider {
      type CustomIdType = String
      val customIdTag: ClassTag[String] = classTag[String]
      def newCustomId(): String = "unused"
      def hashedCustomId(bytes: Array[Byte]): String = "unused"
      def customIdToString(typed: String): String = typed
      def customIdFromString(str: String): Try[String] = Try(str)
      def customIdToBytes(typed: String): Array[Byte] = typed.getBytes("UTF-8")
      def customIdFromBytes(bytes: Array[Byte]): Try[String] = Try(new String(bytes, "UTF-8"))
    }
    val awkward = SpaceTimeQuineId(AwkwardIdProvider.customIdToQid("a b,c=d:e.f*g?h\"i"), defaultNamespaceId, None)
    enqueue(awkward, 2)
    val (sampler, registry) = newSampler(provider = AwkwardIdProvider)

    sampler.sample(Window)

    backlogGauges(registry).keySet shouldBe Set("quine.node.mailbox-backlog.a_b_c_d_e_f_g_h_i")
    drainAndRemove(awkward)
  }

  it should "remove every gauge on removeAll and leave other metrics alone" in {
    enqueue(qid(101), 4)
    enqueue(qid(102), 2)
    val (sampler, registry) = newSampler()
    registry.registerGauge("unrelated.gauge", () => 1)
    sampler.sample(Window)
    backlogGauges(registry).keySet shouldBe Set(backlogName(101), backlogName(102))
    rateGauges(registry).keySet shouldBe Set(rateName(101), rateName(102))

    sampler.removeAll()

    backlogGauges(registry) shouldBe empty
    rateGauges(registry) shouldBe empty
    registry.getGauges.containsKey("unrelated.gauge") shouldBe true
  }

  it should "register nothing and not schedule when disabled" in {
    enqueue(qid(111), 4)
    val (sampler, registry) = newSampler(enabled = false)

    sampler.sample(Window)
    val handle = sampler.start(system)

    backlogGauges(registry) shouldBe empty
    rateGauges(registry) shouldBe empty
    handle.isCancelled shouldBe true
  }
}
