package com.thatdot.quine.graph.edges

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, Promise}
import scala.util.Try

import org.apache.pekko.actor.ActorSystem

import cats.data.NonEmptyList
import com.codahale.metrics.MetricRegistry
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.NodeEvent.WithTime
import com.thatdot.quine.graph.metrics.BinaryHistogramCounter
import com.thatdot.quine.graph.{CostToSleep, EdgeEvent, EventTime, NodeChangeEvent, QuineIdLongProvider}
import com.thatdot.quine.model.{EdgeDirection, HalfEdge}
import com.thatdot.quine.util.TestLogging._

/** What each edge processor reports to `onBatchApplied`, which is the only path by which an edge event reaches
  * `journaledEventsAppliedSinceSnapshot` and therefore the only thing keeping the sleep-time threshold honest for a node
  * that writes edges.
  *
  * Driven against the processors directly rather than through a graph, because the two properties that matter are
  * invisible from outside: that a batch is counted once however many attempts its write takes, and that a write
  * which never succeeds is never counted at all. A graph-level test sees only the snapshot that results.
  */
class EdgeProcessorBatchCountingTest extends AnyFunSuite with BeforeAndAfterAll with Matchers {

  implicit val system: ActorSystem = ActorSystem("edge-processor-batch-counting")
  implicit val idProvider: QuineIdLongProvider = QuineIdLongProvider()

  override def afterAll(): Unit = { val _ = Await.result(system.terminate(), 10.seconds) }

  private val qid: QuineId = idProvider.customIdToQid(1L)
  private val registry = new MetricRegistry

  private def edges(n: Int): List[EdgeEvent] =
    (1 to n).toList.map(i =>
      EdgeEvent.EdgeAdded(HalfEdge(Symbol(s"e$i"), EdgeDirection.Outgoing, idProvider.customIdToQid((100 + i).toLong))),
    )

  private def counter(): BinaryHistogramCounter =
    BinaryHistogramCounter(registry, MetricRegistry.name("test", java.util.UUID.randomUUID().toString))

  private def costToSleep: CostToSleep = new AtomicLong(0L)

  private var tick: Long = 0L
  private def nextTime(): EventTime = { tick += 1; EventTime.fromRaw(tick) }

  /** A journal write that fails its first `failures` attempts and then succeeds. */
  private def flaky(failures: Int, attempts: AtomicInteger)(
    events: NonEmptyList[WithTime[EdgeEvent]],
  ): Future[Unit] = {
    val _ = events
    if (attempts.getAndIncrement() < failures) Future.failed(new RuntimeException("journal unavailable"))
    else Future.unit
  }

  private def memoryFirst(
    persist: NonEmptyList[WithTime[EdgeEvent]] => Future[Unit],
    onBatch: Int => Unit,
  ): MemoryFirstEdgeProcessor =
    new MemoryFirstEdgeProcessor(
      edges = new ReverseOrderedEdgeCollection(qid),
      persistToJournal = persist,
      onBatchApplied = onBatch,
      runPostActions = (_: List[NodeChangeEvent]) => (),
      qid = qid,
      costToSleep = costToSleep,
      nodeEdgesCounter = counter(),
    )

  /** Stands in for the node actor's mailbox pause: run the callback when the write settles, and surface the
    * outcome the same way the real one does.
    */
  private def passThroughPause(f: Future[Unit], onResult: Try[Unit] => Unit, pause: Boolean): Future[Unit] = {
    val _ = pause
    val done = Promise[Unit]()
    f.onComplete { t =>
      onResult(t)
      done.complete(scala.util.Success(()))
    }(system.dispatcher)
    done.future
  }

  private def persistorFirst(
    persist: NonEmptyList[WithTime[EdgeEvent]] => Future[Unit],
    onBatch: Int => Unit,
  ): PersistorFirstEdgeProcessor =
    new PersistorFirstEdgeProcessor(
      edges = new ReverseOrderedEdgeCollection(qid),
      persistToJournal = persist,
      pauseMessageProcessingUntil = passThroughPause,
      onBatchApplied = onBatch,
      runPostActions = (_: List[NodeChangeEvent]) => (),
      qid = qid,
      costToSleep = costToSleep,
      nodeEdgesCounter = counter(),
    )

  private def recorder(): (java.util.concurrent.atomic.AtomicInteger, java.util.concurrent.atomic.AtomicInteger) =
    (new AtomicInteger(0), new AtomicInteger(0)) // (calls, total counted)

  private def record(calls: AtomicInteger, total: AtomicInteger): Int => Unit = { n =>
    calls.incrementAndGet()
    total.addAndGet(n)
    ()
  }

  test("memory-first counts a batch once, whatever its size") {
    val (calls, total) = recorder()
    val p = memoryFirst((_: NonEmptyList[WithTime[EdgeEvent]]) => Future.unit, record(calls, total))
    Await.result(p.processEdgeEvents(edges(5), () => nextTime()), 10.seconds)
    calls.get shouldBe 1
    withClue("every event in the batch must be counted, not just the batch: ")(total.get shouldBe 5)
  }

  test("memory-first counts a retried batch exactly once") {
    val attempts = new AtomicInteger(0)
    val (calls, total) = recorder()
    val p = memoryFirst(flaky(3, attempts), record(calls, total))
    Await.result(p.processEdgeEvents(edges(4), () => nextTime()), 30.seconds)
    withClue("the write should have been attempted more than once: ")(attempts.get should be > 1)
    withClue("but the batch counted once: ")(calls.get shouldBe 1)
    total.get shouldBe 4
  }

  test("persistor-first counts a batch once when the write succeeds") {
    val (calls, total) = recorder()
    val p = persistorFirst((_: NonEmptyList[WithTime[EdgeEvent]]) => Future.unit, record(calls, total))
    Await.result(p.processEdgeEvents(edges(3), () => nextTime()), 10.seconds)
    calls.get shouldBe 1
    total.get shouldBe 3
  }

  test("persistor-first counts nothing when the write fails") {
    val (calls, total) = recorder()
    val p = persistorFirst(
      (_: NonEmptyList[WithTime[EdgeEvent]]) => Future.failed(new RuntimeException("journal unavailable")),
      record(calls, total),
    )
    Await.result(p.processEdgeEvents(edges(3), () => nextTime()), 10.seconds)
    withClue("a batch that never reached the journal must not advance the threshold: ")(calls.get shouldBe 0)
    total.get shouldBe 0
  }

  test("an edge event with no effect is not counted") {
    val (calls, total) = recorder()
    val p = memoryFirst((_: NonEmptyList[WithTime[EdgeEvent]]) => Future.unit, record(calls, total))
    val one = edges(1)
    Await.result(p.processEdgeEvents(one, () => nextTime()), 10.seconds)
    // The same edge again adds nothing, so there is no batch and nothing to count.
    Await.result(p.processEdgeEvents(one, () => nextTime()), 10.seconds)
    calls.get shouldBe 1
    total.get shouldBe 1
  }
}
