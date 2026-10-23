package com.thatdot.quine.graph

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, ConcurrentNavigableMap}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.pattern.Patterns
import org.apache.pekko.stream.Materializer
import org.apache.pekko.util.Timeout

import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Milliseconds, Seconds, Span}

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.model.QuineValue
import com.thatdot.quine.persistor.{
  EventEffectOrder,
  InMemoryPersistor,
  PersistenceConfig,
  PersistenceSchedule,
  PrimePersistor,
  StatelessPrimePersistor,
}
import com.thatdot.quine.util.TestLogging._

/** End-to-end coverage of the snapshot write in `AbstractNodeActor.persistSnapshot`, driven through
  * a real graph rather than by calling the persistor directly.
  *
  * Configured with `PersistenceSchedule.OnNodeUpdate`, so every property write makes the node take a
  * snapshot immediately; a persistor that fails a fixed number of times then succeeds therefore
  * exercises the retry loop guarding that write.
  *
  * The write goes through the agent stack a node actually holds — the store wrapped in
  * `ExceptionWrappingPersistenceAgent` — so the failure the retry loop sees is a real
  * `WrappedPersistorException`, as it would be in a running graph.
  *
  * Each retry costs a real second (the loop's fixed backoff), so failure counts here are kept small
  * deliberately.
  */
class SnapshotPersistRetryTests extends AnyFunSuite with Matchers with Eventually {

  implicit private val patience: PatienceConfig =
    PatienceConfig(timeout = Span(30, Seconds), interval = Span(200, Milliseconds))
  implicit private val timeout: Timeout = Timeout(30.seconds)

  private class StoreFailure extends RuntimeException("store failure")

  private type SnapshotStore = ConcurrentMap[QuineId, ConcurrentNavigableMap[EventTime, Array[Byte]]]

  /** An in-memory store whose snapshot writes fail the first `failureCount` times and then behave
    * normally, so a test can watch a write survive a given number of failures.
    */
  private class FlakySnapshotPersistor(
    failureCount: Int,
    snapshots: SnapshotStore,
    persistenceConfig: PersistenceConfig,
    namespace: NamespaceId,
  ) extends InMemoryPersistor(
        snapshots = snapshots,
        persistenceConfig = persistenceConfig,
        namespace = namespace,
      )(logConfig) {

    val attempts = new AtomicInteger(0)

    override def persistSnapshot(id: QuineId, atTime: EventTime, state: Array[Byte]): Future[Unit] =
      if (attempts.incrementAndGet() <= failureCount) Future.failed(new StoreFailure)
      else super.persistSnapshot(id, atTime, state)
  }

  private val idProvider: QuineIdLongProvider = QuineIdLongProvider()
  private val namespace: NamespaceId = defaultNamespaceId
  private val qid: QuineId = idProvider.customIdToQid(42L)

  /** Stand up a graph writing to a flaky store, run `body`, and shut it down. A fresh graph per test
    * keeps the attempt counter and the snapshot store isolated.
    */
  private def withFlakyGraph[A](
    failureCount: Int,
    effectOrder: EventEffectOrder = EventEffectOrder.MemoryFirst,
  )(body: (LiteralOpsGraph, FlakySnapshotPersistor, SnapshotStore) => A): A = {
    val snapshots: SnapshotStore = new ConcurrentHashMap()
    // The agent is created by the prime persistor once the graph asks for the namespace, so capture
    // it on the way past rather than constructing it up front.
    @volatile var agent: FlakySnapshotPersistor = null

    val persistorMaker: ActorSystem => PrimePersistor = system =>
      new StatelessPrimePersistor(
        PersistenceConfig(
          effectOrder = effectOrder,
          snapshotSchedule = PersistenceSchedule.OnNodeUpdate,
        ),
        None,
        (pc, ns) => {
          val created = new FlakySnapshotPersistor(failureCount, snapshots, pc, ns)
          if (ns == namespace) agent = created
          created
        },
        slug = "flaky-in-memory",
      )(Materializer.matFromSystem(system), logConfig)

    val graph = Await.result(
      GraphService(
        "snapshot-persist-retry-tests",
        effectOrder = effectOrder,
        persistorMaker = persistorMaker,
        idProvider = idProvider,
      ),
      timeout.duration,
    )
    try {
      Await.result(
        Patterns.retry(
          () => Future(graph.requiredGraphIsReady())(graph.system.dispatcher),
          attempts = 100,
          delay = 200.millis,
          graph.system.scheduler,
          graph.system.dispatcher,
        ),
        timeout.duration,
      )
      body(graph, agent, snapshots)
    } finally Await.result(graph.shutdown(), timeout.duration)
  }

  private def setProp(graph: LiteralOpsGraph, value: Long): Unit =
    Await.result(
      graph.literalOps(namespace).setProp(qid, "prop", QuineValue.Integer(value)),
      timeout.duration,
    )

  private def snapshotWritten(snapshots: SnapshotStore): Boolean =
    snapshots.asScala.get(qid).exists(!_.isEmpty)

  test("a snapshot is written on update when the persistor is healthy") {
    withFlakyGraph(failureCount = 0) { (graph, agent, snapshots) =>
      setProp(graph, 1L)
      eventually(snapshotWritten(snapshots) shouldBe true)
      agent.attempts.get() shouldBe 1
    }
  }

  test("a snapshot write that fails is retried until it succeeds") {
    withFlakyGraph(failureCount = 2) { (graph, agent, snapshots) =>
      setProp(graph, 1L)
      eventually(snapshotWritten(snapshots) shouldBe true)
      // Two failures then the write that landed: the loop kept going rather than giving up on the
      // first failure.
      agent.attempts.get() shouldBe 3
    }
  }

  test("snapshot retries also work under PersistorFirst, which stashes messages until the write lands") {
    withFlakyGraph(
      failureCount = 2,
      effectOrder = EventEffectOrder.PersistorFirst,
    ) { (graph, agent, snapshots) =>
      setProp(graph, 1L)
      eventually(snapshotWritten(snapshots) shouldBe true)
      agent.attempts.get() shouldBe 3

      // The node resumes processing after the stash clears, rather than staying wedged.
      val props = Await.result(graph.literalOps(namespace).getProps(qid), timeout.duration)
      props.keySet should contain(Symbol("prop"))
    }
  }
}
