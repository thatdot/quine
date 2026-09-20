package com.thatdot.quine.graph

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.pattern.Patterns
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.util.Timeout

import com.codahale.metrics.MetricRegistry
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.messaging.ShardMessage
import com.thatdot.quine.model.QuineValue
import com.thatdot.quine.persistor.{
  EventEffectOrder,
  InMemoryPersistor,
  PersistenceConfig,
  PrimePersistor,
  StatelessPrimePersistor,
}
import com.thatdot.quine.util.TestLogging._

/** Verifies that the snapshot-economics histograms record what a sleep writes and a wake replays.
  *
  * @see [[com.thatdot.quine.graph.metrics.HostQuineMetrics.SnapshotEconomics]]
  */
class SnapshotEconomicsTest extends AsyncFunSuite with BeforeAndAfterAll with Matchers {

  implicit val timeout: Timeout = Timeout(10.seconds)
  val idProvider: QuineIdLongProvider = QuineIdLongProvider()
  val namespace: NamespaceId = defaultNamespaceId

  private def makeGraph(name: String, persistenceConfig: PersistenceConfig): GraphService = {
    def persistorMaker(system: ActorSystem): PrimePersistor =
      new StatelessPrimePersistor(
        persistenceConfig,
        None,
        (pc, ns) => new InMemoryPersistor(persistenceConfig = pc, namespace = ns),
      )(Materializer.matFromSystem(system), logConfig)

    val graph = Await.result(
      GraphService(
        name,
        effectOrder = EventEffectOrder.PersistorFirst,
        persistorMaker = persistorMaker,
        idProvider = idProvider,
        // Nodes must be allowed to sleep straight after a write, or no snapshot is ever taken.
        declineSleepWhenWriteWithinMillis = 0L,
        metricRegistry = new MetricRegistry,
        enableDebugMetrics = true,
      ),
      timeout.duration,
    )
    graph.requiredGraphIsReady()
    graph
  }

  // A threshold of 0 snapshots on every sleep; these nodes journal too few events to cross any other.
  private val snapshotOnSleepGraph: GraphService =
    makeGraph("snapshot-economics-on-sleep", PersistenceConfig(snapshotAfterEvents = 0))

  private val noSnapshotGraph: GraphService =
    makeGraph("snapshot-economics-never", PersistenceConfig(snapshotAfterEvents = Int.MaxValue))

  implicit val ec: ExecutionContextExecutor = snapshotOnSleepGraph.system.dispatcher

  override def afterAll(): Unit = {
    Await.result(snapshotOnSleepGraph.shutdown(), timeout.duration)
    Await.result(noSnapshotGraph.shutdown(), timeout.duration)
  }

  /** Applies `count` distinct property writes, each of which is one journaled event. */
  private def writeEvents(graph: GraphService, qid: QuineId, count: Int): Future[Unit] =
    (1 to count).foldLeft(Future.unit) { (prior, i) =>
      prior.flatMap(_ => graph.literalOps(namespace).setProp(qid, s"prop$i", QuineValue.Integer(i.toLong)))
    }

  /** `RequestNodeSleep` is acknowledged as soon as sleep is initiated, so the snapshot write it
    * triggers lands later. Poll the shard -- which does not wake the node -- until it is gone.
    */
  private def sleepAndAwait(graph: GraphService, qid: QuineId): Future[Unit] = {
    def stillAwake(): Future[Boolean] =
      graph
        .relayAsk(
          graph.shardFromNode(qid).quineRef,
          ShardMessage.SampleAwakeNodes(namespace, limit = None, atTime = None, _),
        )
        .flatMap(_.map(_.quineId).runWith(Sink.collection[QuineId, Set[QuineId]])(graph.materializer))
        .map(_.contains(qid))

    graph.requestNodeSleep(namespace, qid).flatMap { _ =>
      Patterns
        .retry(
          () => stillAwake().filter(_ == false),
          attempts = 100,
          delay = 50.millis,
          graph.system.scheduler,
          graph.system.dispatcher,
        )
        .map(_ => ())
    }
  }

  test("a snapshot written on sleep records how many journal events it stood in for") {
    val graph = snapshotOnSleepGraph
    val econ = graph.metrics.snapshotEconomics
    val qid = idProvider.customIdToQid(1L)

    for {
      _ <- writeEvents(graph, qid, 3)
      _ <- sleepAndAwait(graph, qid)
    } yield {
      val recorded = econ.eventsSinceSnapshot.getSnapshot
      recorded.size shouldBe 1
      // The three property writes are exactly the replay a threshold would have traded for.
      recorded.getValues.toSeq shouldBe Seq(3L)
    }
  }

  test("journal replayed on wake is recorded when no snapshot bounds it") {
    val graph = noSnapshotGraph
    val econ = graph.metrics.snapshotEconomics
    val qid = idProvider.customIdToQid(3L)

    for {
      _ <- writeEvents(graph, qid, 4)
      _ <- sleepAndAwait(graph, qid)
      // Reading wakes the node, which with snapshots off must replay the whole journal.
      _ <- graph.literalOps(namespace).getProps(qid)
    } yield {
      econ.eventsSinceSnapshot.getCount shouldBe 0L
      econ.journalEventsReplayed.getSnapshot.getValues.toSeq should contain(4L)
    }
  }
}
