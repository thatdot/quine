package com.thatdot.quine.graph

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.pattern.Patterns
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.util.Timeout

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.cypher.MultipleValuesStandingQuery
import com.thatdot.quine.graph.messaging.ShardMessage
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

/** MultipleValues standing query state is deferred to node sleep by
  * [[behavior.MultipleValuesStandingQueryBehavior]] when `standingQuerySchedule` is `OnNodeSleep`, and
  * [[behavior.GoToSleepBehavior]] decides that flush and the snapshot independently. A flush that ran
  * only when a snapshot was also written would silently discard the state under any configuration
  * that does not snapshot on sleep. These tests pin the two schedules as independent.
  */
class SleepPersistenceDecouplingTest extends AsyncFunSuite with BeforeAndAfterAll with Matchers {

  implicit val timeout: Timeout = Timeout(10.seconds)
  val idProvider: QuineIdLongProvider = QuineIdLongProvider()
  val namespace: NamespaceId = defaultNamespaceId

  private val watchedKey = "watched"

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
        declineSleepWhenWriteWithinMillis = 0L,
      ),
      timeout.duration,
    )
    graph.requiredGraphIsReady()

    // A MultipleValues standing query watching one property is enough to make nodes accumulate
    // deferred standing query state as they are written to.
    val pattern = StandingQueryPattern.MultipleValuesQueryPattern(
      MultipleValuesStandingQuery.LocalProperty(
        propKey = Symbol(watchedKey),
        propConstraint = MultipleValuesStandingQuery.LocalProperty.Any,
        aliasedAs = Some(Symbol(watchedKey)),
      ),
      includeCancellation = false,
      origin = PatternOrigin.DirectSqV4,
    )
    graph
      .standingQueries(namespace)
      .getOrElse(fail("default namespace should exist"))
      .createStandingQuery(
        name = "watch-property",
        pattern = pattern,
        outputs = Map.empty,
        sqId = StandingQueryId.fresh(),
      )
    graph
  }

  private val noSnapshotGraph: GraphService =
    makeGraph(
      "decoupling-no-snapshot",
      PersistenceConfig(
        snapshotAfterEvents = Int.MaxValue,
        standingQuerySchedule = PersistenceSchedule.OnNodeSleep,
      ),
    )

  private val noStandingQueryWriteGraph: GraphService =
    makeGraph(
      "decoupling-no-sq-write",
      PersistenceConfig(
        snapshotAfterEvents = 0,
        standingQuerySchedule = PersistenceSchedule.Never,
      ),
    )

  implicit val ec: ExecutionContextExecutor = noSnapshotGraph.system.dispatcher

  override def afterAll(): Unit = {
    Await.result(noSnapshotGraph.shutdown(), timeout.duration)
    Await.result(noStandingQueryWriteGraph.shutdown(), timeout.duration)
  }

  /** `RequestNodeSleep` is acked when sleep is initiated, not finished. Poll the shard -- which
    * does not wake the node -- until it is gone.
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

  private def persistedStateCount(graph: GraphService, qid: QuineId): Future[Int] =
    graph
      .namespacePersistor(namespace)
      .getOrElse(fail("default namespace persistor should exist"))
      .getMultipleValuesStandingQueryStates(qid)
      .map(_.size)

  test("standing query state is flushed on sleep even when snapshots are disabled") {
    val graph = noSnapshotGraph
    val qid = idProvider.customIdToQid(1L)

    for {
      _ <- graph.literalOps(namespace).setProp(qid, watchedKey, QuineValue.Integer(1L))
      _ <- sleepAndAwait(graph, qid)
      count <- persistedStateCount(graph, qid)
    } yield
    // This graph never snapshots, so the flush has to run on its own.
    withClue("MultipleValues standing query states persisted on sleep: ")(count should be > 0)
  }

  test("standing query state is not written on sleep when its schedule is Never") {
    val graph = noStandingQueryWriteGraph
    val qid = idProvider.customIdToQid(2L)

    for {
      _ <- graph.literalOps(namespace).setProp(qid, watchedKey, QuineValue.Integer(1L))
      _ <- sleepAndAwait(graph, qid)
      count <- persistedStateCount(graph, qid)
    } yield
    // This graph snapshots on every sleep, so a zero here is the schedule deciding the write alone.
    withClue("MultipleValues standing query states persisted on sleep: ")(count shouldBe 0)
  }
}
