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
import com.thatdot.quine.graph.messaging.ShardMessage
import com.thatdot.quine.model.{DomainNodeEquiv, PropertyComparisonFunctions, PropertyValue, QuineValue, SingleBranch}
import com.thatdot.quine.persistor.{
  EventEffectOrder,
  InMemoryPersistor,
  PersistenceConfig,
  PrimePersistor,
  StatelessPrimePersistor,
}
import com.thatdot.quine.util.TestLogging._

/** With journaling on, a snapshot only bounds replay on wake, so `snapshotAfterEvents` suppresses
  * snapshots for nodes that have journaled too little to make one worthwhile.
  */
class SnapshotThresholdTest extends AsyncFunSuite with BeforeAndAfterAll with Matchers {

  implicit val timeout: Timeout = Timeout(10.seconds)
  val idProvider: QuineIdLongProvider = QuineIdLongProvider()
  val namespace: NamespaceId = defaultNamespaceId

  private val Threshold = 16

  private def makeGraph(
    name: String,
    persistenceConfig: PersistenceConfig,
    effectOrder: EventEffectOrder = EventEffectOrder.PersistorFirst,
  ): GraphService = {
    def persistorMaker(system: ActorSystem): PrimePersistor =
      new StatelessPrimePersistor(
        persistenceConfig,
        None,
        (pc, ns) => new InMemoryPersistor(persistenceConfig = pc, namespace = ns),
      )(Materializer.matFromSystem(system), logConfig)

    val graph = Await.result(
      GraphService(
        name,
        effectOrder = effectOrder,
        persistorMaker = persistorMaker,
        idProvider = idProvider,
        declineSleepWhenWriteWithinMillis = 0L,
      ),
      timeout.duration,
    )
    graph.requiredGraphIsReady()
    graph
  }

  private val journaled: GraphService =
    makeGraph("threshold-journaled", PersistenceConfig(snapshotAfterEvents = Threshold))

  /** The same threshold under memory-first, where the edge processor applies effects ahead of a write it then
    * retries. Every other graph here is persistor-first, so this is the only cover for that ordering.
    */
  private val memoryFirst: GraphService =
    makeGraph(
      "threshold-memory-first",
      PersistenceConfig(snapshotAfterEvents = Threshold),
      EventEffectOrder.MemoryFirst,
    )

  /** The shipped default: a journal, and a snapshot once sixteen events have accumulated. */
  private val defaulted: GraphService = makeGraph("threshold-default", PersistenceConfig())

  /** No threshold: every sleep that changed anything snapshots. */
  private val everySleep: GraphService = makeGraph("threshold-every-sleep", PersistenceConfig(snapshotAfterEvents = 0))

  /** A DistinctId query rooted on a property none of this suite's nodes has. A node adopts every registered query
    * at its first wake after the registration and journals the adoption, an entry the node did not write itself.
    */
  private def registerQuery(graph: GraphService, i: Int): Unit = {
    val sqId = StandingQueryId.fresh()
    val dgnPackage = SingleBranch(
      DomainNodeEquiv(None, Map(Symbol(s"q$i") -> ((PropertyComparisonFunctions.Wildcard, None))), Set.empty),
      nextBranches = Nil,
    ).toDomainGraphNodePackage
    Await.result(
      graph.dgnRegistry.registerAndPersistDomainGraphNodePackage(dgnPackage, sqId, skipPersistor = true),
      timeout.duration,
    )
    graph
      .standingQueries(namespace)
      .getOrElse(fail("default namespace should exist"))
      .createStandingQuery(
        name = s"distinct-id-q$i",
        pattern = StandingQueryPattern.DomainGraphNodeStandingQueryPattern(
          dgnId = dgnPackage.dgnId,
          formatReturnAsStr = true,
          aliasReturnAs = Symbol("id"),
          includeCancellation = false,
          origin = PatternOrigin.DirectDgb,
        ),
        outputs = Map.empty,
        sqId = sqId,
      )
    ()
  }

  /** Five queries registered before any node in it exists; a sixth is registered mid-test. */
  private val withQueries: GraphService = {
    val graph = makeGraph("threshold-queries", PersistenceConfig(snapshotAfterEvents = Threshold))
    (1 to 5).foreach(registerQuery(graph, _))
    graph
  }

  private val withDistinctId: GraphService = {
    val graph = makeGraph("threshold-distinct-id", PersistenceConfig(snapshotAfterEvents = Threshold))
    val sqId = StandingQueryId.fresh()
    // `SingleBranch.empty` is consistent with being rooted at any node, so every node woken picks
    // up a subscription.
    val dgnPackage = SingleBranch.empty.toDomainGraphNodePackage
    Await.result(
      graph.dgnRegistry.registerAndPersistDomainGraphNodePackage(dgnPackage, sqId, skipPersistor = true),
      timeout.duration,
    )
    graph
      .standingQueries(namespace)
      .getOrElse(fail("default namespace should exist"))
      .createStandingQuery(
        name = "distinct-id-any-node",
        pattern = StandingQueryPattern.DomainGraphNodeStandingQueryPattern(
          dgnId = dgnPackage.dgnId,
          formatReturnAsStr = true,
          aliasReturnAs = Symbol("id"),
          includeCancellation = false,
          origin = PatternOrigin.DirectDgb,
        ),
        outputs = Map.empty,
        sqId = sqId,
      )
    graph
  }

  implicit val ec: ExecutionContextExecutor = journaled.system.dispatcher

  override def afterAll(): Unit = {
    Await.result(journaled.shutdown(), timeout.duration)
    Await.result(memoryFirst.shutdown(), timeout.duration)
    Await.result(defaulted.shutdown(), timeout.duration)
    Await.result(everySleep.shutdown(), timeout.duration)
    Await.result(withDistinctId.shutdown(), timeout.duration)
    Await.result(withQueries.shutdown(), timeout.duration)
  }

  private def writeEvents(graph: GraphService, qid: QuineId, count: Int, offset: Int = 0): Future[Unit] =
    (1 to count).foldLeft(Future.unit) { (prior, i) =>
      prior.flatMap(_ => graph.literalOps(namespace).setProp(qid, s"prop${i + offset}", QuineValue.Integer(i.toLong)))
    }

  /** `count` edge events on `qid`. Each `addEdge` writes one half-edge here and one on the far node, so the count
    * this node journals is `count`.
    */
  private def writeEdges(graph: GraphService, qid: QuineId, count: Int, offset: Int = 0): Future[Unit] =
    (1 to count).foldLeft(Future.unit) { (prior, i) =>
      prior.flatMap(_ =>
        graph.literalOps(namespace).addEdge(qid, idProvider.customIdToQid((900 + i + offset).toLong), s"e${i + offset}"),
      )
    }

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

  private def snapshotExists(graph: GraphService, qid: QuineId): Future[Boolean] =
    graph
      .namespacePersistor(namespace)
      .getOrElse(fail("namespace persistor should exist"))
      .getLatestSnapshot(qid, EventTime.MaxValue)
      .map(_.isDefined)

  /** The key of the newest snapshot, which advances only when a new one is written. */
  private def latestSnapshotTime(graph: GraphService, qid: QuineId): Future[Option[EventTime]] =
    graph
      .namespacePersistor(namespace)
      .getOrElse(fail("namespace persistor should exist"))
      .getLatestSnapshot(qid, EventTime.MaxValue)
      .map(_.map(_.atTime))

  private def props(graph: GraphService, qid: QuineId): Future[Map[Symbol, PropertyValue]] =
    graph.literalOps(namespace).getProps(qid)

  test("the default is a journal that snapshots on every sleep, as before the threshold existed") {
    PersistenceConfig().journalEnabled shouldBe true
    PersistenceConfig().snapshotAfterEvents shouldEqual 0
    val qid = idProvider.customIdToQid(1L)
    for {
      _ <- writeEvents(defaulted, qid, 1)
      _ <- sleepAndAwait(defaulted, qid)
      exists <- snapshotExists(defaulted, qid)
    } yield exists shouldBe true
  }

  test("a threshold of zero snapshots on every sleep, however little was journaled") {
    val qid = idProvider.customIdToQid(11L)
    for {
      _ <- writeEvents(everySleep, qid, 1)
      _ <- sleepAndAwait(everySleep, qid)
      exists <- snapshotExists(everySleep, qid)
    } yield exists shouldBe true
  }

  test("a node below the threshold sleeps without writing a snapshot") {
    val qid = idProvider.customIdToQid(1L)
    for {
      _ <- writeEvents(journaled, qid, 3)
      _ <- sleepAndAwait(journaled, qid)
      exists <- snapshotExists(journaled, qid)
    } yield exists shouldBe false
  }

  test("a node that skipped its snapshot still restores correctly from the journal") {
    val qid = idProvider.customIdToQid(2L)
    for {
      _ <- writeEvents(journaled, qid, 3)
      _ <- sleepAndAwait(journaled, qid)
      exists <- snapshotExists(journaled, qid)
      restored <- props(journaled, qid)
    } yield {
      exists shouldBe false
      restored.keySet.map(_.name) should contain allOf ("prop1", "prop2", "prop3")
    }
  }

  test("a node at or above the threshold does write a snapshot") {
    val qid = idProvider.customIdToQid(3L)
    for {
      _ <- writeEvents(journaled, qid, Threshold + 2)
      _ <- sleepAndAwait(journaled, qid)
      exists <- snapshotExists(journaled, qid)
    } yield exists shouldBe true
  }

  // Edge events reach the journal through the edge processor, not `persistAndApplyEventsEffectsInMemory`, so
  // they are counted on a separate path.
  test("edge writes count towards the threshold as property writes do") {
    val qid = idProvider.customIdToQid(10L)
    for {
      _ <- writeEdges(journaled, qid, Threshold + 2)
      _ <- sleepAndAwait(journaled, qid)
      exists <- snapshotExists(journaled, qid)
    } yield exists shouldBe true
  }

  test("a node below the threshold on edge writes alone still skips its snapshot") {
    val qid = idProvider.customIdToQid(11L)
    for {
      _ <- writeEdges(journaled, qid, Threshold - 2)
      _ <- sleepAndAwait(journaled, qid)
      exists <- snapshotExists(journaled, qid)
    } yield exists shouldBe false
  }

  test("edge and property writes count towards the same threshold") {
    val qid = idProvider.customIdToQid(12L)
    val half = Threshold / 2 + 1 // neither kind alone crosses it; together they do
    for {
      _ <- writeEdges(journaled, qid, half)
      _ <- writeEvents(journaled, qid, half)
      _ <- sleepAndAwait(journaled, qid)
      exists <- snapshotExists(journaled, qid)
    } yield exists shouldBe true
  }

  // Memory-first applies the edge effects and counts the batch before a write it then retries, so the count has
  // to sit outside the retry.
  test("edge writes count towards the threshold under memory-first") {
    val qid = idProvider.customIdToQid(13L)
    for {
      _ <- writeEdges(memoryFirst, qid, Threshold + 2, offset = 100)
      _ <- sleepAndAwait(memoryFirst, qid)
      exists <- snapshotExists(memoryFirst, qid)
    } yield exists shouldBe true
  }

  test("a memory-first node below the threshold on edge writes still skips its snapshot") {
    val qid = idProvider.customIdToQid(14L)
    for {
      _ <- writeEdges(memoryFirst, qid, Threshold - 2, offset = 200)
      _ <- sleepAndAwait(memoryFirst, qid)
      exists <- snapshotExists(memoryFirst, qid)
    } yield exists shouldBe false
  }

  test("journal length accumulates across sleep/wake cycles rather than restarting") {
    val qid = idProvider.customIdToQid(4L)
    val half = Threshold / 2 + 1 // two rounds clear the threshold; neither does alone
    for {
      _ <- writeEvents(journaled, qid, half)
      _ <- sleepAndAwait(journaled, qid)
      afterFirst <- snapshotExists(journaled, qid)
      _ <- writeEvents(journaled, qid, half, offset = half)
      _ <- sleepAndAwait(journaled, qid)
      afterSecond <- snapshotExists(journaled, qid)
    } yield {
      withClue("first sleep is below the threshold on its own: ")(afterFirst shouldBe false)
      // Only correct if the woken node counted the journal it replayed instead of restarting at 0.
      withClue("second sleep crosses the threshold cumulatively: ")(afterSecond shouldBe true)
    }
  }

  test("a registered query costs a node one journal entry, at its first wake after the registration") {
    val graph = withQueries
    val qid = idProvider.customIdToQid(8L)
    for {
      // The node's first wake adopts the five queries: five entries, then ten writes, fifteen.
      _ <- writeEvents(graph, qid, 10)
      _ <- sleepAndAwait(graph, qid)
      afterTenWrites <- latestSnapshotTime(graph, qid)
      // Sixteen.
      _ <- writeEvents(graph, qid, 1, offset = 10)
      _ <- sleepAndAwait(graph, qid)
      firstSnapshot <- latestSnapshotTime(graph, qid)
      // The wake finds the five subscriptions in the snapshot and adopts nothing: fourteen writes are fourteen.
      _ <- writeEvents(graph, qid, 14, offset = 11)
      _ <- sleepAndAwait(graph, qid)
      afterFourteenMore <- latestSnapshotTime(graph, qid)
      // A sixth query. The next wake adopts it and nothing else: fifteen.
      _ = registerQuery(graph, 6)
      _ <- props(graph, qid)
      _ <- sleepAndAwait(graph, qid)
      afterSixthQuery <- latestSnapshotTime(graph, qid)
      // Sixteen.
      _ <- writeEvents(graph, qid, 1, offset = 25)
      _ <- sleepAndAwait(graph, qid)
      secondSnapshot <- latestSnapshotTime(graph, qid)
    } yield {
      afterTenWrites shouldBe None
      firstSnapshot should not be None
      afterFourteenMore shouldBe firstSnapshot
      afterSixthQuery shouldBe firstSnapshot
      secondSnapshot should not be firstSnapshot
    }
  }

  test("a node carrying DistinctId subscriber state is subject to the threshold like any other") {
    val graph = withDistinctId
    val qid = idProvider.customIdToQid(6L)
    for {
      _ <- writeEvents(graph, qid, 3) // well under the threshold
      _ <- sleepAndAwait(graph, qid)
      exists <- snapshotExists(graph, qid)
      restored <- props(graph, qid)
    } yield {
      exists shouldBe false
      restored.keySet.map(_.name) should contain allOf ("prop1", "prop2", "prop3")
    }
  }
}
