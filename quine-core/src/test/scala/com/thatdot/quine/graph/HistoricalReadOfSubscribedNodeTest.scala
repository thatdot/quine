package com.thatdot.quine.graph

import java.util.concurrent.{ConcurrentHashMap, ConcurrentNavigableMap}

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.util.Timeout

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.messaging.ShardMessage
import com.thatdot.quine.model.{
  DependsUpon,
  DomainEdge,
  DomainNodeEquiv,
  EdgeDirection,
  GenericEdge,
  Milliseconds,
  PropertyComparisonFunctions,
  QuineValue,
  SingleBranch,
}
import com.thatdot.quine.persistor.{
  EventEffectOrder,
  InMemoryPersistor,
  PersistenceConfig,
  PrimePersistor,
  StatelessPrimePersistor,
}
import com.thatdot.quine.util.TestLogging._

/** A historical read of a node that carries DistinctId bookkeeping.
  *
  * The historical actor restores from the same snapshot and journal as the current-time one, up to its time, and
  * then serves reads and nothing else: it sends nothing to the subscriptions it restored, and everything it would
  * write is refused. So a read at a past time gives the properties as of that time, wakes no current-time actor,
  * and leaves storage as it was.
  */
class HistoricalReadOfSubscribedNodeTest extends AnyFunSuite with BeforeAndAfterAll with Matchers {

  implicit val timeout: Timeout = Timeout(10.seconds)
  private val idProvider: QuineIdLongProvider = QuineIdLongProvider()
  private val namespace: NamespaceId = defaultNamespaceId

  private val journals = new ConcurrentHashMap[QuineId, ConcurrentNavigableMap[EventTime, NodeChangeEvent]]()
  private val domainIndexEvents = new ConcurrentHashMap[QuineId, ConcurrentNavigableMap[EventTime, DomainIndexEvent]]()
  private val snapshots = new ConcurrentHashMap[QuineId, ConcurrentNavigableMap[EventTime, Array[Byte]]]()

  private val graph: GraphService = {
    def persistorMaker(system: ActorSystem): PrimePersistor =
      new StatelessPrimePersistor(
        // No snapshot on sleep, so the historical actor is built from the journal alone.
        PersistenceConfig(snapshotAfterEvents = Int.MaxValue),
        None,
        (pc, ns) =>
          new InMemoryPersistor(
            journals = journals,
            domainIndexEvents = domainIndexEvents,
            snapshots = snapshots,
            persistenceConfig = pc,
            namespace = ns,
          ),
      )(Materializer.matFromSystem(system), logConfig)
    val g = Await.result(
      GraphService(
        "historical-read-of-subscribed-node",
        effectOrder = EventEffectOrder.PersistorFirst,
        persistorMaker = persistorMaker,
        idProvider = idProvider,
        declineSleepWhenWriteWithinMillis = 0L,
      ),
      timeout.duration,
    )
    g.requiredGraphIsReady()
    g
  }

  override def afterAll(): Unit = Await.result(graph.shutdown(), timeout.duration)

  private def hasProperty(key: String): DomainNodeEquiv =
    DomainNodeEquiv(None, Map(Symbol(key) -> ((PropertyComparisonFunctions.Wildcard, None))), Set.empty)

  private val twoHop: SingleBranch = SingleBranch(
    hasProperty("kind"),
    nextBranches = List(
      DomainEdge(
        GenericEdge(Symbol("to"), EdgeDirection.Outgoing),
        DependsUpon,
        SingleBranch(hasProperty("region"), nextBranches = Nil),
      ),
    ),
  )

  private def awakeNodes(): Set[QuineId] =
    Await.result(
      graph
        .relayAsk(
          graph.shardFromNode(idProvider.customIdToQid(1L)).quineRef,
          ShardMessage.SampleAwakeNodes(namespace, limit = None, atTime = None, _),
        )
        .flatMap(_.map(_.quineId).runWith(Sink.collection[QuineId, Set[QuineId]])(graph.materializer))(
          graph.system.dispatcher,
        ),
      timeout.duration,
    )

  private def sleep(qid: QuineId): Unit = {
    Await.result(graph.requestNodeSleep(namespace, qid), timeout.duration)
    var waited = 0
    while (awakeNodes().contains(qid) && waited < 100) { Thread.sleep(50); waited += 1 }
    if (awakeNodes().contains(qid)) fail("node never slept after being asked")
  }

  private def storedRows(qid: QuineId): (Int, Int, Int) = (
    Option(journals.get(qid)).fold(0)(_.size),
    Option(domainIndexEvents.get(qid)).fold(0)(_.size),
    Option(snapshots.get(qid)).fold(0)(_.size),
  )

  test("a read at a past time of a matched root gives that time's properties and writes nothing") {
    val root = idProvider.customIdToQid(1L)
    val neighbour = idProvider.customIdToQid(2L)
    val ops = graph.literalOps(namespace)
    val sqns = graph.standingQueries(namespace).getOrElse(fail("default namespace should exist"))

    val dgnPackage = twoHop.toDomainGraphNodePackage
    val sqId = StandingQueryId.fresh()
    Await.result(
      graph.dgnRegistry.registerAndPersistDomainGraphNodePackage(dgnPackage, sqId, skipPersistor = true),
      timeout.duration,
    )
    sqns.createStandingQuery(
      name = "historical-read",
      pattern = StandingQueryPattern.DomainGraphNodeStandingQueryPattern(
        dgnId = dgnPackage.dgnId,
        formatReturnAsStr = false,
        aliasReturnAs = Symbol("id"),
        includeCancellation = false,
        origin = PatternOrigin.DirectDgb,
      ),
      outputs = Map.empty,
      sqId = sqId,
    )

    // The root matches through its neighbour, so its journal holds its property writes, the query's subscription
    // to it, and the neighbour's answer, all of which the historical actor replays.
    Await.result(ops.setProp(neighbour, "region", QuineValue.Str("r")), timeout.duration)
    Await.result(ops.setProp(root, "kind", QuineValue.Str("k1")), timeout.duration)
    Await.result(ops.addEdge(root, neighbour, "to"), timeout.duration)
    Await.result(sqns.propagateStandingQueries(None), timeout.duration)
    Thread.sleep(1000)
    val earlier = Milliseconds.currentTime()
    Thread.sleep(5)
    Await.result(ops.setProp(root, "kind", QuineValue.Str("k2")), timeout.duration)
    Thread.sleep(500)

    sleep(root)
    sleep(neighbour)
    val rowsBefore = (storedRows(root), storedRows(neighbour))

    val propsThen = Await.result(ops.getProps(root, atTime = Some(earlier)), timeout.duration)

    withClue("the properties as of that time: ")(
      propsThen.get(Symbol("kind")).map(_.deserialized.get) shouldBe Some(QuineValue.Str("k1")),
    )
    withClue("the current-time node was not woken by the read: ")(awakeNodes() should not contain root)
    withClue("nothing was written by the read: ")((storedRows(root), storedRows(neighbour)) shouldBe rowsBefore)
  }
}
