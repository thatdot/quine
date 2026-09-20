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
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.model.{
  DependsUpon,
  DomainEdge,
  DomainNodeEquiv,
  EdgeDirection,
  GenericEdge,
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

/** A write on one node must not reach its neighbours when nothing changed for them.
  *
  * A root of a DistinctId pattern holds one index slot per neighbour it asked, and each neighbour holds the
  * subscription. Once those exist, a change on the root that leaves its answer as it was is the root's business
  * alone: no message to a neighbour, so no neighbour woken, and nothing new in any neighbour's journal. The same
  * holds across the root's sleep and wake, because the slots and the subscriptions are persisted on both sides.
  *
  * Counted rather than reasoned about, since the cost is proportional to the root's degree and shows up on exactly
  * the hubs a snapshot threshold is meant to help.
  */
class SubscriptionFanOutTest extends AnyFunSuite with BeforeAndAfterAll with Matchers {

  implicit val timeout: Timeout = Timeout(10.seconds)
  private val idProvider: QuineIdLongProvider = QuineIdLongProvider()
  private val namespace: NamespaceId = defaultNamespaceId

  private val journals = new ConcurrentHashMap[QuineId, ConcurrentNavigableMap[EventTime, NodeChangeEvent]]()
  private val domainIndexEvents = new ConcurrentHashMap[QuineId, ConcurrentNavigableMap[EventTime, DomainIndexEvent]]()

  private val graph: GraphService = {
    def persistorMaker(system: ActorSystem): PrimePersistor =
      new StatelessPrimePersistor(
        // No snapshot on sleep, so the journals are the whole record and every write is visible in them.
        PersistenceConfig(snapshotAfterEvents = Int.MaxValue),
        None,
        (pc, ns) =>
          new InMemoryPersistor(
            journals = journals,
            domainIndexEvents = domainIndexEvents,
            persistenceConfig = pc,
            namespace = ns,
          ),
      )(Materializer.matFromSystem(system), logConfig)
    val g = Await.result(
      GraphService(
        "subscription-fan-out",
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

  private val child: SingleBranch = SingleBranch(hasProperty("region"), nextBranches = Nil)
  private val childDgn: DomainGraphNodeId = child.toDomainGraphNodePackage.dgnId
  private val twoHop: SingleBranch = SingleBranch(
    hasProperty("kind"),
    nextBranches = List(DomainEdge(GenericEdge(Symbol("to"), EdgeDirection.Outgoing), DependsUpon, child)),
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
    if (awakeNodes().contains(qid)) fail(s"node never slept after being asked")
  }

  private def journalRows(qid: QuineId): Int =
    Option(journals.get(qid)).fold(0)(_.size) + Option(domainIndexEvents.get(qid)).fold(0)(_.size)

  private def subscriptionsOf(qid: QuineId): Set[(DomainGraphNodeId, QuineId)] =
    Await
      .result(graph.literalOps(namespace).logState(qid), timeout.duration)
      .sqStateResults
      .subscriptions
      .map(r => (r.dgnId, r.peer))
      .toSet

  test(
    "a write on a woken hub that changes nothing for its answered neighbours wakes none and journals nothing on them",
  ) {
    val hub = idProvider.customIdToQid(1L)
    val members = (1 to 12).map(i => idProvider.customIdToQid(100L + i)).toList
    val ops = graph.literalOps(namespace)
    val sqns = graph.standingQueries(namespace).getOrElse(fail("default namespace should exist"))

    // The pattern, and a hub matching it through every member.
    val dgnPackage = twoHop.toDomainGraphNodePackage
    val sqId = StandingQueryId.fresh()
    Await.result(
      graph.dgnRegistry.registerAndPersistDomainGraphNodePackage(dgnPackage, sqId, skipPersistor = true),
      timeout.duration,
    )
    sqns.createStandingQuery(
      name = "fan-out",
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
    members.foreach(m => Await.result(ops.setProp(m, "region", QuineValue.Str("r")), timeout.duration))
    Await.result(ops.setProp(hub, "kind", QuineValue.Str("k1")), timeout.duration)
    members.foreach(m => Await.result(ops.addEdge(hub, m, "to"), timeout.duration))
    Await.result(sqns.propagateStandingQueries(None), timeout.duration)
    Thread.sleep(1000)
    withClue("the hub holds every member's answer: ")(
      subscriptionsOf(hub) should contain allElementsOf members.map(m => (childDgn, m)),
    )

    members.foreach(sleep)
    sleep(hub)
    val rowsBefore = members.map(m => m -> journalRows(m)).toMap

    // Wake the hub and write the property the pattern watches, with a value that keeps the answer as it was.
    Await.result(ops.getProps(hub), timeout.duration)
    Await.result(ops.setProp(hub, "kind", QuineValue.Str("k2")), timeout.duration)
    Thread.sleep(1000)

    withClue("no member was woken by the hub's write: ")(awakeNodes().intersect(members.toSet) shouldBe empty)
    withClue("no member's journal grew: ")(members.map(m => m -> journalRows(m)).toMap shouldBe rowsBefore)
    withClue("and the hub still holds every member's answer: ")(
      subscriptionsOf(hub) should contain allElementsOf members.map(m => (childDgn, m)),
    )
  }
}
