package com.thatdot.quine.graph

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.util.Timeout

import cats.data.NonEmptyList
import com.codahale.metrics.MetricRegistry
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.EdgeEvent.{EdgeAdded, EdgeRemoved}
import com.thatdot.quine.graph.PropertyEvent.{PropertyRemoved, PropertySet}
import com.thatdot.quine.model.{EdgeDirection, HalfEdge, PropertyValue}
import com.thatdot.quine.persistor.{EventEffectOrder, InMemoryPersistor, PersistenceConfig, StatelessPrimePersistor}
import com.thatdot.quine.util.TestLogging._

/** Tests that journal-replay is resilient--that a node can apply arbitrary node events without crashing.
  *
  * Two tests:
  *   - Tests a hardcoded sequence of node events that contains known problem spots (such as an edge-removal
  *     event for a nonexistent edge).
  *   - Tests a randomly generated sequence of node events. No exceptions should occur.
  */
class JournalReplayResilienceTest
    extends AnyFunSuite
    with BeforeAndAfterAll
    with Matchers
    with ScalaCheckPropertyChecks {

  implicit private val timeout: Timeout = Timeout(5.seconds)

  // ReverseOrderedEdgeCollection is the edge store that used to throw on removing an absent edge.
  private val graph: GraphService = Await.result(
    GraphService(
      "journal-replay-resilience",
      persistorMaker = (system: ActorSystem) =>
        new StatelessPrimePersistor(
          PersistenceConfig(),
          None,
          (pc, ns) => new InMemoryPersistor(persistenceConfig = pc, namespace = ns),
        )(Materializer.matFromSystem(system), logConfig),
      idProvider = IdentityIdProvider,
      effectOrder = EventEffectOrder.PersistorFirst,
      edgeCollectionFactory = new edges.ReverseOrderedEdgeCollection(_),
      metricRegistry = new MetricRegistry,
    ),
    10.seconds,
  )
  graph.requiredGraphIsReady()

  override def afterAll(): Unit = {
    Await.result(graph.shutdown(), 30.seconds)
    ()
  }

  /** Writes `journal` to the store as the journal of `qid`, one event per sequence number in a single millisecond. */
  private def persistJournal(qid: QuineId, journal: List[NodeChangeEvent]): Unit = {
    val persistor = graph.namespacePersistor(defaultNamespaceId).getOrElse(fail("no default namespace persistor"))
    val events = journal.zipWithIndex.map { case (event, i) =>
      NodeEvent.WithTime(event, EventTime(1_000_000L, timestampSequence = i.toLong))
    }
    Await.result(persistor.persistNodeChangeEvents(qid, NonEmptyList.fromListUnsafe(events)), 10.seconds)
  }

  test("journal-replay should not crash on this devilishly handcrafted journal") {
    val qid = QuineId(Array(1.toByte))
    val toA = HalfEdge(Symbol("knows"), EdgeDirection.Outgoing, QuineId(Array(2.toByte)))
    val toB = HalfEdge(Symbol("knows"), EdgeDirection.Outgoing, QuineId(Array(3.toByte)))
    val toC = HalfEdge(Symbol("likes"), EdgeDirection.Incoming, QuineId(Array(4.toByte)))

    persistJournal(
      qid,
      List(
        PropertySet(Symbol("a"), PropertyValue(1L)),
        PropertySet(Symbol("b"), PropertyValue(2L)),
        PropertySet(Symbol("a"), PropertyValue(3L)), // Sets a property that is already present.
        PropertyRemoved(Symbol("b"), PropertyValue(2L)),
        PropertyRemoved(Symbol("b"), PropertyValue(2L)), // Removes a property that was just removed.
        PropertyRemoved(Symbol("c"), PropertyValue(0L)), // Removes a property that never existed.
        EdgeAdded(toA),
        EdgeAdded(toA), // Adds an edge that is present.
        EdgeAdded(toB),
        EdgeRemoved(toB),
        EdgeRemoved(toB), // Removes an edge that was just removed.
        EdgeRemoved(toC), // Removes an edge whose type the node has never had.
      ),
    )

    Await.result(graph.literalOps(defaultNamespaceId).getProps(qid), timeout.duration) shouldBe Map(
      Symbol("a") -> PropertyValue(3L),
    )
    Await.result(graph.literalOps(defaultNamespaceId).getHalfEdges(qid), timeout.duration) shouldBe Set(toA)
  }

  test("journal-replay should not crash on a randomly generated journal") {
    // Few keys and edges, so that random journals often repeat a set or add, or remove something absent.
    val keyGen = Gen.oneOf(Symbol("a"), Symbol("b"), Symbol("c"))
    val valueGen = Gen.choose(0L, 3L).map(PropertyValue(_))
    val edgeGen = for {
      edgeType <- Gen.oneOf(Symbol("knows"), Symbol("likes"))
      direction <- Gen.oneOf(EdgeDirection.Outgoing, EdgeDirection.Incoming)
      other <- Gen.oneOf(QuineId(Array(2.toByte)), QuineId(Array(3.toByte)))
    } yield HalfEdge(edgeType, direction, other)
    val eventGen: Gen[NodeChangeEvent] = Gen.oneOf(
      Gen.zip(keyGen, valueGen).map { case (k, v) => PropertySet(k, v) },
      Gen.zip(keyGen, valueGen).map { case (k, v) => PropertyRemoved(k, v) },
      edgeGen.map(EdgeAdded(_)),
      edgeGen.map(EdgeRemoved(_)),
    )
    val journalGen = Gen.choose(1, 30).flatMap(Gen.listOfN(_, eventGen))

    // Each journal goes to a fresh node, so every read wakes a node that has never been awake.
    val nextNode = new AtomicInteger(1000)

    forAll(journalGen, minSuccessful(100)) { journal =>
      val qid = HalfEdgeGen.intToQuineId(nextNode.getAndIncrement())
      persistJournal(qid, journal)
      noException should be thrownBy {
        Await.result(graph.literalOps(defaultNamespaceId).getProps(qid), timeout.duration)
        Await.result(graph.literalOps(defaultNamespaceId).getHalfEdges(qid), timeout.duration)
      }
    }
  }
}
