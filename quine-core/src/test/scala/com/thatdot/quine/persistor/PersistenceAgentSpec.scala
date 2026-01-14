package com.thatdot.quine.persistor

import java.util.UUID

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}

import org.apache.pekko.actor.ActorSystem

import cats.data.NonEmptyList
import cats.syntax.functor._
import org.scalacheck.rng.Seed
import org.scalatest.funspec.AsyncFunSpec
import org.scalatest.matchers.should
import org.scalatest.{Assertion, BeforeAndAfterAll, Inspectors, OptionValues}

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.DomainIndexEvent.CancelDomainNodeSubscription
import com.thatdot.quine.graph.PropertyEvent.PropertySet
import com.thatdot.quine.graph.TestDataFactory.generateN
import com.thatdot.quine.graph.{
  ArbitraryInstances,
  DomainIndexEvent,
  EventTime,
  MultipleValuesStandingQueryPartId,
  NamespaceId,
  NodeChangeEvent,
  NodeEvent,
  PatternOrigin,
  QuineUUIDProvider,
  ScalaTestInstances,
  StandingQueryId,
  StandingQueryInfo,
  StandingQueryPattern,
  defaultNamespaceId,
  namespaceFromString,
  namespaceToString,
}
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.model.{DomainGraphNode, PropertyValue, QuineValue}
import com.thatdot.quine.persistor.PersistenceAgentSpec.assertArraysEqual

/** Abstract test suite that can be implemented just by specifying `persistor`.
  * The intent is that every new persistor should be able to extend this
  * abstract `Spec` and quickly be able to check that the expected persistor
  * properties hold
  *
  * TODO: add tests for standing queries
  */
abstract class PersistenceAgentSpec
    extends AsyncFunSpec
    with BeforeAndAfterAll
    with Inspectors
    with OptionValues
    with ArbitraryInstances
    with ScalaTestInstances
    with should.Matchers {

  implicit val system: ActorSystem = ActorSystem("test-system")

  // Override this if tests need to be skipped
  def runnable: Boolean = true

  // Override this to opt-out of tests that delete records (eg to perform manual inspection)
  // When this is false, tests will likely require manual intervention to clean up the database between runs
  def runDeletionTests: Boolean = true

  // Override this to opt-out of running the "purge namespace" test which does not work on all persistors
  // ex: AWS Keyspaces
  def runPurgeNamespaceTest: Boolean = true

  def persistor: PrimePersistor

  // main namespace used for tests
  val testNamespace: NamespaceId = namespaceFromString("persistenceSpec")
  // alternate namespaces used for tests specifically about namespace isolation / interop
  val altNamespace1: NamespaceId = namespaceFromString("persistenceSpec1")
  val altNamespace2: NamespaceId = namespaceFromString("persistenceSpec2")

  // default NamespaceIds -- `defaultNamespacedNamed` relies on breaking the `NamespaceId` abstraction
  // and should be rejected by the persistence layer
  val defaultNamespaceNamed: NamespaceId = Some(Symbol(namespaceToString(defaultNamespaceId)))
  val defaultNamespaceUnnamed: NamespaceId = defaultNamespaceId

  // initialized in beforeAll
  final var namespacedPersistor: NamespacedPersistenceAgent = _
  final var altPersistor1: NamespacedPersistenceAgent = _
  final var altPersistor2: NamespacedPersistenceAgent = _

  private def getOrInitTestNamespace(name: NamespaceId): NamespacedPersistenceAgent = persistor(name).getOrElse(
    Await.result(
      persistor
        .prepareNamespace(name)
        .map { _ =>
          persistor.createNamespace(name)
          persistor(name).get // this should be defined -- we just created it, after all!
        }(ExecutionContext.parasitic),
      41.seconds, // potentially creates database tables, which is potentially slow depending on the database
    ),
  )
  override def beforeAll(): Unit = {
    super.beforeAll()
    namespacedPersistor = getOrInitTestNamespace(testNamespace)
    altPersistor1 = getOrInitTestNamespace(altNamespace1)
    altPersistor2 = getOrInitTestNamespace(altNamespace2)
  }

  private var incrementingTimestamp: Long = 0L
  def nextTimestamp(): EventTime = {
    incrementingTimestamp += 1L
    EventTime(incrementingTimestamp)
  }
  def withIncrementedTime[T <: NodeEvent](events: NonEmptyList[T]): NonEmptyList[NodeEvent.WithTime[T]] =
    events.map(n => NodeEvent.WithTime(n, nextTimestamp()))

  def sortedByTime[T <: NodeEvent](events: NonEmptyList[NodeEvent.WithTime[T]]): NonEmptyList[NodeEvent.WithTime[T]] =
    events.sortBy(_.atTime)

  override def afterAll(): Unit = {
    super.afterAll()
    Await.ready(
      {
        // Can't use the implicit SerialExecutionContext outside of a test scope
        implicit val ec = ExecutionContext.parasitic
        for {
          _ <- Future.traverse(Seq(testNamespace, altNamespace1, altNamespace2))(persistor.deleteNamespace)
          _ <- persistor.shutdown()
          _ <- system.terminate()
        } yield ()
      },
      25.seconds,
    )
    ()
  }

  val idProvider: QuineUUIDProvider.type = QuineUUIDProvider

  def qidFromInt(i: Int): QuineId = idProvider.customIdToQid(new UUID(0, i.toLong))

  val qid0: QuineId = idProvider.customIdStringToQid("00000000-0000-0000-0000-000000000000").get
  val qid1: QuineId = idProvider.customIdStringToQid("00000000-0000-0000-0000-000000000001").get
  val qid2: QuineId = idProvider.customIdStringToQid("77747265-9ea9-4d61-a419-d7758c8b097a").get
  val qid3: QuineId = idProvider.customIdStringToQid("45cc12b5-f498-4f72-89d3-29180df76e34").get
  val qid4: QuineId = idProvider.customIdStringToQid("ffffffff-ffff-ffff-ffff-ffffffffffff").get
  val allQids: Seq[QuineId] = Seq(qid0, qid1, qid2, qid3, qid4)

  val event0: PropertySet = PropertySet(Symbol("foo"), PropertyValue(QuineValue(0L)))
  val event1: PropertySet = PropertySet(Symbol("foo"), PropertyValue(QuineValue(1L)))
  val event2: PropertySet = PropertySet(Symbol("foo"), PropertyValue(QuineValue(2L)))
  val event3: PropertySet = PropertySet(Symbol("foo"), PropertyValue(QuineValue(3L)))
  val event4: PropertySet = PropertySet(Symbol("foo"), PropertyValue(QuineValue(4L)))

  // arbitrary byte arrays
  val snapshot0: Array[Byte] = Array[Byte](1)
  val snapshot1: Array[Byte] = Array[Byte](-87, 60, 83, 99)
  val snapshot2: Array[Byte] = Array[Byte](11)
  val snapshot3: Array[Byte] = Array[Byte](89, -71, 2)
  val snapshot4: Array[Byte] = Array.tabulate(200 * 1000)(i => i % 256 - 127).map(_.toByte)

  val sqId1: StandingQueryId = StandingQueryId(new UUID(0L, 0L)) // min unsigned representation
  val sqId2: StandingQueryId = StandingQueryId(new UUID(256389790107965554L, 7806099684324575116L))
  val sqId3: StandingQueryId = StandingQueryId(new UUID(-2866009460452510937L, 8633904949869711978L))
  val sqId4: StandingQueryId = StandingQueryId(new UUID(-1L, -1L)) // max unsigned representation

  val sqPartId1: MultipleValuesStandingQueryPartId = MultipleValuesStandingQueryPartId(new UUID(0L, 0L))
  val sqPartId2: MultipleValuesStandingQueryPartId = MultipleValuesStandingQueryPartId(
    new UUID(1096520000288222086L, 748609736042323025L),
  )
  val sqPartId3: MultipleValuesStandingQueryPartId = MultipleValuesStandingQueryPartId(
    new UUID(-1613026160293696877L, 6732331004029745690L),
  )
  val sqPartId4: MultipleValuesStandingQueryPartId = MultipleValuesStandingQueryPartId(new UUID(-1L, -1L))

  // arbitrary byte arrays
  val sqState1: Array[Byte] = Array[Byte]()
  val sqState2: Array[Byte] = Array[Byte](0)
  val sqState3: Array[Byte] = Array[Byte](-98, 123, 5, 78)
  val sqState4: Array[Byte] = Array[Byte](34, 92, -1, 20)

  // arbitrary metadata keys
  val metadata0 = "foo"
  val metadata1 = "bar"
  val metadata2 = "123"
  val metadata3: String = Seq.tabulate(1024)(i => ('a' + i % 26).toChar).mkString
  val metadata4 = "weird characters {&*@(} spooky"

  /** Mash together a bunch of async actions into one assertion */
  def allOfConcurrent[A](asyncTests: Future[A]*): Future[Assertion] = {
    assume(runnable)
    Future.sequence(asyncTests) as succeed
  }

  describe("persistEvent") {
    it("can record events at various time") {
      allOfConcurrent(
        namespacedPersistor.persistNodeChangeEvents(
          qid1,
          NonEmptyList.of(
            NodeEvent.WithTime(event0, EventTime.fromRaw(34L)),
            NodeEvent.WithTime(event1, EventTime.fromRaw(36L)),
            NodeEvent.WithTime(event2, EventTime.fromRaw(38L)),
            NodeEvent.WithTime(event3, EventTime.fromRaw(40L)),
            NodeEvent.WithTime(event4, EventTime.fromRaw(44L)),
          ),
        ),
      )
    }

    it("supports EventTime.MaxValue and EventTime.MinValue") {
      allOfConcurrent(
        // "minimum qid" (all 0 bits)
        namespacedPersistor.persistNodeChangeEvents(
          qid0,
          NonEmptyList.of(
            NodeEvent.WithTime(event0, EventTime.MinValue),
            NodeEvent.WithTime(event1, EventTime.fromRaw(2394872938L)),
            NodeEvent.WithTime(event2, EventTime.fromRaw(-129387432L)),
            NodeEvent.WithTime(event3, EventTime.MaxValue),
          ),
        ),
        // in between qid
        namespacedPersistor.persistNodeChangeEvents(
          qid2,
          NonEmptyList.of(
            NodeEvent.WithTime(event0, EventTime.MinValue),
            NodeEvent.WithTime(event1, EventTime.fromRaw(2394872938L)),
            NodeEvent.WithTime(event2, EventTime.fromRaw(-129387432L)),
            NodeEvent.WithTime(event3, EventTime.MaxValue),
          ),
        ),
        // "maximum qid" (all 1 bits)
        namespacedPersistor.persistNodeChangeEvents(
          qid4,
          NonEmptyList.of(
            NodeEvent.WithTime(event0, EventTime.MinValue),
            NodeEvent.WithTime(event1, EventTime.fromRaw(2394872938L)),
            NodeEvent.WithTime(event2, EventTime.fromRaw(-129387432L)),
            NodeEvent.WithTime(event3, EventTime.MaxValue),
          ),
        ),
      )
    }
  }

  describe("getJournal") {

    it("can query a full journal of a node") {
      allOfConcurrent(
        namespacedPersistor
          .getJournal(qid0, EventTime.MinValue, EventTime.MaxValue, includeDomainIndexEvents = true)
          .map { journal =>
            (journal shouldEqual Seq(event0, event1, event2, event3))
          },
        namespacedPersistor
          .getJournal(qid1, EventTime.MinValue, EventTime.MaxValue, includeDomainIndexEvents = true)
          .map { journal =>
            (journal shouldEqual Seq(event0, event1, event2, event3, event4))
          },
        namespacedPersistor
          .getJournal(qid2, EventTime.MinValue, EventTime.MaxValue, includeDomainIndexEvents = true)
          .map { journal =>
            (journal shouldEqual Seq(event0, event1, event2, event3))
          },
        namespacedPersistor
          .getJournal(qid3, EventTime.MinValue, EventTime.MaxValue, includeDomainIndexEvents = true)
          .map { journal =>
            (journal shouldEqual Seq.empty)
          },
        namespacedPersistor
          .getJournal(qid4, EventTime.MinValue, EventTime.MaxValue, includeDomainIndexEvents = false)
          .map { journal =>
            (journal shouldEqual Seq(event0, event1, event2, event3))
          },
      )
    }

    it("can query with EventTime.MinValue lower bound") {
      allOfConcurrent(
        // before anything
        namespacedPersistor
          .getJournal(qid1, EventTime.MinValue, EventTime.fromRaw(2L), includeDomainIndexEvents = true)
          .map { journal =>
            (journal shouldEqual Seq.empty)
          },
        // right up to one event
        namespacedPersistor
          .getJournal(qid1, EventTime.MinValue, EventTime.fromRaw(34L), includeDomainIndexEvents = true)
          .map { journal =>
            (journal shouldEqual Seq(event0))
          },
        // right after one event
        namespacedPersistor
          .getJournal(qid1, EventTime.MinValue, EventTime.fromRaw(37L), includeDomainIndexEvents = true)
          .map { journal =>
            (journal shouldEqual Seq(event0, event1))
          },
        // after all events
        namespacedPersistor
          .getJournal(qid1, EventTime.MinValue, EventTime.fromRaw(48L), includeDomainIndexEvents = true)
          .map { journal =>
            (journal shouldEqual Seq(event0, event1, event2, event3, event4))
          },
        // first event is the min value
        namespacedPersistor
          .getJournal(qid0, EventTime.MinValue, EventTime.MinValue, includeDomainIndexEvents = true)
          .map { journal =>
            (journal shouldEqual Seq(event0))
          },
        namespacedPersistor
          .getJournal(qid2, EventTime.MinValue, EventTime.MinValue, includeDomainIndexEvents = true)
          .map { journal =>
            (journal shouldEqual Seq(event0))
          },
        namespacedPersistor
          .getJournal(qid4, EventTime.MinValue, EventTime.MinValue, includeDomainIndexEvents = true)
          .map { journal =>
            (journal shouldEqual Seq(event0))
          },
      )
    }

    it("can query with EventTime.MaxValue upper bound") {
      allOfConcurrent(
        // before anything
        namespacedPersistor
          .getJournal(qid1, EventTime.fromRaw(2L), EventTime.MaxValue, includeDomainIndexEvents = true)
          .map { journal =>
            (journal shouldEqual Seq(event0, event1, event2, event3, event4))
          },
        // before one event
        namespacedPersistor
          .getJournal(qid1, EventTime.fromRaw(42L), EventTime.MaxValue, includeDomainIndexEvents = true)
          .map { journal =>
            (journal shouldEqual Seq(event4))
          },
        // starting exactly at one event
        namespacedPersistor
          .getJournal(qid1, EventTime.fromRaw(44L), EventTime.MaxValue, includeDomainIndexEvents = true)
          .map { journal =>
            (journal shouldEqual Seq(event4))
          },
        // starting exactly at the first event
        namespacedPersistor
          .getJournal(qid1, EventTime.fromRaw(34L), EventTime.MaxValue, includeDomainIndexEvents = true)
          .map { journal =>
            (journal shouldEqual Seq(event0, event1, event2, event3, event4))
          },
        // after all events
        namespacedPersistor
          .getJournal(qid1, EventTime.fromRaw(48L), EventTime.MaxValue, includeDomainIndexEvents = true)
          .map { journal =>
            (journal shouldEqual Seq.empty)
          },
        // first event is the min value
        namespacedPersistor
          .getJournal(qid0, EventTime.MaxValue, EventTime.MaxValue, includeDomainIndexEvents = true)
          .map { journal =>
            (journal shouldEqual Seq(event3))
          },
        namespacedPersistor
          .getJournal(qid2, EventTime.MaxValue, EventTime.MaxValue, includeDomainIndexEvents = true)
          .map { journal =>
            (journal shouldEqual Seq(event3))
          },
        namespacedPersistor
          .getJournal(qid4, EventTime.MaxValue, EventTime.MaxValue, includeDomainIndexEvents = true)
          .map { journal =>
            (journal shouldEqual Seq(event3))
          },
      )
    }

    it("can query with bounds that are not maximums") {
      allOfConcurrent(
        // start and end before any events
        namespacedPersistor
          .getJournal(qid1, EventTime.fromRaw(2L), EventTime.fromRaw(33L), includeDomainIndexEvents = true)
          .map { journal =>
            (journal shouldEqual Seq.empty)
          },
        // start and end between events
        namespacedPersistor
          .getJournal(qid1, EventTime.fromRaw(42L), EventTime.fromRaw(43L), includeDomainIndexEvents = true)
          .map { journal =>
            (journal shouldEqual Seq.empty)
          },
        // right up to one event
        namespacedPersistor
          .getJournal(qid1, EventTime.fromRaw(2L), EventTime.fromRaw(34L), includeDomainIndexEvents = true)
          .map { journal =>
            (journal shouldEqual Seq(event0))
          },
        // right after one event
        namespacedPersistor
          .getJournal(qid1, EventTime.fromRaw(2L), EventTime.fromRaw(35L), includeDomainIndexEvents = true)
          .map { journal =>
            (journal shouldEqual Seq(event0))
          },
        // starting exactly at one event
        namespacedPersistor
          .getJournal(qid1, EventTime.fromRaw(34L), EventTime.fromRaw(35L), includeDomainIndexEvents = true)
          .map { journal =>
            (journal shouldEqual Seq(event0))
          },
        // start and end on events
        namespacedPersistor
          .getJournal(qid1, EventTime.fromRaw(36L), EventTime.fromRaw(40L), includeDomainIndexEvents = true)
          .map { journal =>
            (journal shouldEqual Seq(event1, event2, event3))
          },
        namespacedPersistor
          .getJournal(qid1, EventTime.fromRaw(34L), EventTime.fromRaw(48L), includeDomainIndexEvents = true)
          .map { journal =>
            journal shouldEqual Seq(
              event0,
              event1,
              event2,
              event3,
              event4,
            )
          },
      )
    }

    it("can handle unsigned EventTime") {
      allOfConcurrent(
        // event time needs to be treated as unsigned
        namespacedPersistor
          .getJournal(qid0, EventTime.fromRaw(-200000000L), EventTime.fromRaw(-2L), includeDomainIndexEvents = true)
          .map { journal =>
            (journal shouldEqual Seq(event2))
          },
        namespacedPersistor
          .getJournal(qid2, EventTime.fromRaw(-200000000L), EventTime.fromRaw(-2L), includeDomainIndexEvents = true)
          .map { journal =>
            (journal shouldEqual Seq(event2))
          },
        namespacedPersistor
          .getJournal(qid4, EventTime.fromRaw(-200000000L), EventTime.fromRaw(-2L), includeDomainIndexEvents = true)
          .map { journal =>
            (journal shouldEqual Seq(event2))
          },
        // event time needs to be treated as unsigned
        namespacedPersistor
          .getJournal(qid0, EventTime.fromRaw(2L), EventTime.fromRaw(-2L), includeDomainIndexEvents = true)
          .map { journal =>
            (journal shouldEqual Seq(event1, event2))
          },
        namespacedPersistor
          .getJournal(qid2, EventTime.fromRaw(2L), EventTime.fromRaw(-2L), includeDomainIndexEvents = true)
          .map { journal =>
            (journal shouldEqual Seq(event1, event2))
          },
        namespacedPersistor
          .getJournal(qid4, EventTime.fromRaw(2L), EventTime.fromRaw(-2L), includeDomainIndexEvents = true)
          .map { journal =>
            (journal shouldEqual Seq(event1, event2))
          },
      )
    }
  }

  describe("persistSnapshot") {
    it("can record snapshots at various time") {
      allOfConcurrent(
        namespacedPersistor.persistSnapshot(qid1, EventTime.fromRaw(34L), snapshot0),
        namespacedPersistor.persistSnapshot(qid1, EventTime.fromRaw(36L), snapshot1),
        namespacedPersistor.persistSnapshot(qid1, EventTime.fromRaw(38L), snapshot2),
        namespacedPersistor.persistSnapshot(qid1, EventTime.fromRaw(40L), snapshot3),
        namespacedPersistor.persistSnapshot(qid1, EventTime.fromRaw(44L), snapshot4),
      )
    }

    it("supports EventTime.MaxValue and EventTime.MinValue") {
      allOfConcurrent(
        // "minimum qid" (all 0 bits)
        namespacedPersistor.persistSnapshot(qid0, EventTime.MinValue, snapshot0),
        namespacedPersistor.persistSnapshot(qid0, EventTime.fromRaw(2394872938L), snapshot1),
        namespacedPersistor.persistSnapshot(qid0, EventTime.fromRaw(-129387432L), snapshot2),
        namespacedPersistor.persistSnapshot(qid0, EventTime.MaxValue, snapshot3),
        // in between qid
        namespacedPersistor.persistSnapshot(qid2, EventTime.MinValue, snapshot0),
        namespacedPersistor.persistSnapshot(qid2, EventTime.fromRaw(2394872938L), snapshot1),
        namespacedPersistor.persistSnapshot(qid2, EventTime.fromRaw(-129387432L), snapshot2),
        namespacedPersistor.persistSnapshot(qid2, EventTime.MaxValue, snapshot3),
        // "maximum qid" (all 1 bits)
        namespacedPersistor.persistSnapshot(qid4, EventTime.MinValue, snapshot0),
        namespacedPersistor.persistSnapshot(qid4, EventTime.fromRaw(2394872938L), snapshot1),
        namespacedPersistor.persistSnapshot(qid4, EventTime.fromRaw(-129387432L), snapshot2),
        namespacedPersistor.persistSnapshot(qid4, EventTime.MaxValue, snapshot3),
      )
    }
  }

  describe("getLatestSnapshot") {

    it("can query the latest snapshot of a node") {
      allOfConcurrent(
        namespacedPersistor.getLatestSnapshot(qid0, EventTime.MaxValue).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assertArraysEqual(snapshot, snapshot3)
        },
        namespacedPersistor.getLatestSnapshot(qid1, EventTime.MaxValue).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assertArraysEqual(snapshot, snapshot4)
        },
        namespacedPersistor.getLatestSnapshot(qid2, EventTime.MaxValue).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assertArraysEqual(snapshot, snapshot3)
        },
        namespacedPersistor.getLatestSnapshot(qid3, EventTime.MaxValue).map { snapshotOpt =>
          assert(snapshotOpt.isEmpty)
        },
        namespacedPersistor.getLatestSnapshot(qid4, EventTime.MaxValue).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assertArraysEqual(snapshot, snapshot3)
        },
      )
    }

    it("can query with EventTime.MinValue as the target time") {
      allOfConcurrent(
        namespacedPersistor.getLatestSnapshot(qid0, EventTime.MinValue).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assertArraysEqual(snapshot, snapshot0)
        },
        namespacedPersistor.getLatestSnapshot(qid1, EventTime.MinValue).map { snapshotOpt =>
          assert(snapshotOpt.isEmpty)
        },
        namespacedPersistor.getLatestSnapshot(qid2, EventTime.MinValue).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assertArraysEqual(snapshot, snapshot0)
        },
        namespacedPersistor.getLatestSnapshot(qid3, EventTime.MinValue).map { snapshotOpt =>
          assert(snapshotOpt.isEmpty)
        },
        namespacedPersistor.getLatestSnapshot(qid4, EventTime.MinValue).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assertArraysEqual(snapshot, snapshot0)
        },
      )
    }

    it("can query with bounds that are not maximums") {
      allOfConcurrent(
        // before any snapshots
        namespacedPersistor.getLatestSnapshot(qid1, EventTime.fromRaw(33L)).map { snapshotOpt =>
          assert(snapshotOpt.isEmpty)
        },
        // right up to one snapshot
        namespacedPersistor.getLatestSnapshot(qid1, EventTime.fromRaw(34L)).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assertArraysEqual(snapshot, snapshot0)
        },
        // right after one snapshot
        namespacedPersistor.getLatestSnapshot(qid1, EventTime.fromRaw(35L)).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assertArraysEqual(snapshot, snapshot0)
        },
        // after some snapshots, before others
        namespacedPersistor.getLatestSnapshot(qid1, EventTime.fromRaw(37L)).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assertArraysEqual(snapshot, snapshot1)
        },
        namespacedPersistor.getLatestSnapshot(qid1, EventTime.fromRaw(38L)).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assertArraysEqual(snapshot, snapshot2)
        },
        namespacedPersistor.getLatestSnapshot(qid1, EventTime.fromRaw(48L)).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assertArraysEqual(snapshot, snapshot4)
        },
      )
    }

    it("can handle unsigned EventTime") {
      allOfConcurrent(
        namespacedPersistor.getLatestSnapshot(qid0, EventTime.fromRaw(-2L)).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assertArraysEqual(snapshot, snapshot2)
        },
        namespacedPersistor.getLatestSnapshot(qid1, EventTime.fromRaw(-2L)).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assertArraysEqual(snapshot, snapshot4)
        },
        namespacedPersistor.getLatestSnapshot(qid2, EventTime.fromRaw(-2L)).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assertArraysEqual(snapshot, snapshot2)
        },
        namespacedPersistor.getLatestSnapshot(qid3, EventTime.fromRaw(-2L)).map { snapshotOpt =>
          assert(snapshotOpt.isEmpty)
        },
        namespacedPersistor.getLatestSnapshot(qid4, EventTime.fromRaw(-2L)).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assertArraysEqual(snapshot, snapshot2)
        },
      )
    }

    it("reads a smaller snapshot written after a larger one with the same timestamp") {
      for {
        _ <- altPersistor1.persistSnapshot(qid0, EventTime.MaxValue, snapshot4)
        _ <- altPersistor1.persistSnapshot(qid0, EventTime.MaxValue, snapshot3)
        snapshotAfter <- altPersistor1.getLatestSnapshot(qid0, EventTime.MaxValue)
      } yield snapshotAfter should contain(snapshot3)
    }
  }

  if (runDeletionTests) {

    describe("deleteSnapshot") {
      it("deletes all snapshots for the given QuineId") {
        forAll(allQids) { qid =>
          for {
            _ <- namespacedPersistor.deleteSnapshots(qid)
            after <- namespacedPersistor.getLatestSnapshot(qid, EventTime.MinValue)
          } yield after shouldBe empty
        }.map(_ => succeed)(ExecutionContext.parasitic)
      }
    }
  }

  if (runDeletionTests) {

    describe("removeStandingQuery") {
      it("successfully does nothing when given a degenerate standing query id to remove while empty") {
        val standingQuery = StandingQueryInfo(
          name = "",
          id = StandingQueryId(new UUID(-1, -1)),
          queryPattern = StandingQueryPattern.DomainGraphNodeStandingQueryPattern(
            dgnId = 1L,
            formatReturnAsStr = true,
            aliasReturnAs = Symbol("foo"),
            includeCancellation = true,
            origin = PatternOrigin.DirectDgb,
          ),
          queueBackpressureThreshold = 1,
          queueMaxSize = 1,
          shouldCalculateResultHashCode = true,
        )
        for {
          _ <- namespacedPersistor.removeStandingQuery(standingQuery)
          after <- namespacedPersistor.getStandingQueries
        } yield after shouldBe empty
      }
    }
  }

  describe("setStandingQueryState") {
    it("can set multiple states for one node") {
      allOfConcurrent(
        namespacedPersistor.setMultipleValuesStandingQueryState(sqId1, qid1, sqPartId1, Some(sqState1)),
        namespacedPersistor.setMultipleValuesStandingQueryState(sqId1, qid1, sqPartId2, Some(sqState2)),
        namespacedPersistor.setMultipleValuesStandingQueryState(sqId1, qid1, sqPartId3, Some(sqState3)),
        namespacedPersistor.setMultipleValuesStandingQueryState(sqId1, qid1, sqPartId4, Some(sqState4)),
      )
    }

    it("can set the same state on multiple nodes") {
      allOfConcurrent(
        namespacedPersistor.setMultipleValuesStandingQueryState(sqId1, qid2, sqPartId1, Some(sqState1)),
        namespacedPersistor.setMultipleValuesStandingQueryState(sqId1, qid3, sqPartId1, Some(sqState2)),
        namespacedPersistor.setMultipleValuesStandingQueryState(sqId1, qid4, sqPartId1, Some(sqState3)),
      )
    }

    it("can set states on various nodes") {
      allOfConcurrent(
        namespacedPersistor.setMultipleValuesStandingQueryState(sqId2, qid4, sqPartId4, Some(sqState1)),
        namespacedPersistor.setMultipleValuesStandingQueryState(sqId4, qid3, sqPartId1, Some(sqState3)),
        namespacedPersistor.setMultipleValuesStandingQueryState(sqId2, qid1, sqPartId3, Some(sqState4)),
        namespacedPersistor.setMultipleValuesStandingQueryState(sqId2, qid1, sqPartId4, Some(sqState3)),
        namespacedPersistor.setMultipleValuesStandingQueryState(sqId3, qid4, sqPartId3, Some(sqState1)),
      )
    }

    if (runDeletionTests) {
      it("can remove states") {
        allOfConcurrent(
          namespacedPersistor.setMultipleValuesStandingQueryState(sqId2, qid1, sqPartId3, None),
          namespacedPersistor.setMultipleValuesStandingQueryState(sqId3, qid2, sqPartId1, None),
        )
      }
    }
  }

  describe("getStandingQueryState") {
    it("can return an empty set of states") {
      allOfConcurrent(
        namespacedPersistor.getMultipleValuesStandingQueryStates(qid0).map { sqStates =>
          (sqStates shouldEqual Map.empty)
        },
      )
    }

    it("can find a single state associated with a node") {
      allOfConcurrent(
        namespacedPersistor.getMultipleValuesStandingQueryStates(qid2).map { sqStates =>
          sqStates.keySet shouldEqual Set(sqId1 -> sqPartId1)
          assertArraysEqual(sqStates(sqId1 -> sqPartId1), sqState1)
        },
      )
    }

    it("can find states associated with multiple queries") {
      allOfConcurrent(
        namespacedPersistor.getMultipleValuesStandingQueryStates(qid1).map { sqStates =>
          sqStates.keySet shouldEqual Set(
            sqId1 -> sqPartId1,
            sqId1 -> sqPartId2,
            sqId1 -> sqPartId3,
            sqId2 -> sqPartId4,
            sqId1 -> sqPartId4,
          )

          assertArraysEqual(sqStates(sqId1 -> sqPartId1), sqState1)
          assertArraysEqual(sqStates(sqId1 -> sqPartId2), sqState2)
          assertArraysEqual(sqStates(sqId1 -> sqPartId3), sqState3)
          assertArraysEqual(sqStates(sqId2 -> sqPartId4), sqState3)
          assertArraysEqual(sqStates(sqId1 -> sqPartId4), sqState4)
        },
        namespacedPersistor.getMultipleValuesStandingQueryStates(qid3).map { sqStates =>
          sqStates.keySet shouldEqual Set(sqId1 -> sqPartId1, sqId4 -> sqPartId1)
          assertArraysEqual(sqStates(sqId1 -> sqPartId1), sqState2)
          assertArraysEqual(sqStates(sqId4 -> sqPartId1), sqState3)
        },
        namespacedPersistor.getMultipleValuesStandingQueryStates(qid4).map { sqStates =>
          sqStates.keySet shouldEqual Set(sqId1 -> sqPartId1, sqId2 -> sqPartId4, sqId3 -> sqPartId3)
          assertArraysEqual(sqStates(sqId1 -> sqPartId1), sqState3)
          assertArraysEqual(sqStates(sqId2 -> sqPartId4), sqState1)
          assertArraysEqual(sqStates(sqId3 -> sqPartId3), sqState1)
        },
      )
    }
  }

  if (runDeletionTests) {
    describe("deleteMultipleValuesStandingQueryStates") {
      it("deletes all multiple value query states for the given QuineId") {
        for {
          before <- namespacedPersistor.getMultipleValuesStandingQueryStates(qid1)
          _ <- namespacedPersistor.deleteMultipleValuesStandingQueryStates(qid1)
          after <- namespacedPersistor.getMultipleValuesStandingQueryStates(qid1)
        } yield {
          // be sure that this test does something since it depends on previous tests adding states
          before should not be empty
          after shouldBe empty
        }
      }
    }
  }

  describe("metadata") {
    it("can set multiple metadata keys") {
      allOfConcurrent(
        persistor.setMetaData(metadata0, Some(snapshot0)),
        persistor.setMetaData(metadata1, Some(snapshot1)),
        persistor.setMetaData(metadata2, Some(snapshot2)),
        persistor.setMetaData(metadata3, Some(snapshot3)),
        persistor.setMetaData(metadata4, Some(snapshot4)),
      )
    }
    it("can set metadata without polluting local metadata") {
      allOfConcurrent(
        persistor.getLocalMetaData(metadata0, 0).map(opt => assert(opt.isEmpty)),
        persistor.getLocalMetaData(metadata1, -1).map(opt => assert(opt.isEmpty)),
        persistor.getLocalMetaData(metadata2, 100).map(opt => assert(opt.isEmpty)),
        persistor.getLocalMetaData(metadata3, 12).map(opt => assert(opt.isEmpty)),
        persistor.getLocalMetaData(metadata4, 1).map(opt => assert(opt.isEmpty)),
      )
    }
    it("can get all metadata") {
      persistor.getAllMetaData().map { metadata =>
        (metadata.keySet shouldEqual Set(metadata0, metadata1, metadata2, metadata3, metadata4))
        assertArraysEqual(metadata(metadata0), snapshot0)
        assertArraysEqual(metadata(metadata1), snapshot1)
        assertArraysEqual(metadata(metadata2), snapshot2)
        assertArraysEqual(metadata(metadata3), snapshot3)
        assertArraysEqual(metadata(metadata4), snapshot4)
      }
    }
    it("can get metadata by key") {
      allOfConcurrent(
        persistor.getMetaData(metadata0).map(datum => assertArraysEqual(datum.value, snapshot0)),
        persistor.getMetaData(metadata1).map(datum => assertArraysEqual(datum.value, snapshot1)),
        persistor.getMetaData(metadata2).map(datum => assertArraysEqual(datum.value, snapshot2)),
        persistor.getMetaData(metadata3).map(datum => assertArraysEqual(datum.value, snapshot3)),
        persistor.getMetaData(metadata4).map(datum => assertArraysEqual(datum.value, snapshot4)),
      )
    }
    it("can set local metadata") {
      allOfConcurrent(
        persistor.setLocalMetaData(metadata0, 0, Some(snapshot4)),
        persistor.setLocalMetaData(metadata1, 1, Some(snapshot3)),
        persistor.setLocalMetaData(metadata2, 2, Some(snapshot0)),
        persistor.setLocalMetaData(metadata3, 3, Some(snapshot1)),
        persistor.setLocalMetaData(metadata4, 4, Some(snapshot2)),
      )
    }
    it("can get local metadata") {
      allOfConcurrent(
        persistor.getLocalMetaData(metadata0, 0).map(datum => assertArraysEqual(datum.value, snapshot4)),
        persistor.getLocalMetaData(metadata1, 1).map(datum => assertArraysEqual(datum.value, snapshot3)),
        persistor.getLocalMetaData(metadata2, 2).map(datum => assertArraysEqual(datum.value, snapshot0)),
        persistor.getLocalMetaData(metadata3, 3).map(datum => assertArraysEqual(datum.value, snapshot1)),
        persistor.getLocalMetaData(metadata4, 4).map(datum => assertArraysEqual(datum.value, snapshot2)),
      )
    }
    it("can overwrite local metadata") {
      allOfConcurrent(
        persistor.setLocalMetaData(metadata0, 0, Some(snapshot0)),
        persistor.setLocalMetaData(metadata1, 1, Some(snapshot1)),
        persistor.setLocalMetaData(metadata2, 2, Some(snapshot2)),
        persistor.setLocalMetaData(metadata3, 3, Some(snapshot3)),
        persistor.setLocalMetaData(metadata4, 4, Some(snapshot4)),
      )
    }
    it("can get overwritten local metadata") {
      allOfConcurrent(
        persistor.getLocalMetaData(metadata0, 0).map(datum => assertArraysEqual(datum.value, snapshot0)),
        persistor.getLocalMetaData(metadata1, 1).map(datum => assertArraysEqual(datum.value, snapshot1)),
        persistor.getLocalMetaData(metadata2, 2).map(datum => assertArraysEqual(datum.value, snapshot2)),
        persistor.getLocalMetaData(metadata3, 3).map(datum => assertArraysEqual(datum.value, snapshot3)),
        persistor.getLocalMetaData(metadata4, 4).map(datum => assertArraysEqual(datum.value, snapshot4)),
      )
    }
    it("can set local metadata without polluting global metadata") {
      // same assertion as "can get metadata by key"
      allOfConcurrent(
        persistor.getMetaData(metadata0).map(datum => assertArraysEqual(datum.value, snapshot0)),
        persistor.getMetaData(metadata1).map(datum => assertArraysEqual(datum.value, snapshot1)),
        persistor.getMetaData(metadata2).map(datum => assertArraysEqual(datum.value, snapshot2)),
        persistor.getMetaData(metadata3).map(datum => assertArraysEqual(datum.value, snapshot3)),
        persistor.getMetaData(metadata4).map(datum => assertArraysEqual(datum.value, snapshot4)),
      )
    }
    if (runDeletionTests) {
      it("can remove metadata by key") {
        allOfConcurrent(
          persistor.setMetaData(metadata0, None),
          persistor.setMetaData(metadata1, None),
          persistor.setMetaData(metadata2, None),
          persistor.setMetaData(metadata3, None),
          persistor.setMetaData(metadata4, None),
        )
      }
      it("can remove metadata without removing local metadata") {
        allOfConcurrent(
          // metadata is really removed
          persistor.getMetaData(metadata0).map(datum => assert(datum.isEmpty)),
          persistor.getMetaData(metadata1).map(datum => assert(datum.isEmpty)),
          persistor.getMetaData(metadata2).map(datum => assert(datum.isEmpty)),
          persistor.getMetaData(metadata3).map(datum => assert(datum.isEmpty)),
          persistor.getMetaData(metadata4).map(datum => assert(datum.isEmpty)),
          // local metadata is still present
          persistor.getLocalMetaData(metadata0, 0).map(datum => assertArraysEqual(datum.value, snapshot0)),
          persistor.getLocalMetaData(metadata1, 1).map(datum => assertArraysEqual(datum.value, snapshot1)),
          persistor.getLocalMetaData(metadata2, 2).map(datum => assertArraysEqual(datum.value, snapshot2)),
          persistor.getLocalMetaData(metadata3, 3).map(datum => assertArraysEqual(datum.value, snapshot3)),
          persistor.getLocalMetaData(metadata4, 4).map(datum => assertArraysEqual(datum.value, snapshot4)),
        )
      }
    }
    it("can get local metadata with getAllMetadata") {
      persistor.getAllMetaData().map[Assertion] { metadata =>
        // all local metadata keys are represented [indirectly]
        for {
          expectedKeySubstring <- Set(metadata0, metadata1, metadata2, metadata3, metadata4)
        } assert(metadata.keySet.exists(_.contains(expectedKeySubstring)))
        // all local metadata values are represented
        for {
          expectedValue <- Set(snapshot0, snapshot1, snapshot2, snapshot3, snapshot4)
        } assert(metadata.values.exists(_ sameElements expectedValue))

        succeed
      }
    }

    if (runDeletionTests) {
      it("can remove local metadata") {
        allOfConcurrent(
          persistor.setLocalMetaData(metadata0, 0, None),
          persistor.setLocalMetaData(metadata1, 1, None),
          persistor.setLocalMetaData(metadata2, 2, None),
          persistor.setLocalMetaData(metadata3, 3, None),
          persistor.setLocalMetaData(metadata4, 4, None),
        ).flatMap(_ =>
          allOfConcurrent(
            persistor.getLocalMetaData(metadata0, 0).map(datum => assert(datum.isEmpty)),
            persistor.getLocalMetaData(metadata1, 1).map(datum => assert(datum.isEmpty)),
            persistor.getLocalMetaData(metadata2, 2).map(datum => assert(datum.isEmpty)),
            persistor.getLocalMetaData(metadata3, 3).map(datum => assert(datum.isEmpty)),
            persistor.getLocalMetaData(metadata4, 4).map(datum => assert(datum.isEmpty)),
          ),
        )
      }
    }
  }

  describe("persistDomainGraphNodes") {
    val generated = generateN[DomainGraphNode](2, 2).map(dgn => DomainGraphNode.id(dgn) -> dgn).toMap
    it("write") {
      persistor.persistDomainGraphNodes(generated) as succeed
    }
    it("read") {
      persistor.getDomainGraphNodes() map { n =>
        assert(
          n === generated,
        ) // TODO `shouldEqual` doesn't quite respect `PropertyValue.Serialized`/`PropertyValue.Deserialized` equivalence.
      }
    }
    if (runDeletionTests) {
      it("delete") {
        for {
          _ <- persistor.removeDomainGraphNodes(generated.keySet)
          n <- persistor.getDomainGraphNodes()
        } yield assert(n.isEmpty)
      }
    }
  }

  describe("persistNodeChangeEvents") {
    val qid = qidFromInt(5)
    // A collection of some generated NodeEvent.WithTime, sorted by time. */
    val generated: Array[NodeChangeEvent] = generateN[NodeChangeEvent](10, 10)
    val withTimeUnsorted = withIncrementedTime(NonEmptyList.fromListUnsafe(generated.toList))
    val sorted = sortedByTime(withTimeUnsorted)

    it("write") {
      //we should be able to write events without worrying about sort order
      namespacedPersistor.persistNodeChangeEvents(qid, withTimeUnsorted) as succeed
    }

    it("read") {
      val minTime = sorted.head.atTime
      val maxTime = sorted.last.atTime
      namespacedPersistor
        .getJournalWithTime(qid, minTime, maxTime, includeDomainIndexEvents = true)
        .map(_ shouldEqual sorted.toList)
    }
  }

  if (runDeletionTests) {
    describe("deleteNodeChangeEvents") {
      it("can delete all record events for a given Quine Id") {
        forAll(allQids)(qid =>
          for {
            _ <- namespacedPersistor.deleteNodeChangeEvents(qid)
            journalEntries <- namespacedPersistor.getNodeChangeEventsWithTime(
              qid,
              EventTime.MinValue,
              EventTime.MaxValue,
            )
          } yield journalEntries shouldBe empty,
        ).map(_ => succeed)(ExecutionContext.parasitic)
      }
    }
  }

  describe("persistDomainIndexEvents") {
    val qid = qidFromInt(1)
    // A collection of some randomly generated NodeEvent.WithTime, sorted by time. */
    val generated: Array[DomainIndexEvent] = generateN[DomainIndexEvent](10, 10)
    val withTimeUnsorted = withIncrementedTime(NonEmptyList.fromListUnsafe(generated.toList))
    val sorted = sortedByTime(withTimeUnsorted)

    it("write") {
      //we should be able to write events without worrying about sort order
      namespacedPersistor.persistDomainIndexEvents(qid, withTimeUnsorted) as succeed
    }
    it("read") {
      val minTime = sorted.head.atTime
      val maxTime = sorted.last.atTime
      namespacedPersistor
        .getJournalWithTime(qid, minTime, maxTime, includeDomainIndexEvents = true)
        .map(e => e shouldEqual sorted.toList)
    }
    if (runDeletionTests) {
      it("delete") {
        for {
          _ <- namespacedPersistor.deleteDomainIndexEvents(qid)
          after <- namespacedPersistor.getDomainIndexEventsWithTime(qid, EventTime.MinValue, EventTime.MaxValue)
        } yield after shouldBe empty
      }
    }
  }

  if (runDeletionTests) {

    describe("deleteDomainIndexEventsByDgnId") {

      val dgnId1 = 11L
      val dgnId2 = 12L

      // a map of (randomQuineId -> (DomainIndexEvent(randomDgnId(0), DomainIndexEvent(randomDgnId(1))
      val events = 1
        .to(5)
        .map(i =>
          idProvider.newQid() -> withIncrementedTime(
            NonEmptyList.of(
              CancelDomainNodeSubscription(dgnId1, qidFromInt(i)),
              CancelDomainNodeSubscription(dgnId2, qidFromInt(i * 10)),
            ),
          ),
        )

      /** returns Success iff the events could be read and deserialized successfully. Returned value is the count of events retrieved * */
      def eventCount(): Future[Int] = Future
        .traverse(events) { case (qid, _) =>
          namespacedPersistor
            .getDomainIndexEventsWithTime(qid, EventTime.MinValue, EventTime.MaxValue)
            .map(_.size)
        }
        .map(_.sum)

      def deleteForDgnId(dgnId: DomainGraphNodeId): Future[Unit] =
        namespacedPersistor.deleteDomainIndexEventsByDgnId(dgnId)

      it("should read back domain index events, and support deletes") {
        for {
          _ <- Future.traverse(events)(t => namespacedPersistor.persistDomainIndexEvents(t._1, t._2))
          firstCount <- eventCount()
          _ <- deleteForDgnId(dgnId1)
          postDeleteCount <- eventCount()
          _ <- deleteForDgnId(dgnId2)
          postSecondDeleteCount <- eventCount()
        } yield {
          assert(firstCount == 10)
          assert(postDeleteCount == 5)
          assert(postSecondDeleteCount == 0)
        }
      }
    }
  }

  describe("Namespaced persistors") {
    val testTimestamp1 = EventTime.fromRaw(127L)
    it("should write snapshots to any namespace at the same QuineId and AtTime") {
      allOfConcurrent(
        altPersistor1.persistSnapshot(qid1, testTimestamp1, snapshot0),
        altPersistor2.persistSnapshot(qid1, testTimestamp1, snapshot1),
      )
    }
    it("should retrieve snapshots from different namespaces with the same QuineId and AtTime") {
      allOfConcurrent(
        altPersistor1.getLatestSnapshot(qid1, testTimestamp1).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assertArraysEqual(snapshot, snapshot0)
        },
        altPersistor2.getLatestSnapshot(qid1, testTimestamp1).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assertArraysEqual(snapshot, snapshot1)
        },
      )
    }
    it("should write journals for the same QuineId to different namespaces") {
      allOfConcurrent(
        altPersistor1.persistNodeChangeEvents(
          qid2,
          NonEmptyList.of(
            NodeEvent.WithTime(event0, EventTime.fromRaw(34L)),
            NodeEvent.WithTime(event2, EventTime.fromRaw(38L)),
            NodeEvent.WithTime(event4, EventTime.fromRaw(44L)),
          ),
        ),
        altPersistor2.persistNodeChangeEvents(
          qid2,
          NonEmptyList.of(
            NodeEvent.WithTime(event1, EventTime.fromRaw(36L)),
            NodeEvent.WithTime(event3, EventTime.fromRaw(40L)),
          ),
        ),
      )
    }
    it("should retrieve journals for the same QuineId from different namespaces") {
      allOfConcurrent(
        altPersistor1.getJournal(qid2, EventTime.MinValue, EventTime.MaxValue, includeDomainIndexEvents = true).map {
          journal =>
            (journal shouldEqual Seq(event0, event2, event4))
        },
        altPersistor2.getJournal(qid2, EventTime.MinValue, EventTime.MaxValue, includeDomainIndexEvents = true).map {
          journal =>
            (journal shouldEqual Seq(event1, event3))
        },
      )
    }
    // Arbitrary DomainIndexEvents
    def generateDomainIndexEventsFromSeed(i: Long): NonEmptyList[NodeEvent.WithTime[DomainIndexEvent]] = {
      val generated = NonEmptyList.fromListUnsafe(generateN[DomainIndexEvent](10, 10, Seed(i)).toList)
      withIncrementedTime(generated)
    }
    val domainIndexEvents1 = generateDomainIndexEventsFromSeed(1L)
    val domainIndexEvents2 = generateDomainIndexEventsFromSeed(2L)
    it("should write (generated) DomainIndexEvents for the same QuineId to different namespaces") {
      allOfConcurrent(
        altPersistor1.persistDomainIndexEvents(
          qid3,
          domainIndexEvents1,
        ),
        altPersistor2.persistDomainIndexEvents(
          qid3,
          domainIndexEvents2,
        ),
      )
    }
    it("should read (generated) DomainIndexEvents for the same QuineId from different namespaces") {
      allOfConcurrent(
        altPersistor1
          .getJournalWithTime(qid3, EventTime.MinValue, EventTime.MaxValue, includeDomainIndexEvents = true)
          .map { journal =>
            (journal shouldEqual domainIndexEvents1.toList)
          },
        altPersistor2
          .getJournalWithTime(qid3, EventTime.MinValue, EventTime.MaxValue, includeDomainIndexEvents = true)
          .map { journal =>
            (journal shouldEqual domainIndexEvents2.toList)
          },
      )
    }
    it(
      "should register MultipleValuesStandingQueryStates associated with the same SqId-QuineId-SqPartId in different namespaces",
    ) {
      allOfConcurrent(
        altPersistor1.setMultipleValuesStandingQueryState(sqId1, qid4, sqPartId1, Some(sqState2)),
        altPersistor2.setMultipleValuesStandingQueryState(sqId1, qid4, sqPartId1, Some(sqState4)),
      )
    }
    it(
      "should read MultipleValuesStandingQueryStates associated with the same SqId-QuineId-SqPartId from different namespaces",
    ) {
      allOfConcurrent(
        altPersistor1.getMultipleValuesStandingQueryStates(qid4).map { sqStates =>
          sqStates.keySet shouldEqual Set(sqId1 -> sqPartId1)
          assertArraysEqual(sqStates(sqId1 -> sqPartId1), sqState2)
        },
        altPersistor2.getMultipleValuesStandingQueryStates(qid4).map { sqStates =>
          sqStates.keySet shouldEqual Set(sqId1 -> sqPartId1)
          assertArraysEqual(sqStates(sqId1 -> sqPartId1), sqState4)
        },
      )
    }
    if (runDeletionTests && runPurgeNamespaceTest) {
      it("should purge one namespace without affecting the other") {
        val alt1Deleted = persistor.deleteNamespace(altNamespace1)
        alt1Deleted.flatMap { _ =>
          allOfConcurrent(
            altPersistor2.getLatestSnapshot(qid1, testTimestamp1).map { snapshotOpt =>
              val snapshot = snapshotOpt.get
              assertArraysEqual(snapshot, snapshot1)
            },
            altPersistor2
              .getJournal(qid2, EventTime.MinValue, EventTime.MaxValue, includeDomainIndexEvents = true)
              .map { journal =>
                (journal shouldEqual Seq(event1, event3))
              },
            altPersistor2
              .getJournalWithTime(qid3, EventTime.MinValue, EventTime.MaxValue, includeDomainIndexEvents = true)
              .map { journal =>
                (journal shouldEqual domainIndexEvents2.toList)
              },
            altPersistor2.getMultipleValuesStandingQueryStates(qid4).map { sqStates =>
              sqStates.keySet shouldEqual Set(sqId1 -> sqPartId1)
              assertArraysEqual(sqStates(sqId1 -> sqPartId1), sqState4)
            },
            getOrInitTestNamespace(altNamespace1)
              .getJournal(qid2, EventTime.MinValue, EventTime.MaxValue, includeDomainIndexEvents = true)
              .map { journal =>
                (journal shouldEqual Seq.empty)
              },
          )
        }
      }
    }

  }
  describe("Default namespace") {
    // resolution of Some("default") to None is handled by routes/apps, and should not reach the persistence agent.
    it(
      s"should only be able to resolve the default namespace as $defaultNamespaceUnnamed and not $defaultNamespaceNamed",
    ) {
      // Have to create the default namespace before using it:
      persistor.initializeOnce
      persistor(defaultNamespaceNamed) should not be defined
      persistor(defaultNamespaceUnnamed) shouldBe defined
    }
  }
}
object PersistenceAgentSpec extends should.Matchers {
  def assertArraysEqual(l: Array[Byte], r: Array[Byte]): Assertion =
    l should contain theSameElementsInOrderAs r
//    l shouldEqual r
}
