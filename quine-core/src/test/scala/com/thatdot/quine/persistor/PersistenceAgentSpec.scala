package com.thatdot.quine.persistor

import java.util.UUID

import scala.compat.ExecutionContexts
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.Random

import org.apache.pekko.actor.ActorSystem

import cats.data.NonEmptyList
import cats.syntax.functor._
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.scalatest.funspec.AsyncFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, BeforeAndAfterAll, Inspectors, OptionValues}

import com.thatdot.quine.graph.DomainIndexEvent.CancelDomainNodeSubscription
import com.thatdot.quine.graph.Generators.{generate1, generateN}
import com.thatdot.quine.graph.PropertyEvent.PropertySet
import com.thatdot.quine.graph.{
  ArbitraryInstances,
  DomainIndexEvent,
  EventTime,
  MultipleValuesStandingQueryPartId,
  NodeChangeEvent,
  NodeEvent,
  PatternOrigin,
  QuineUUIDProvider,
  ScalaTestInstances,
  StandingQuery,
  StandingQueryId,
  StandingQueryPattern
}
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.model.{DomainGraphNode, PropertyValue, QuineId, QuineValue}

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
    with Matchers
    with Inspectors
    with OptionValues
    with ArbitraryInstances
    with ScalaTestInstances {

  implicit val system: ActorSystem = ActorSystem("test-system")

  // Override this if tests need to be skipped
  def runnable: Boolean = true

  def persistor: PersistenceAgent

  def withRandomTime[T <: NodeEvent](events: NonEmptyList[T]): NonEmptyList[NodeEvent.WithTime[T]] =
    events.map(n => NodeEvent.WithTime(n, EventTime.fromRaw(Random.nextLong())))

  def sortedByTime[T <: NodeEvent](events: NonEmptyList[NodeEvent.WithTime[T]]): NonEmptyList[NodeEvent.WithTime[T]] =
    events.sortBy(_.atTime)
  override def afterAll(): Unit = {
    Await.result(persistor.shutdown(), 10.seconds)
    Await.result(system.terminate(), 10.seconds)
    ()
  }

  val idProvider: QuineUUIDProvider.type = QuineUUIDProvider

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
    new UUID(1096520000288222086L, 748609736042323025L)
  )
  val sqPartId3: MultipleValuesStandingQueryPartId = MultipleValuesStandingQueryPartId(
    new UUID(-1613026160293696877L, 6732331004029745690L)
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
        persistor.persistNodeChangeEvents(
          qid1,
          NonEmptyList.of(
            NodeEvent.WithTime(event0, EventTime.fromRaw(34L)),
            NodeEvent.WithTime(event1, EventTime.fromRaw(36L)),
            NodeEvent.WithTime(event2, EventTime.fromRaw(38L)),
            NodeEvent.WithTime(event3, EventTime.fromRaw(40L)),
            NodeEvent.WithTime(event4, EventTime.fromRaw(44L))
          )
        )
      )
    }

    it("supports EventTime.MaxValue and EventTime.MinValue") {
      allOfConcurrent(
        // "minimum qid" (all 0 bits)
        persistor.persistNodeChangeEvents(
          qid0,
          NonEmptyList.of(
            NodeEvent.WithTime(event0, EventTime.MinValue),
            NodeEvent.WithTime(event1, EventTime.fromRaw(2394872938L)),
            NodeEvent.WithTime(event2, EventTime.fromRaw(-129387432L)),
            NodeEvent.WithTime(event3, EventTime.MaxValue)
          )
        ),
        // in between qid
        persistor.persistNodeChangeEvents(
          qid2,
          NonEmptyList.of(
            NodeEvent.WithTime(event0, EventTime.MinValue),
            NodeEvent.WithTime(event1, EventTime.fromRaw(2394872938L)),
            NodeEvent.WithTime(event2, EventTime.fromRaw(-129387432L)),
            NodeEvent.WithTime(event3, EventTime.MaxValue)
          )
        ),
        // "maximum qid" (all 1 bits)
        persistor.persistNodeChangeEvents(
          qid4,
          NonEmptyList.of(
            NodeEvent.WithTime(event0, EventTime.MinValue),
            NodeEvent.WithTime(event1, EventTime.fromRaw(2394872938L)),
            NodeEvent.WithTime(event2, EventTime.fromRaw(-129387432L)),
            NodeEvent.WithTime(event3, EventTime.MaxValue)
          )
        )
      )
    }
  }

  describe("getJournal") {

    it("can query a full journal of a node") {
      allOfConcurrent(
        persistor.getJournal(qid0, EventTime.MinValue, EventTime.MaxValue, includeDomainIndexEvents = true).map {
          journal =>
            assert(journal === Seq(event0, event1, event2, event3))
        },
        persistor.getJournal(qid1, EventTime.MinValue, EventTime.MaxValue, includeDomainIndexEvents = true).map {
          journal =>
            assert(journal === Seq(event0, event1, event2, event3, event4))
        },
        persistor.getJournal(qid2, EventTime.MinValue, EventTime.MaxValue, includeDomainIndexEvents = true).map {
          journal =>
            assert(journal === Seq(event0, event1, event2, event3))
        },
        persistor.getJournal(qid3, EventTime.MinValue, EventTime.MaxValue, includeDomainIndexEvents = true).map {
          journal =>
            assert(journal === Seq.empty)
        },
        persistor.getJournal(qid4, EventTime.MinValue, EventTime.MaxValue, includeDomainIndexEvents = true).map {
          journal =>
            assert(journal === Seq(event0, event1, event2, event3))
        }
      )
    }

    it("can query with EventTime.MinValue lower bound") {
      allOfConcurrent(
        // before anything
        persistor.getJournal(qid1, EventTime.MinValue, EventTime.fromRaw(2L), includeDomainIndexEvents = true).map {
          journal =>
            assert(journal === Seq.empty)
        },
        // right up to one event
        persistor.getJournal(qid1, EventTime.MinValue, EventTime.fromRaw(34L), includeDomainIndexEvents = true).map {
          journal =>
            assert(journal === Seq(event0))
        },
        // right after one event
        persistor.getJournal(qid1, EventTime.MinValue, EventTime.fromRaw(37L), includeDomainIndexEvents = true).map {
          journal =>
            assert(journal === Seq(event0, event1))
        },
        // after all events
        persistor.getJournal(qid1, EventTime.MinValue, EventTime.fromRaw(48L), includeDomainIndexEvents = true).map {
          journal =>
            assert(journal === Seq(event0, event1, event2, event3, event4))
        },
        // first event is the min value
        persistor.getJournal(qid0, EventTime.MinValue, EventTime.MinValue, includeDomainIndexEvents = true).map {
          journal =>
            assert(journal === Seq(event0))
        },
        persistor.getJournal(qid2, EventTime.MinValue, EventTime.MinValue, includeDomainIndexEvents = true).map {
          journal =>
            assert(journal === Seq(event0))
        },
        persistor.getJournal(qid4, EventTime.MinValue, EventTime.MinValue, includeDomainIndexEvents = true).map {
          journal =>
            assert(journal === Seq(event0))
        }
      )
    }

    it("can query with EventTime.MaxValue upper bound") {
      allOfConcurrent(
        // before anything
        persistor.getJournal(qid1, EventTime.fromRaw(2L), EventTime.MaxValue, includeDomainIndexEvents = true).map {
          journal =>
            assert(journal === Seq(event0, event1, event2, event3, event4))
        },
        // before one event
        persistor.getJournal(qid1, EventTime.fromRaw(42L), EventTime.MaxValue, includeDomainIndexEvents = true).map {
          journal =>
            assert(journal === Seq(event4))
        },
        // starting exactly at one event
        persistor.getJournal(qid1, EventTime.fromRaw(44L), EventTime.MaxValue, includeDomainIndexEvents = true).map {
          journal =>
            assert(journal === Seq(event4))
        },
        // starting exactly at the first event
        persistor.getJournal(qid1, EventTime.fromRaw(34L), EventTime.MaxValue, includeDomainIndexEvents = true).map {
          journal =>
            assert(journal === Seq(event0, event1, event2, event3, event4))
        },
        // after all events
        persistor.getJournal(qid1, EventTime.fromRaw(48L), EventTime.MaxValue, includeDomainIndexEvents = true).map {
          journal =>
            assert(journal === Seq.empty)
        },
        // first event is the min value
        persistor.getJournal(qid0, EventTime.MaxValue, EventTime.MaxValue, includeDomainIndexEvents = true).map {
          journal =>
            assert(journal === Seq(event3))
        },
        persistor.getJournal(qid2, EventTime.MaxValue, EventTime.MaxValue, includeDomainIndexEvents = true).map {
          journal =>
            assert(journal === Seq(event3))
        },
        persistor.getJournal(qid4, EventTime.MaxValue, EventTime.MaxValue, includeDomainIndexEvents = true).map {
          journal =>
            assert(journal === Seq(event3))
        }
      )
    }

    it("can query with bounds that are not maximums") {
      allOfConcurrent(
        // start and end before any events
        persistor.getJournal(qid1, EventTime.fromRaw(2L), EventTime.fromRaw(33L), includeDomainIndexEvents = true).map {
          journal =>
            assert(journal === Seq.empty)
        },
        // start and end between events
        persistor
          .getJournal(qid1, EventTime.fromRaw(42L), EventTime.fromRaw(43L), includeDomainIndexEvents = true)
          .map { journal =>
            assert(journal === Seq.empty)
          },
        // right up to one event
        persistor.getJournal(qid1, EventTime.fromRaw(2L), EventTime.fromRaw(34L), includeDomainIndexEvents = true).map {
          journal =>
            assert(journal === Seq(event0))
        },
        // right after one event
        persistor.getJournal(qid1, EventTime.fromRaw(2L), EventTime.fromRaw(35L), includeDomainIndexEvents = true).map {
          journal =>
            assert(journal === Seq(event0))
        },
        // starting exactly at one event
        persistor
          .getJournal(qid1, EventTime.fromRaw(34L), EventTime.fromRaw(35L), includeDomainIndexEvents = true)
          .map { journal =>
            assert(journal === Seq(event0))
          },
        // start and end on events
        persistor
          .getJournal(qid1, EventTime.fromRaw(36L), EventTime.fromRaw(40L), includeDomainIndexEvents = true)
          .map { journal =>
            assert(journal === Seq(event1, event2, event3))
          },
        persistor
          .getJournal(qid1, EventTime.fromRaw(34L), EventTime.fromRaw(48L), includeDomainIndexEvents = true)
          .map { journal =>
            assert(
              journal === Seq(
                event0,
                event1,
                event2,
                event3,
                event4
              )
            )
          }
      )
    }

    it("can handle unsigned EventTime") {
      allOfConcurrent(
        // event time needs to be treated as unsigned
        persistor
          .getJournal(qid0, EventTime.fromRaw(-200000000L), EventTime.fromRaw(-2L), includeDomainIndexEvents = true)
          .map { journal =>
            assert(journal === Seq(event2))
          },
        persistor
          .getJournal(qid2, EventTime.fromRaw(-200000000L), EventTime.fromRaw(-2L), includeDomainIndexEvents = true)
          .map { journal =>
            assert(journal === Seq(event2))
          },
        persistor
          .getJournal(qid4, EventTime.fromRaw(-200000000L), EventTime.fromRaw(-2L), includeDomainIndexEvents = true)
          .map { journal =>
            assert(journal === Seq(event2))
          },
        // event time needs to be treated as unsigned
        persistor.getJournal(qid0, EventTime.fromRaw(2L), EventTime.fromRaw(-2L), includeDomainIndexEvents = true).map {
          journal =>
            assert(journal === Seq(event1, event2))
        },
        persistor.getJournal(qid2, EventTime.fromRaw(2L), EventTime.fromRaw(-2L), includeDomainIndexEvents = true).map {
          journal =>
            assert(journal === Seq(event1, event2))
        },
        persistor.getJournal(qid4, EventTime.fromRaw(2L), EventTime.fromRaw(-2L), includeDomainIndexEvents = true).map {
          journal =>
            assert(journal === Seq(event1, event2))
        }
      )
    }
  }

  describe("persistSnapshot") {
    it("can record snapshots at various time") {
      allOfConcurrent(
        persistor.persistSnapshot(qid1, EventTime.fromRaw(34L), snapshot0),
        persistor.persistSnapshot(qid1, EventTime.fromRaw(36L), snapshot1),
        persistor.persistSnapshot(qid1, EventTime.fromRaw(38L), snapshot2),
        persistor.persistSnapshot(qid1, EventTime.fromRaw(40L), snapshot3),
        persistor.persistSnapshot(qid1, EventTime.fromRaw(44L), snapshot4)
      )
    }

    it("supports EventTime.MaxValue and EventTime.MinValue") {
      allOfConcurrent(
        // "minimum qid" (all 0 bits)
        persistor.persistSnapshot(qid0, EventTime.MinValue, snapshot0),
        persistor.persistSnapshot(qid0, EventTime.fromRaw(2394872938L), snapshot1),
        persistor.persistSnapshot(qid0, EventTime.fromRaw(-129387432L), snapshot2),
        persistor.persistSnapshot(qid0, EventTime.MaxValue, snapshot3),
        // in between qid
        persistor.persistSnapshot(qid2, EventTime.MinValue, snapshot0),
        persistor.persistSnapshot(qid2, EventTime.fromRaw(2394872938L), snapshot1),
        persistor.persistSnapshot(qid2, EventTime.fromRaw(-129387432L), snapshot2),
        persistor.persistSnapshot(qid2, EventTime.MaxValue, snapshot3),
        // "maximum qid" (all 1 bits)
        persistor.persistSnapshot(qid4, EventTime.MinValue, snapshot0),
        persistor.persistSnapshot(qid4, EventTime.fromRaw(2394872938L), snapshot1),
        persistor.persistSnapshot(qid4, EventTime.fromRaw(-129387432L), snapshot2),
        persistor.persistSnapshot(qid4, EventTime.MaxValue, snapshot3)
      )
    }
  }

  describe("getLatestSnapshot") {

    it("can query the latest snapshot of a node") {
      allOfConcurrent(
        persistor.getLatestSnapshot(qid0, EventTime.MaxValue).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assert(snapshot === snapshot3)
        },
        persistor.getLatestSnapshot(qid1, EventTime.MaxValue).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assert(snapshot === snapshot4)
        },
        persistor.getLatestSnapshot(qid2, EventTime.MaxValue).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assert(snapshot === snapshot3)
        },
        persistor.getLatestSnapshot(qid3, EventTime.MaxValue).map { snapshotOpt =>
          assert(snapshotOpt.isEmpty)
        },
        persistor.getLatestSnapshot(qid4, EventTime.MaxValue).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assert(snapshot === snapshot3)
        }
      )
    }

    it("can query with EventTime.MinValue as the target time") {
      allOfConcurrent(
        persistor.getLatestSnapshot(qid0, EventTime.MinValue).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assert(snapshot === snapshot0)
        },
        persistor.getLatestSnapshot(qid1, EventTime.MinValue).map { snapshotOpt =>
          assert(snapshotOpt.isEmpty)
        },
        persistor.getLatestSnapshot(qid2, EventTime.MinValue).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assert(snapshot === snapshot0)
        },
        persistor.getLatestSnapshot(qid3, EventTime.MinValue).map { snapshotOpt =>
          assert(snapshotOpt.isEmpty)
        },
        persistor.getLatestSnapshot(qid4, EventTime.MinValue).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assert(snapshot === snapshot0)
        }
      )
    }

    it("can query with bounds that are not maximums") {
      allOfConcurrent(
        // before any snapshots
        persistor.getLatestSnapshot(qid1, EventTime.fromRaw(33L)).map { snapshotOpt =>
          assert(snapshotOpt.isEmpty)
        },
        // right up to one snapshot
        persistor.getLatestSnapshot(qid1, EventTime.fromRaw(34L)).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assert(snapshot === snapshot0)
        },
        // right after one snapshot
        persistor.getLatestSnapshot(qid1, EventTime.fromRaw(35L)).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assert(snapshot === snapshot0)
        },
        // after some snapshots, before others
        persistor.getLatestSnapshot(qid1, EventTime.fromRaw(37L)).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assert(snapshot === snapshot1)
        },
        persistor.getLatestSnapshot(qid1, EventTime.fromRaw(38L)).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assert(snapshot === snapshot2)
        },
        persistor.getLatestSnapshot(qid1, EventTime.fromRaw(48L)).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assert(snapshot === snapshot4)
        }
      )
    }

    it("can handle unsigned EventTime") {
      allOfConcurrent(
        persistor.getLatestSnapshot(qid0, EventTime.fromRaw(-2L)).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assert(snapshot === snapshot2)
        },
        persistor.getLatestSnapshot(qid1, EventTime.fromRaw(-2L)).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assert(snapshot === snapshot4)
        },
        persistor.getLatestSnapshot(qid2, EventTime.fromRaw(-2L)).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assert(snapshot === snapshot2)
        },
        persistor.getLatestSnapshot(qid3, EventTime.fromRaw(-2L)).map { snapshotOpt =>
          assert(snapshotOpt.isEmpty)
        },
        persistor.getLatestSnapshot(qid4, EventTime.fromRaw(-2L)).map { snapshotOpt =>
          val snapshot = snapshotOpt.get
          assert(snapshot === snapshot2)
        }
      )
    }
  }

  describe("deleteSnapshot") {
    it("deletes all snapshots for the given QuineId") {
      forAll(allQids) { qid =>
        for {
          _ <- persistor.deleteSnapshots(qid)
          after <- persistor.getLatestSnapshot(qid, EventTime.MinValue)
        } yield after shouldBe empty
      }.map(_ => succeed)(ExecutionContexts.parasitic)
    }
  }

  describe("removeStandingQuery") {
    it("successfully does nothing when given a degenerate standing query id to remove while empty") {
      val standingQuery = StandingQuery(
        name = "",
        id = StandingQueryId(new UUID(-1, -1)),
        query = StandingQueryPattern.DomainGraphNodeStandingQueryPattern(
          dgnId = 1L,
          formatReturnAsStr = true,
          aliasReturnAs = Symbol("foo"),
          includeCancellation = true,
          origin = PatternOrigin.DirectDgb
        ),
        queueBackpressureThreshold = 1,
        queueMaxSize = 1,
        shouldCalculateResultHashCode = true
      )
      for {
        _ <- persistor.removeStandingQuery(standingQuery)
        after <- persistor.getStandingQueries
      } yield after shouldBe empty
    }
  }

  describe("setStandingQueryState") {
    it("can set multiple states for one node") {
      allOfConcurrent(
        persistor.setMultipleValuesStandingQueryState(sqId1, qid1, sqPartId1, Some(sqState1)),
        persistor.setMultipleValuesStandingQueryState(sqId1, qid1, sqPartId2, Some(sqState2)),
        persistor.setMultipleValuesStandingQueryState(sqId1, qid1, sqPartId3, Some(sqState3)),
        persistor.setMultipleValuesStandingQueryState(sqId1, qid1, sqPartId4, Some(sqState4))
      )
    }

    it("can set the same state on multiple nodes") {
      allOfConcurrent(
        persistor.setMultipleValuesStandingQueryState(sqId1, qid2, sqPartId1, Some(sqState1)),
        persistor.setMultipleValuesStandingQueryState(sqId1, qid3, sqPartId1, Some(sqState2)),
        persistor.setMultipleValuesStandingQueryState(sqId1, qid4, sqPartId1, Some(sqState3))
      )
    }

    it("can set states on various nodes") {
      allOfConcurrent(
        persistor.setMultipleValuesStandingQueryState(sqId2, qid4, sqPartId4, Some(sqState1)),
        persistor.setMultipleValuesStandingQueryState(sqId4, qid3, sqPartId1, Some(sqState3)),
        persistor.setMultipleValuesStandingQueryState(sqId2, qid1, sqPartId3, Some(sqState4)),
        persistor.setMultipleValuesStandingQueryState(sqId2, qid1, sqPartId4, Some(sqState3)),
        persistor.setMultipleValuesStandingQueryState(sqId3, qid4, sqPartId3, Some(sqState1))
      )
    }

    it("can remove states") {
      allOfConcurrent(
        persistor.setMultipleValuesStandingQueryState(sqId2, qid1, sqPartId3, None),
        persistor.setMultipleValuesStandingQueryState(sqId3, qid2, sqPartId1, None)
      )
    }
  }

  describe("getStandingQueryState") {
    it("can return an empty set of states") {
      allOfConcurrent(
        persistor.getMultipleValuesStandingQueryStates(qid0).map { sqStates =>
          assert(sqStates === Map.empty)
        }
      )
    }

    it("can find a single state associated with a node") {
      allOfConcurrent(
        persistor.getMultipleValuesStandingQueryStates(qid2).map { sqStates =>
          assert(sqStates.size === 1)
          assert(sqStates(sqId1 -> sqPartId1) === sqState1)
        }
      )
    }

    it("can find states associated with multiple queries") {
      allOfConcurrent(
        persistor.getMultipleValuesStandingQueryStates(qid1).map { sqStates =>
          assert(sqStates.size === 5)
          assert(sqStates(sqId1 -> sqPartId1) === sqState1)
          assert(sqStates(sqId1 -> sqPartId2) === sqState2)
          assert(sqStates(sqId1 -> sqPartId3) === sqState3)
          assert(sqStates(sqId2 -> sqPartId4) === sqState3)
          assert(sqStates(sqId1 -> sqPartId4) === sqState4)
        },
        persistor.getMultipleValuesStandingQueryStates(qid3).map { sqStates =>
          assert(sqStates.size === 2)
          assert(sqStates(sqId1 -> sqPartId1) === sqState2)
          assert(sqStates(sqId4 -> sqPartId1) === sqState3)
        },
        persistor.getMultipleValuesStandingQueryStates(qid4).map { sqStates =>
          assert(sqStates.size === 3)
          assert(sqStates(sqId1 -> sqPartId1) === sqState3)
          assert(sqStates(sqId2 -> sqPartId4) === sqState1)
          assert(sqStates(sqId3 -> sqPartId3) === sqState1)
        }
      )
    }
  }

  describe("deleteMultipleValuesStandingQueryStates") {
    it("deletes all multiple value query states for the given QuineId") {
      for {
        before <- persistor.getMultipleValuesStandingQueryStates(qid1)
        _ <- persistor.deleteMultipleValuesStandingQueryStates(qid1)
        after <- persistor.getMultipleValuesStandingQueryStates(qid1)
      } yield {
        // be sure that this test does something since it depends on previous tests adding states
        before should not be empty
        after shouldBe empty
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
        persistor.setMetaData(metadata4, Some(snapshot4))
      )
    }
    it("can set metadata without polluting local metadata") {
      allOfConcurrent(
        persistor.getLocalMetaData(metadata0, 0).map(opt => assert(opt.isEmpty)),
        persistor.getLocalMetaData(metadata1, -1).map(opt => assert(opt.isEmpty)),
        persistor.getLocalMetaData(metadata2, 100).map(opt => assert(opt.isEmpty)),
        persistor.getLocalMetaData(metadata3, 12).map(opt => assert(opt.isEmpty)),
        persistor.getLocalMetaData(metadata4, 1).map(opt => assert(opt.isEmpty))
      )
    }
    it("can get all metadata") {
      persistor.getAllMetaData().map { metadata =>
        assert(metadata.keySet === Set(metadata0, metadata1, metadata2, metadata3, metadata4))
        assert(metadata(metadata0) === snapshot0)
        assert(metadata(metadata1) === snapshot1)
        assert(metadata(metadata2) === snapshot2)
        assert(metadata(metadata3) === snapshot3)
        assert(metadata(metadata4) === snapshot4)
      }
    }
    it("can get metadata by key") {
      allOfConcurrent(
        persistor.getMetaData(metadata0).map(datum => assert(datum.value === snapshot0)),
        persistor.getMetaData(metadata1).map(datum => assert(datum.value === snapshot1)),
        persistor.getMetaData(metadata2).map(datum => assert(datum.value === snapshot2)),
        persistor.getMetaData(metadata3).map(datum => assert(datum.value === snapshot3)),
        persistor.getMetaData(metadata4).map(datum => assert(datum.value === snapshot4))
      )
    }
    it("can set local metadata") {
      allOfConcurrent(
        persistor.setLocalMetaData(metadata0, 0, Some(snapshot0)),
        persistor.setLocalMetaData(metadata1, 1, Some(snapshot1)),
        persistor.setLocalMetaData(metadata2, 2, Some(snapshot2)),
        persistor.setLocalMetaData(metadata3, 3, Some(snapshot3)),
        persistor.setLocalMetaData(metadata4, 4, Some(snapshot4))
      )
    }
    it("can get local metadata") {
      allOfConcurrent(
        persistor.getLocalMetaData(metadata0, 0).map(datum => assert(datum.value === snapshot0)),
        persistor.getLocalMetaData(metadata1, 1).map(datum => assert(datum.value === snapshot1)),
        persistor.getLocalMetaData(metadata2, 2).map(datum => assert(datum.value === snapshot2)),
        persistor.getLocalMetaData(metadata3, 3).map(datum => assert(datum.value === snapshot3)),
        persistor.getLocalMetaData(metadata4, 4).map(datum => assert(datum.value === snapshot4))
      )
    }
    it("can set local metadata without polluting global metadata") {
      // same assertion as "can get metadata by key"
      allOfConcurrent(
        persistor.getMetaData(metadata0).map(datum => assert(datum.value === snapshot0)),
        persistor.getMetaData(metadata1).map(datum => assert(datum.value === snapshot1)),
        persistor.getMetaData(metadata2).map(datum => assert(datum.value === snapshot2)),
        persistor.getMetaData(metadata3).map(datum => assert(datum.value === snapshot3)),
        persistor.getMetaData(metadata4).map(datum => assert(datum.value === snapshot4))
      )
    }
    it("can remove metadata by key") {
      allOfConcurrent(
        persistor.setMetaData(metadata0, None),
        persistor.setMetaData(metadata1, None),
        persistor.setMetaData(metadata2, None),
        persistor.setMetaData(metadata3, None),
        persistor.setMetaData(metadata4, None)
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
        persistor.getLocalMetaData(metadata0, 0).map(datum => assert(datum.value === snapshot0)),
        persistor.getLocalMetaData(metadata1, 1).map(datum => assert(datum.value === snapshot1)),
        persistor.getLocalMetaData(metadata2, 2).map(datum => assert(datum.value === snapshot2)),
        persistor.getLocalMetaData(metadata3, 3).map(datum => assert(datum.value === snapshot3)),
        persistor.getLocalMetaData(metadata4, 4).map(datum => assert(datum.value === snapshot4))
      )
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
        } assert(metadata.values.exists(_ === expectedValue))

        succeed
      }
    }
    it("can remove local metadata") {
      allOfConcurrent(
        persistor.setLocalMetaData(metadata0, 0, None),
        persistor.setLocalMetaData(metadata1, 1, None),
        persistor.setLocalMetaData(metadata2, 2, None),
        persistor.setLocalMetaData(metadata3, 3, None),
        persistor.setLocalMetaData(metadata4, 4, None)
      ).flatMap(_ =>
        allOfConcurrent(
          persistor.getLocalMetaData(metadata0, 0).map(datum => assert(datum.isEmpty)),
          persistor.getLocalMetaData(metadata1, 1).map(datum => assert(datum.isEmpty)),
          persistor.getLocalMetaData(metadata2, 2).map(datum => assert(datum.isEmpty)),
          persistor.getLocalMetaData(metadata3, 3).map(datum => assert(datum.isEmpty)),
          persistor.getLocalMetaData(metadata4, 4).map(datum => assert(datum.isEmpty))
        )
      )
    }
  }

  describe("persistDomainGraphNodes") {
    val generated = generateN[DomainGraphNode](2, 2).map(dgn => DomainGraphNode.id(dgn) -> dgn).toMap
    it("write") {
      persistor.persistDomainGraphNodes(generated) as succeed
    }
    it("read") {
      persistor.getDomainGraphNodes() map { n =>
        assert(n === generated)
      }
    }
    it("delete") {
      for {
        _ <- persistor.removeDomainGraphNodes(generated.keySet)
        n <- persistor.getDomainGraphNodes()
      } yield assert(n.isEmpty)
    }
  }

  describe("persistNodeChangeEvents") {
//Using this instead of arbitraries to avoid repeated boundary values that can cause spurious test failures.
    val qid = idProvider.newQid()
    // A collection of some randomly generated NodeEvent.WithTime, sorted by time. */
    val generated: Array[NodeChangeEvent] = generateN[NodeChangeEvent](10, Random.nextInt(10) + 1)
    val withTimeUnsorted = withRandomTime(NonEmptyList.fromListUnsafe(generated.toList))
    val sorted = sortedByTime(withTimeUnsorted)

    it("write") {
      //we should be able to write events without worrying about sort order
      persistor.persistNodeChangeEvents(qid, withTimeUnsorted) as succeed
    }

    it("read") {
      val minTime = sorted.head.atTime
      val maxTime = sorted.last.atTime
      persistor
        .getJournalWithTime(qid, minTime, maxTime, includeDomainIndexEvents = true)
        .map(_ should contain theSameElementsInOrderAs sorted.toList)
    }
  }

  describe("deleteNodeChangeEvents") {
    it("can delete all record events for a given Quine Id") {
      forAll(allQids)(qid =>
        for {
          _ <- persistor.deleteNodeChangeEvents(qid)
          journalEntries <- persistor.getNodeChangeEventsWithTime(qid, EventTime.MinValue, EventTime.MaxValue)
        } yield journalEntries shouldBe empty
      ).map(_ => succeed)(ExecutionContexts.parasitic)
    }
  }

  describe("persistDomainIndexEvents") {
    //Using this instead of arbitraries to avoid repeated boundary values that can cause spurious test failures.
    val qid = idProvider.newQid()
    // A collection of some randomly generated NodeEvent.WithTime, sorted by time. */
    val generated: Array[DomainIndexEvent] = generateN[DomainIndexEvent](10, Random.nextInt(10) + 1)
    val withTimeUnsorted = withRandomTime(NonEmptyList.fromListUnsafe(generated.toList))
    val sorted = sortedByTime(withTimeUnsorted)

    it("write") {
      //we should be able to write events without worrying about sort order
      persistor.persistDomainIndexEvents(qid, withTimeUnsorted) as succeed
    }
    it("read") {
      val minTime = sorted.head.atTime
      val maxTime = sorted.last.atTime
      persistor
        .getJournalWithTime(qid, minTime, maxTime, includeDomainIndexEvents = true)
        .map(e => assert(e === sorted.toList))
    }
    it("delete") {
      for {
        _ <- persistor.deleteDomainIndexEvents(qid)
        after <- persistor.getDomainIndexEventsWithTime(qid, EventTime.MinValue, EventTime.MaxValue)
      } yield after shouldBe empty
    }
  }

  describe("deleteDomainIndexEventsByDgnId") {
    // Note that this test has occasionally failed in CI for MapDB or RocksDB datastores.
    // The larger list seems to mitigate these failures. The delete works correctly but
    // this should be investigated if intermittent failures are seen in testing.
    val dgnIds = Gen.listOfN(20, arbitrary[DomainGraphNodeId]).sample.get
    // a map of (randomQuineId -> (DomainIndexEvent(randomDgnId(0), DomainIndexEvent(randomDgnId(1))
    val events = 1
      .to(5)
      .map(_ =>
        idProvider.newQid() -> withRandomTime(
          NonEmptyList.of(
            CancelDomainNodeSubscription(dgnIds(0), generate1[QuineId](1)),
            CancelDomainNodeSubscription(dgnIds(1), generate1[QuineId](1))
          )
        )
      )

    /** returns Success iff the events could be read and deserialized successfully. Returned value is the count of events retrieved * */
    def eventCount(): Future[Int] = Future
      .traverse(events) { case (qid, _) =>
        persistor
          .getDomainIndexEventsWithTime(qid, EventTime.MinValue, EventTime.MaxValue)
          .map(_.size)
      }
      .map(_.sum)

    def deleteForDgnId(dgnId: DomainGraphNodeId): Future[Unit] =
      persistor.deleteDomainIndexEventsByDgnId(dgnId)

    // TODO: this randomly fails on CI for InMemoryPersistorSpec
    // Maybe move it out of here and into something else which everything but InMemoryPeristenceAgentSpec uses?
    ignore("should read back domain index events, and support deletes") {
      for {
        _ <- Future.traverse(events)(t => persistor.persistDomainIndexEvents(t._1, t._2))
        firstCount <- eventCount()
        _ <- deleteForDgnId(dgnIds(0))
        postDeleteCount <- eventCount()
        _ <- deleteForDgnId(dgnIds(1))
        postSecondDeleteCount <- eventCount()
      } yield {
        assert(firstCount == 10)
        assert(postDeleteCount == 5)
        assert(postSecondDeleteCount == 0)
      }
    }

  }

}
