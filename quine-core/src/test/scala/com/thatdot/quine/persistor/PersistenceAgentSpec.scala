package com.thatdot.quine.persistor

import java.util.UUID

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

import akka.actor.ActorSystem

import org.scalatest.funspec.AsyncFunSpec
import org.scalatest.{Assertion, BeforeAndAfterAll}

import com.thatdot.quine.graph.{EventTime, NodeChangeEvent, QuineUUIDProvider, StandingQueryId, StandingQueryPartId}
import com.thatdot.quine.model.{PropertyValue, QuineId, QuineValue}

/** Abstract test suite that can be implemented just by specifying `persistor`.
  * The intent is that every new persistor should be able to extend this
  * abstract `Spec` and quickly be able to check that the expected persistor
  * properties hold
  *
  * TODO: add tests for snapshots, standing query states, and metadata
  */
abstract class PersistenceAgentSpec extends AsyncFunSpec with BeforeAndAfterAll {

  implicit val system: ActorSystem = ActorSystem()

  // Override this if tests need to be skipped
  def runnable: Boolean = true

  def persistor: PersistenceAgent

  override def afterAll(): Unit =
    Await.result(persistor.shutdown(), 10.seconds)

  val idProvider = QuineUUIDProvider

  val qid0: QuineId = idProvider.customIdStringToQid("00000000-0000-0000-0000-000000000000").get
  val qid1: QuineId = idProvider.customIdStringToQid("00000000-0000-0000-0000-000000000001").get
  val qid2: QuineId = idProvider.customIdStringToQid("77747265-9ea9-4d61-a419-d7758c8b097a").get
  val qid3: QuineId = idProvider.customIdStringToQid("45cc12b5-f498-4f72-89d3-29180df76e34").get
  val qid4: QuineId = idProvider.customIdStringToQid("ffffffff-ffff-ffff-ffff-ffffffffffff").get

  val event0: NodeChangeEvent.PropertySet = NodeChangeEvent.PropertySet(Symbol("foo"), PropertyValue(QuineValue(0L)))
  val event1: NodeChangeEvent.PropertySet = NodeChangeEvent.PropertySet(Symbol("foo"), PropertyValue(QuineValue(1L)))
  val event2: NodeChangeEvent.PropertySet = NodeChangeEvent.PropertySet(Symbol("foo"), PropertyValue(QuineValue(2L)))
  val event3: NodeChangeEvent.PropertySet = NodeChangeEvent.PropertySet(Symbol("foo"), PropertyValue(QuineValue(3L)))
  val event4: NodeChangeEvent.PropertySet = NodeChangeEvent.PropertySet(Symbol("foo"), PropertyValue(QuineValue(4L)))

  // arbitary byte arrays
  val snapshot0: Array[Byte] = Array[Byte](1)
  val snapshot1: Array[Byte] = Array[Byte](-87, 60, 83, 99)
  val snapshot2: Array[Byte] = Array[Byte](11)
  val snapshot3: Array[Byte] = Array[Byte](89, -71, 2)
  val snapshot4: Array[Byte] = Array[Byte](123, 41, -57)

  val sqId1: StandingQueryId = StandingQueryId(new UUID(0L, 0L)) // min unsigned representation
  val sqId2: StandingQueryId = StandingQueryId(new UUID(256389790107965554L, 7806099684324575116L))
  val sqId3: StandingQueryId = StandingQueryId(new UUID(-2866009460452510937L, 8633904949869711978L))
  val sqId4: StandingQueryId = StandingQueryId(new UUID(-1L, -1L)) // max unsigned representation

  val sqPartId1: StandingQueryPartId = StandingQueryPartId(new UUID(0L, 0L))
  val sqPartId2: StandingQueryPartId = StandingQueryPartId(new UUID(1096520000288222086L, 748609736042323025L))
  val sqPartId3: StandingQueryPartId = StandingQueryPartId(new UUID(-1613026160293696877L, 6732331004029745690L))
  val sqPartId4: StandingQueryPartId = StandingQueryPartId(new UUID(-1L, -1L))

  // arbitrary byte arrays
  val sqState1: Array[Byte] = Array[Byte]()
  val sqState2: Array[Byte] = Array[Byte](0)
  val sqState3: Array[Byte] = Array[Byte](-98, 123, 5, 78)
  val sqState4: Array[Byte] = Array[Byte](34, 92, -1, 20)

  /** Mash together a bunch of async actions into one assertion */
  def allOfConcurrent[A](asyncTests: Future[A]*): Future[Assertion] = {
    assume(runnable)
    Future.sequence(asyncTests.toList).map(_ => succeed)
  }

  describe("persistEvent") {
    it("can record events at various time") {
      allOfConcurrent(
        persistor.persistEvents(
          qid1,
          Seq(
            NodeChangeEvent.WithTime(event0, EventTime.fromRaw(34L)),
            NodeChangeEvent.WithTime(event1, EventTime.fromRaw(36L)),
            NodeChangeEvent.WithTime(event2, EventTime.fromRaw(38L)),
            NodeChangeEvent.WithTime(event3, EventTime.fromRaw(40L)),
            NodeChangeEvent.WithTime(event4, EventTime.fromRaw(44L))
          )
        )
      )
    }
  }

  it("supports EventTime.MaxValue and EventTime.MinValue") {
    allOfConcurrent(
      // "minimum qid" (all 0 bits)
      persistor.persistEvents(
        qid0,
        Seq(
          NodeChangeEvent.WithTime(event0, EventTime.MinValue),
          NodeChangeEvent.WithTime(event1, EventTime.fromRaw(2394872938L)),
          NodeChangeEvent.WithTime(event2, EventTime.fromRaw(-129387432L)),
          NodeChangeEvent.WithTime(event3, EventTime.MaxValue)
        )
      ),
      // in between qid
      persistor.persistEvents(
        qid2,
        Seq(
          NodeChangeEvent.WithTime(event0, EventTime.MinValue),
          NodeChangeEvent.WithTime(event1, EventTime.fromRaw(2394872938L)),
          NodeChangeEvent.WithTime(event2, EventTime.fromRaw(-129387432L)),
          NodeChangeEvent.WithTime(event3, EventTime.MaxValue)
        )
      ),
      // "maximum qid" (all 1 bits)
      persistor.persistEvents(
        qid4,
        Seq(
          NodeChangeEvent.WithTime(event0, EventTime.MinValue),
          NodeChangeEvent.WithTime(event1, EventTime.fromRaw(2394872938L)),
          NodeChangeEvent.WithTime(event2, EventTime.fromRaw(-129387432L)),
          NodeChangeEvent.WithTime(event3, EventTime.MaxValue)
        )
      )
    )
  }

  describe("getJournal") {

    it("can query a full journal of a node") {
      allOfConcurrent(
        persistor.getJournal(qid0, EventTime.MinValue, EventTime.MaxValue).map { journal =>
          assert(journal === Seq(event0, event1, event2, event3))
        },
        persistor.getJournal(qid1, EventTime.MinValue, EventTime.MaxValue).map { journal =>
          assert(journal === Seq(event0, event1, event2, event3, event4))
        },
        persistor.getJournal(qid2, EventTime.MinValue, EventTime.MaxValue).map { journal =>
          assert(journal === Seq(event0, event1, event2, event3))
        },
        persistor.getJournal(qid3, EventTime.MinValue, EventTime.MaxValue).map { journal =>
          assert(journal === Seq.empty)
        },
        persistor.getJournal(qid4, EventTime.MinValue, EventTime.MaxValue).map { journal =>
          assert(journal === Seq(event0, event1, event2, event3))
        }
      )
    }

    it("can query with EventTime.MinValue lower bound") {
      allOfConcurrent(
        // before anything
        persistor.getJournal(qid1, EventTime.MinValue, EventTime.fromRaw(2L)).map { journal =>
          assert(journal === Seq.empty)
        },
        // right up to one event
        persistor.getJournal(qid1, EventTime.MinValue, EventTime.fromRaw(34L)).map { journal =>
          assert(journal === Seq(event0))
        },
        // right after one event
        persistor.getJournal(qid1, EventTime.MinValue, EventTime.fromRaw(37L)).map { journal =>
          assert(journal === Seq(event0, event1))
        },
        // after all events
        persistor.getJournal(qid1, EventTime.MinValue, EventTime.fromRaw(48L)).map { journal =>
          assert(journal === Seq(event0, event1, event2, event3, event4))
        },
        // first event is the min value
        persistor.getJournal(qid0, EventTime.MinValue, EventTime.MinValue).map { journal =>
          assert(journal === Seq(event0))
        },
        persistor.getJournal(qid2, EventTime.MinValue, EventTime.MinValue).map { journal =>
          assert(journal === Seq(event0))
        },
        persistor.getJournal(qid4, EventTime.MinValue, EventTime.MinValue).map { journal =>
          assert(journal === Seq(event0))
        }
      )
    }

    it("can query with EventTime.MaxValue upper bound") {
      allOfConcurrent(
        // before anything
        persistor.getJournal(qid1, EventTime.fromRaw(2L), EventTime.MaxValue).map { journal =>
          assert(journal === Seq(event0, event1, event2, event3, event4))
        },
        // before one event
        persistor.getJournal(qid1, EventTime.fromRaw(42L), EventTime.MaxValue).map { journal =>
          assert(journal === Seq(event4))
        },
        // starting exactly at one event
        persistor.getJournal(qid1, EventTime.fromRaw(44L), EventTime.MaxValue).map { journal =>
          assert(journal === Seq(event4))
        },
        // starting exactly at the first event
        persistor.getJournal(qid1, EventTime.fromRaw(34L), EventTime.MaxValue).map { journal =>
          assert(journal === Seq(event0, event1, event2, event3, event4))
        },
        // after all events
        persistor.getJournal(qid1, EventTime.fromRaw(48L), EventTime.MaxValue).map { journal =>
          assert(journal === Seq.empty)
        },
        // first event is the min value
        persistor.getJournal(qid0, EventTime.MaxValue, EventTime.MaxValue).map { journal =>
          assert(journal === Seq(event3))
        },
        persistor.getJournal(qid2, EventTime.MaxValue, EventTime.MaxValue).map { journal =>
          assert(journal === Seq(event3))
        },
        persistor.getJournal(qid4, EventTime.MaxValue, EventTime.MaxValue).map { journal =>
          assert(journal === Seq(event3))
        }
      )
    }

    it("can query with bounds that are not maximums") {
      allOfConcurrent(
        // start and end before any events
        persistor.getJournal(qid1, EventTime.fromRaw(2L), EventTime.fromRaw(33L)).map { journal =>
          assert(journal === Seq.empty)
        },
        // start and end between events
        persistor.getJournal(qid1, EventTime.fromRaw(42L), EventTime.fromRaw(43L)).map { journal =>
          assert(journal === Seq.empty)
        },
        // right up to one event
        persistor.getJournal(qid1, EventTime.fromRaw(2L), EventTime.fromRaw(34L)).map { journal =>
          assert(journal === Seq(event0))
        },
        // right after one event
        persistor.getJournal(qid1, EventTime.fromRaw(2L), EventTime.fromRaw(35L)).map { journal =>
          assert(journal === Seq(event0))
        },
        // starting exactly at one event
        persistor.getJournal(qid1, EventTime.fromRaw(34L), EventTime.fromRaw(35L)).map { journal =>
          assert(journal === Seq(event0))
        },
        // start and end on events
        persistor.getJournal(qid1, EventTime.fromRaw(36L), EventTime.fromRaw(40L)).map { journal =>
          assert(journal === Seq(event1, event2, event3))
        },
        persistor.getJournal(qid1, EventTime.fromRaw(34L), EventTime.fromRaw(48L)).map { journal =>
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
        persistor.getJournal(qid0, EventTime.fromRaw(-200000000L), EventTime.fromRaw(-2L)).map { journal =>
          assert(journal === Seq(event2))
        },
        persistor.getJournal(qid2, EventTime.fromRaw(-200000000L), EventTime.fromRaw(-2L)).map { journal =>
          assert(journal === Seq(event2))
        },
        persistor.getJournal(qid4, EventTime.fromRaw(-200000000L), EventTime.fromRaw(-2L)).map { journal =>
          assert(journal === Seq(event2))
        },
        // event time needs to be treated as unsigned
        persistor.getJournal(qid0, EventTime.fromRaw(2L), EventTime.fromRaw(-2L)).map { journal =>
          assert(journal === Seq(event1, event2))
        },
        persistor.getJournal(qid2, EventTime.fromRaw(2L), EventTime.fromRaw(-2L)).map { journal =>
          assert(journal === Seq(event1, event2))
        },
        persistor.getJournal(qid4, EventTime.fromRaw(2L), EventTime.fromRaw(-2L)).map { journal =>
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
          val (atTime, snapshot) = snapshotOpt.get
          assert(atTime === EventTime.MaxValue)
          assert(snapshot sameElements snapshot3)
        },
        persistor.getLatestSnapshot(qid1, EventTime.MaxValue).map { snapshotOpt =>
          val (atTime, snapshot) = snapshotOpt.get
          assert(atTime === EventTime.fromRaw(44L))
          assert(snapshot sameElements snapshot4)
        },
        persistor.getLatestSnapshot(qid2, EventTime.MaxValue).map { snapshotOpt =>
          val (atTime, snapshot) = snapshotOpt.get
          assert(atTime === EventTime.MaxValue)
          assert(snapshot sameElements snapshot3)
        },
        persistor.getLatestSnapshot(qid3, EventTime.MaxValue).map { snapshotOpt =>
          assert(snapshotOpt.isEmpty)
        },
        persistor.getLatestSnapshot(qid4, EventTime.MaxValue).map { snapshotOpt =>
          val (atTime, snapshot) = snapshotOpt.get
          assert(atTime === EventTime.MaxValue)
          assert(snapshot sameElements snapshot3)
        }
      )
    }

    it("can query with EventTime.MinValue as the target time") {
      allOfConcurrent(
        persistor.getLatestSnapshot(qid0, EventTime.MinValue).map { snapshotOpt =>
          val (atTime, snapshot) = snapshotOpt.get
          assert(atTime === EventTime.MinValue)
          assert(snapshot sameElements snapshot0)
        },
        persistor.getLatestSnapshot(qid1, EventTime.MinValue).map { snapshotOpt =>
          assert(snapshotOpt.isEmpty)
        },
        persistor.getLatestSnapshot(qid2, EventTime.MinValue).map { snapshotOpt =>
          val (atTime, snapshot) = snapshotOpt.get
          assert(atTime === EventTime.MinValue)
          assert(snapshot sameElements snapshot0)
        },
        persistor.getLatestSnapshot(qid3, EventTime.MinValue).map { snapshotOpt =>
          assert(snapshotOpt.isEmpty)
        },
        persistor.getLatestSnapshot(qid4, EventTime.MinValue).map { snapshotOpt =>
          val (atTime, snapshot) = snapshotOpt.get
          assert(atTime === EventTime.MinValue)
          assert(snapshot sameElements snapshot0)
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
          val (atTime, snapshot) = snapshotOpt.get
          assert(atTime === EventTime.fromRaw(34L))
          assert(snapshot sameElements snapshot0)
        },
        // right after one snapshot
        persistor.getLatestSnapshot(qid1, EventTime.fromRaw(35L)).map { snapshotOpt =>
          val (atTime, snapshot) = snapshotOpt.get
          assert(atTime === EventTime.fromRaw(34L))
          assert(snapshot sameElements snapshot0)
        },
        // after some snapshots, before others
        persistor.getLatestSnapshot(qid1, EventTime.fromRaw(37L)).map { snapshotOpt =>
          val (atTime, snapshot) = snapshotOpt.get
          assert(atTime === EventTime.fromRaw(36L))
          assert(snapshot sameElements snapshot1)
        },
        persistor.getLatestSnapshot(qid1, EventTime.fromRaw(38L)).map { snapshotOpt =>
          val (atTime, snapshot) = snapshotOpt.get
          assert(atTime === EventTime.fromRaw(38L))
          assert(snapshot sameElements snapshot2)
        },
        persistor.getLatestSnapshot(qid1, EventTime.fromRaw(48L)).map { snapshotOpt =>
          val (atTime, snapshot) = snapshotOpt.get
          assert(atTime === EventTime.fromRaw(44L))
          assert(snapshot sameElements snapshot4)
        }
      )
    }

    it("can handle unsigned EventTime") {
      allOfConcurrent(
        persistor.getLatestSnapshot(qid0, EventTime.fromRaw(-2L)).map { snapshotOpt =>
          val (atTime, snapshot) = snapshotOpt.get
          assert(atTime === EventTime.fromRaw(-129387432L))
          assert(snapshot sameElements snapshot2)
        },
        persistor.getLatestSnapshot(qid1, EventTime.fromRaw(-2L)).map { snapshotOpt =>
          val (atTime, snapshot) = snapshotOpt.get
          assert(atTime === EventTime.fromRaw(44L))
          assert(snapshot sameElements snapshot4)
        },
        persistor.getLatestSnapshot(qid2, EventTime.fromRaw(-2L)).map { snapshotOpt =>
          val (atTime, snapshot) = snapshotOpt.get
          assert(atTime === EventTime.fromRaw(-129387432L))
          assert(snapshot sameElements snapshot2)
        },
        persistor.getLatestSnapshot(qid3, EventTime.fromRaw(-2L)).map { snapshotOpt =>
          assert(snapshotOpt.isEmpty)
        },
        persistor.getLatestSnapshot(qid4, EventTime.fromRaw(-2L)).map { snapshotOpt =>
          val (atTime, snapshot) = snapshotOpt.get
          assert(atTime === EventTime.fromRaw(-129387432L))
          assert(snapshot sameElements snapshot2)
        }
      )
    }
  }

  describe("setStandingQueryState") {
    it("can set multiple states for one node") {
      allOfConcurrent(
        persistor.setStandingQueryState(sqId1, qid1, sqPartId1, Some(sqState1)),
        persistor.setStandingQueryState(sqId1, qid1, sqPartId2, Some(sqState2)),
        persistor.setStandingQueryState(sqId1, qid1, sqPartId3, Some(sqState3)),
        persistor.setStandingQueryState(sqId1, qid1, sqPartId4, Some(sqState4))
      )
    }

    it("can set the same state on multiple nodes") {
      allOfConcurrent(
        persistor.setStandingQueryState(sqId1, qid2, sqPartId1, Some(sqState1)),
        persistor.setStandingQueryState(sqId1, qid3, sqPartId1, Some(sqState2)),
        persistor.setStandingQueryState(sqId1, qid4, sqPartId1, Some(sqState3))
      )
    }

    it("can set states on various nodes") {
      allOfConcurrent(
        persistor.setStandingQueryState(sqId2, qid4, sqPartId4, Some(sqState1)),
        persistor.setStandingQueryState(sqId4, qid3, sqPartId1, Some(sqState3)),
        persistor.setStandingQueryState(sqId2, qid1, sqPartId3, Some(sqState4)),
        persistor.setStandingQueryState(sqId2, qid1, sqPartId4, Some(sqState3)),
        persistor.setStandingQueryState(sqId3, qid4, sqPartId3, Some(sqState1))
      )
    }

    it("can remove states") {
      allOfConcurrent(
        persistor.setStandingQueryState(sqId2, qid1, sqPartId3, None),
        persistor.setStandingQueryState(sqId3, qid2, sqPartId1, None)
      )
    }
  }

  describe("getStandingQueryState") {
    it("can return an empty set of states") {
      allOfConcurrent(
        persistor.getStandingQueryStates(qid0).map { sqStates =>
          assert(sqStates === Map.empty)
        }
      )
    }

    it("can find a single state associated with a node") {
      allOfConcurrent(
        persistor.getStandingQueryStates(qid2).map { sqStates =>
          assert(sqStates.size === 1)
          assert(sqStates(sqId1 -> sqPartId1) sameElements sqState1)
        }
      )
    }

    it("can find states associated with multiple queries") {
      allOfConcurrent(
        persistor.getStandingQueryStates(qid1).map { sqStates =>
          assert(sqStates.size === 5)
          assert(sqStates(sqId1 -> sqPartId1) sameElements sqState1)
          assert(sqStates(sqId1 -> sqPartId2) sameElements sqState2)
          assert(sqStates(sqId1 -> sqPartId3) sameElements sqState3)
          assert(sqStates(sqId2 -> sqPartId4) sameElements sqState3)
          assert(sqStates(sqId1 -> sqPartId4) sameElements sqState4)
        },
        persistor.getStandingQueryStates(qid3).map { sqStates =>
          assert(sqStates.size === 2)
          assert(sqStates(sqId1 -> sqPartId1) sameElements sqState2)
          assert(sqStates(sqId4 -> sqPartId1) sameElements sqState3)
        },
        persistor.getStandingQueryStates(qid4).map { sqStates =>
          assert(sqStates.size === 3)
          assert(sqStates(sqId1 -> sqPartId1) sameElements sqState3)
          assert(sqStates(sqId2 -> sqPartId4) sameElements sqState1)
          assert(sqStates(sqId3 -> sqPartId3) sameElements sqState1)
        }
      )
    }
  }
}
