package com.thatdot.quine.persistor

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

import akka.actor.ActorSystem

import org.scalatest.funspec.AsyncFunSpec
import org.scalatest.{Assertion, BeforeAndAfterAll}

import com.thatdot.quine.graph.{EventTime, NodeChangeEvent, QuineUUIDProvider}
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

  /** Mash together a bunch of async actions into one assertion */
  def allOfConcurrent[A](asyncTests: Future[A]*): Future[Assertion] = {
    assume(runnable)
    Future.sequence(asyncTests.toList).map(_ => succeed)
  }

  describe("persistEvent") {
    it("can record events at various time") {
      allOfConcurrent(
        persistor.persistEvent(qid1, EventTime.fromRaw(34L), event0),
        persistor.persistEvent(qid1, EventTime.fromRaw(36L), event1),
        persistor.persistEvent(qid1, EventTime.fromRaw(38L), event2),
        persistor.persistEvent(qid1, EventTime.fromRaw(40L), event3),
        persistor.persistEvent(qid1, EventTime.fromRaw(44L), event4)
      )
    }

    it("supports EventTime.MaxValue and EventTime.MinValue") {
      allOfConcurrent(
        // "minimum qid" (all 0 bits)
        persistor.persistEvent(qid0, EventTime.MinValue, event0),
        persistor.persistEvent(qid0, EventTime.fromRaw(2394872938L), event1),
        persistor.persistEvent(qid0, EventTime.fromRaw(-129387432L), event2),
        persistor.persistEvent(qid0, EventTime.MaxValue, event3),
        // in between qid
        persistor.persistEvent(qid2, EventTime.MinValue, event0),
        persistor.persistEvent(qid2, EventTime.fromRaw(2394872938L), event1),
        persistor.persistEvent(qid2, EventTime.fromRaw(-129387432L), event2),
        persistor.persistEvent(qid2, EventTime.MaxValue, event3),
        // "maximum qid" (all 1 bits)
        persistor.persistEvent(qid4, EventTime.MinValue, event0),
        persistor.persistEvent(qid4, EventTime.fromRaw(2394872938L), event1),
        persistor.persistEvent(qid4, EventTime.fromRaw(-129387432L), event2),
        persistor.persistEvent(qid4, EventTime.MaxValue, event3)
      )
    }
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

}
