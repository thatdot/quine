package com.thatdot.quine.persistor

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

import akka.actor.ActorSystem

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec

import com.thatdot.quine.graph.{EventTime, NodeChangeEvent, QuineUUIDProvider}
import com.thatdot.quine.model.{PropertyValue, QuineId, QuineValue}

/** Abstract test suite that can be implemented just by specifying `persistor`.
  * The intent is that every new persistor should be able to extend this
  * abstract `Spec` and quickly be able to check that the expected persistor
  * properties hold
  *
  * TODO: add tests for snapshots, standing query states, and metadata
  */
abstract class PersistenceAgentSpec extends AnyFunSpec with BeforeAndAfterAll {

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

  def persistEventSync(qid: QuineId, atTime: EventTime, event: NodeChangeEvent): Unit =
    Await.result(persistor.persistEvent(qid, atTime, event), 10.seconds)

  def getJournalSync(qid: QuineId, from: EventTime, to: EventTime): Seq[NodeChangeEvent] =
    Await.result(persistor.getJournal(qid, from, to), 10.seconds)

  describe("persistEvent") {
    it("can record events at various time") {
      assume(runnable)

      persistEventSync(qid1, EventTime.fromRaw(34L), event0)
      persistEventSync(qid1, EventTime.fromRaw(36L), event1)
      persistEventSync(qid1, EventTime.fromRaw(38L), event2)
      persistEventSync(qid1, EventTime.fromRaw(40L), event3)
      persistEventSync(qid1, EventTime.fromRaw(44L), event4)
    }

    it("supports EventTime.MaxValue and EventTime.MinValue") {
      assume(runnable)

      // "minimum qid" (all 0 bits)
      persistEventSync(qid0, EventTime.MinValue, event0)
      persistEventSync(qid0, EventTime.fromRaw(2394872938L), event1)
      persistEventSync(qid0, EventTime.fromRaw(-129387432L), event2)
      persistEventSync(qid0, EventTime.MaxValue, event3)

      // in between qid
      persistEventSync(qid2, EventTime.MinValue, event0)
      persistEventSync(qid2, EventTime.fromRaw(2394872938L), event1)
      persistEventSync(qid2, EventTime.fromRaw(-129387432L), event2)
      persistEventSync(qid2, EventTime.MaxValue, event3)

      // "maximum qid" (all 1 bits)
      persistEventSync(qid4, EventTime.MinValue, event0)
      persistEventSync(qid4, EventTime.fromRaw(2394872938L), event1)
      persistEventSync(qid4, EventTime.fromRaw(-129387432L), event2)
      persistEventSync(qid4, EventTime.MaxValue, event3)
    }
  }

  describe("getJournal") {

    it("can query a full journal of a node") {
      assume(runnable)
      assert(getJournalSync(qid0, EventTime.MinValue, EventTime.MaxValue) === Seq(event0, event1, event2, event3))
      assert(
        getJournalSync(qid1, EventTime.MinValue, EventTime.MaxValue) === Seq(event0, event1, event2, event3, event4)
      )
      assert(getJournalSync(qid2, EventTime.MinValue, EventTime.MaxValue) === Seq(event0, event1, event2, event3))
      assert(getJournalSync(qid3, EventTime.MinValue, EventTime.MaxValue) === Seq.empty)
      assert(getJournalSync(qid4, EventTime.MinValue, EventTime.MaxValue) === Seq(event0, event1, event2, event3))
    }

    it("can query with EventTime.MinValue lower bound") {
      assume(runnable)

      // before anything
      assert(getJournalSync(qid1, EventTime.MinValue, EventTime.fromRaw(2L)) === Seq.empty)

      // right up to one event
      assert(getJournalSync(qid1, EventTime.MinValue, EventTime.fromRaw(34L)) === Seq(event0))

      // right after one event
      assert(getJournalSync(qid1, EventTime.MinValue, EventTime.fromRaw(37L)) === Seq(event0, event1))

      // after all events
      assert(
        getJournalSync(qid1, EventTime.MinValue, EventTime.fromRaw(48L)) === Seq(event0, event1, event2, event3, event4)
      )

      // first event is the min value
      assert(getJournalSync(qid0, EventTime.MinValue, EventTime.MinValue) === Seq(event0))
      assert(getJournalSync(qid2, EventTime.MinValue, EventTime.MinValue) === Seq(event0))
      assert(getJournalSync(qid4, EventTime.MinValue, EventTime.MinValue) === Seq(event0))
    }

    it("can query with EventTime.MaxValue upper bound") {
      assume(runnable)

      // before anything
      assert(
        getJournalSync(qid1, EventTime.fromRaw(2L), EventTime.MaxValue) === Seq(event0, event1, event2, event3, event4)
      )

      // before one event
      assert(getJournalSync(qid1, EventTime.fromRaw(42L), EventTime.MaxValue) === Seq(event4))

      // starting exactly at one event
      assert(getJournalSync(qid1, EventTime.fromRaw(44L), EventTime.MaxValue) === Seq(event4))

      // starting exactly at the first event
      assert(
        getJournalSync(qid1, EventTime.fromRaw(34L), EventTime.MaxValue) === Seq(event0, event1, event2, event3, event4)
      )

      // after all events
      assert(getJournalSync(qid1, EventTime.fromRaw(48L), EventTime.MaxValue) === Seq.empty)

      // first event is the min value
      assert(getJournalSync(qid0, EventTime.MaxValue, EventTime.MaxValue) === Seq(event3))
      assert(getJournalSync(qid2, EventTime.MaxValue, EventTime.MaxValue) === Seq(event3))
      assert(getJournalSync(qid4, EventTime.MaxValue, EventTime.MaxValue) === Seq(event3))
    }

    it("can query with bounds that are not maximums") {
      assume(runnable)

      // start and end before any events
      assert(getJournalSync(qid1, EventTime.fromRaw(2L), EventTime.fromRaw(33L)) === Seq.empty)

      // start and end between events
      assert(getJournalSync(qid1, EventTime.fromRaw(42L), EventTime.fromRaw(43L)) === Seq.empty)

      // right up to one event
      assert(getJournalSync(qid1, EventTime.fromRaw(2L), EventTime.fromRaw(34L)) === Seq(event0))

      // right after one event
      assert(getJournalSync(qid1, EventTime.fromRaw(2L), EventTime.fromRaw(35L)) === Seq(event0))

      // starting exactly at one event
      assert(getJournalSync(qid1, EventTime.fromRaw(34L), EventTime.fromRaw(35L)) === Seq(event0))

      // start and end on events
      assert(getJournalSync(qid1, EventTime.fromRaw(36L), EventTime.fromRaw(40L)) === Seq(event1, event2, event3))
      assert(
        getJournalSync(qid1, EventTime.fromRaw(34L), EventTime.fromRaw(48L)) === Seq(
          event0,
          event1,
          event2,
          event3,
          event4
        )
      )
    }

    it("can handle unsigned EventTime") {
      assume(runnable)

      // event time needs to be treated as unsigned
      assert(getJournalSync(qid0, EventTime.fromRaw(-200000000L), EventTime.fromRaw(-2L)) === Seq(event2))
      assert(getJournalSync(qid2, EventTime.fromRaw(-200000000L), EventTime.fromRaw(-2L)) === Seq(event2))
      assert(getJournalSync(qid4, EventTime.fromRaw(-200000000L), EventTime.fromRaw(-2L)) === Seq(event2))

      // event time needs to be treated as unsigned
      assert(getJournalSync(qid0, EventTime.fromRaw(2L), EventTime.fromRaw(-2L)) === Seq(event1, event2))
      assert(getJournalSync(qid2, EventTime.fromRaw(2L), EventTime.fromRaw(-2L)) === Seq(event1, event2))
      assert(getJournalSync(qid4, EventTime.fromRaw(2L), EventTime.fromRaw(-2L)) === Seq(event1, event2))
    }
  }

}
