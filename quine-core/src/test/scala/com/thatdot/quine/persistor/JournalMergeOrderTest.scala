package com.thatdot.quine.persistor

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.util.Random

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Sink

import cats.data.NonEmptyList
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.DomainIndexEvent.CancelDomainNodeSubscription
import com.thatdot.quine.graph.PropertyEvent.PropertySet
import com.thatdot.quine.graph.{DomainIndexEvent, EventTime, NodeChangeEvent, NodeEvent}
import com.thatdot.quine.model.{Milliseconds, PropertyValue, QuineValue}

/** Pins the order [[PersistenceAgent.getJournalWithTime]] merges the two journal stores in.
  *
  * A node's journal is replayed in order to rebuild its state on wakeup, and node-change events and
  * standing-query-index events are read from two separate stores. Any change to how those two are
  * interleaved changes what a node wakes up as.
  *
  * The order is ascending [[EventTime]], whole. A node stamps every event it journals from one
  * `tickEventSequence`, whichever of the two stores it then goes to, so the sequence numbers below
  * the millisecond are what say which of two events in the same millisecond happened first. Sorting
  * on the millisecond alone discards that and hands back an order the node never applied.
  */
class JournalMergeOrderTest extends AnyFunSuite with BeforeAndAfterAll with should.Matchers {
  implicit val system: ActorSystem = ActorSystem("journal-merge-order-test")
  implicit val ec: ExecutionContextExecutor = system.dispatcher
  implicit val logConfig: LogConfig = LogConfig.permissive

  override def afterAll(): Unit = {
    Await.result(system.terminate(), 10.seconds)
    ()
  }

  private val qid = QuineId(Array[Byte](1, 2, 3))

  /** The merge order this must always produce: ascending whole [[EventTime]] across both stores. */
  private def expectedMerge(
    nodeChanges: Vector[NodeEvent.WithTime[NodeChangeEvent]],
    domainIndex: Vector[NodeEvent.WithTime[DomainIndexEvent]],
  ): Vector[NodeEvent.WithTime[NodeEvent]] =
    (nodeChanges ++ domainIndex).sortBy(_.atTime)

  private def propertyEvent(name: String, at: EventTime): NodeEvent.WithTime[NodeChangeEvent] =
    NodeEvent.WithTime(PropertySet(Symbol(name), PropertyValue(QuineValue.Integer(1L))), at)

  private def indexEvent(dgnId: Long, at: EventTime): NodeEvent.WithTime[DomainIndexEvent] =
    NodeEvent.WithTime(CancelDomainNodeSubscription(dgnId, qid), at)

  /** `count` successive moments the node's clock would stamp inside one millisecond. */
  private def withinMillisecond(millis: Long, count: Int): Vector[EventTime] =
    (1 until count).foldLeft(Vector(EventTime.fromMillis(Milliseconds(millis)))) { (acc, _) =>
      acc :+ acc.last.tickEventSequence(None)
    }

  private def merged(
    nodeChanges: Vector[NodeEvent.WithTime[NodeChangeEvent]],
    domainIndex: Vector[NodeEvent.WithTime[DomainIndexEvent]],
    includeDomainIndexEvents: Boolean,
  ): Vector[NodeEvent.WithTime[NodeEvent]] = {
    val persistor = new InMemoryPersistor()(LogConfig.strict)
    Await.result(
      for {
        _ <- NonEmptyList
          .fromList(nodeChanges.toList)
          .fold(scala.concurrent.Future.unit)(
            persistor.persistNodeChangeEvents(qid, _),
          )
        _ <- NonEmptyList
          .fromList(domainIndex.toList)
          .fold(scala.concurrent.Future.unit)(
            persistor.persistDomainIndexEvents(qid, _),
          )
      } yield (),
      10.seconds,
    )
    Await
      .result(
        persistor
          .getJournalWithTime(qid, EventTime.MinValue, EventTime.MaxValue, includeDomainIndexEvents)
          .runWith(Sink.seq),
        10.seconds,
      )
      .toVector
  }

  test("a domain-index event stamped between two node-change events comes back between them") {
    // Every event in one millisecond, so the sequence numbers are the only thing separating them.
    val times = withinMillisecond(1000L, 3)
    val first = propertyEvent("a", times(0))
    val index = indexEvent(1L, times(1))
    val second = propertyEvent("b", times(2))

    val result = merged(Vector(first, second), Vector(index), includeDomainIndexEvents = true)

    result shouldEqual expectedMerge(Vector(first, second), Vector(index))
    result.map(_.event) shouldEqual Vector(first.event, index.event, second.event)
  }

  test("events in distinct milliseconds interleave by timestamp regardless of store") {
    val nodeChanges = Vector(propertyEvent("a", ms(10)), propertyEvent("b", ms(30)))
    val domainIndex = Vector(indexEvent(1L, ms(20)), indexEvent(2L, ms(40)))

    val result = merged(nodeChanges, domainIndex, includeDomainIndexEvents = true)

    result shouldEqual expectedMerge(nodeChanges, domainIndex)
    result.map(_.atTime.millis) shouldEqual Vector(10L, 20L, 30L, 40L)
  }

  test("randomised interleavings all come back in ascending EventTime order") {
    val random = new Random(20260904L)
    forAllInterleavings(random)
  }

  /** Assign each millisecond in `millis` a distinct [[EventTime]], ticking the sequence counter
    * when a millisecond repeats. A node stamps its events from one such counter whichever store
    * they go to, so this is called once over both stores' events rather than once per store: no
    * two events share an EventTime, and the merge has no tie to break.
    */
  private def distinctTimes(millis: Vector[Long]): Vector[EventTime] =
    millis
      .foldLeft(Vector.empty[EventTime]) { (acc, m) =>
        val base = EventTime.fromMillis(Milliseconds(m))
        acc :+ acc.lastOption.filter(_.millis == m).fold(base)(_.tickEventSequence(None))
      }

  private def forAllInterleavings(random: Random): Unit =
    (1 to 40).foreach { case _ =>
      // Draw from a small pool of milliseconds so collisions are frequent, both within a store and
      // across the two of them.
      val toIndexStore = Vector.fill(random.nextInt(10))(random.nextBoolean())
      val times = distinctTimes(Vector.fill(toIndexStore.size)(random.nextInt(4).toLong * 10L).sorted)
      val nodeChanges = times.zip(toIndexStore).collect { case (t, false) => t }.zipWithIndex.map { case (t, i) =>
        propertyEvent(s"p$i", t)
      }
      val domainIndex = times.zip(toIndexStore).collect { case (t, true) => t }.zipWithIndex.map { case (t, i) =>
        indexEvent(i.toLong, t)
      }

      if (nodeChanges.nonEmpty || domainIndex.nonEmpty) {
        val result = merged(nodeChanges, domainIndex, includeDomainIndexEvents = true)
        withClue(s"nodeChanges=${nodeChanges.map(_.atTime.millis)} domainIndex=${domainIndex.map(_.atTime.millis)}: ") {
          result shouldEqual expectedMerge(nodeChanges, domainIndex)
        }
      }
    }

  test("excluding domain index events yields the node-change events alone, in order") {
    val times = withinMillisecond(1000L, 3)
    val first = propertyEvent("a", times(0))
    val index = indexEvent(1L, times(1))
    val second = propertyEvent("b", times(2))

    val result = merged(Vector(first, second), Vector(index), includeDomainIndexEvents = false)

    result.map(_.event) shouldEqual Vector(first.event, second.event)
  }

  /** Distinct events one millisecond apart, for readability in the interleaving tests. */
  private def ms(millis: Long): EventTime = EventTime.fromMillis(Milliseconds(millis))
}
