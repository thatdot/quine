package com.thatdot.quine.graph

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Sink

import cats.data.NonEmptyList
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.persistor.{InMemoryPersistor, PersistenceConfig}
import com.thatdot.quine.util.TestLogging._

/** Replay folds the journal in the order the persistor hands it back, so that order is part of
  * the contract rather than an implementation detail: `getJournalWithTime` documents itself as
  * returning events "ordered by ascending timestamp".
  *
  * Every replay defect found so far has been a node reaching a conclusion from state the fold had
  * not finished rebuilding. Order decides what "not finished" means, so it is worth asserting over
  * generated interleavings rather than over the one interleaving a workload happens to produce.
  */
class JournalOrderingProperties
    extends AnyFunSuite
    with BeforeAndAfterAll
    with Matchers
    with ScalaCheckPropertyChecks
    with ArbitraryInstances {

  // Journal reads stream, so running one needs a materializer.
  implicit private val system: ActorSystem = ActorSystem("journal-ordering-properties")
  implicit private val materializer: Materializer = Materializer.matFromSystem(system)

  override def afterAll(): Unit = {
    Await.result(system.terminate(), 10.seconds)
    ()
  }

  private val qid = QuineId(Array[Byte](1, 2, 3, 4))

  test("a journal is handed back in ascending EventTime order, however the two streams interleave") {
    // Same millisecond, different sequence numbers: the case the packing exists for, and the one
    // a millisecond-granularity sort cannot distinguish.
    forAll(org.scalacheck.Gen.choose(1, 8), org.scalacheck.Gen.choose(1, 8)) { (nChanges: Int, nIndex: Int) =>
      val persistor = new InMemoryPersistor(persistenceConfig = PersistenceConfig(), namespace = defaultNamespaceId)
      val millis = 1_000_000L

      // interleave the two kinds across the sequence space within one millisecond
      val changes = (0 until nChanges).map { i =>
        NodeEvent.WithTime(
          PropertyEvent.PropertySet(Symbol(s"p$i"), com.thatdot.quine.model.PropertyValue(i.toLong)),
          EventTime(millis, timestampSequence = (i * 2).toLong),
        )
      }.toList
      val indexes = (0 until nIndex).map { i =>
        NodeEvent.WithTime(
          DomainIndexEvent.CancelDomainNodeSubscription(i.toLong, qid),
          EventTime(millis, timestampSequence = (i * 2 + 1).toLong),
        )
      }.toList

      NonEmptyList.fromList(changes).foreach(es => Await.result(persistor.persistNodeChangeEvents(qid, es), 5.seconds))
      NonEmptyList.fromList(indexes).foreach(es => Await.result(persistor.persistDomainIndexEvents(qid, es), 5.seconds))

      val got = Await
        .result(
          persistor
            .getJournalWithTime(qid, EventTime.MinValue, EventTime.MaxValue, includeDomainIndexEvents = true)
            .runWith(Sink.seq),
          5.seconds,
        )
        .toList
        .map(_.atTime)

      withClue(s"$nChanges change event(s) and $nIndex domain-index event(s) in one millisecond: ") {
        got shouldBe got.sorted
      }
    }
  }
}
