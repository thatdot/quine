package com.thatdot.quine.persistor

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.immutable.SortedMap
import scala.concurrent.{ExecutionContext, Future}

import org.scalatest.OptionValues
import org.scalatest.funspec.AsyncFunSpec
import org.scalatest.matchers.should.Matchers

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.EventTime
import com.thatdot.quine.persistor.MultipartSnapshotPersistenceAgent.{MultipartSnapshot, MultipartSnapshotPart}

class MultipartSnapshotPersistenceAgentSpec extends AsyncFunSpec with Matchers with OptionValues {

  implicit private val eventTimeOrdering: Ordering[EventTime] = (a, b) => a.compare(b)

  private val qid: QuineId = QuineId(Array[Byte](1))

  /** A multipart-snapshot persistor whose store is pre-seeded, so a test can inject a corrupt
    * (structurally-invalid) snapshot at a chosen time and observe how [[getLatestSnapshot]] recovers.
    *
    * [[getLatestMultipartSnapshot]] fails fast if it is called far more often than there are stored
    * snapshots, so the "retry the corrupt snapshot forever" regression surfaces as a test failure
    * rather than a hang.
    */
  private class SeededMultipartPersistor(snapshots: Seq[(EventTime, Seq[MultipartSnapshotPart])])
      extends EmptyPersistor
      with MultipartSnapshotPersistenceAgent {

    private val store: SortedMap[EventTime, Seq[MultipartSnapshotPart]] = SortedMap.from(snapshots)
    val lookups = new AtomicInteger(0)

    protected val multipartSnapshotExecutionContext: ExecutionContext = ExecutionContext.parasitic
    protected val snapshotPartMaxSizeBytes: Int = 1

    override def getLatestSnapshot(id: QuineId, upToTime: EventTime): Future[Option[StoredSnapshot]] =
      super[MultipartSnapshotPersistenceAgent].getLatestSnapshot(id, upToTime)

    override def persistSnapshot(id: QuineId, atTime: EventTime, state: Array[Byte]): Future[Unit] =
      super[MultipartSnapshotPersistenceAgent].persistSnapshot(id, atTime, state)

    def persistSnapshotPart(id: QuineId, atTime: EventTime, part: MultipartSnapshotPart): Future[Unit] = Future.unit

    def getLatestMultipartSnapshot(id: QuineId, upToTime: EventTime): Future[Option[MultipartSnapshot]] =
      if (lookups.incrementAndGet() > store.size + 3)
        Future.failed(new AssertionError(s"getLatestMultipartSnapshot called too many times; likely retrying forever"))
      else
        Future.successful(
          store.rangeTo(upToTime).lastOption.map { case (time, parts) => MultipartSnapshot(time, parts) },
        )
  }

  /** Parts that reassemble cleanly: `multipartIndex` and `multipartCount` line up with the sequence. */
  private def validParts(bytes: Byte*): Seq[MultipartSnapshotPart] = {
    val count = bytes.length
    bytes.zipWithIndex.map { case (b, i) => MultipartSnapshotPart(Array(b), i, count) }
  }

  /** Parts left behind by a partial multipart write: the stored parts claim a `multipartCount`
    * larger than the number of parts actually present.
    */
  private def corruptParts(bytes: Byte*): Seq[MultipartSnapshotPart] = {
    val claimedCount = bytes.length + 1
    bytes.zipWithIndex.map { case (b, i) => MultipartSnapshotPart(Array(b), i, claimedCount) }
  }

  describe("MultipartSnapshotPersistenceAgent.getLatestSnapshot") {
    it("returns the reassembled bytes for an intact snapshot") {
      val persistor = new SeededMultipartPersistor(Seq(EventTime.fromRaw(10L) -> validParts(1, 2, 3)))
      persistor.getLatestSnapshot(qid, EventTime.MaxValue).map { result =>
        result.value.bytes.toSeq shouldBe Seq[Byte](1, 2, 3)
        result.value.atTime shouldBe EventTime.fromRaw(10L)
      }
    }

    it("falls back to the newest intact snapshot older than a corrupt one") {
      val persistor = new SeededMultipartPersistor(
        Seq(
          EventTime.fromRaw(10L) -> validParts(1, 2, 3),
          EventTime.fromRaw(20L) -> corruptParts(9, 9),
        ),
      )
      persistor.getLatestSnapshot(qid, EventTime.MaxValue).map { result =>
        result.value.bytes.toSeq shouldBe Seq[Byte](1, 2, 3)
        // The reported time is the older snapshot's, not the corrupt one's: callers key storage
        // repairs off this, so it has to name the row the bytes actually came from.
        result.value.atTime shouldBe EventTime.fromRaw(10L)
      }
    }

    it("returns None (rather than looping forever) when the only snapshot is corrupt") {
      val persistor = new SeededMultipartPersistor(Seq(EventTime.fromRaw(20L) -> corruptParts(9, 9)))
      persistor.getLatestSnapshot(qid, EventTime.MaxValue).map { result =>
        result shouldBe None
      }
    }

    it("terminates when a corrupt snapshot is stored at EventTime.MinValue") {
      val persistor = new SeededMultipartPersistor(Seq(EventTime.MinValue -> corruptParts(9, 9)))
      persistor.getLatestSnapshot(qid, EventTime.MaxValue).map { result =>
        result shouldBe None
      }
    }

    it("terminates when a corrupt snapshot is stored at EventTime.MaxValue (singleton-style)") {
      // Singleton snapshots all share EventTime.MaxValue, so there is nothing older to fall back to.
      val persistor = new SeededMultipartPersistor(Seq(EventTime.MaxValue -> corruptParts(9, 9)))
      persistor.getLatestSnapshot(qid, EventTime.MaxValue).map { result =>
        result shouldBe None
      }
    }
  }
}
