package com.thatdot.quine.ingest2.source

import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Success, Try}

import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.{Done, NotUsed}

import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.data.DataFoldableFrom
import com.thatdot.quine.app.model.ingest2.V2IngestEntities
import com.thatdot.quine.app.model.ingest2.source.{DecodedSource, QuineIngestQuery}
import com.thatdot.quine.app.model.ingest2.sources.withKillSwitches
import com.thatdot.quine.app.routes.IngestMetered
import com.thatdot.quine.app.{IngestTestGraph, Metrics, ShutdownSwitch}
import com.thatdot.quine.graph.metrics.HostQuineMetrics
import com.thatdot.quine.graph.{GraphService, cypher, defaultNamespaceId}
import com.thatdot.quine.ingest2.IngestSourceTestSupport.randomString
import com.thatdot.quine.util.TestLogging._

/** Under `RetryStreamError`, a retried attempt re-reads the source's range FROM THE BEGINNING, and
  * nothing downstream of the restart wrapper can see that happen: the stream does not end, no
  * element marks the boundary, records simply start again at the first one.
  *
  * That matters to anything keeping a position by counting what comes out. A cluster-ingest slice
  * does exactly that -- its resume point is the number of records of its range that have finished,
  * counted downstream of the ordered write ([[WriteStageOrderingSpec]] pins the ordering half of
  * that argument) -- and a count that keeps climbing across a re-read is no longer a position. It
  * is attempts summed together, so the checkpoint is written past what was processed and the next
  * owner of the slice resumes beyond records nobody has done: a silent gap, which is the one
  * outcome the checkpoint exists to prevent.
  *
  * `toQuineIngestSource`'s `onAttemptStart` is what makes the boundary visible. Both halves are
  * asserted below, the second being the defect itself, so this fails if the hook stops firing per
  * attempt rather than merely reporting a number nobody can interpret.
  */
class RestartRereadsTheRangeSpec extends AnyFunSuite {

  /** Enough records to tell the three interesting totals apart: 6 read, 10 double-counted. */
  private val recordCount = 6

  /** The first attempt fails here, so it delivers records 0..3 and then dies restartably. */
  private val failAt = 4

  /** Run one ingest whose first attempt fails mid-range, counting completed records the way a
    * slice worker does, and answer with the count it finished on.
    *
    * @param rebaseOnAttempt whether the counter is told that a fresh attempt has begun
    */
  private def finalCount(rebaseOnAttempt: Boolean): Long = {
    val graph: GraphService = IngestTestGraph.makeGraph()
    try {
      val meter = IngestMetered.ingestMeter(
        defaultNamespaceId,
        randomString(),
        HostQuineMetrics(enableDebugMetrics = false, metricRegistry = Metrics, omitDefaultNamespace = true),
      )

      val attempts = new AtomicInteger(0)
      val decoded = new DecodedSource(meter) {
        type Decoded = String
        type Frame = Int
        override val foldableFrame: DataFoldableFrom[Int] =
          DataFoldableFrom.stringDataFoldable.contramap(_.toString)
        override val foldable: DataFoldableFrom[String] = DataFoldableFrom.stringDataFoldable
        override def content(input: Int): Array[Byte] = Array.emptyByteArray

        /** `lazySource` so the range is re-read per materialization, which is what a real bounded
          * source does and the behaviour under test. The attempt counter is the source's own, so
          * the test does not decide what an attempt is by consulting the hook it is checking.
          */
        def stream: Source[(() => Try[String], Int), ShutdownSwitch] =
          withKillSwitches(
            Source
              .lazySource { () =>
                val attempt = attempts.incrementAndGet()
                Source(0 until recordCount).map { i =>
                  // A TimeoutException, because `IngestSrcDef.isRestartableIngestFailure` is what
                  // decides whether there is a second attempt at all.
                  if (attempt == 1 && i == failAt) throw new TimeoutException("injected transient failure")
                  (() => Success(i.toString), i)
                }
              }
              .mapMaterializedValue(_ => NotUsed),
          )
      }

      val ingestQuery: QuineIngestQuery = (_: cypher.Value) => Future.unit

      // Starts where a freshly built worker starts, and is re-based to the same place, so the
      // number means "records of this range that are finished" at every point.
      val position = new AtomicLong(0L)

      val ingestSource = decoded.toQuineIngestSource(
        randomString(),
        ingestQuery,
        transformation = None,
        cypherGraph = graph,
        onStreamErrorHandler = V2IngestEntities.RetryStreamError(
          retryCount = 3,
          within = 30.seconds,
          minBackoff = 1.milli,
          maxBackoff = 5.millis,
        ),
        onAttemptStart = if (rebaseOnAttempt) () => position.set(0L) else () => (),
      )

      val terminated = Promise[Done]()
      ingestSource
        .stream(defaultNamespaceId, (done: Future[Done]) => { terminated.completeWith(done); () })
        // Downstream of the restart wrapper, exactly where a slice counts its resume point.
        .map { token => position.incrementAndGet(); token }
        .runWith(graph.masterStream.ingestCompletionsSink)(graph.materializer)

      Await.result(terminated.future, 30.seconds)
      assert(attempts.get() == 2, "the injected failure did not produce a second attempt")
      position.get()
    } finally {
      val _ = graph.shutdown()
    }
  }

  test("a counter told about each attempt ends at the number of records the range holds") {
    assert(finalCount(rebaseOnAttempt = true) == recordCount.toLong)
  }

  test("a counter not told about each attempt ends past the end of its own range") {
    // 4 delivered before the failure, then all 6 again: a resume point two records beyond a range
    // that only has six, and nothing anywhere reports a problem.
    assert(finalCount(rebaseOnAttempt = false) == (failAt + recordCount).toLong)
  }
}
