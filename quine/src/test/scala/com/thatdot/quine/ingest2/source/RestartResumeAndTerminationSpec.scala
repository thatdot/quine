package com.thatdot.quine.ingest2.source

import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Success, Try}

import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.{Done, NotUsed}

import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.data.DataFoldableFrom
import com.thatdot.quine.app.model.ingest2.V2IngestEntities.RetryStreamError
import com.thatdot.quine.app.model.ingest2.source.{DecodedSource, QuineIngestQuery}
import com.thatdot.quine.app.model.ingest2.sources.withKillSwitches
import com.thatdot.quine.app.routes.{IngestMeter, IngestMetered}
import com.thatdot.quine.app.{IngestTestGraph, Metrics, ShutdownSwitch}
import com.thatdot.quine.graph.metrics.HostQuineMetrics
import com.thatdot.quine.graph.{GraphService, cypher, defaultNamespaceId}
import com.thatdot.quine.ingest2.IngestSourceTestSupport.randomString
import com.thatdot.quine.util.TestLogging._

/** What a restart does to the two things a cluster-ingest slice asks of its stream: where it has
  * got to, and whether it has ended.
  *
  * Both answers come from [[DecodedSource.toQuineIngestSource]], and a restart is where the honest
  * answer differs from the convenient one. A slice's bounds live inside the blueprint
  * `RestartSource` re-materializes, so a restart re-reads the same records; and a restartable
  * failure ends a materialization without ending the ingest. Answering either question from the
  * failed materialization loses records: the first by resuming past a gap, the second by retiring a
  * worker whose stream is about to come back without it.
  */
class RestartResumeAndTerminationSpec extends AnyFunSuite {

  private type Record = (() => Try[String], Int)

  private def newMeter(): IngestMeter = IngestMetered.ingestMeter(
    defaultNamespaceId,
    randomString(),
    HostQuineMetrics(enableDebugMetrics = false, metricRegistry = Metrics, omitDefaultNamespace = true),
  )

  private val noOpQuery: QuineIngestQuery = new QuineIngestQuery {
    def apply(deserialized: cypher.Value): Future[Unit] = Future.unit
  }

  /** A source whose FIRST materialization emits `failAfter` records and then fails with something
    * the restart policy treats as transient, and whose later materializations emit the whole range
    * of `recordCount` records. `materializations` counts how many times it has been built.
    *
    * `lazySource` is what makes the two differ: the blueprint is assembled once, so a plain
    * `Source` would replay the first materialization's behaviour on the restart too.
    */
  private def flakyOnce(
    meter: IngestMeter,
    recordCount: Int,
    failAfter: Int,
    materializations: AtomicInteger,
  ): DecodedSource = new DecodedSource(meter) {
    type Decoded = String
    type Frame = Int
    override val foldableFrame: DataFoldableFrom[Int] =
      DataFoldableFrom.stringDataFoldable.contramap(_.toString)
    override val foldable: DataFoldableFrom[String] = DataFoldableFrom.stringDataFoldable
    override def content(input: Int): Array[Byte] = Array.emptyByteArray
    def stream: Source[Record, ShutdownSwitch] =
      withKillSwitches(
        Source
          .lazySource { () =>
            val records: Source[Record, NotUsed] =
              Source(0 until recordCount).map(i => (() => Success(i.toString), i))
            val transientFailure: Source[Record, NotUsed] =
              Source.failed(new TimeoutException("transient"))
            if (materializations.incrementAndGet() == 1)
              records.take(failAfter.toLong).concat(transientFailure)
            else records
          }
          .mapMaterializedValue(_ => NotUsed),
      )
  }

  test("a restartable failure does not end the ingest: the termination hook stays pending in backoff") {
    val graph: GraphService = IngestTestGraph.makeGraph()
    try {
      val materializations = new AtomicInteger(0)
      val decoded = flakyOnce(newMeter(), recordCount = 4, failAfter = 1, materializations)

      val ingestSource = decoded.toQuineIngestSource(
        "restart-termination-test",
        noOpQuery,
        transformation = None,
        cypherGraph = graph,
        onStreamErrorHandler = RetryStreamError(
          retryCount = 3,
          within = 30.seconds,
          minBackoff = 3.seconds,
          maxBackoff = 3.seconds,
        ),
      )

      // The hook, not `control.termSignal`. `termSignal` answers for the CURRENT attempt and
      // completes the moment a restartable error fires; the hook is handed the whole restartable
      // stream's termination, which is the one signal that means the ingest is over. That
      // distinction is the finding: a sweep reading the per-attempt signal retires a worker whose
      // stream is coming back, drains a kill switch that belongs to the attempt that already died,
      // and lets the restart fire with nothing holding it. `ClusterIngestSliceState` completes its
      // `terminalSignal` from this hook for that reason.
      val terminated = Promise[Done]()
      ingestSource
        .stream(defaultNamespaceId, (done: Future[Done]) => { terminated.completeWith(done); () })
        .runWith(graph.masterStream.ingestCompletionsSink)(graph.materializer)

      // Inside the backoff window: the first materialization has failed and the second is 3s away.
      awaitCondition(10.seconds)(materializations.get() >= 1)
      Thread.sleep(500L)
      assert(materializations.get() == 1, "the restart fired before the window this test is about")
      assert(
        !terminated.future.isCompleted,
        "the ingest reported itself ended while its restart was only backing off",
      )

      Await.result(terminated.future, 60.seconds)
      assert(materializations.get() == 2, "the stream never restarted, so the signal was never at risk")
    } finally {
      val _ = graph.shutdown()
    }
  }

  private def awaitCondition(within: FiniteDuration)(p: => Boolean): Unit = {
    val deadline = System.nanoTime() + within.toNanos
    while (!p && System.nanoTime() < deadline) Thread.sleep(25L)
  }
}
