package com.thatdot.quine.ingest2.source

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, Promise}
import scala.jdk.CollectionConverters._
import scala.util.{Success, Try}

import org.apache.pekko.Done
import org.apache.pekko.stream.scaladsl.{Flow, Source}

import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.data.DataFoldableFrom
import com.thatdot.quine.app.model.ingest2.source.{DecodedSource, QuineIngestQuery}
import com.thatdot.quine.app.model.ingest2.sources.withKillSwitches
import com.thatdot.quine.app.routes.IngestMetered
import com.thatdot.quine.app.{IngestTestGraph, Metrics, ShutdownSwitch}
import com.thatdot.quine.graph.metrics.HostQuineMetrics
import com.thatdot.quine.graph.{GraphService, cypher, defaultNamespaceId}
import com.thatdot.quine.ingest2.IngestSourceTestSupport.randomString
import com.thatdot.quine.util.TestLogging._

/** The write stage (`mapAsync`, not `mapAsyncUnordered`, in [[DecodedSource.toQuineIngestSource]])
  * emits records downstream in SOURCE order even when their ingest-query futures complete in any
  * other order. Two things are load-bearing on that:
  *
  *  - Kafka's offset committer: the ack flow commits the offset of each frame it sees, so a frame
  *    surfacing ahead of an uncompleted earlier record would commit past data not yet written.
  *  - Cluster-ingest slice checkpoints: a slice's resume point is a COUNT of elements emitted
  *    downstream of the write stage, and a count is only a position if those elements are a
  *    contiguous prefix. Unordered emission would make a moved slice resume past records that
  *    were still in flight when it moved, and silently skip them.
  *
  * This test drives the real pipeline with queries completing in exactly REVERSE order and asserts
  * the ack flow still observes source order. If someone switches the stage to `mapAsyncUnordered`
  * for throughput, this fails rather than the resume points quietly corrupting.
  */
class WriteStageOrderingSpec extends AnyFunSuite {

  test("records reach the post-write ack flow in source order, even when queries complete in reverse") {
    val graph: GraphService = IngestTestGraph.makeGraph()
    try {
      val recordCount = 8
      val meter = IngestMetered.ingestMeter(
        defaultNamespaceId,
        randomString(),
        HostQuineMetrics(enableDebugMetrics = false, metricRegistry = Metrics, omitDefaultNamespace = true),
      )

      val seenByAck = new ConcurrentLinkedQueue[Int]()
      val decoded = new DecodedSource(meter) {
        type Decoded = String
        type Frame = Int
        override val foldableFrame: DataFoldableFrom[Int] =
          DataFoldableFrom.stringDataFoldable.contramap(_.toString)
        override val foldable: DataFoldableFrom[String] = DataFoldableFrom.stringDataFoldable
        override def content(input: Int): Array[Byte] = Array.emptyByteArray
        def stream: Source[(() => Try[String], Int), ShutdownSwitch] =
          withKillSwitches(Source(0 until recordCount).map(i => (() => Success(i.toString), i)))
        // The observation point: this flow sits downstream of the write stage, so the order it
        // sees IS the emission order the offset committer and the slice checkpoint counter see.
        override val ack: Option[Flow[Int, Done, org.apache.pekko.NotUsed]] =
          Some(Flow[Int].map { i => seenByAck.add(i); Done })
      }

      // One gate per record; all gates released in REVERSE once the last query has been invoked.
      // Every record is in flight together (parallelism >= recordCount), so with an unordered
      // stage the ack flow would see ~reverse order; the ordered stage must still emit 0..N-1.
      val gates = Vector.fill(recordCount)(Promise[Unit]())
      val invoked = new AtomicInteger(0)
      val ingestQuery: QuineIngestQuery = new QuineIngestQuery {
        def apply(deserialized: cypher.Value): Future[Unit] = {
          val idx = invoked.getAndIncrement()
          if (idx == recordCount - 1) (recordCount - 1) to 0 by -1 foreach (i => gates(i).trySuccess(()))
          gates(idx).future
        }
      }

      val ingestSource = decoded.toQuineIngestSource(
        "write-stage-ordering-test",
        ingestQuery,
        transformation = None,
        cypherGraph = graph,
        parallelism = recordCount,
      )

      val terminated = Promise[Done]()
      ingestSource
        .stream(defaultNamespaceId, (done: Future[Done]) => { terminated.completeWith(done); () })
        .runWith(graph.masterStream.ingestCompletionsSink)(graph.materializer)

      Await.result(terminated.future, 30.seconds)
      assert(
        seenByAck.iterator.asScala.toList == (0 until recordCount).toList,
        "the write stage emitted out of source order; offset commits and slice checkpoints are both unsound if this holds",
      )
    } finally {
      val _ = graph.shutdown()
    }
  }
}
