package com.thatdot.quine.ingest2.source

import java.io.{BufferedOutputStream, File, FileOutputStream}

import scala.annotation.nowarn
import scala.concurrent.duration.DurationInt
import scala.util.Using

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Keep, Source}

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.funspec.AsyncFunSpec
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.app.model.ingest2.V2IngestEntities.FileFormat.JsonFormat
import com.thatdot.quine.app.model.ingest2.source.{IngestBounds, QuineValueIngestQuery}
import com.thatdot.quine.app.model.ingest2.sources.{DEFAULT_MAXIMUM_LINE_SIZE, NumberIteratorSource}
import com.thatdot.quine.app.routes.{IngestMeter, IngestMetered}
import com.thatdot.quine.app.{IngestTestGraph, Metrics}
import com.thatdot.quine.compiler.{cypher => cyComp}
import com.thatdot.quine.graph.cypher.RunningCypherQuery
import com.thatdot.quine.graph.metrics.HostQuineMetrics
import com.thatdot.quine.graph.{GraphService, MasterStream, cypher}
import com.thatdot.quine.ingest2.IngestSourceTestSupport.{buildDecodedSource, srcFromString}
import com.thatdot.quine.serialization.ProtobufSchemaCache
import com.thatdot.quine.util.TestLogging._

class DecodedSourceSpec extends AsyncFunSpec with Matchers with LazyLogging {

  @nowarn implicit val protobufSchemaCache: ProtobufSchemaCache.Blocking.type = ProtobufSchemaCache.Blocking

  def fileFromString(s: String): File = {
    val tempFile = File.createTempFile(s"IngestStreamConfigurationToSource${System.currentTimeMillis()}", ".jsonl")
    Using(new BufferedOutputStream(new FileOutputStream(tempFile))) { bos =>
      bos.write(s.getBytes)
      bos.flush()
    }
    tempFile
  }

  describe("IngestStreamConfigurationToSource") {
    // Ignore until the awkward Thread.sleep is removed.
    it("runs one supported configuration") {
      val graph: GraphService = IngestTestGraph.makeGraph()
      val rawJson = 1.to(5).map(i => s"""{ "foo":$i }""").mkString("\n")
      val decodedSource =
        buildDecodedSource(srcFromString(rawJson), JsonFormat, IngestBounds(), DEFAULT_MAXIMUM_LINE_SIZE, Seq())

      val ingestQuery = QuineValueIngestQuery.build(graph, "CREATE ($that)", "that", None).get

      val ingestSource = decodedSource.toQuineIngestSource("test", ingestQuery, None, graph)
      val ingestStream: Source[MasterStream.IngestSrcExecToken, NotUsed] = ingestSource.stream(None, _ => ())
      ingestStream.runWith(graph.masterStream.ingestCompletionsSink)(graph.materializer)
      Thread.sleep(1000)

      val queryFuture: RunningCypherQuery = cyComp.queryCypherValues("match (n) return count(n.foo)", None)(graph)
      IngestTestGraph.collect(queryFuture.results)(graph.materializer).head shouldEqual Vector(cypher.Expr.Integer(5L))
    }
  }

  it("number iterator throttle") {
    val graph: GraphService = IngestTestGraph.makeGraph()

    val meter: IngestMeter =
      IngestMetered.ingestMeter(
        None,
        "test",
        HostQuineMetrics(enableDebugMetrics = false, metricRegistry = Metrics, omitDefaultNamespace = true),
      )

    val decodedSource =
      NumberIteratorSource(IngestBounds(0L, Some(1_000_000_000_000L)), meter).decodedSource

    val ingestQuery =
      QuineValueIngestQuery.build(graph, "MATCH (n) WHERE id(n) = idFrom($that) SET n.num = $that", "that", None).get

    val ingestSource = decodedSource.toQuineIngestSource("test", ingestQuery, None, graph)
    val stats = ingestSource.meter

    val ingestStream =
      ingestSource
        .stream(None, _ => ())
        .watchTermination()(Keep.right)
        .to(graph.masterStream.ingestCompletionsSink)

    graph.masterStream.enableIngestThrottle(10)
    ingestStream.run()(graph.materializer)

    implicit val ec = graph.system.dispatcher

    // Throttled to 10/sec, so after 5 seconds should have ~50 (allowing overhead)
    import org.scalatest.concurrent.Eventually._
    eventually(timeout(5.seconds), interval(1.second)) {
      val count = stats.counts.getCount
      count should be >= 10L // At least some progress
      count should be <= 100L // But throttled (10/sec * 5sec + buffer)
    }

    // Now disable throttle and verify it ingests much faster
    val countBeforeDisable = stats.counts.getCount
    graph.masterStream.disableIngestThrottle()

    eventually(timeout(5.seconds), interval(500.millis)) {
      val countAfterDisable = stats.counts.getCount
      val gained = countAfterDisable - countBeforeDisable
      gained should be > 500L // Should ingest much faster without throttle
    }

  }

  it("number iterator throttle disabled ") {
    val graph: GraphService = IngestTestGraph.makeGraph()

    val meter: IngestMeter =
      IngestMetered.ingestMeter(
        None,
        "test",
        HostQuineMetrics(enableDebugMetrics = false, metricRegistry = Metrics, omitDefaultNamespace = true),
      )

    val decodedSource =
      NumberIteratorSource(IngestBounds(0L, Some(1_000_000_000_000L)), meter).decodedSource

    val ingestQuery =
      QuineValueIngestQuery.build(graph, "MATCH (n) WHERE id(n) = idFrom($that) SET n.num = $that", "that", None).get

    val ingestSource = decodedSource.toQuineIngestSource("test", ingestQuery, None, graph)
    val stats = ingestSource.meter

    val ingestStream =
      ingestSource
        .stream(None, _ => ())
        .watchTermination()(Keep.right)
        .to(graph.masterStream.ingestCompletionsSink)

    ingestStream.run()(graph.materializer)

    implicit val ec = graph.system.dispatcher

    // Without throttle, should ingest much faster - expect > 500 in a few seconds
    import org.scalatest.concurrent.Eventually._
    eventually(timeout(5.seconds), interval(500.millis)) {
      stats.counts.getCount should be > 500L
    }

  }

}
