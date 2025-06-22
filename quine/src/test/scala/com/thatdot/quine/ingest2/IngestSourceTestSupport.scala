package com.thatdot.quine.ingest2

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Failure, Random, Success}

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.util.ByteString

import com.thatdot.quine.app.Metrics
import com.thatdot.quine.app.data.QuineDataFoldersTo
import com.thatdot.quine.app.model.ingest.serialization.ContentDecoder
import com.thatdot.quine.app.model.ingest2.V2IngestEntities.FileFormat
import com.thatdot.quine.app.model.ingest2.source.{DecodedSource, IngestBounds}
import com.thatdot.quine.app.model.ingest2.sources.FileSource.decodedSourceFromFileStream
import com.thatdot.quine.app.model.ingest2.sources.{DEFAULT_CHARSET, DEFAULT_MAXIMUM_LINE_SIZE}
import com.thatdot.quine.app.routes.IngestMetered
import com.thatdot.quine.graph.cypher.Value
import com.thatdot.quine.graph.metrics.HostQuineMetrics

object IngestSourceTestSupport {

  def srcFromString(raw: String): Source[ByteString, NotUsed] = Source(raw.map(ByteString(_)))

  /** Collect generated cypher values from a decoded source. Assumes all values are a success. */
  def streamedCypherValues(src: DecodedSource)(implicit mat: Materializer): immutable.Iterable[Value] = {
    val results = src.stream
      .map { case (triedDecoded, frame) => (triedDecoded(), frame) }
      .map {
        case (Success(a), _) => src.foldable.fold(a, QuineDataFoldersTo.cypherValueFolder)
        case (Failure(e), _) => throw e
      }
      .runWith(Sink.collection)

    Await.result(results, Duration.Inf)
  }

  def randomString(length: Int = 10): String = Random.alphanumeric.take(length).mkString("")

  def buildDecodedSource(
    source: Source[ByteString, NotUsed],
    format: FileFormat,
    bounds: IngestBounds = IngestBounds(),
    maximumLineSize: Int = DEFAULT_MAXIMUM_LINE_SIZE,
    contentDecoders: Seq[ContentDecoder] = Seq(),
  ): DecodedSource = {
    val meter = IngestMetered.ingestMeter(
      None,
      randomString(),
      HostQuineMetrics(enableDebugMetrics = false, metricRegistry = Metrics, omitDefaultNamespace = true),
    )

    decodedSourceFromFileStream(
      source,
      format,
      DEFAULT_CHARSET,
      maximumLineSize,
      bounds,
      meter,
      contentDecoders,
    ).toOption.get

  }
}
