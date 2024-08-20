package com.thatdot.quine.ingest2

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Failure, Random, Success}

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.util.ByteString

import com.thatdot.quine.app.ingest2.core.DataFolderTo
import com.thatdot.quine.app.ingest2.source.DecodedSource
import com.thatdot.quine.graph.cypher.Value

object IngestSourceTestSupport {

  def srcFromString(raw: String): Source[ByteString, NotUsed] = Source(raw.map(ByteString(_)))

  /** Collect generated cypher values from a decoded source. Assumes all values are a success. */
  def streamedCypherValues(src: DecodedSource)(implicit mat: Materializer): immutable.Iterable[Value] = {
    val results = src.stream
      .map {
        case (Success(a), _) => src.foldable.fold(a, DataFolderTo.cypherValueFolder)
        case (Failure(e), _) => throw e
      }
      .runWith(Sink.collection)

    Await.result(results, Duration.Inf)
  }

  def randomString(length: Int = 10): String = Random.alphanumeric.take(length).mkString("")

}
