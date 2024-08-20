package com.thatdot.quine.app.ingest2.source

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow

/** A source that provides a source of raw values.
  * Most sources are defined in terms of a provider of frames, where each frame contains a value
  * that can be decoded from the bytes corresponding to a single value. To retrieve decoded values a [[FramedSource]] must be paired with a [[com.thatdot.quine.app.ingest2.codec.FrameDecoder]].
  *
  * For a few types decoding is entangled with framing and so we generate decoded sources directly.
  */
trait FramedSourceProvider[A] {
  def framedSource: FramedSource[A]
}

case class IngestBounds(startAtOffset: Long = 0L, ingestLimit: Option[Long] = None)

/** A source that can be bounded by start and end settings. */
trait BoundedSource {

  val bounds: IngestBounds

  def boundingFlow[A]: Flow[A, A, NotUsed] =
    bounds.ingestLimit.fold(Flow[A].drop(bounds.startAtOffset))(limit => Flow[A].drop(bounds.startAtOffset).take(limit))
}
