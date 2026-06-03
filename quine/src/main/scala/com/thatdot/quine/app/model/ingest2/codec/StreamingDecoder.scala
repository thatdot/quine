package com.thatdot.quine.app.model.ingest2.codec

import scala.util.Try

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.util.ByteString

import com.thatdot.data.DataFoldableFrom

/** Decoder for a self-delimited forward-only byte stream — formats whose record framing
  * is internal to the byte stream itself (e.g. Avro container files).
  *
  * Per-record decode failures are emitted as `Failure[A]` so callers can route them to
  * a dead-letter sink. Init/header failures propagate as stream failures so callers can
  * retry via re-materialization (when their upstream byte source is restartable).
  *
  * Companion abstraction to [[FrameDecoder]] (for upstream-framed formats) and
  * [[RandomAccessDecoder]] (for seekable formats).
  */
trait StreamingDecoder[A] {
  def dataFoldableFrom: DataFoldableFrom[A]

  /** A Flow that consumes raw bytes and emits one `Try[A]` per decoded record. */
  def decodeFlow: Flow[ByteString, Try[A], NotUsed]
}
