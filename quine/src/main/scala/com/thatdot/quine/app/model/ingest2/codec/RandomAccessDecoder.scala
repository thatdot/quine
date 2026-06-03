package com.thatdot.quine.app.model.ingest2.codec

import scala.util.Try

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import com.thatdot.data.DataFoldableFrom

/** Decoder for formats that require random access to their input — formats whose
  * metadata is footer-indexed or otherwise not consumable in a single forward pass
  * (e.g. Apache Parquet, ORC).
  *
  * Per-record decode failures are emitted as `Failure[A]` so callers can route them
  * to a dead-letter sink. Companion abstraction to [[FrameDecoder]] (for upstream-framed
  * formats) and [[StreamingDecoder]] (for self-delimited forward-only formats).
  */
trait RandomAccessDecoder[A] {
  def dataFoldableFrom: DataFoldableFrom[A]

  /** Decode all records from `input`, emitting one `Try[A]` per record.
    *
    * The caller is responsible for materializing the right [[SeekableInput]] for
    * its byte source — a local file should use a file-backed input; an in-memory
    * upload should use a buffered input.
    */
  def decode(input: SeekableInput): Source[Try[A], NotUsed]
}
