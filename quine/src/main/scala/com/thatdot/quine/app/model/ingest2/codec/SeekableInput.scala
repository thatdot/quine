package com.thatdot.quine.app.model.ingest2.codec

import java.io.InputStream

/** A handle to a fixed-size sequence of bytes that supports random-access reads.
  *
  * Used to provide [[RandomAccessDecoder]] implementations with a uniform input
  * abstraction across local files, in-memory buffers, and (future) remote sources
  * that support range reads.
  *
  * Owning this trait — rather than re-exporting a third-party random-access type —
  * keeps the interface oriented around our needs and leaves each decoder responsible
  * for bridging to whatever shape its underlying library requires.
  */
trait SeekableInput {

  /** Total length in bytes. */
  def length: Long

  /** Open a fresh seekable stream over the bytes, positioned at 0.
    * The caller is responsible for closing it.
    */
  def newStream(): SeekableInputStream
}

/** An `InputStream` extended with random-access position control.
  *
  * Implementations only need to provide `position`, `seek`, and the basic `read`
  * methods. Higher-level convenience methods (`readFully`, `ByteBuffer` reads, etc.)
  * are layered on by callers that need them — for example,
  * `org.apache.parquet.io.DelegatingSeekableInputStream` provides those defaults
  * for parquet-mr's seek interface, given an underlying `InputStream` plus `getPos`
  * and `seek`.
  */
trait SeekableInputStream extends InputStream {

  /** Current byte position. The next `read` starts here. */
  def position: Long

  /** Set the position. Must be in `[0, length]`; behavior outside that range is
    * implementation-defined (typically `IOException`).
    */
  def seek(newPos: Long): Unit
}
