package com.thatdot.quine.app.model.ingest2.codec

import java.io.IOException

/** A [[SeekableInput]] backed by an in-memory byte array.
  *
  * Used when the input is already in memory — e.g. small uploads, content fetched
  * over HTTP, or test fixtures. Implements the random-access semantics that
  * footer-indexed formats require without writing to disk or requiring a Hadoop
  * filesystem.
  */
final class BufferedSeekableInput(bytes: Array[Byte]) extends SeekableInput {

  override def length: Long = bytes.length.toLong

  override def newStream(): SeekableInputStream = new BufferedSeekableInputStream(bytes)
}

final private class BufferedSeekableInputStream(bytes: Array[Byte]) extends SeekableInputStream {
  private var pos: Long = 0L

  override def position: Long = pos

  override def seek(newPos: Long): Unit =
    if (newPos < 0L || newPos > bytes.length.toLong)
      throw new IOException(s"BufferedSeekableInput: seek to $newPos outside [0, ${bytes.length}]")
    else
      pos = newPos

  override def read(): Int =
    if (pos >= bytes.length.toLong) -1
    else {
      val b = bytes(pos.toInt) & 0xFF
      pos += 1
      b
    }

  override def read(buf: Array[Byte], off: Int, len: Int): Int = {
    if (off < 0 || len < 0 || len > buf.length - off)
      throw new IndexOutOfBoundsException
    if (pos >= bytes.length.toLong) -1
    else if (len == 0) 0
    else {
      val available = (bytes.length.toLong - pos).toInt
      val toCopy = math.min(len, available)
      System.arraycopy(bytes, pos.toInt, buf, off, toCopy)
      pos += toCopy.toLong
      toCopy
    }
  }

  override def available(): Int = math.max(0, bytes.length - pos.toInt)

  override def skip(n: Long): Long =
    if (n <= 0L) 0L
    else {
      val skipped = math.min(n, bytes.length.toLong - pos)
      pos += skipped
      skipped
    }
}
