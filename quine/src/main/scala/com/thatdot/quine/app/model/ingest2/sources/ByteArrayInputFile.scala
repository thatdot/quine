package com.thatdot.quine.app.model.ingest2.sources

import java.io.ByteArrayInputStream

import org.apache.parquet.io.{DelegatingSeekableInputStream, InputFile, SeekableInputStream}

/** Adapts an Array[Byte] to the org.apache.parquet.io.InputFile interface,
  * allowing parquet4s / parquet-java to deserialize Parquet data from memory
  * without writing to disk or requiring a Hadoop filesystem.
  */
final class ByteArrayInputFile(bytes: Array[Byte]) extends InputFile {

  override def getLength: Long = bytes.length.toLong

  override def newStream(): SeekableInputStream = {
    val inner = new ByteArrayInputStream(bytes)

    new DelegatingSeekableInputStream(inner) {
      private var pos: Long = 0L

      override def getPos: Long = pos

      override def seek(newPos: Long): Unit = {
        inner.reset()
        val skipped = inner.skip(newPos)
        if (skipped != newPos)
          throw new java.io.IOException(
            s"ByteArrayInputFile: seek to $newPos failed (skipped $skipped of ${bytes.length} bytes)",
          )
        pos = newPos
      }

      override def read(): Int = {
        val b = super.read()
        if (b >= 0) pos += 1
        b
      }

      override def read(buf: Array[Byte], off: Int, len: Int): Int = {
        val n = super.read(buf, off, len)
        if (n > 0) pos += n
        n
      }
    }
  }
}
