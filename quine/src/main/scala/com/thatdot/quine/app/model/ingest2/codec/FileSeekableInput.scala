package com.thatdot.quine.app.model.ingest2.codec

import java.io.{File, IOException, RandomAccessFile}

/** A [[SeekableInput]] backed by a local file path.
  *
  * Used when the input is a file already on disk — avoids the cost of buffering the
  * entire file into memory. Each `newStream()` call opens a fresh handle on the file,
  * positioned at 0; the caller closes it.
  */
final class FileSeekableInput(file: File) extends SeekableInput {

  override def length: Long = file.length()

  override def newStream(): SeekableInputStream = new FileSeekableInputStream(file)
}

final private class FileSeekableInputStream(file: File) extends SeekableInputStream {
  private val handle = new RandomAccessFile(file, "r")
  private val len = handle.length()

  override def position: Long = handle.getFilePointer

  override def seek(newPos: Long): Unit =
    if (newPos < 0L || newPos > len)
      throw new IOException(s"FileSeekableInput: seek to $newPos outside [0, $len]")
    else
      handle.seek(newPos)

  override def read(): Int = handle.read()

  override def read(buf: Array[Byte], off: Int, len: Int): Int = handle.read(buf, off, len)

  override def available(): Int = math.max(0, (len - handle.getFilePointer).toInt)

  override def skip(n: Long): Long =
    if (n <= 0L) 0L
    else {
      val current = handle.getFilePointer
      val target = math.min(current + n, len)
      handle.seek(target)
      target - current
    }

  override def close(): Unit = handle.close()
}
