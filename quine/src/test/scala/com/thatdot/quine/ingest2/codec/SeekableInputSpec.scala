package com.thatdot.quine.ingest2.codec

import java.io.{File, IOException}
import java.nio.file.Files

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.app.model.ingest2.codec.{
  BufferedSeekableInput,
  FileSeekableInput,
  SeekableInput,
  SeekableInputStream,
}

class SeekableInputSpec extends AnyFunSpec with Matchers {

  // A reproducible byte pattern we can spot-check at any position
  private def pattern(length: Int): Array[Byte] = Array.tabulate(length)(i => (i % 256).toByte)

  private def writeTemp(bytes: Array[Byte]): File = {
    val f = Files.createTempFile("seekable-input-test-", ".bin").toFile
    Files.write(f.toPath, bytes)
    f.deleteOnExit()
    f
  }

  /** Tests that should hold for any [[SeekableInput]] implementation, run twice. */
  private def behavesAsSeekableInput(name: String, mkInput: Array[Byte] => SeekableInput): Unit =
    describe(name) {
      it("reports the correct length") {
        mkInput(pattern(100)).length shouldEqual 100L
      }

      it("opens at position 0 and reads sequentially") {
        val s = mkInput(pattern(10)).newStream()
        try {
          s.position shouldEqual 0L
          for (i <- 0 until 10) {
            s.read() shouldEqual i // pattern(i) = (i % 256) as unsigned
            s.position shouldEqual (i + 1).toLong
          }
          s.read() shouldEqual -1 // EOF
        } finally s.close()
      }

      it("reads into a buffer with offset and length") {
        val s = mkInput(pattern(20)).newStream()
        try {
          val buf = new Array[Byte](10)
          val n = s.read(buf, 2, 5)
          n shouldEqual 5
          // buf[0..1] untouched (zero); buf[2..6] = pattern[0..4]; buf[7..9] untouched
          buf.toSeq shouldEqual Seq[Byte](0, 0, 0, 1, 2, 3, 4, 0, 0, 0)
          s.position shouldEqual 5L
        } finally s.close()
      }

      it("seeks to an arbitrary position and reads from there") {
        val s = mkInput(pattern(50)).newStream()
        try {
          s.seek(30L)
          s.position shouldEqual 30L
          s.read() shouldEqual 30
          s.read() shouldEqual 31
        } finally s.close()
      }

      it("seeks forward and backward") {
        val s = mkInput(pattern(50)).newStream()
        try {
          s.seek(40L)
          s.read() shouldEqual 40
          s.seek(10L)
          s.read() shouldEqual 10
          s.seek(0L)
          s.read() shouldEqual 0
        } finally s.close()
      }

      it("seeks to end-of-stream (length) and reads EOF") {
        val s = mkInput(pattern(10)).newStream()
        try {
          s.seek(10L)
          s.position shouldEqual 10L
          s.read() shouldEqual -1
        } finally s.close()
      }

      it("rejects seek to negative position") {
        val s = mkInput(pattern(10)).newStream()
        try a[IOException] should be thrownBy s.seek(-1L)
        finally s.close()
      }

      it("rejects seek past end-of-stream") {
        val s = mkInput(pattern(10)).newStream()
        try a[IOException] should be thrownBy s.seek(11L)
        finally s.close()
      }

      it("each newStream() opens a fresh position-0 view") {
        val input = mkInput(pattern(10))
        val a = input.newStream()
        try {
          a.seek(5L)
          a.read() shouldEqual 5
        } finally a.close()
        val b = input.newStream()
        try {
          b.position shouldEqual 0L
          b.read() shouldEqual 0
        } finally b.close()
      }

      it("partial read at EOF returns -1") {
        val s = mkInput(pattern(5)).newStream()
        try {
          val buf = new Array[Byte](10)
          val n1 = s.read(buf, 0, 10)
          n1 shouldEqual 5
          val n2 = s.read(buf, 0, 10)
          n2 shouldEqual -1
        } finally s.close()
      }

      it("skip advances position without reading") {
        val s = mkInput(pattern(20)).newStream()
        try {
          s.skip(7L) shouldEqual 7L
          s.position shouldEqual 7L
          s.read() shouldEqual 7
        } finally s.close()
      }

      it("skip clamps to length") {
        val s = mkInput(pattern(10)).newStream()
        try {
          s.skip(1000L) shouldEqual 10L
          s.read() shouldEqual -1
        } finally s.close()
      }
    }

  // Empty-input edge cases worth checking on each backend
  private def behavesAsEmptySeekableInput(name: String, mkInput: Array[Byte] => SeekableInput): Unit =
    describe(s"$name with empty input") {
      it("has length 0") {
        mkInput(Array.empty).length shouldEqual 0L
      }

      it("reads EOF immediately") {
        val s = mkInput(Array.empty).newStream()
        try s.read() shouldEqual -1
        finally s.close()
      }

      it("allows seek to 0") {
        val s = mkInput(Array.empty).newStream()
        try {
          s.seek(0L)
          s.position shouldEqual 0L
        } finally s.close()
      }
    }

  behavesAsSeekableInput("BufferedSeekableInput", bytes => new BufferedSeekableInput(bytes))
  behavesAsEmptySeekableInput("BufferedSeekableInput", bytes => new BufferedSeekableInput(bytes))

  behavesAsSeekableInput("FileSeekableInput", bytes => new FileSeekableInput(writeTemp(bytes)))
  behavesAsEmptySeekableInput("FileSeekableInput", bytes => new FileSeekableInput(writeTemp(bytes)))

  // Cross-backend equivalence: any read sequence produces the same bytes
  describe("BufferedSeekableInput and FileSeekableInput agree") {
    it("on a full sequential read") {
      val bytes = pattern(1024)
      val fromBuf = drain(new BufferedSeekableInput(bytes).newStream())
      val fromFile = drain(new FileSeekableInput(writeTemp(bytes)).newStream())
      fromBuf.toSeq shouldEqual fromFile.toSeq
    }

    it("on a seek-and-read pattern") {
      val bytes = pattern(1024)
      def readAt(s: SeekableInputStream, pos: Long, len: Int): Array[Byte] = {
        s.seek(pos)
        val buf = new Array[Byte](len)
        var read = 0
        while (read < len) {
          val n = s.read(buf, read, len - read)
          if (n < 0) throw new RuntimeException("unexpected EOF")
          read += n
        }
        buf
      }
      val buf = new BufferedSeekableInput(bytes).newStream()
      val file = new FileSeekableInput(writeTemp(bytes)).newStream()
      try Seq(0L, 100L, 500L, 800L, 1023L).foreach { pos =>
        val n = math.min(16, (1024L - pos).toInt)
        readAt(buf, pos, n).toSeq shouldEqual readAt(file, pos, n).toSeq
      } finally {
        buf.close()
        file.close()
      }
    }
  }

  private def drain(s: SeekableInputStream): Array[Byte] = {
    val out = new java.io.ByteArrayOutputStream()
    val tmp = new Array[Byte](64)
    var n = s.read(tmp)
    while (n > 0) {
      out.write(tmp, 0, n)
      n = s.read(tmp)
    }
    s.close()
    out.toByteArray
  }
}
