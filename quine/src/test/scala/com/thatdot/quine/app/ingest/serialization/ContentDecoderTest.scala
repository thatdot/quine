package com.thatdot.quine.app.ingest.serialization
import java.io.ByteArrayOutputStream
import java.util.Base64
import java.util.zip.{Deflater, GZIPOutputStream}

import scala.util.Random

import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.app.model.ingest.serialization.ContentDecoder

class ContentDecoderTest extends AnyFunSuite {

  def gzipCompress(input: Array[Byte]): Array[Byte] = {
    val bos = new ByteArrayOutputStream(input.length)
    val gzip = new GZIPOutputStream(bos)
    gzip.write(input)
    gzip.close()
    val compressed = bos.toByteArray
    bos.close()
    compressed
  }

  def zlibCompress(inData: Array[Byte]): Array[Byte] = {
    val deflater: Deflater = new Deflater()
    deflater.setInput(inData)
    deflater.finish()
    val compressedData = new Array[Byte](inData.size * 2) // compressed data can be larger than original data
    val count: Int = deflater.deflate(compressedData)
    compressedData.take(count)
  }

  test("decompress base64 gzip") {
    1.to(10).foreach { i =>
      val s = Random.nextString(10)
      val encodedBytes = Base64.getEncoder.encode(gzipCompress(s.getBytes()))
      val decodedBytes =
        ContentDecoder.decode(Seq(ContentDecoder.Base64Decoder, ContentDecoder.GzipDecoder), encodedBytes)
      assert(new String(decodedBytes) == s)
    }

  }

  test("decompress base64 zlib") {
    1.to(10).foreach { i =>
      val s = Random.nextString(10)
      val encodedBytes = Base64.getEncoder.encode(zlibCompress(s.getBytes()))
      val decodedBytes =
        ContentDecoder.decode(Seq(ContentDecoder.Base64Decoder, ContentDecoder.ZlibDecoder), encodedBytes)
      assert(new String(decodedBytes) == s)
    }

  }

}
