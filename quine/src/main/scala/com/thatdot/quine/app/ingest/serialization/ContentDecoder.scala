package com.thatdot.quine.app.ingest.serialization

import java.io.ByteArrayInputStream
import java.util.Base64
import java.util.zip.{GZIPInputStream, InflaterInputStream}

import com.thatdot.quine.routes.RecordDecodingType

/** A class that corresponds to a single type of content decoding.
  * Instances define a flow that performs decoding of the specified type.
  */

sealed abstract class ContentDecoder() {
  def decode(bytes: Array[Byte]): Array[Byte]
}

object ContentDecoder {

  case object Base64Decoder extends ContentDecoder {

    private val base64Decoder: Base64.Decoder = Base64.getDecoder
    def decode(bytes: Array[Byte]): Array[Byte] = base64Decoder.decode(bytes)

  }

  case object GzipDecoder extends ContentDecoder {

    def decode(bytes: Array[Byte]): Array[Byte] = {
      val is = new GZIPInputStream(new ByteArrayInputStream(bytes))
      try is.readAllBytes()
      finally is.close()
    }
  }

  case object ZlibDecoder extends ContentDecoder {
    def decode(bytes: Array[Byte]): Array[Byte] = {
      val is = new InflaterInputStream(new ByteArrayInputStream(bytes))
      try is.readAllBytes()
      finally is.close()
    }
  }

  def apply(encodingType: RecordDecodingType): ContentDecoder = encodingType match {
    case RecordDecodingType.Base64 => Base64Decoder
    case RecordDecodingType.Gzip => GzipDecoder
    case RecordDecodingType.Zlib => ZlibDecoder
  }

  def decode(decoders: Seq[ContentDecoder], bytes: Array[Byte]): Array[Byte] =
    decoders.foldLeft(bytes)((b, d) => d.decode(b))

}
