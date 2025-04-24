package com.thatdot.quine.app.model.ingest.serialization

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.Base64
import java.util.zip.{GZIPInputStream, GZIPOutputStream, InflaterInputStream, InflaterOutputStream}

import org.apache.pekko.NotUsed
import org.apache.pekko.http.scaladsl.coding.Coders
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.util.ByteString

import com.thatdot.quine.routes.RecordDecodingType

/** A class that corresponds to a single type of content decoding.
  * Instances define a flow that performs decoding of the specified type.
  */

sealed abstract class ContentDecoder() {
  def decode(bytes: Array[Byte]): Array[Byte]
  def encode(bytes: Array[Byte]): Array[Byte]
  def decoderFlow: Flow[ByteString, ByteString, NotUsed]
  def encoderFlow: Flow[ByteString, ByteString, NotUsed]
}

object ContentDecoder {

  case object Base64Decoder extends ContentDecoder {

    private val base64Decoder: Base64.Decoder = Base64.getDecoder
    private val base64Encoder: Base64.Encoder = Base64.getEncoder
    override def decode(bytes: Array[Byte]): Array[Byte] = base64Decoder.decode(bytes)

    override def encode(bytes: Array[Byte]): Array[Byte] = base64Encoder.encode(bytes)

    override def decoderFlow: Flow[ByteString, ByteString, NotUsed] =
      Flow[ByteString].map(bs => ByteString(decode(bs.toArrayUnsafe())))
    override def encoderFlow: Flow[ByteString, ByteString, NotUsed] =
      Flow[ByteString].map(bs => ByteString(encode(bs.toArrayUnsafe())))
  }

  case object GzipDecoder extends ContentDecoder {

    override def decode(bytes: Array[Byte]): Array[Byte] = {
      val is = new GZIPInputStream(new ByteArrayInputStream(bytes))
      try is.readAllBytes()
      finally is.close()
    }
    override def encode(bytes: Array[Byte]): Array[Byte] = {
      val out = new ByteArrayOutputStream(bytes.length)
      val gzOut = new GZIPOutputStream(out)
      gzOut.write(bytes)
      gzOut.close()
      out.toByteArray
    }

    def decoderFlow: Flow[ByteString, ByteString, NotUsed] = Coders.Gzip.decoderFlow
    def encoderFlow: Flow[ByteString, ByteString, NotUsed] = Coders.Gzip.encoderFlow
  }

  case object ZlibDecoder extends ContentDecoder {
    override def decode(bytes: Array[Byte]): Array[Byte] = {
      val is = new InflaterInputStream(new ByteArrayInputStream(bytes))
      try is.readAllBytes()
      finally is.close()
    }
    override def encode(bytes: Array[Byte]): Array[Byte] = {
      val out = new ByteArrayOutputStream()
      val zOut = new InflaterOutputStream(out)
      zOut.write(bytes)
      zOut.flush()
      zOut.close()
      out.toByteArray
    }

    def decoderFlow: Flow[ByteString, ByteString, NotUsed] = Coders.Deflate.decoderFlow
    def encoderFlow: Flow[ByteString, ByteString, NotUsed] = Coders.Deflate.encoderFlow
  }

  /** V1 entities. */
  def apply(encodingType: RecordDecodingType): ContentDecoder = encodingType match {
    case RecordDecodingType.Base64 => Base64Decoder
    case RecordDecodingType.Gzip => GzipDecoder
    case RecordDecodingType.Zlib => ZlibDecoder
  }

  def encode(decoders: Seq[ContentDecoder], bytes: Array[Byte]): Array[Byte] =
    decoders.foldRight(bytes)((d, b) => d.encode(b))
  def decode(decoders: Seq[ContentDecoder], bytes: Array[Byte]): Array[Byte] =
    decoders.foldLeft(bytes)((b, d) => d.decode(b))

  def decode(decoders: Seq[ContentDecoder], bytes: ByteString): ByteString =
    if (decoders.nonEmpty) ByteString(decode(decoders, bytes.toArrayUnsafe())) else bytes

  def decoderFlow(decoders: Seq[ContentDecoder]): Flow[ByteString, ByteString, NotUsed] =
    decoders.foldLeft(Flow[ByteString])((flow, decoder) => flow.via(decoder.decoderFlow))

  def encoderFlow(decoders: Seq[ContentDecoder]): Flow[ByteString, ByteString, NotUsed] =
    decoders.foldRight(Flow[ByteString])((decoder, flow) => flow.via(decoder.encoderFlow))

}
