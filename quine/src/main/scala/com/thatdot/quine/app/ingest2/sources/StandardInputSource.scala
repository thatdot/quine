package com.thatdot.quine.app.ingest2.sources

import java.nio.charset.Charset

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Source, StreamConverters}
import org.apache.pekko.util.ByteString

import com.thatdot.quine.app.ingest.serialization.ContentDecoder
import com.thatdot.quine.app.ingest2.source._
import com.thatdot.quine.app.ingest2.sources.FileSource.decodedSourceFromFileStream
import com.thatdot.quine.app.routes.IngestMeter
import com.thatdot.quine.routes.FileIngestFormat

case class StandardInputSource(
  format: FileIngestFormat,
  maximumLineSize: Int,
  charset: Charset = DEFAULT_CHARSET,
  meter: IngestMeter,
  decoders: Seq[ContentDecoder] = Seq(),
) {

  val meteredDecompressedSource: Source[ByteString, NotUsed] =
    StreamConverters
      .fromInputStream(() => System.in)
      .mapMaterializedValue(_ => NotUsed)
      .via(metered(meter, _.size))
      .via(decompressingFlow(decoders))

  def decodedSource: DecodedSource = decodedSourceFromFileStream(
    meteredDecompressedSource,
    format,
    charset,
    maximumLineSize,
    IngestBounds(),
    meter,
    decoders,
  )

}
