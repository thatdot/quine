package com.thatdot.quine.app.model.ingest2.sources

import java.nio.charset.Charset

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Source, StreamConverters}
import org.apache.pekko.util.ByteString

import cats.data.ValidatedNel

import com.thatdot.quine.app.model.ingest.serialization.ContentDecoder
import com.thatdot.quine.app.model.ingest2.V2IngestEntities.FileFormat
import com.thatdot.quine.app.model.ingest2.source._
import com.thatdot.quine.app.model.ingest2.sources.FileSource.decodedSourceFromFileStream
import com.thatdot.quine.app.model.ingest2.sources.StandardInputSource.stdInSource
import com.thatdot.quine.app.routes.IngestMeter
import com.thatdot.quine.util.BaseError

case class StandardInputSource(
  format: FileFormat,
  maximumLineSize: Int,
  charset: Charset = DEFAULT_CHARSET,
  meter: IngestMeter,
  decoders: Seq[ContentDecoder] = Seq(),
) {

  def decodedSource: ValidatedNel[BaseError, DecodedSource] = decodedSourceFromFileStream(
    stdInSource,
    format,
    charset,
    maximumLineSize,
    IngestBounds(),
    meter,
    decoders,
  )

}

object StandardInputSource {
  def stdInSource: Source[ByteString, NotUsed] =
    StreamConverters
      .fromInputStream(() => System.in)
      .mapMaterializedValue(_ => NotUsed)
}
