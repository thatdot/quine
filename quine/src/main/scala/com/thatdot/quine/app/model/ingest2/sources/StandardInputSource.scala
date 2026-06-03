package com.thatdot.quine.app.model.ingest2.sources

import java.nio.charset.Charset

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Source, StreamConverters}
import org.apache.pekko.util.ByteString

import cats.data.Validated.invalidNel
import cats.data.ValidatedNel

import com.thatdot.quine.app.model.ingest.serialization.ContentDecoder
import com.thatdot.quine.app.model.ingest2.FileFormat
import com.thatdot.quine.app.model.ingest2.source._
import com.thatdot.quine.app.model.ingest2.sources.FileSource.decodedSourceFromFileStream
import com.thatdot.quine.app.model.ingest2.sources.StandardInputSource.stdInSource
import com.thatdot.quine.app.routes.IngestMeter
import com.thatdot.quine.exceptions.IngestSourceFormatException
import com.thatdot.quine.serialization.AvroSchemaCache
import com.thatdot.quine.util.BaseError

case class StandardInputSource(
  format: FileFormat,
  maximumLineSize: Int,
  charset: Charset = DEFAULT_CHARSET,
  meter: IngestMeter,
  decoders: Seq[ContentDecoder] = Seq(),
)(implicit avroSchemaCache: AvroSchemaCache) {

  def decodedSource: ValidatedNel[BaseError, DecodedSource] = format match {
    case _: FileFormat.RandomAccess =>
      // Random-access formats need to seek the input; stdin is a non-seekable forward stream.
      // We deliberately refuse here rather than buffer all of stdin to memory.
      invalidNel(
        IngestSourceFormatException(s"$format is not supported for stdin ingest (requires seekable input)"),
      )
    case _ =>
      decodedSourceFromFileStream(
        stdInSource,
        format,
        charset,
        maximumLineSize,
        IngestBounds(),
        meter,
        decoders,
      )
  }
}

object StandardInputSource {
  def stdInSource: Source[ByteString, NotUsed] =
    StreamConverters
      .fromInputStream(() => System.in)
      .mapMaterializedValue(_ => NotUsed)
}
