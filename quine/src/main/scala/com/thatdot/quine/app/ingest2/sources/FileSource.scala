package com.thatdot.quine.app.ingest2.sources

import java.nio.charset.Charset
import java.nio.file.Paths

import org.apache.pekko.NotUsed
import org.apache.pekko.http.scaladsl.common.EntityStreamingSupport
import org.apache.pekko.stream.scaladsl.{Flow, Framing, Source}
import org.apache.pekko.util.ByteString

import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.app.ingest.NamedPipeSource
import com.thatdot.quine.app.ingest.serialization.ContentDecoder
import com.thatdot.quine.app.ingest2.codec.{CypherStringDecoder, FrameDecoder, JsonDecoder}
import com.thatdot.quine.app.ingest2.source._
import com.thatdot.quine.app.routes.IngestMeter
import com.thatdot.quine.routes.FileIngestFormat.{CypherCsv, CypherJson, CypherLine}
import com.thatdot.quine.routes.{FileIngestFormat, FileIngestMode}
import com.thatdot.quine.util.Log.LogConfig

case class FramedFileSource(
  src: Source[ByteString, NotUsed],
  charset: Charset = DEFAULT_CHARSET,
  delimiterFlow: Flow[ByteString, ByteString, NotUsed],
  bounds: IngestBounds = IngestBounds(),
  ingestMeter: IngestMeter
) extends BoundedSource {

  private val framedSource: FramedSource[ByteString] =
    new FramedSource[ByteString](
      withKillSwitches(
        src
          .via(transcodingFlow(charset))
          .via(delimiterFlow)
          //Note: bounding is applied _after_ delimiter.
          .via(boundingFlow)
          .via(metered(ingestMeter, _.size))
      ),
      ingestMeter
    ) {
      override def content(input: ByteString): Array[Byte] = input.toArrayUnsafe()
    }

  def decodedSource[A](decoder: FrameDecoder[A]): DecodedSource = framedSource.toDecoded(decoder)
}

object FileSource extends LazyLogging {

  private val jsonDelimitingFlow: Flow[ByteString, ByteString, NotUsed] = EntityStreamingSupport.json().framingDecoder

  private def lineDelimitingFlow(maximumLineSize: Int): Flow[ByteString, ByteString, NotUsed] = Framing
    .delimiter(ByteString("\n"), maximumLineSize, allowTruncation = true)
    .map(line => if (!line.isEmpty && line.last == '\r') line.dropRight(1) else line)

  def srcFromIngest(path: String, fileIngestMode: Option[FileIngestMode])(implicit
    logConfig: LogConfig
  ): Source[ByteString, NotUsed] =
    NamedPipeSource.fileOrNamedPipeSource(Paths.get(path), fileIngestMode)

  def decodedSourceFromFileStream(
    fileSource: Source[ByteString, NotUsed],
    format: FileIngestFormat,
    charset: Charset,
    maximumLineSize: Int,
    bounds: IngestBounds = IngestBounds(),
    meter: IngestMeter,
    decoders: Seq[ContentDecoder] = Seq()
  ): DecodedSource = {

    val decompressedSource =
      fileSource.via(decompressed(decoders))

    format match {
      case _: CypherLine =>
        FramedFileSource(
          decompressedSource,
          charset,
          lineDelimitingFlow(maximumLineSize),
          bounds,
          meter
        ).decodedSource(CypherStringDecoder)
      case _: CypherJson =>
        FramedFileSource(
          decompressedSource,
          charset,
          jsonDelimitingFlow,
          bounds,
          meter
        ).decodedSource(JsonDecoder)
      case CypherCsv(_, _, headers, delimiter, quoteChar, escapeChar) =>
        CsvFileSource(
          decompressedSource,
          meter,
          bounds,
          headers,
          charset,
          delimiter.byte,
          quoteChar.byte,
          escapeChar.byte,
          maximumLineSize
        ).decodedSource
    }
  }

}
