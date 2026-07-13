package com.thatdot.quine.app.model.ingest2.sources

import java.nio.charset.{Charset, StandardCharsets}

import scala.util.{Success, Try}

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.connectors.csv.scaladsl.{CsvParsing, CsvToMap}
import org.apache.pekko.stream.scaladsl.{Flow, Source}
import org.apache.pekko.util.ByteString

import com.thatdot.data.DataFoldableFrom
import com.thatdot.data.DataFoldableFrom._
import com.thatdot.quine.app.ShutdownSwitch
import com.thatdot.quine.app.model.ingest.serialization.ContentDecoder
import com.thatdot.quine.app.model.ingest2.source.{DecodedSource, IngestBounds}
import com.thatdot.quine.app.model.ingest2.sources
import com.thatdot.quine.app.routes.IngestMeter
case class CsvFileSource(
  src: Source[ByteString, NotUsed],
  ingestBounds: IngestBounds,
  ingestMeter: IngestMeter,
  headers: Either[Boolean, List[String]],
  charset: Charset,
  delimiterChar: Byte,
  quoteChar: Byte,
  escapeChar: Byte,
  maximumLineSize: Int,
  decoders: Seq[ContentDecoder] = Seq(),
) {

  private val csvLineParser: Flow[ByteString, List[ByteString], NotUsed] = {
    val lineScanner = CsvParsing.lineScanner(delimiterChar, quoteChar, escapeChar, maximumLineSize)
    charset match {
      case StandardCharsets.UTF_8 | StandardCharsets.ISO_8859_1 | StandardCharsets.US_ASCII => lineScanner
      case _ =>
        sources
          .transcodingFlow(charset)
          .via(lineScanner)
          .map(_.map(bs => ByteString(bs.decodeString(StandardCharsets.UTF_8), charset)))
    }
  }

  def decodedSource: DecodedSource = headers match {

    case Right(h) => toDecodedSource(CsvToMap.withHeadersAsStrings(charset, h: _*), stringMapDataFoldable)

    case Left(true) => toDecodedSource(CsvToMap.toMapAsStrings(charset), stringMapDataFoldable)

    case Left(false) =>
      toDecodedSource(
        Flow[List[ByteString]]
          .map(l => l.map(bs => bs.decodeString(charset))),
        stringIterableDataFoldable,
      )
  }

  private def toDecodedSource[T](parsingFlow: Flow[List[ByteString], T, NotUsed], foldableFrom: DataFoldableFrom[T]) =
    new DecodedSource(ingestMeter) {
      type Decoded = T
      type Frame = ByteString

      override val foldableFrame: DataFoldableFrom[ByteString] = byteStringDataFoldable

      override def content(input: ByteString): Array[Byte] = input.toArrayUnsafe()

      def stream: Source[(() => Try[T], Frame), ShutdownSwitch] = {

        val csvStream: Source[(() => Try[T], ByteString), NotUsed] = src
          .via(decompressingFlow(decoders))
          .via(csvLineParser)
          .via(boundingFlow(ingestBounds))
          .wireTap(bs => meter.mark(bs.map(_.length).sum))
          .via(parsingFlow)
          // Always-Success with an empty Frame: pekko-streams CSV parsing throws on malformed
          // input and aborts the whole stream rather than producing a per-row Failure, and the
          // original row bytes are not recoverable after parsing. Possible improvement: pre-split
          // on newlines (losing quoted-newline support) so each row can be tried independently,
          // and carry the joined row bytes through as Frame for DLQ purposes.
          .map(value => (() => Success(value), ByteString.empty))

        withKillSwitches(csvStream)
      }

      val foldable: DataFoldableFrom[T] = foldableFrom
    }

}
