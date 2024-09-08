package com.thatdot.quine.app.ingest2.sources

import java.nio.charset.Charset

import scala.util.{Success, Try}

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.connectors.csv.scaladsl.{CsvParsing, CsvToMap}
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Source}
import org.apache.pekko.util.ByteString

import com.thatdot.quine.app.ShutdownSwitch
import com.thatdot.quine.app.ingest.serialization.ContentDecoder
import com.thatdot.quine.app.ingest2.core.DataFoldableFrom
import com.thatdot.quine.app.ingest2.core.DataFoldableFrom._
import com.thatdot.quine.app.ingest2.source.{DecodedSource, IngestBounds}
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

  private val csvLineParser: Flow[ByteString, List[ByteString], NotUsed] =
    CsvParsing.lineScanner(delimiterChar, quoteChar, escapeChar, maximumLineSize)

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

      def stream: Source[(Try[T], Frame), ShutdownSwitch] = {

        val csvStream: Source[Success[T], NotUsed] = src
          .via(decompressingFlow(decoders))
          .via(csvLineParser)
          .via(boundingFlow(ingestBounds))
          .wireTap(bs => meter.mark(bs.map(_.length).sum))
          .via(parsingFlow)
          .map(scala.util.Success(_)) //TODO meaningfully extract errors

        withKillSwitches(csvStream.zipWith(src)(Keep.both))
      }

      val foldable: DataFoldableFrom[T] = foldableFrom
    }

}
