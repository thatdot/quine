package com.thatdot.quine.app.model.ingest2.sources

import java.nio.charset.{Charset, StandardCharsets}

import org.apache.pekko.NotUsed
import org.apache.pekko.http.scaladsl.common.EntityStreamingSupport
import org.apache.pekko.stream.connectors.csv.scaladsl.{CsvParsing, CsvToMap}
import org.apache.pekko.stream.scaladsl.{Flow, Framing, JsonFraming, Source}
import org.apache.pekko.util.ByteString

import cats.data.ValidatedNel
import cats.implicits.catsSyntaxValidatedId
import cats.syntax.either._
import com.typesafe.scalalogging.LazyLogging
import io.circe.parser

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.data.DataFoldableFrom
import com.thatdot.quine.app.ShutdownSwitch
import com.thatdot.quine.app.config.FileAccessPolicy
import com.thatdot.quine.app.model.ingest.NamedPipeSource
import com.thatdot.quine.app.model.ingest.serialization.ContentDecoder
import com.thatdot.quine.app.model.ingest2.V2IngestEntities.FileFormat
import com.thatdot.quine.app.model.ingest2.codec.{CypherStringDecoder, FrameDecoder, JsonDecoder}
import com.thatdot.quine.app.model.ingest2.source.{DecodedSource, FramedSource, IngestBounds}
import com.thatdot.quine.app.routes.IngestMeter
import com.thatdot.quine.routes.FileIngestMode
import com.thatdot.quine.util.BaseError

/** Build a framed source from a file-like stream of ByteStrings. In practice this
  * means a finite, non-streaming source: File sources, S3 file sources, and std ingest.
  *
  * This framing provides
  * - ingest bounds
  * - char encoding
  * - compression
  * - record delimit sizing
  * - metering
  *
  * so these capabilities should not be applied to the provided src stream.
  */
case class FramedFileSource(
  src: Source[ByteString, NotUsed],
  charset: Charset = DEFAULT_CHARSET,
  delimiterFlow: Flow[ByteString, ByteString, NotUsed],
  ingestBounds: IngestBounds = IngestBounds(),
  decoders: Seq[ContentDecoder] = Seq(),
  ingestMeter: IngestMeter,
) {

  val source: Source[ByteString, NotUsed] =
    src
      .via(decompressingFlow(decoders))
      .via(transcodingFlow(charset))
      .via(delimiterFlow) // TODO note this will not properly delimit streaming binary formats (e.g. protobuf)
      //Note: bounding is applied _after_ delimiter.
      .via(boundingFlow(ingestBounds))
      .via(metered(ingestMeter, _.size))

  private def framedSource: FramedSource =
    new FramedSource {

      type SrcFrame = ByteString
      val stream: Source[SrcFrame, ShutdownSwitch] = withKillSwitches(source)
      val meter: IngestMeter = ingestMeter

      def content(input: SrcFrame): Array[Byte] = input.toArrayUnsafe()
      val foldableFrame: DataFoldableFrom[SrcFrame] = DataFoldableFrom.byteStringDataFoldable

    }

  def decodedSource[A](decoder: FrameDecoder[A]): DecodedSource = framedSource.toDecoded(decoder)

}

object FileSource extends LazyLogging {

  private def jsonDelimitingFlow(maximumLineSize: Int): Flow[ByteString, ByteString, NotUsed] =
    EntityStreamingSupport.json(maximumLineSize).framingDecoder

  private def lineDelimitingFlow(maximumLineSize: Int): Flow[ByteString, ByteString, NotUsed] = Framing
    .delimiter(ByteString("\n"), maximumLineSize, allowTruncation = true)
    .map(line => if (!line.isEmpty && line.last == '\r') line.dropRight(1) else line)

  def srcFromIngest(
    path: String,
    fileIngestMode: Option[FileIngestMode],
    fileAccessPolicy: FileAccessPolicy,
  )(implicit
    logConfig: LogConfig,
  ): ValidatedNel[BaseError, Source[ByteString, NotUsed]] =
    FileAccessPolicy.validatePath(path, fileAccessPolicy).map { validatedPath =>
      NamedPipeSource.fileOrNamedPipeSource(validatedPath, fileIngestMode)
    }

  def decodedSourceFromFileStream(
    fileSource: Source[ByteString, NotUsed],
    format: FileFormat,
    charset: Charset,
    maximumLineSize: Int,
    bounds: IngestBounds = IngestBounds(),
    meter: IngestMeter,
    decoders: Seq[ContentDecoder] = Seq(),
  ): ValidatedNel[BaseError, DecodedSource] =
    format match {
      case FileFormat.LineFormat =>
        FramedFileSource(
          fileSource,
          charset,
          lineDelimitingFlow(maximumLineSize),
          bounds,
          decoders,
          meter,
        ).decodedSource(CypherStringDecoder).valid
      case FileFormat.JsonLinesFormat =>
        FramedFileSource(
          fileSource,
          charset,
          lineDelimitingFlow(maximumLineSize),
          bounds,
          decoders,
          meter,
        ).decodedSource(JsonDecoder).valid
      case FileFormat.JsonFormat =>
        FramedFileSource(
          fileSource,
          charset,
          jsonDelimitingFlow(maximumLineSize),
          bounds,
          decoders,
          meter,
        ).decodedSource(JsonDecoder).valid
      case FileFormat.CsvFormat(headers, delimiter, quoteChar, escapeChar) =>
        CsvFileSource(
          fileSource,
          bounds,
          meter,
          headers,
          charset,
          delimiter.byte,
          quoteChar.byte,
          escapeChar.byte,
          maximumLineSize,
          decoders,
        ).decodedSource.valid

    }

  def decodingFoldableFrom(fileFormat: FileFormat, meter: IngestMeter, maximumLineSize: Int): DecodingFoldableFrom =
    fileFormat match {
      case FileFormat.LineFormat =>
        new DecodingFoldableFrom {
          override type Element = String

          override def decodingFlow: Flow[ByteString, Element, NotUsed] =
            lineDelimitingFlow(maximumLineSize).map { byteString =>
              val bytes = byteString.toArray
              meter.mark(bytes.length)
              new String(bytes, StandardCharsets.UTF_8)
            }
          override val dataFoldableFrom: DataFoldableFrom[String] = DataFoldableFrom.stringDataFoldable
        }
      case FileFormat.JsonLinesFormat =>
        new DecodingFoldableFrom {
          override type Element = io.circe.Json

          override def decodingFlow: Flow[ByteString, Element, NotUsed] =
            Framing
              .delimiter(ByteString("\n"), maximumFrameLength = Int.MaxValue, allowTruncation = true)
              .wireTap(line => meter.mark(line.length))
              .map((bs: ByteString) => parser.parse(bs.utf8String).valueOr(throw _))

          override val dataFoldableFrom: DataFoldableFrom[Element] = DataFoldableFrom.jsonDataFoldable
        }
      case FileFormat.JsonFormat =>
        new DecodingFoldableFrom {
          override type Element = io.circe.Json

          override def decodingFlow: Flow[ByteString, Element, NotUsed] =
            JsonFraming
              .objectScanner(maximumObjectLength = Int.MaxValue)
              .wireTap(obj => meter.mark(obj.length))
              .map((bs: ByteString) => parser.parse(bs.utf8String).valueOr(throw _))

          override val dataFoldableFrom: DataFoldableFrom[Element] = DataFoldableFrom.jsonDataFoldable
        }
      case FileFormat.CsvFormat(headers, delimiter, quoteChar, escapeChar) =>
        def lineBytes(line: List[ByteString]): Int =
          line.foldLeft(0)((size, field) => size + field.length)

        def meterLineBytes: List[ByteString] => Unit = { line =>
          meter.mark(lineBytes(line))
        }

        headers match {
          case Left(firstLineIsHeader) =>
            if (firstLineIsHeader) {
              new DecodingFoldableFrom {
                override type Element = Map[String, String]
                override val dataFoldableFrom: DataFoldableFrom[Element] = DataFoldableFrom.stringMapDataFoldable

                override def decodingFlow: Flow[ByteString, Element, NotUsed] = CsvParsing
                  .lineScanner(
                    delimiter = delimiter.byte,
                    quoteChar = quoteChar.byte,
                    escapeChar = escapeChar.byte,
                  )
                  .wireTap(meterLineBytes)
                  .via(CsvToMap.toMapAsStrings())
              }
            } else {
              new DecodingFoldableFrom {
                override type Element = Vector[String]
                override val dataFoldableFrom: DataFoldableFrom[Element] = DataFoldableFrom.stringVectorDataFoldable

                override def decodingFlow: Flow[ByteString, Element, NotUsed] = CsvParsing
                  .lineScanner(
                    delimiter = delimiter.byte,
                    quoteChar = quoteChar.byte,
                    escapeChar = escapeChar.byte,
                  )
                  .wireTap(meterLineBytes)
                  .map(byteStringList => byteStringList.map(_.utf8String).toVector)
              }
            }
          case Right(staticFieldNames) =>
            new DecodingFoldableFrom {
              override type Element = Map[String, String]
              override val dataFoldableFrom: DataFoldableFrom[Element] = DataFoldableFrom.stringMapDataFoldable

              override def decodingFlow: Flow[ByteString, Element, NotUsed] = CsvParsing
                .lineScanner(
                  delimiter = delimiter.byte,
                  quoteChar = quoteChar.byte,
                  escapeChar = escapeChar.byte,
                )
                .wireTap(meterLineBytes)
                .via(CsvToMap.withHeaders(staticFieldNames: _*).map(_.view.mapValues(_.utf8String).toMap))
            }

        }
    }
}
