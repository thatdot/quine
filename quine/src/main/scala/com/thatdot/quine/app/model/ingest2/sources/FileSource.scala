package com.thatdot.quine.app.model.ingest2.sources

import java.nio.charset.{Charset, StandardCharsets}

import scala.util.{Failure, Success, Try}

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
import com.thatdot.quine.app.model.ingest2.FileFormat
import com.thatdot.quine.app.model.ingest2.codec.{
  AvroContainerDecoder,
  BufferedSeekableInput,
  CypherStringDecoder,
  FrameDecoder,
  JsonDecoder,
  ParquetDecoder,
  RandomAccessDecoder,
  SeekableInput,
  StreamingDecoder,
}
import com.thatdot.quine.app.model.ingest2.source.{DecodedSource, FramedSource, IngestBounds}
import com.thatdot.quine.app.routes.IngestMeter
import com.thatdot.quine.routes.FileIngestMode
import com.thatdot.quine.serialization.AvroSchemaCache
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
  )(implicit avroSchemaCache: AvroSchemaCache): ValidatedNel[BaseError, DecodedSource] =
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
      case FileFormat.AvroContainerFormat(schemaUrl) =>
        decodedSourceFromStreamingDecoder(
          new AvroContainerDecoder(schemaUrl),
          fileSource,
          bounds,
          meter,
          decoders,
        ).valid
      case r: FileFormat.RandomAccess =>
        // Random-access formats can't stream — collect the byte source to memory, then
        // decode through a BufferedSeekableInput. Suitable for S3 and similar inputs that
        // present as a Source[ByteString] but where we have to read the whole object
        // anyway. For local files use `decodedSourceFromSeekableInput` to avoid the buffer.
        decodedSourceFromBufferedRandomAccess(
          randomAccessDecoder(r),
          fileSource,
          bounds,
          meter,
          decoders,
        ).valid
    }

  /** Wrap a [[StreamingDecoder]] as a [[DecodedSource]] by feeding upstream bytes through
    * decompression, the decoder, then bounding and metering. Byte counts are taken upstream
    * of decoding (so they reflect transferred bytes, not decoded record sizes); record
    * counts are taken downstream of decoding for successful decodes only.
    *
    * Successful records emit `ByteString.empty` for the Frame slot. Failures emit
    * `ByteString.empty` with the `Failure[_]` carrying any diagnostic — for self-delimited
    * formats the original per-record wire bytes are not recoverable, so emitting a
    * fabricated Frame would be misleading.
    */
  private def decodedSourceFromStreamingDecoder[A](
    decoder: StreamingDecoder[A],
    fileSource: Source[ByteString, NotUsed],
    bounds: IngestBounds,
    ingestMeter: IngestMeter,
    decoders: Seq[ContentDecoder],
  ): DecodedSource =
    new DecodedSource(ingestMeter) {
      type Decoded = A
      type Frame = ByteString
      val foldable: DataFoldableFrom[A] = decoder.dataFoldableFrom
      val foldableFrame: DataFoldableFrom[ByteString] = DataFoldableFrom.byteStringDataFoldable

      override def content(input: ByteString): Array[Byte] = input.toArrayUnsafe()

      override def stream: Source[(() => Try[A], ByteString), ShutdownSwitch] = {
        // NB: on a pre-first-success retry the byte source is re-materialized and bytes are
        // re-metered. Acceptable because such retries fire at very low byte counts (header
        // / schema-fetch failures) so the over-count is bounded.
        val recordSource: Source[(() => Try[A], ByteString), NotUsed] = fileSource
          .wireTap(b => ingestMeter.markBytes(b.size.toLong))
          .via(decompressingFlow(decoders))
          .via(decoder.decodeFlow)
          .via(boundingFlow(bounds))
          .wireTap(_.foreach(_ => ingestMeter.markRecord()))
          .map {
            case Success(r) => (() => Success(r)) -> ByteString.empty
            case f @ Failure(_) => (() => f) -> ByteString.empty
          }
        withKillSwitches(recordSource)
      }
    }

  /** Wrap a [[RandomAccessDecoder]] as a [[DecodedSource]] over a byte source by collecting
    * the bytes to memory and presenting them via [[BufferedSeekableInput]]. Use when the
    * underlying source can't be opened seekably (e.g. S3).
    *
    * Bytes are metered as they arrive (upstream of buffering). Records are metered after
    * decode for successful decodes only. The Frame slot follows the same "honest" rule as
    * the streaming-decoder helper: empty for both success and failure.
    */
  private def decodedSourceFromBufferedRandomAccess[A](
    decoder: RandomAccessDecoder[A],
    fileSource: Source[ByteString, NotUsed],
    bounds: IngestBounds,
    ingestMeter: IngestMeter,
    decoders: Seq[ContentDecoder],
  ): DecodedSource =
    new DecodedSource(ingestMeter) {
      type Decoded = A
      type Frame = ByteString
      val foldable: DataFoldableFrom[A] = decoder.dataFoldableFrom
      val foldableFrame: DataFoldableFrom[ByteString] = DataFoldableFrom.byteStringDataFoldable

      override def content(input: ByteString): Array[Byte] = input.toArrayUnsafe()

      override def stream: Source[(() => Try[A], ByteString), ShutdownSwitch] = {
        val recordSource: Source[(() => Try[A], ByteString), NotUsed] = fileSource
          .wireTap(b => ingestMeter.markBytes(b.size.toLong))
          .via(decompressingFlow(decoders))
          .fold(ByteString.empty)(_ ++ _)
          .flatMapConcat(bs => decoder.decode(new BufferedSeekableInput(bs.toArrayUnsafe())))
          .via(boundingFlow(bounds))
          .wireTap(_.foreach(_ => ingestMeter.markRecord()))
          .map {
            case Success(r) => (() => Success(r)) -> ByteString.empty
            case f @ Failure(_) => (() => f) -> ByteString.empty
          }
        withKillSwitches(recordSource)
      }
    }

  /** Wrap a [[RandomAccessDecoder]] as a [[DecodedSource]] over a [[SeekableInput]].
    * Used when the input is natively seekable (local file) and we can avoid buffering.
    *
    * Byte metering reports the input length once at materialization time (the underlying
    * seekable source doesn't surface per-read byte counts). Record metering is per-record.
    */
  def decodedSourceFromSeekableInput[A](
    decoder: RandomAccessDecoder[A],
    seekableInput: SeekableInput,
    bounds: IngestBounds,
    ingestMeter: IngestMeter,
  ): DecodedSource =
    new DecodedSource(ingestMeter) {
      type Decoded = A
      type Frame = ByteString
      val foldable: DataFoldableFrom[A] = decoder.dataFoldableFrom
      val foldableFrame: DataFoldableFrom[ByteString] = DataFoldableFrom.byteStringDataFoldable

      override def content(input: ByteString): Array[Byte] = input.toArrayUnsafe()

      override def stream: Source[(() => Try[A], ByteString), ShutdownSwitch] = {
        val recordSource: Source[(() => Try[A], ByteString), NotUsed] = Source
          .lazySource { () =>
            ingestMeter.markBytes(seekableInput.length)
            decoder.decode(seekableInput)
          }
          .mapMaterializedValue(_ => NotUsed)
          .via(boundingFlow(bounds))
          .wireTap(_.foreach(_ => ingestMeter.markRecord()))
          .map {
            case Success(r) => (() => Success(r)) -> ByteString.empty
            case f @ Failure(_) => (() => f) -> ByteString.empty
          }
        withKillSwitches(recordSource)
      }
    }

  /** Build a random-access decoder for the given format. Public for callers (e.g. dispatch
    * sites in `DecodedSource`) that need to construct a decoder directly.
    */
  def randomAccessDecoder(format: FileFormat.RandomAccess): RandomAccessDecoder[_] = format match {
    case FileFormat.ParquetFormat => new ParquetDecoder
  }

  def decodingFoldableFrom(
    fileFormat: FileFormat,
    meter: IngestMeter,
    maximumLineSize: Int,
  )(implicit avroSchemaCache: AvroSchemaCache): DecodingFoldableFrom =
    fileFormat match {
      case _: FileFormat.RandomAccess =>
        // Random-access formats need to seek their input; a websocket upload is a forward-only
        // stream with no usable seek semantics. The dispatch above (in `decodedSourceFromFileStream`)
        // tolerates this for byte-source callers by buffering, but that's inappropriate for a
        // websocket upload where the producer may be untrusted and unbounded.
        throw new com.thatdot.quine.exceptions.IngestSourceFormatException(
          s"$fileFormat is not supported for websocket file upload (requires seekable input)",
        )
      case FileFormat.AvroContainerFormat(schemaUrl) =>
        val decoder = new AvroContainerDecoder(schemaUrl)
        new DecodingFoldableFrom {
          override type Element = org.apache.avro.generic.GenericRecord
          override val dataFoldableFrom: DataFoldableFrom[Element] = decoder.dataFoldableFrom

          override def decodingFlow: Flow[ByteString, Element, NotUsed] =
            Flow[ByteString]
              .wireTap(b => meter.markBytes(b.size.toLong))
              .via(decoder.decodeFlow)
              .map(_.get) // websocket upload is one-shot; per-record failures fail the stream
              .wireTap(_ => meter.markRecord())
        }
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
