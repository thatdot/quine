package com.thatdot.quine.app.model.ingest2

import java.time.Instant

import scala.util.{Failure, Success, Try}

import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.{Decoder, Encoder}
import sttp.tapir.Schema
import sttp.tapir.Schema.annotations.{description, title}

import com.thatdot.api.v2.TypeDiscriminatorConfig.instances.circeConfig
import com.thatdot.api.v2.codec.ThirdPartyCodecs.jdk.{instantDecoder, instantEncoder}
import com.thatdot.common.logging.Log.LazySafeLogging
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest.OnRecordErrorHandler
import com.thatdot.quine.serialization.EncoderDecoder
import com.thatdot.quine.{routes => V1}

/** Base trait for all ingest formats. */
sealed trait IngestFormat

object IngestFormat {
  implicit lazy val schema: Schema[IngestFormat] =
    Schema
      .derived[IngestFormat]
      .description("Ingest format")

  /** Encoder for the IngestFormat union type. */
  implicit lazy val encoder: Encoder[IngestFormat] = Encoder.instance {
    case f: FileFormat => FileFormat.encoder(f)
    case s: StreamingFormat => StreamingFormat.encoder(s)
  }

  /** Decoder for the IngestFormat union type.
    *
    * Note: This decoder has an inherent ambiguity for JsonFormat because both FileFormat.JsonFormat
    * and StreamingFormat.JsonFormat serialize to `{"type": "JsonFormat"}`. This decoder tries
    * FileFormat first, so `{"type": "JsonFormat"}` always decodes to FileFormat.JsonFormat.
    *
    * This is not a problem in practice because runtime code uses specific types (FileFormat or
    * StreamingFormat) based on the IngestSource subtype, not this union decoder.
    */
  implicit lazy val decoder: Decoder[IngestFormat] =
    FileFormat.decoder.map(f => f: IngestFormat).or(StreamingFormat.decoder.map(s => s: IngestFormat))
}

/** Data format that reads a single value from an externally delimited frame. */
sealed trait StreamingFormat extends IngestFormat

object StreamingFormat {
  case object JsonFormat extends StreamingFormat

  case object RawFormat extends StreamingFormat

  final case class ProtobufFormat(
    schemaUrl: String,
    typeName: String,
  ) extends StreamingFormat

  object ProtobufFormat {
    implicit lazy val schema: Schema[ProtobufFormat] = Schema.derived
  }

  case class AvroFormat(
    schemaUrl: String,
  ) extends StreamingFormat

  case object DropFormat extends StreamingFormat

  def apply(v1Format: V1.StreamedRecordFormat): StreamingFormat =
    v1Format match {
      case V1.StreamedRecordFormat.CypherJson(_, _) => JsonFormat
      case V1.StreamedRecordFormat.CypherRaw(_, _) => RawFormat
      case V1.StreamedRecordFormat.CypherProtobuf(_, _, schemaUrl, typeName) =>
        ProtobufFormat(schemaUrl, typeName)
      //note : Avro is not supported in v1
      case V1.StreamedRecordFormat.Drop => DropFormat
      case _ => sys.error(s"Unsupported version 1 format: $v1Format")
    }

  implicit lazy val schema: Schema[StreamingFormat] = Schema.derived
  implicit lazy val encoder: Encoder[StreamingFormat] = deriveConfiguredEncoder
  implicit lazy val decoder: Decoder[StreamingFormat] = deriveConfiguredDecoder
}

@title("File Ingest Format")
@description("Format for decoding a stream of elements from a file for ingest.")
sealed trait FileFormat extends IngestFormat

object FileFormat {
  import V1IngestSchemas.csvCharacterSchema

  /** Read each line in as a single string element. */
  case object LineFormat extends FileFormat

  /** Read each line as a JSON value */
  case object JsonLinesFormat extends FileFormat

  case object JsonFormat extends FileFormat

  /** Comma (or other delimiter) separated values. Each line is a record, separated by a field delimiter. */
  case class CsvFormat(
    headers: Either[Boolean, List[String]] = Left(false),
    delimiter: V1.CsvCharacter = V1.CsvCharacter.Comma,
    quoteChar: V1.CsvCharacter = V1.CsvCharacter.DoubleQuote,
    escapeChar: V1.CsvCharacter = V1.CsvCharacter.Backslash,
  ) extends FileFormat {
    require(delimiter != quoteChar, "Different characters must be used for `delimiter` and `quoteChar`.")
    require(delimiter != escapeChar, "Different characters must be used for `delimiter` and `escapeChar`.")
    require(quoteChar != escapeChar, "Different characters must be used for `quoteChar` and `escapeChar`.")
  }

  object CsvFormat {
    import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
    import V1IngestCodecs.{csvCharacterEncoder, csvCharacterDecoder}

    // Explicit Either codec for headers field
    implicit private val headersEncoder: Encoder[Either[Boolean, List[String]]] = Encoder.instance {
      case Left(b) => io.circe.Json.fromBoolean(b)
      case Right(l) => io.circe.Json.arr(l.map(io.circe.Json.fromString): _*)
    }
    implicit private val headersDecoder: Decoder[Either[Boolean, List[String]]] = Decoder.instance { c =>
      c.as[Boolean].map(Left(_)).orElse(c.as[List[String]].map(Right(_)))
    }

    implicit lazy val schema: Schema[CsvFormat] = Schema.derived
    implicit lazy val encoder: Encoder[CsvFormat] = deriveEncoder
    implicit lazy val decoder: Decoder[CsvFormat] = deriveDecoder
  }

  def apply(v1Format: V1.FileIngestFormat): FileFormat = v1Format match {
    case V1.FileIngestFormat.CypherLine(_, _) => LineFormat
    case V1.FileIngestFormat.CypherJson(_, _) => JsonLinesFormat
    case V1.FileIngestFormat.CypherCsv(_, _, headers, delimiter, quoteChar, escapeChar) =>
      CsvFormat(headers, delimiter, quoteChar, escapeChar)
    case _ => sys.error(s"Unsupported version 1 format: $v1Format")
  }

  implicit lazy val schema: Schema[FileFormat] = Schema.derived
  implicit lazy val encoder: Encoder[FileFormat] = deriveConfiguredEncoder
  implicit lazy val decoder: Decoder[FileFormat] = deriveConfiguredDecoder
}

object V2IngestEntities {

  /** Ingest definition and status representation used for persistence */
  final case class QuineIngestStreamWithStatus(
    config: QuineIngestConfiguration,
    status: Option[V1.IngestStreamStatus],
  )

  object QuineIngestStreamWithStatus {
    import V1IngestCodecs.{ingestStreamStatusEncoder, ingestStreamStatusDecoder}

    implicit lazy val encoder: Encoder[QuineIngestStreamWithStatus] = deriveConfiguredEncoder
    implicit lazy val decoder: Decoder[QuineIngestStreamWithStatus] = deriveConfiguredDecoder
    implicit lazy val encoderDecoder: EncoderDecoder[QuineIngestStreamWithStatus] = EncoderDecoder.ofEncodeDecode
  }

  case class IngestStreamInfo(
    status: IngestStreamStatus,
    message: Option[String],
    settings: IngestSource,
    stats: IngestStreamStats,
  ) {
    def withName(name: String): IngestStreamInfoWithName =
      IngestStreamInfoWithName(name, status, message, settings, stats)
  }

  object IngestStreamInfo {
    implicit lazy val encoder: Encoder[IngestStreamInfo] = deriveConfiguredEncoder
    implicit lazy val decoder: Decoder[IngestStreamInfo] = deriveConfiguredDecoder
  }

  case class IngestStreamInfoWithName(
    name: String,
    status: IngestStreamStatus,
    message: Option[String],
    settings: IngestSource,
    stats: IngestStreamStats,
  )

  object IngestStreamInfoWithName {
    implicit lazy val encoder: Encoder[IngestStreamInfoWithName] = deriveConfiguredEncoder
    implicit lazy val decoder: Decoder[IngestStreamInfoWithName] = deriveConfiguredDecoder
  }

  sealed trait IngestStreamStatus

  object IngestStreamStatus {
    case object Running extends IngestStreamStatus

    case object Paused extends IngestStreamStatus

    case object Restored extends IngestStreamStatus

    case object Completed extends IngestStreamStatus

    case object Terminated extends IngestStreamStatus

    case object Failed extends IngestStreamStatus

    implicit val encoder: Encoder[IngestStreamStatus] = deriveConfiguredEncoder
    implicit val decoder: Decoder[IngestStreamStatus] = deriveConfiguredDecoder
  }

  sealed trait ValvePosition

  object ValvePosition {
    case object Open extends ValvePosition

    case object Closed extends ValvePosition
  }

  case class IngestStreamStats(
    ingestedCount: Long,
    rates: RatesSummary,
    byteRates: RatesSummary,
    startTime: Instant,
    totalRuntime: Long,
  )

  object IngestStreamStats {
    implicit val encoder: Encoder[IngestStreamStats] = deriveConfiguredEncoder
    implicit val decoder: Decoder[IngestStreamStats] = deriveConfiguredDecoder
  }

  case class RatesSummary(
    count: Long,
    oneMinute: Double,
    fiveMinute: Double,
    fifteenMinute: Double,
    overall: Double,
  )

  object RatesSummary {
    implicit val encoder: Encoder[RatesSummary] = deriveConfiguredEncoder
    implicit val decoder: Decoder[RatesSummary] = deriveConfiguredDecoder
  }

  sealed trait OnStreamErrorHandler

  object OnStreamErrorHandler {
    implicit lazy val schema: Schema[OnStreamErrorHandler] = Schema.derived
    implicit val encoder: Encoder[OnStreamErrorHandler] = deriveConfiguredEncoder
    implicit val decoder: Decoder[OnStreamErrorHandler] = deriveConfiguredDecoder
  }

  @title("Retry Stream Error Handler")
  @description("Retry the stream on failure.")
  case class RetryStreamError(retryCount: Int) extends OnStreamErrorHandler

  @title("Log Stream Error Handler")
  @description("If the stream fails log a message but do not retry.")
  case object LogStreamError extends OnStreamErrorHandler

  /** Enforce shared structure between quine and novelty ingest usages. */
  trait V2IngestConfiguration {
    val source: IngestSource
    val parallelism: Int
    val maxPerSecond: Option[Int]
    val onRecordError: OnRecordErrorHandler
    val onStreamError: OnStreamErrorHandler
  }

  sealed trait Transformation

  object Transformation {
    case class JavaScript(
      /* JavaScript source code of the function, must be callable */
      function: String,
    ) extends Transformation

    implicit lazy val schema: Schema[Transformation] = Schema.derived
    implicit val encoder: Encoder[Transformation] = deriveConfiguredEncoder
    implicit val decoder: Decoder[Transformation] = deriveConfiguredDecoder
  }

  case class QuineIngestConfiguration(
    name: String,
    source: IngestSource,
    query: String,
    parameter: String = "that",
    transformation: Option[Transformation] = None,
    parallelism: Int = V1.IngestRoutes.defaultWriteParallelism,
    maxPerSecond: Option[Int] = None,
    onRecordError: OnRecordErrorHandler = OnRecordErrorHandler(),
    onStreamError: OnStreamErrorHandler = LogStreamError,
  ) extends V2IngestConfiguration
      with LazySafeLogging {
    def asV1IngestStreamConfiguration: V1.IngestStreamConfiguration = {

      def asV1StreamedRecordFormat(format: StreamingFormat): Try[V1.StreamedRecordFormat] = format match {
        case StreamingFormat.JsonFormat => Success(V1.StreamedRecordFormat.CypherJson(query, parameter))
        case StreamingFormat.RawFormat => Success(V1.StreamedRecordFormat.CypherRaw(query, parameter))
        case StreamingFormat.ProtobufFormat(schemaUrl, typeName) =>
          Success(V1.StreamedRecordFormat.CypherProtobuf(query, parameter, schemaUrl, typeName))
        case _: StreamingFormat.AvroFormat =>
          Failure(
            new UnsupportedOperationException(
              "Avro is not supported in Api V1",
            ),
          )
        case _: StreamingFormat.DropFormat.type => Success(V1.StreamedRecordFormat.Drop)
      }

      def asV1FileIngestFormat(format: FileFormat): Try[V1.FileIngestFormat] = format match {
        case FileFormat.LineFormat => Success(V1.FileIngestFormat.CypherLine(query, parameter))
        case FileFormat.JsonFormat | FileFormat.JsonLinesFormat =>
          Success(V1.FileIngestFormat.CypherJson(query, parameter))
        case FileFormat.CsvFormat(headers, delimiter, quoteChar, escapeChar) =>
          Success(V1.FileIngestFormat.CypherCsv(query, parameter, headers, delimiter, quoteChar, escapeChar))
      }

      val tryConfig: Try[V1.IngestStreamConfiguration] = source match {
        case FileIngest(format, path, fileIngestMode, maximumLineSize, startOffset, limit, charset, _) =>
          asV1FileIngestFormat(format).map { fmt =>
            V1.FileIngest(
              fmt,
              path,
              charset.name(),
              parallelism,
              maximumLineSize.getOrElse(Integer.MAX_VALUE),
              startOffset,
              limit,
              maxPerSecond,
              fileIngestMode,
            )
          }
        case S3Ingest(format, bucket, key, credentials, maximumLineSize, startOffset, limit, charset, _) =>
          // last param recordDecoders unsupported in V1
          asV1FileIngestFormat(format).map { fmt =>
            V1.S3Ingest(
              fmt,
              bucket,
              key,
              charset.name(),
              parallelism,
              credentials,
              maximumLineSize.getOrElse(Integer.MAX_VALUE),
              startOffset,
              limit,
              maxPerSecond,
            )
          }
        case StdInputIngest(format, maximumLineSize, charset) =>
          asV1FileIngestFormat(format).map { fmt =>
            V1.StandardInputIngest(
              fmt,
              charset.name(),
              parallelism,
              maximumLineSize.getOrElse(Integer.MAX_VALUE),
              maxPerSecond,
            )
          }
        case NumberIteratorIngest(_, startOffset, limit) =>
          Success(
            V1.NumberIteratorIngest(V1.IngestRoutes.defaultNumberFormat, startOffset, limit, maxPerSecond, parallelism),
          )

        case WebsocketIngest(format, url, initMessages, keepAlive, charset) =>
          asV1StreamedRecordFormat(format).map { fmt =>
            V1.WebsocketSimpleStartupIngest(
              fmt,
              url,
              initMessages,
              keepAlive,
              parallelism,
              charset.name(),
            )
          }
        case KinesisIngest(
              format,
              streamName,
              shardIds,
              credentials,
              region,
              iteratorType,
              numRetries,
              recordDecoders,
            ) =>
          asV1StreamedRecordFormat(format).map { fmt =>
            V1.KinesisIngest(
              fmt,
              streamName,
              shardIds,
              parallelism,
              credentials,
              region,
              iteratorType,
              numRetries,
              maxPerSecond,
              recordDecoders,
            )
          }

        case ServerSentEventIngest(format, url, recordDecoders) =>
          asV1StreamedRecordFormat(format).map { fmt =>
            V1.ServerSentEventsIngest(fmt, url, parallelism, maxPerSecond, recordDecoders)
          }

        case SQSIngest(format, queueUrl, readParallelism, credentials, region, deleteReadMessages, recordDecoders) =>
          asV1StreamedRecordFormat(format).map { fmt =>
            V1.SQSIngest(
              fmt,
              queueUrl,
              readParallelism,
              parallelism,
              credentials,
              region,
              deleteReadMessages,
              maxPerSecond,
              recordDecoders,
            )
          }
        case KafkaIngest(
              format,
              topics,
              bootstrapServers,
              groupId,
              securityProtocol,
              offsetCommitting,
              autoOffsetReset,
              kafkaProperties,
              endingOffset,
              recordDecoders,
            ) =>
          asV1StreamedRecordFormat(format).map { fmt =>
            V1.KafkaIngest(
              fmt,
              topics,
              parallelism,
              bootstrapServers,
              groupId,
              securityProtocol,
              offsetCommitting,
              autoOffsetReset,
              kafkaProperties,
              endingOffset,
              maxPerSecond,
              recordDecoders,
            )
          }
        case _: KinesisKclIngest =>
          Failure(new Exception("v2 KCL Kinesis unsupported in v1 ingests"))
        case _: ReactiveStreamIngest =>
          Failure(new Exception("Reactive Streams unsupported in v1 ingests"))
        case _: WebSocketFileUpload =>
          Failure(new Exception("WebSocket File Upload unsupported in v1 ingests"))
      }
      tryConfig match {
        case Success(v1Config) => v1Config
        case Failure(_) =>
          /*
              Note: This value is only here in the case that we're trying to render v2 ingests in the v1 api where we
              need to convert them to the v1 format. In these cases if we've created a v2 ingest that's not render-able
              as a v1 configuration this returns an empty placeholder object so that the api doesn't throw a 500.

              Note that creating this situation is only possible by creating an ingest in the v2 api and then trying
              to view it via the v1 api.
           */
          V1.StandardInputIngest(
            V1.FileIngestFormat.CypherLine("Unrenderable", "Unrenderable"),
            "UTF-8",
            0,
            0,
            None,
          )
      }
    }
  }

  object QuineIngestConfiguration {
    implicit lazy val encoder: Encoder[QuineIngestConfiguration] = deriveConfiguredEncoder
    implicit lazy val decoder: Decoder[QuineIngestConfiguration] = deriveConfiguredDecoder
    implicit lazy val encoderDecoder: EncoderDecoder[QuineIngestConfiguration] = EncoderDecoder.ofEncodeDecode
  }

  /** WebSocket file upload feedback messages sent from server to client */
  object WebSocketFileUploadFeedback {

    /** Type of JSON message sent back in a WebSocket file upload stream */
    sealed trait FeedbackMessage

    /** Acknowledgement that WebSocket connection is established */
    case object Ack extends FeedbackMessage

    /** Progress update indicating number of records processed */
    final case class Progress(count: Long) extends FeedbackMessage

    /** Error occurred during processing */
    final case class Error(message: String, index: Option[Long], record: Option[String]) extends FeedbackMessage

    object FeedbackMessage {
      import io.circe.generic.extras.semiauto

      implicit val feedbackMessageEncoder: Encoder[FeedbackMessage] = semiauto.deriveConfiguredEncoder
    }
  }
}
