package com.thatdot.quine.app.v2api.endpoints

import java.nio.charset.Charset

import scala.util.{Failure, Success, Try}

import sttp.tapir.Schema.annotations.{description, title}

import com.thatdot.quine.routes.StreamedRecordFormat.{CypherJson, CypherProtobuf, CypherRaw}
import com.thatdot.quine.routes.{
  AwsCredentials,
  AwsRegion,
  CsvCharacter,
  FileIngest => V1FileIngest,
  FileIngestFormat,
  FileIngestMode,
  IngestStreamConfiguration,
  KafkaAutoOffsetReset,
  KafkaIngest => V1KafkaIngest,
  KafkaOffsetCommitting,
  KafkaSecurityProtocol,
  KinesisIngest => V1KinesisIngest,
  NumberIteratorIngest => V1NumberIteratorIngest,
  RecordDecodingType,
  S3Ingest => V1S3Ingest,
  SQSIngest => V1SQSIngest,
  ServerSentEventsIngest,
  StandardInputIngest,
  StreamedRecordFormat,
  WebsocketSimpleStartupIngest,
}
import com.thatdot.quine.util.Log._
object V2IngestEntities {

  /** Ingest supports charset specification. */
  trait IngestCharsetSupport {
    val characterEncoding: Charset
  }

  /** Ingest supports start and end bounding. */
  trait IngestBoundingSupport {
    val startOffset: Long
    val limit: Option[Long]
  }

  /** Ingest supports decompression (e.g. Base64, gzip, zip) */
  trait IngestDecompressionSupport {
    val recordDecoders: Seq[RecordDecodingType]
  }

  // ---------------
  // Type
  // ---------------
  @title("Ingest type")
  sealed trait IngestSourceType

  @title("File Ingest")
  @description("An active stream of data being ingested from a file.")
  case class FileIngest(
    path: String,
    fileIngestMode: Option[FileIngestMode],
    maximumLineSize: Option[Int] = None,
    startOffset: Long,
    limit: Option[Long],
    characterEncoding: Charset,
    recordDecoders: Seq[RecordDecodingType] = Seq(),
  ) extends IngestSourceType
      with IngestCharsetSupport
      with IngestBoundingSupport
      with IngestDecompressionSupport

  @title("S3 File ingest")
  @description(
    """An ingest stream from a file in S3, newline delimited. This ingest source is
      |experimental and is subject to change without warning. In particular, there are
      |known issues with durability when the stream is inactive for at least 1 minute.""".stripMargin
      .replace('\n', ' '),
  )
  case class S3Ingest(
    bucket: String,
    key: String,
    credentials: Option[AwsCredentials],
    maximumLineSize: Option[Int] = None,
    startOffset: Long,
    limit: Option[Long],
    characterEncoding: Charset,
    recordDecoders: Seq[RecordDecodingType] = Seq(),
  ) extends IngestSourceType
      with IngestCharsetSupport
      with IngestBoundingSupport
      with IngestDecompressionSupport

  @title("Standard Input Ingest Stream")
  @description("An active stream of data being ingested from standard input to this Quine process.")
  case class StdInputIngest(maximumLineSize: Option[Int] = None, characterEncoding: Charset)
      extends IngestSourceType
      with IngestCharsetSupport

  @title("Number Iterator Ingest")
  @description(
    "An infinite ingest stream which requires no data source and just produces new sequential numbers" +
    " every time the stream is (re)started. The numbers are Java `Long`s` and will wrap at their max value.",
  )
  case class NumberIteratorIngest(startOffset: Long, limit: Option[Long])
      extends IngestSourceType
      with IngestBoundingSupport

  @title("Websockets Ingest Stream (Simple Startup)")
  @description("A websocket stream started after a sequence of text messages.")
  case class WebsocketIngest(
    url: String,
    initMessages: Seq[String],
    keepAlive: WebsocketSimpleStartupIngest.KeepaliveProtocol = WebsocketSimpleStartupIngest.PingPongInterval(),
    characterEncoding: Charset,
  ) extends IngestSourceType
      with IngestCharsetSupport

  @title("Kinesis Data Stream")
  @description("A stream of data being ingested from Kinesis.")
  case class KinesisIngest(
    streamName: String,
    shardIds: Option[Set[String]],
    credentials: Option[AwsCredentials],
    region: Option[AwsRegion],
    iteratorType: V1KinesisIngest.IteratorType = V1KinesisIngest.IteratorType.Latest,
    numRetries: Int = 3,
    recordDecoders: Seq[RecordDecodingType] = Seq(),
  ) extends IngestSourceType
      with IngestDecompressionSupport

  @title("Server Sent Events Stream")
  @description(
    "A server-issued event stream, as might be handled by the EventSource JavaScript API. Only consumes the `data` portion of an event.",
  )
  case class ServerSentEventIngest(url: String, recordDecoders: Seq[RecordDecodingType] = Seq())
      extends IngestSourceType
      with IngestDecompressionSupport

  @title("Simple Queue Service Queue")
  @description("An active stream of data being ingested from AWS SQS.")
  case class SQSIngest(
    @description("URL of the queue to ingest.") queueUrl: String,
    @description("Maximum number of records to read from the queue simultaneously.") readParallelism: Int = 1,
    credentials: Option[AwsCredentials],
    region: Option[AwsRegion],
    @description("Whether the queue consumer should acknowledge receipt of in-flight messages.")
    deleteReadMessages: Boolean = true,
    recordDecoders: Seq[RecordDecodingType] = Seq(),
  ) extends IngestSourceType
      with IngestDecompressionSupport

  @title("Kafka Ingest Stream")
  @description("A stream of data being ingested from Kafka.")
  case class KafkaIngest(
    @description(
      """Kafka topics from which to ingest: Either an array of topic names, or an object whose keys are topic names and
                              |whose values are partition indices.""".stripMargin
        .replace('\n', ' '),
    )
    topics: Either[V1KafkaIngest.Topics, V1KafkaIngest.PartitionAssignments],
    @description("A comma-separated list of Kafka broker servers.")
    bootstrapServers: String,
    @description(
      "Consumer group ID that this ingest stream should report belonging to; defaults to the name of the ingest stream.",
    )
    groupId: Option[String],
    securityProtocol: KafkaSecurityProtocol = KafkaSecurityProtocol.PlainText,
    offsetCommitting: Option[KafkaOffsetCommitting],
    autoOffsetReset: KafkaAutoOffsetReset = KafkaAutoOffsetReset.Latest,
    @description(
      "Map of Kafka client properties. See <https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html#ak-consumer-configurations-for-cp>",
    )
    kafkaProperties: V1KafkaIngest.KafkaProperties = Map.empty[String, String],
    @description(
      "The offset at which this stream should complete; offsets are sequential integers starting at 0.",
    ) endingOffset: Option[Long],
    recordDecoders: Seq[RecordDecodingType] = Seq(),
  ) extends IngestSourceType
      with IngestDecompressionSupport

  // ---------------
  // Format
  // ---------------
  @title("Ingest format")
  sealed trait IngestFormat

  @title("Json format")
  case object JsonIngestFormat extends IngestFormat

  @title("CSV format")
  case class CsvIngestFormat(
    @description("""Read a CSV file containing headers in the file's first row (`true`) or with no headers (`false`).
                                             |Alternatively, an array of column headers can be passed in. If headers are not supplied, the resulting
                                             |type available to the Cypher query will be a List of strings with values accessible by index. When
                                             |headers are available (supplied or read from the file), the resulting type available to the Cypher
                                             |query will be a Map[String, String], with values accessible using the corresponding header string.
                                             |CSV rows containing more records than the `headers` will have items that don't match a header column
                                             |discarded. CSV rows with fewer columns than the `headers` will have `null` values for the missing headers.
                                             |Default: `false`.""".stripMargin)

    headers: Either[Boolean, List[String]] = Left(false),
    @description("CSV row delimiter character.")
    delimiter: CsvCharacter = CsvCharacter.Comma,
    @description(
      "Character used to quote values in a field. Special characters (like new lines) inside of a quoted section will be a part of the CSV value.",
    )
    quoteChar: CsvCharacter = CsvCharacter.DoubleQuote,
    @description("Character used to escape special characters.")
    escapeChar: CsvCharacter = CsvCharacter.Backslash,
  ) extends IngestFormat

  @title("String format")
  case object StringIngestFormat extends IngestFormat

  @title("Protobuf format")
  case class ProtobufIngestFormat(
    @description(
      "URL (or local filename) of the Protobuf `.desc` file to load to parse the `typeName`.",
    )
    schemaUrl: String,
    @description("Message type name to use from the given `.desc` file as the incoming message type.")
    typeName: String,
  ) extends IngestFormat

  case object RawIngestFormat extends IngestFormat

  case object DropFormat extends IngestFormat

  // --------------------
  // Record Error Handler
  // --------------------
  sealed trait OnStreamErrorHandler

  case class RetryStreamError(retryCount: Int) extends OnStreamErrorHandler

  case object LogStreamError extends OnStreamErrorHandler

  // --------------------
  // Stream Error Handler
  // --------------------
  sealed trait OnRecordErrorHandler

  case class DeadLetterErrorHandler(
    destination: String, //TODO placeholder parameter
  ) extends OnRecordErrorHandler

  case object LogRecordErrorHandler extends OnRecordErrorHandler

  case class IngestConfiguration(
    source: IngestSourceType,
    query: String = "CREATE ($that)",
    parameter: String = "that",
    parallelism: Int = 1,
    maxPerSecond: Option[Int] = None,
    format: IngestFormat,
    onRecordError: OnRecordErrorHandler = LogRecordErrorHandler,
    onStreamError: OnStreamErrorHandler = LogStreamError,
  ) extends LazySafeLogging {
    def asV1IngestStreamConfiguration(implicit logConfig: LogConfig): IngestStreamConfiguration = {

      def asV1StreamedRecordFormat(format: IngestFormat): Try[StreamedRecordFormat] = format match {
        case JsonIngestFormat => Success(CypherJson(query, parameter))
        case ProtobufIngestFormat(schemaUrl, typeName) => Success(CypherProtobuf(query, parameter, schemaUrl, typeName))
        case RawIngestFormat => Success(CypherRaw(query, parameter))
        case DropFormat => Success(StreamedRecordFormat.Drop)
        case other => //csv, string - not representable as v1 streaming ingest formats
          Failure(new UnsupportedOperationException(s"$other not convertable to a StreamedRecordFormat"))
      }

      def asV1FileIngestFormat(format: IngestFormat): Try[FileIngestFormat] = format match {
        case JsonIngestFormat => Success(FileIngestFormat.CypherJson(query, parameter))
        case CsvIngestFormat(headers, delimiter, quoteChar, escapeChar) =>
          Success(FileIngestFormat.CypherCsv(query, parameter, headers, delimiter, quoteChar, escapeChar))
        case StringIngestFormat => Success(FileIngestFormat.CypherLine(query, parameter))
        case other => //protobuf, raw, drop - not representable as v1 file ingest formats
          Failure(new UnsupportedOperationException(s"$other not convertable to a FileIngestFormat"))
      }

      val tryConfig: Try[IngestStreamConfiguration] = source match {
        case FileIngest(path, fileIngestMode, maximumLineSize, startOffset, limit, charset, _) =>
          asV1FileIngestFormat(format).map { fmt =>
            V1FileIngest(
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
        case S3Ingest(bucket, key, credentials, maximumLineSize, startOffset, limit, charset, _) =>
          // last param recordDecoders unsupported in V1
          asV1FileIngestFormat(format).map { fmt =>
            V1S3Ingest(
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
        case StdInputIngest(maximumLineSize, charset) =>
          asV1FileIngestFormat(format).map { fmt =>
            StandardInputIngest(
              fmt,
              charset.name(),
              parallelism,
              maximumLineSize.getOrElse(Integer.MAX_VALUE),
              maxPerSecond,
            )
          }
        case NumberIteratorIngest(startOffset, limit) =>
          asV1FileIngestFormat(format).map { fmt =>
            V1NumberIteratorIngest(fmt, startOffset, limit, maxPerSecond, parallelism)

          }
        case WebsocketIngest(url, initMessages, keepAlive, charset) =>
          asV1StreamedRecordFormat(format).map { fmt =>
            WebsocketSimpleStartupIngest(
              fmt,
              url,
              initMessages,
              keepAlive,
              parallelism,
              charset.name(),
            )
          }
        case KinesisIngest(streamName, shardIds, credentials, region, iteratorType, numRetries, recordDecoders) =>
          //Note V1 checkpoint settings don't appear to be used.
          asV1StreamedRecordFormat(format).map { fmt =>
            val optionKinesisCheckpointSettings = None
            V1KinesisIngest(
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
              optionKinesisCheckpointSettings,
            )
          }

        case ServerSentEventIngest(url, recordDecoders) =>
          asV1StreamedRecordFormat(format).map { fmt =>
            ServerSentEventsIngest(fmt, url, parallelism, maxPerSecond, recordDecoders)
          }

        case SQSIngest(
              queueUrl,
              readParallelism,
              credentials,
              region,
              deleteReadMessages,
              recordDecoders,
            ) =>
          asV1StreamedRecordFormat(format).map { fmt =>
            V1SQSIngest(
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
            V1KafkaIngest(
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
          logger.warn(log"Could not render  ${this.toString} as a v1 ingest")
          StandardInputIngest(
            FileIngestFormat.CypherLine("Unrenderable", "Unrenderable"),
            "UTF-8",
            0,
            0,
            None,
          )
      }

    }
  }

  /** (IngestFormat, query, parameter) */
  private def fromFormat(format: StreamedRecordFormat): (IngestFormat, String, String) =
    format match {
      case CypherJson(query, parameter) => (JsonIngestFormat, query, parameter)
      case CypherRaw(query, parameter) => (RawIngestFormat, query, parameter)
      case CypherProtobuf(query, parameter, schemaUrl, typeName) =>
        (ProtobufIngestFormat(schemaUrl, typeName), query, parameter)
      case StreamedRecordFormat.Drop => (DropFormat, "", "$that")
    }

  def fromFormat(format: FileIngestFormat): (IngestFormat, String, String) = format match {
    case FileIngestFormat.CypherLine(query, parameter) => (StringIngestFormat, query, parameter)
    case FileIngestFormat.CypherJson(query, parameter) => (JsonIngestFormat, query, parameter)
    case FileIngestFormat.CypherCsv(query, parameter, headers, delimiter, quoteChar, escapeChar) =>
      (CsvIngestFormat(headers, delimiter, quoteChar, escapeChar), query, parameter)
  }

  def fromV1Ingest(v1IngestConfiguration: IngestStreamConfiguration): IngestConfiguration =
    v1IngestConfiguration match {
      case V1KafkaIngest(
            format,
            topics,
            parallelism,
            bootstrapServers,
            groupId,
            securityProtocol,
            offsetCommitting,
            autoOffsetReset,
            kafkaProperties,
            endingOffset,
            maximumPerSecond,
            recordDecoders,
          ) =>
        val (f, q, p) = fromFormat(format)
        val kafka: KafkaIngest = KafkaIngest(
          topics,
          bootstrapServers,
          groupId,
          securityProtocol,
          offsetCommitting,
          autoOffsetReset,
          kafkaProperties,
          endingOffset,
          recordDecoders,
        )
        IngestConfiguration(
          kafka,
          q,
          p,
          parallelism,
          maximumPerSecond,
          f,
          LogRecordErrorHandler,
          LogStreamError,
        )

      case V1KinesisIngest(
            format,
            streamName,
            shardIds,
            parallelism,
            credentials,
            region,
            iteratorType,
            numRetries,
            maximumPerSecond,
            recordDecoders,
            _, //checkpointSettings - not used
          ) =>
        val (f, q, p) = fromFormat(format)
        val v2kinesis: KinesisIngest =
          KinesisIngest(streamName, shardIds, credentials, region, iteratorType, numRetries, recordDecoders)
        IngestConfiguration(
          v2kinesis,
          q,
          p,
          parallelism,
          maximumPerSecond,
          f,
          LogRecordErrorHandler,
          LogStreamError,
        )

      case ServerSentEventsIngest(format, url, parallelism, maximumPerSecond, recordDecoders) =>
        val (f, q, p) = fromFormat(format)
        IngestConfiguration(
          ServerSentEventIngest(url, recordDecoders),
          q,
          p,
          parallelism,
          maximumPerSecond,
          f,
          LogRecordErrorHandler,
          LogStreamError,
        )
      case V1SQSIngest(
            format,
            queueUrl,
            readParallelism,
            writeParallelism,
            credentials,
            region,
            deleteReadMessages,
            maximumPerSecond,
            recordDecoders,
          ) =>
        val (f, q, p) = fromFormat(format)
        val sqs = SQSIngest(
          queueUrl,
          readParallelism,
          credentials,
          region,
          deleteReadMessages,
          recordDecoders,
        )
        IngestConfiguration(
          sqs,
          q,
          p,
          writeParallelism,
          maximumPerSecond,
          f,
          LogRecordErrorHandler,
          LogStreamError,
        )

      case WebsocketSimpleStartupIngest(format, url, initMessages, keepAlive, parallelism, encoding) =>
        val (f, q, p) = fromFormat(format)
        val ws = WebsocketIngest(url, initMessages, keepAlive, Charset.forName(encoding))
        IngestConfiguration(
          ws,
          q,
          p,
          parallelism,
          None,
          f,
          LogRecordErrorHandler,
          LogStreamError,
        )

      case V1FileIngest(
            format,
            path,
            encoding,
            parallelism,
            maximumLineSize,
            startAtOffset,
            ingestLimit,
            maximumPerSecond,
            fileIngestMode,
          ) =>
        val (f, q, p) = fromFormat(format)
        val file = FileIngest(
          path,
          fileIngestMode,
          Some(maximumLineSize),
          startAtOffset,
          ingestLimit,
          Charset.forName(encoding),
          Seq(),
        )
        IngestConfiguration(
          file,
          q,
          p,
          parallelism,
          maximumPerSecond,
          f,
          LogRecordErrorHandler,
          LogStreamError,
        )

      case V1S3Ingest(
            format,
            bucket,
            key,
            encoding,
            parallelism,
            credentials,
            maximumLineSize,
            startAtOffset,
            ingestLimit,
            maximumPerSecond,
          ) =>
        val (f, q, p) = fromFormat(format)
        val s3 = S3Ingest(
          bucket,
          key,
          credentials,
          Some(maximumLineSize),
          startAtOffset,
          ingestLimit,
          Charset.forName(encoding),
          Seq(),
        )
        IngestConfiguration(
          s3,
          q,
          p,
          parallelism,
          maximumPerSecond,
          f,
          LogRecordErrorHandler,
          LogStreamError,
        )

      case StandardInputIngest(format, encoding, parallelism, maximumLineSize, maximumPerSecond) =>
        val (f, q, p) = fromFormat(format)
        IngestConfiguration(
          StdInputIngest(Some(maximumLineSize), Charset.forName(encoding)),
          q,
          p,
          parallelism,
          maximumPerSecond,
          f,
          LogRecordErrorHandler,
          LogStreamError,
        )

      case V1NumberIteratorIngest(format, startAtOffset, ingestLimit, maximumPerSecond, parallelism) =>
        val (f, q, p) = fromFormat(format)
        IngestConfiguration(
          NumberIteratorIngest(startAtOffset, ingestLimit),
          q,
          p,
          parallelism,
          maximumPerSecond,
          f,
          LogRecordErrorHandler,
          LogStreamError,
        )
    }
}
