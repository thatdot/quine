package com.thatdot.quine.app.v2api.endpoints

import java.nio.charset.Charset

import scala.util.{Failure, Success, Try}

import com.typesafe.scalalogging.LazyLogging
import sttp.tapir.Schema.annotations.{description, title}

import com.thatdot.quine.routes.FileIngestFormat.CypherCsv
import com.thatdot.quine.routes.IngestRoutes.defaultNumberFormat
import com.thatdot.quine.routes.StreamedRecordFormat.{CypherJson, CypherProtobuf, CypherRaw, Drop}
import com.thatdot.quine.routes.{
  AwsCredentials,
  AwsRegion,
  CsvCharacter,
  FileIngest => V1FileIngest,
  FileIngestFormat,
  FileIngestMode,
  IngestRoutes,
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

  @title("Ingest source")
  sealed trait IngestSource {
    val format: IngestFormat
  }

  sealed trait FileIngestSource extends IngestSource {
    val format: FileFormat
  }
  sealed trait StreamingIngestSource extends IngestSource {
    val format: StreamingFormat
  }

  @title("File Ingest")
  @description("An active stream of data being ingested from a file on this Quine host.")
  case class FileIngest(
    @description("format used to decode each incoming line from a file")
    format: FileFormat,
    @description("Local file path.")
    path: String,
    fileIngestMode: Option[FileIngestMode],
    @description("Maximum size (in bytes) of any line in the file.")
    maximumLineSize: Option[Int] = None,
    @description(
      s"""Begin processing at the record with the given index. Useful for skipping some number of lines (e.g. CSV headers) or
         |resuming ingest from a partially consumed file.""".stripMargin,
    )
    startOffset: Long,
    @description(s"Optionally limit how many records are ingested from this file.")
    limit: Option[Long],
    @description(
      "The text encoding scheme for the file. UTF-8, US-ASCII and ISO-8859-1 are " +
      "supported -- other encodings will transcoded to UTF-8 on the fly (and ingest may be slower).",
    )
    characterEncoding: Charset,
    @description(
      "List of decodings to be applied to each input. The specified decodings are applied in declared array order.",
    )
    recordDecoders: Seq[RecordDecodingType] = Seq(),
  ) extends FileIngestSource
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
    @description("format used to decode each incoming line from a file in S3")
    format: FileFormat,
    bucket: String,
    @description("S3 file name")
    key: String,
    @description("AWS credentials to apply to this request")
    credentials: Option[AwsCredentials],
    @description("Maximum size (in bytes) of any line in the file.")
    maximumLineSize: Option[Int] = None,
    @description(
      s"""Begin processing at the record with the given index. Useful for skipping some number of lines (e.g. CSV headers) or
                                                    |resuming ingest from a partially consumed file.""".stripMargin,
    )
    startOffset: Long,
    @description(s"Optionally limit how many records are ingested from this file.")
    limit: Option[Long],
    @description(
      "text encoding used to read the file. Only UTF-8, US-ASCII and ISO-8859-1 are directly " +
      "supported -- other encodings will transcoded to UTF-8 on the fly (and ingest may be slower).",
    )
    characterEncoding: Charset,
    @description(
      "List of decodings to be applied to each input. The specified decodings are applied in declared array order.",
    )
    recordDecoders: Seq[RecordDecodingType] = Seq(),
  ) extends FileIngestSource
      with IngestCharsetSupport
      with IngestBoundingSupport
      with IngestDecompressionSupport

  @title("Standard Input Ingest Stream")
  @description("An active stream of data being ingested from standard input to this Quine process.")
  case class StdInputIngest(
    @description("format used to decode each incoming line from stdIn")
    format: FileFormat,
    @description("Maximum size (in bytes) of any line in the file.")
    maximumLineSize: Option[Int] = None,
    @description(
      "text encoding used to read the file. Only UTF-8, US-ASCII and ISO-8859-1 are directly " +
      "supported -- other encodings will transcoded to UTF-8 on the fly (and ingest may be slower).",
    )
    characterEncoding: Charset,
  ) extends FileIngestSource
      with IngestCharsetSupport

  @title("Number Iterator Ingest")
  @description(
    "An infinite ingest stream which requires no data source and just produces new sequential numbers" +
    " every time the stream is (re)started. The numbers are Java `Long`s` and will wrap at their max value.",
  )
  case class NumberIteratorIngest(
    format: StreamingFormat,
    @description("Begin the stream with this number.")
    startOffset: Long = 0L,
    @description("Optionally end the stream after consuming this many items.")
    limit: Option[Long],
  ) extends StreamingIngestSource
      with IngestBoundingSupport

  @title("Websockets Ingest Stream (Simple Startup)")
  @description("A websocket stream started after a sequence of text messages.")
  case class WebsocketIngest(
    @description("Format used to decode each incoming message.")
    format: StreamingFormat,
    @description("Websocket (ws: or wss:) url to connect to.")
    url: String,
    @description("Initial messages to send to the server on connecting.")
    initMessages: Seq[String],
    @description("Strategy to use for sending keepalive messages, if any.")
    keepAlive: WebsocketSimpleStartupIngest.KeepaliveProtocol = WebsocketSimpleStartupIngest.PingPongInterval(),
    characterEncoding: Charset,
  ) extends StreamingIngestSource
      with IngestCharsetSupport

  @title("Kinesis Data Stream")
  @description("A stream of data being ingested from Kinesis.")
  case class KinesisIngest(
    @description("The format used to decode each Kinesis record.")
    format: StreamingFormat,
    @description("Name of the Kinesis stream to ingest.")
    streamName: String,
    @description(
      "Shards IDs within the named kinesis stream to ingest; if empty or excluded, all shards on the stream are processed.",
    )
    shardIds: Option[Set[String]],
    @description("AWS credentials for this Kinesis stream")
    credentials: Option[AwsCredentials],
    @description("AWS region for this Kinesis stream")
    region: Option[AwsRegion],
    @description("Shard iterator type.") iteratorType: V1KinesisIngest.IteratorType =
      V1KinesisIngest.IteratorType.Latest,
    @description("Number of retries to attempt on Kineses error.") numRetries: Int = 3,
    @description(
      "List of decodings to be applied to each input, where specified decodings are applied in declared array order.",
    )
    recordDecoders: Seq[RecordDecodingType] = Seq(),
  ) extends StreamingIngestSource
      with IngestDecompressionSupport

  @title("Server Sent Events Stream")
  @description(
    "A server-issued event stream, as might be handled by the EventSource JavaScript API. Only consumes the `data` portion of an event.",
  )
  case class ServerSentEventIngest(
    @description("Format used to decode each event's `data`.")
    format: StreamingFormat,
    @description("URL of the server sent event stream.")
    url: String,
    @description(
      "List of decodings to be applied to each input, where specified decodings are applied in declared array order.",
    )
    recordDecoders: Seq[RecordDecodingType] = Seq(),
  ) extends StreamingIngestSource
      with IngestDecompressionSupport

  @title("Simple Queue Service Queue")
  @description("An active stream of data being ingested from AWS SQS.")
  case class SQSIngest(
    format: StreamingFormat,
    @description("URL of the queue to ingest.") queueUrl: String,
    @description("Maximum number of records to read from the queue simultaneously.") readParallelism: Int = 1,
    credentials: Option[AwsCredentials],
    region: Option[AwsRegion],
    @description("Whether the queue consumer should acknowledge receipt of in-flight messages.")
    deleteReadMessages: Boolean = true,
    @description(
      "List of decodings to be applied to each input, where specified decodings are applied in declared array order.",
    )
    recordDecoders: Seq[RecordDecodingType] = Seq(),
  ) extends StreamingIngestSource
      with IngestDecompressionSupport

  @title("Kafka Ingest Stream")
  @description("A stream of data being ingested from Kafka.")
  case class KafkaIngest(
    format: StreamingFormat,
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
    @description(
      "List of decodings to be applied to each input, where specified decodings are applied in declared array order.",
    )
    recordDecoders: Seq[RecordDecodingType] = Seq(),
  ) extends StreamingIngestSource
      with IngestDecompressionSupport

  // ---------------
  // Format
  // ---------------

  sealed trait IngestFormat

  @title("Streamed Record Format")
  @description("Format by which streamed records are decoded.")
  sealed trait StreamingFormat extends IngestFormat

  object StreamingFormat {

    @title("Json")
    @description("""Records are JSON values. For every record received, the
        |given Cypher query will be re-executed with the parameter in the query set
        |equal to the new JSON value.
  """.stripMargin)
    case object JsonFormat extends StreamingFormat

    @title("Raw Bytes")
    @description("""Records may have any format. For every record received, the
        |given Cypher query will be re-executed with the parameter in the query set
        |equal to the new value as a Cypher byte array.
  """.stripMargin)
    case object RawFormat extends StreamingFormat

    @title("Protobuf via Cypher")
    @description(
      "Records are serialized instances of `typeName` as described in the schema (a `.desc` descriptor file) at " +
      "`schemaUrl`. For every record received, the given Cypher query will be re-executed with the parameter " +
      "in the query set equal to the new (deserialized) Protobuf message.",
    )
    final case class ProtobufFormat(
      @description(
        "URL (or local filename) of the Protobuf `.desc` file to load to parse the `typeName`.",
      ) schemaUrl: String,
      @description(
        "Message type name to use from the given `.desc` file as the incoming message type.",
      ) typeName: String,
    ) extends StreamingFormat

    @title("Avro format")
    case class AvroFormat(
      @description(
        "URL (or local filename) of the file to load to parse the avro schema.",
      )
      schemaUrl: String,
    ) extends StreamingFormat

    @title("Drop")
    @description("Ignore the data without further processing.")
    case object DropFormat extends StreamingFormat

    def apply(v1Format: StreamedRecordFormat): StreamingFormat =
      v1Format match {
        case CypherJson(_, _) => JsonFormat
        case CypherRaw(_, _) => RawFormat
        case CypherProtobuf(_, _, schemaUrl, typeName) =>
          ProtobufFormat(schemaUrl, typeName)
        //note : Avro is not supported in v1
        case StreamedRecordFormat.Drop => DropFormat
      }
  }

  @title("File Ingest Format")
  @description("Format by which a file will be interpreted as a stream of elements for ingest.")
  sealed trait FileFormat extends IngestFormat
  object FileFormat {

    /** Create using a cypher query, passing each line in as a string */
    @title("Line")
    @description("""For every line (LF/CRLF delimited) in the source, the given Cypher query will be
        |re-executed with the parameter in the query set equal to a string matching
        |the new line value. The newline is not included in this string.
  """.stripMargin.replace('\n', ' '))
    case object LineFormat extends FileFormat

    /** Create using a cypher query, expecting each line to be a JSON record */
    @title("Json")
    @description("""Lines in the file should be JSON values. For every value received, the
        |given Cypher query will be re-executed with the parameter in the query set
        |equal to the new JSON value.
  """.stripMargin.replace('\n', ' '))
    case object JsonFormat extends FileFormat

    /** Create using a cypher query, expecting each line to be a single row CSV record */
    @title("CSV")
    @description(
      """For every row in a CSV file, the given Cypher query will be re-executed with the parameter in the query set
        |to the parsed row. Rows are parsed into either a Cypher List of strings or a Map, depending on whether a
        |`headers` row is available.""".stripMargin.replace('\n', ' '),
    )
    case class CsvFormat(
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
      @description("""Character used to quote values in a field. Special characters (like new lines) inside of a quoted
                                    |section will be a part of the CSV value.""".stripMargin)
      quoteChar: CsvCharacter = CsvCharacter.DoubleQuote,
      @description("Character used to escape special characters.")
      escapeChar: CsvCharacter = CsvCharacter.Backslash,
    ) extends FileFormat {
      require(delimiter != quoteChar, "Different characters must be used for `delimiter` and `quoteChar`.")
      require(delimiter != escapeChar, "Different characters must be used for `delimiter` and `escapeChar`.")
      require(quoteChar != escapeChar, "Different characters must be used for `quoteChar` and `escapeChar`.")
    }

    def apply(v1Format: FileIngestFormat): FileFormat = v1Format match {
      case FileIngestFormat.CypherLine(_, _) => LineFormat
      case FileIngestFormat.CypherJson(_, _) => JsonFormat
      case FileIngestFormat.CypherCsv(_, _, headers, delimiter, quoteChar, escapeChar) =>
        CsvFormat(headers, delimiter, quoteChar, escapeChar)
    }
  }

  // --------------------
  // Record Error Handler
  // --------------------
  sealed trait OnStreamErrorHandler

  @title("Retry Stream Error Handler")
  @description("Retry the stream on failure")
  case class RetryStreamError(retryCount: Int) extends OnStreamErrorHandler

  @title("Log Stream Error Handler")
  @description("If the stream fails log a message but do not retry.")
  case object LogStreamError extends OnStreamErrorHandler

  // --------------------
  // Stream Error Handler
  // --------------------
  /** Error handler defined for errors that affect only a single record. This is intended to handle errors in
    * a configurable way distinct from stream-level errors, where the entire stream fails - e.g. handling
    * a single corrupt record rather than a failure in the stream communication.
    */
  sealed trait OnRecordErrorHandler {
    def handleError[A, Frame](processRecordAttempt: (Try[A], Frame)): Unit =
      processRecordAttempt match {
        case (Failure(e), frame) => onError(e, frame)
        case _ => ()
      }

    def onError[Frame](e: Throwable, frame: Frame): Unit
  }

  @title("Log Record Error Handler")
  @description("Log a message for each message that encounters an error in processing")
  case object LogRecordErrorHandler extends OnRecordErrorHandler with LazyLogging {
    def onError[Frame](e: Throwable, frame: Frame): Unit =
      logger.warn(s"error decoding: $frame: ${e.getMessage}")
  }

  @title("Dead-letter Record Error Handler")
  @description(
    "Preserve records that encounter an error in processing by forwarding them to a specified dead-letter destination (TBD)",
  )
  case object DeadLetterErrorHandler extends OnRecordErrorHandler {
    override def onError[Frame](e: Throwable, frame: Frame): Unit = ()
  }

  /** Enforce shared structure between quine and novelty ingest useages.
    * Novelty ingests are identical to quine ingests with the exeception
    * of omitting query and parameter fields.
    */
  trait V2IngestConfiguration {
    val source: IngestSource
    val parallelism: Int
    val maxPerSecond: Option[Int]
    val onRecordError: OnRecordErrorHandler
    val onStreamError: OnStreamErrorHandler
  }

  @title("Ingest Configuration")
  @description("A specification of a data source and rules for consuming data from that source.")
  case class QuineIngestConfiguration(
    source: IngestSource,
    @description("Cypher query to execute on each record.")
    query: String,
    @description("Name of the Cypher parameter to populate with the JSON value.")
    parameter: String = "that",
    @description("Maximum number of records to process at once.")
    parallelism: Int = IngestRoutes.defaultWriteParallelism,
    @description("Maximum number of records to process per second.")
    maxPerSecond: Option[Int] = None,
    @description("Action to take on a single failed record")
    onRecordError: OnRecordErrorHandler = LogRecordErrorHandler,
    @description("Action to take on a failure of the input stream")
    onStreamError: OnStreamErrorHandler = LogStreamError,
  ) extends V2IngestConfiguration
      with LazySafeLogging {
    def asV1IngestStreamConfiguration: IngestStreamConfiguration = {

      def asV1StreamedRecordFormat(format: StreamingFormat): Try[StreamedRecordFormat] = format match {
        case StreamingFormat.JsonFormat => Success(CypherJson(query, parameter))
        case StreamingFormat.RawFormat => Success(CypherRaw(query, parameter))
        case StreamingFormat.ProtobufFormat(schemaUrl, typeName) =>
          Success(CypherProtobuf(query, parameter, schemaUrl, typeName))
        case _: StreamingFormat.AvroFormat =>
          Failure(
            new UnsupportedOperationException(
              "Avro is not supported in Api V1",
            ),
          )
        case _: StreamingFormat.DropFormat.type => Success(Drop)
      }

      def asV1FileIngestFormat(format: FileFormat): Try[FileIngestFormat] = format match {
        case FileFormat.LineFormat => Success(FileIngestFormat.CypherLine(query, parameter))
        case FileFormat.JsonFormat => Success(FileIngestFormat.CypherJson(query, parameter))
        case FileFormat.CsvFormat(headers, delimiter, quoteChar, escapeChar) =>
          Success(CypherCsv(query, parameter, headers, delimiter, quoteChar, escapeChar))
      }

      val tryConfig: Try[IngestStreamConfiguration] = source match {
        case FileIngest(format, path, fileIngestMode, maximumLineSize, startOffset, limit, charset, _) =>
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
        case S3Ingest(format, bucket, key, credentials, maximumLineSize, startOffset, limit, charset, _) =>
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
        case StdInputIngest(format, maximumLineSize, charset) =>
          asV1FileIngestFormat(format).map { fmt =>
            StandardInputIngest(
              fmt,
              charset.name(),
              parallelism,
              maximumLineSize.getOrElse(Integer.MAX_VALUE),
              maxPerSecond,
            )
          }
        case NumberIteratorIngest(_, startOffset, limit) =>
          Success(V1NumberIteratorIngest(defaultNumberFormat, startOffset, limit, maxPerSecond, parallelism))

        case WebsocketIngest(format, url, initMessages, keepAlive, charset) =>
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

        case ServerSentEventIngest(format, url, recordDecoders) =>
          asV1StreamedRecordFormat(format).map { fmt =>
            ServerSentEventsIngest(fmt, url, parallelism, maxPerSecond, recordDecoders)
          }

        case SQSIngest(format, queueUrl, readParallelism, credentials, region, deleteReadMessages, recordDecoders) =>
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

}
