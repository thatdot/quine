package com.thatdot.quine.app.v2api.endpoints

import java.nio.charset.Charset

import sttp.tapir.Schema.annotations.{description, title}

import com.thatdot.quine.routes.StreamedRecordFormat.{CypherJson, CypherProtobuf, CypherRaw}
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
  KafkaIngest,
  KafkaOffsetCommitting,
  KafkaSecurityProtocol,
  KinesisIngest => V1KinesisIngest,
  NumberIteratorIngest => V1NumberIteratorIngest,
  RecordDecodingType,
  S3Ingest => V1S3Ingest,
  SQSIngest,
  ServerSentEventsIngest,
  StandardInputIngest,
  StreamedRecordFormat,
  WebsocketSimpleStartupIngest
}

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
  sealed trait IngestSourceType //(@description("ingest type") name: String)

  case class V2FileIngest(
    path: String,
    fileIngestMode: Option[FileIngestMode],
    maximumLineSize: Option[Int] = None,
    startOffset: Long,
    limit: Option[Long],
    characterEncoding: Charset,
    recordDecoders: Seq[RecordDecodingType] = Seq()
  ) extends IngestSourceType
      with IngestCharsetSupport
      with IngestBoundingSupport
      with IngestDecompressionSupport

  case class V2S3Ingest(
    bucket: String,
    key: String,
    credentials: Option[AwsCredentials],
    maximumLineSize: Option[Int] = None,
    startOffset: Long,
    limit: Option[Long],
    characterEncoding: Charset,
    recordDecoders: Seq[RecordDecodingType] = Seq()
  ) extends IngestSourceType
      with IngestCharsetSupport
      with IngestBoundingSupport
      with IngestDecompressionSupport

  case class V2StdInputIngest(maximumLineSize: Option[Int] = None, characterEncoding: Charset)
      extends IngestSourceType
      with IngestCharsetSupport

  case class V2NumberIteratorIngest(startOffset: Long, limit: Option[Long])
      extends IngestSourceType
      with IngestBoundingSupport

  case class V2WebsocketIngest(
    url: String,
    initMessages: Seq[String],
    keepAlive: WebsocketSimpleStartupIngest.KeepaliveProtocol = WebsocketSimpleStartupIngest.PingPongInterval(),
    characterEncoding: Charset
  ) extends IngestSourceType
      with IngestCharsetSupport

  case class V2KinesisIngest(
    streamName: String,
    shardIds: Option[Set[String]],
    credentials: Option[AwsCredentials],
    region: Option[AwsRegion],
    iteratorType: V1KinesisIngest.IteratorType = V1KinesisIngest.IteratorType.Latest,
    numRetries: Int = 3,
    recordDecoders: Seq[RecordDecodingType] = Seq()
  ) extends IngestSourceType
      with IngestDecompressionSupport

  case class V2ServerSentEventIngest(url: String, recordDecoders: Seq[RecordDecodingType] = Seq())
      extends IngestSourceType
      with IngestDecompressionSupport

  case class V2SQSIngest(
    @description("URL of the queue to ingest.") queueUrl: String,
    @description("Maximum number of records to read from the queue simultaneously.") readParallelism: Int = 1,
    @description("Maximum number of records to ingest simultaneously.")
    writeParallelism: Int = IngestRoutes.defaultWriteParallelism,
    credentials: Option[AwsCredentials],
    region: Option[AwsRegion],
    @description("Whether the queue consumer should acknowledge receipt of in-flight messages.")
    deleteReadMessages: Boolean = true,
    recordDecoders: Seq[RecordDecodingType] = Seq()
  ) extends IngestSourceType
      with IngestDecompressionSupport

  case class V2KafkaIngest(
    @description(
      """Kafka topics from which to ingest: Either an array of topic names, or an object whose keys are topic names and
                                |whose values are partition indices.""".stripMargin
        .replace('\n', ' ')
    )
    topics: Either[KafkaIngest.Topics, KafkaIngest.PartitionAssignments],
    @description("A comma-separated list of Kafka broker servers.")
    bootstrapServers: String,
    @description(
      "Consumer group ID that this ingest stream should report belonging to; defaults to the name of the ingest stream."
    )
    groupId: Option[String],
    securityProtocol: KafkaSecurityProtocol = KafkaSecurityProtocol.PlainText,
    offsetCommitting: Option[KafkaOffsetCommitting],
    autoOffsetReset: KafkaAutoOffsetReset = KafkaAutoOffsetReset.Latest,
    @description(
      "Map of Kafka client properties. See <https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html#ak-consumer-configurations-for-cp>"
    )
    kafkaProperties: KafkaIngest.KafkaProperties = Map.empty[String, String],
    @description(
      "The offset at which this stream should complete; offsets are sequential integers starting at 0."
    ) endingOffset: Option[Long],
    recordDecoders: Seq[RecordDecodingType] = Seq()
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
      "Character used to quote values in a field. Special characters (like new lines) inside of a quoted section will be a part of the CSV value."
    )
    quoteChar: CsvCharacter = CsvCharacter.DoubleQuote,
    @description("Character used to escape special characters.")
    escapeChar: CsvCharacter = CsvCharacter.Backslash
  ) extends IngestFormat

  @title("String format")
  case object StringIngestFormat extends IngestFormat

  @title("Protobuf format")
  case class ProtobufIngestFormat(
    @description(
      "URL (or local filename) of the Protobuf `.desc` file to load to parse the `typeName`."
    )
    schemaUrl: String,
    @description("Message type name to use from the given `.desc` file as the incoming message type.")
    typeName: String
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
    destination: String //TODO placeholder parameter
  ) extends OnRecordErrorHandler

  case object LogRecordErrorHandler extends OnRecordErrorHandler

  case class IngestConfiguration(
    source: IngestSourceType,
    query: String,
    parameter: String = "that",
    parallelism: Int,
    maxPerSecond: Option[Int] = None,
    format: IngestFormat,
    onRecordError: OnRecordErrorHandler,
    onStreamError: OnStreamErrorHandler
  ) {

    def asV1IngestStreamConfiguration: IngestStreamConfiguration = {

      def asV1StreamedRecordFormat(format: IngestFormat): StreamedRecordFormat = format match {
        case JsonIngestFormat => CypherJson(query, parameter)
        case ProtobufIngestFormat(schemaUrl, typeName) => CypherProtobuf(query, parameter, schemaUrl, typeName)
        case RawIngestFormat => CypherRaw(query, parameter)
        case DropFormat => StreamedRecordFormat.Drop
        case other => //csv, string - not representable as v1 streaming ingest formats
          throw new UnsupportedOperationException(s"$other not convertable to a StreamedRecordFormat")
      }

      def asV1FileIngestFormat(format: IngestFormat): FileIngestFormat = format match {
        case JsonIngestFormat => FileIngestFormat.CypherJson(query, parameter)
        case CsvIngestFormat(headers, delimiter, quoteChar, escapeChar) =>
          FileIngestFormat.CypherCsv(query, parameter, headers, delimiter, quoteChar, escapeChar)
        case StringIngestFormat => FileIngestFormat.CypherLine(query, parameter)
        case other => //protobuf, raw, drop - not representable as v1 file ingest formats
          throw new UnsupportedOperationException(s"$other not convertable to a FileIngestFormat")
      }

      source match {
        case V2FileIngest(path, fileIngestMode, maximumLineSize, startOffset, limit, charset, _) =>
          V1FileIngest(
            asV1FileIngestFormat(format),
            path,
            charset.name(),
            parallelism,
            maximumLineSize.getOrElse(Integer.MAX_VALUE),
            startOffset,
            limit,
            maxPerSecond,
            fileIngestMode
          )
        case V2S3Ingest(bucket, key, credentials, maximumLineSize, startOffset, limit, charset, _) =>
          // last param recordDecoders unsupported in V1
          V1S3Ingest(
            asV1FileIngestFormat(format),
            bucket,
            key,
            charset.name(),
            parallelism,
            credentials,
            maximumLineSize.getOrElse(Integer.MAX_VALUE),
            startOffset,
            limit,
            maxPerSecond
          )
        case V2StdInputIngest(maximumLineSize, characterEncoding) =>
          StandardInputIngest(
            asV1FileIngestFormat(format),
            characterEncoding.name(),
            parallelism,
            maximumLineSize.getOrElse(Integer.MAX_VALUE),
            maxPerSecond
          )
        case V2NumberIteratorIngest(startOffset, limit) =>
          V1NumberIteratorIngest(asV1FileIngestFormat(format), startOffset, limit, maxPerSecond, parallelism)
        case V2WebsocketIngest(url, initMessages, keepAlive, charset) =>
          WebsocketSimpleStartupIngest(
            asV1StreamedRecordFormat(format),
            url,
            initMessages,
            keepAlive,
            parallelism,
            charset.name()
          )
        case V2KinesisIngest(streamName, shardIds, credentials, region, iteratorType, numRetries, recordDecoders) =>
          //Note V1 checkpoint settings don't appear to be used.
          val optionKinesisCheckpointSettings = None
          V1KinesisIngest(
            asV1StreamedRecordFormat(format),
            streamName,
            shardIds,
            parallelism,
            credentials,
            region,
            iteratorType,
            numRetries,
            maxPerSecond,
            recordDecoders,
            optionKinesisCheckpointSettings
          )
        case V2ServerSentEventIngest(url, recordDecoders) =>
          ServerSentEventsIngest(asV1StreamedRecordFormat(format), url, parallelism, maxPerSecond, recordDecoders)
        case V2SQSIngest(
              queueUrl,
              readParallelism,
              writeParallelism,
              credentials,
              region,
              deleteReadMessages,
              recordDecoders
            ) =>
          SQSIngest(
            asV1StreamedRecordFormat(format),
            queueUrl,
            readParallelism,
            writeParallelism,
            credentials,
            region,
            deleteReadMessages,
            maxPerSecond,
            recordDecoders
          )
        case V2KafkaIngest(
              topics,
              bootstrapServers,
              groupId,
              securityProtocol,
              offsetCommitting,
              autoOffsetReset,
              kafkaProperties,
              endingOffset,
              recordDecoders
            ) =>
          KafkaIngest(
            asV1StreamedRecordFormat(format),
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
            recordDecoders
          )
      }
    }
  }

  /** (IngestFormat, query, parameter) */
  def fromFormat(format: StreamedRecordFormat): (IngestFormat, String, String) =
    format match {
      case CypherJson(query, parameter) => (JsonIngestFormat, query, parameter)
      case CypherRaw(query, parameter) => (RawIngestFormat, query, parameter)
      case CypherProtobuf(query, parameter, schemaUrl, typeName) =>
        (ProtobufIngestFormat(schemaUrl, typeName), query, parameter)
      case StreamedRecordFormat.Drop => (DropFormat, "", "$that")
    }

  /** (IngestFormat, query, parameter) */
  def fromFormat(format: FileIngestFormat): (IngestFormat, String, String) = format match {
    case FileIngestFormat.CypherLine(query, parameter) => (StringIngestFormat, query, parameter)
    case FileIngestFormat.CypherJson(query, parameter) => (JsonIngestFormat, query, parameter)
    case FileIngestFormat.CypherCsv(query, parameter, headers, delimiter, quoteChar, escapeChar) =>
      (CsvIngestFormat(headers, delimiter, quoteChar, escapeChar), query, parameter)
  }

  def fromV1Ingest(v1IngestConfiguration: IngestStreamConfiguration): IngestConfiguration =
    v1IngestConfiguration match {
      case KafkaIngest(
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
            recordDecoders
          ) =>
        val (f, q, p) = fromFormat(format)
        val kafka: V2KafkaIngest = V2KafkaIngest(
          topics,
          bootstrapServers,
          groupId,
          securityProtocol,
          offsetCommitting,
          autoOffsetReset,
          kafkaProperties,
          endingOffset,
          recordDecoders
        )
        IngestConfiguration(
          kafka,
          q,
          p,
          parallelism,
          maximumPerSecond,
          f,
          LogRecordErrorHandler,
          LogStreamError
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
            _ //checkpointSettings - not used
          ) =>
        val (f, q, p) = fromFormat(format)
        val v2kinesis: V2KinesisIngest =
          V2KinesisIngest(streamName, shardIds, credentials, region, iteratorType, numRetries, recordDecoders)
        IngestConfiguration(
          v2kinesis,
          q,
          p,
          parallelism,
          maximumPerSecond,
          f,
          LogRecordErrorHandler,
          LogStreamError
        )

      case ServerSentEventsIngest(format, url, parallelism, maximumPerSecond, recordDecoders) =>
        val (f, q, p) = fromFormat(format)
        IngestConfiguration(
          V2ServerSentEventIngest(url, recordDecoders),
          q,
          p,
          parallelism,
          maximumPerSecond,
          f,
          LogRecordErrorHandler,
          LogStreamError
        )
      case SQSIngest(
            format,
            queueUrl,
            readParallelism,
            writeParallelism,
            credentials,
            region,
            deleteReadMessages,
            maximumPerSecond,
            recordDecoders
          ) =>
        val (f, q, p) = fromFormat(format)
        val sqs = V2SQSIngest(
          queueUrl,
          readParallelism,
          writeParallelism,
          credentials,
          region,
          deleteReadMessages,
          recordDecoders
        )
        IngestConfiguration(
          sqs,
          q,
          p,
          readParallelism,
          maximumPerSecond,
          f,
          LogRecordErrorHandler,
          LogStreamError
        )

      case WebsocketSimpleStartupIngest(format, url, initMessages, keepAlive, parallelism, encoding) =>
        val (f, q, p) = fromFormat(format)
        val ws = V2WebsocketIngest(url, initMessages, keepAlive, Charset.forName(encoding))
        IngestConfiguration(
          ws,
          q,
          p,
          parallelism,
          None,
          f,
          LogRecordErrorHandler,
          LogStreamError
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
            fileIngestMode
          ) =>
        val (f, q, p) = fromFormat(format)
        val file = V2FileIngest(
          path,
          fileIngestMode,
          Some(maximumLineSize),
          startAtOffset,
          ingestLimit,
          Charset.forName(encoding),
          Seq()
        )
        IngestConfiguration(
          file,
          q,
          p,
          parallelism,
          maximumPerSecond,
          f,
          LogRecordErrorHandler,
          LogStreamError
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
            maximumPerSecond
          ) =>
        val (f, q, p) = fromFormat(format)
        val s3 = V2S3Ingest(
          bucket,
          key,
          credentials,
          Some(maximumLineSize),
          startAtOffset,
          ingestLimit,
          Charset.forName(encoding),
          Seq()
        )
        IngestConfiguration(
          s3,
          q,
          p,
          parallelism,
          maximumPerSecond,
          f,
          LogRecordErrorHandler,
          LogStreamError
        )

      case StandardInputIngest(format, encoding, parallelism, maximumLineSize, maximumPerSecond) =>
        val (f, q, p) = fromFormat(format)
        IngestConfiguration(
          V2StdInputIngest(Some(maximumLineSize), Charset.forName(encoding)),
          q,
          p,
          parallelism,
          maximumPerSecond,
          f,
          LogRecordErrorHandler,
          LogStreamError
        )

      case V1NumberIteratorIngest(format, startAtOffset, ingestLimit, maximumPerSecond, parallelism) =>
        val (f, q, p) = fromFormat(format)
        IngestConfiguration(
          V2NumberIteratorIngest(startAtOffset, ingestLimit),
          q,
          p,
          parallelism,
          maximumPerSecond,
          f,
          LogRecordErrorHandler,
          LogStreamError
        )
    }

}
