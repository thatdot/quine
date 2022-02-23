package com.thatdot.quine.app

import java.nio.charset.{Charset, StandardCharsets}
import java.nio.file.Paths
import java.time.Instant

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

import akka.NotUsed
import akka.kafka.{Subscriptions => KafkaSubscriptions}
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
import akka.stream.alpakka.kinesis.ShardIterator._
import akka.stream.alpakka.text.scaladsl.TextFlow
import akka.stream.contrib.{SwitchMode, Valve, ValveSwitch}
import akka.stream.scaladsl._
import akka.stream.{KillSwitches, Materializer, UniqueKillSwitch}
import akka.util.ByteString

import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.common.TopicPartition

import com.thatdot.quine.app.importers.Kinesis.KinesisSourceDef
import com.thatdot.quine.app.importers.SQS.SqsStreamDef
import com.thatdot.quine.app.importers.serialization.ImportFormat
import com.thatdot.quine.app.importers.serialization.ImportFormat.RetriableIngestFailure
import com.thatdot.quine.app.routes.IngestMeter
import com.thatdot.quine.compiler
import com.thatdot.quine.graph.MasterStream.{IngestSrcExecToken, IngestSrcType}
import com.thatdot.quine.graph.cypher.Value
import com.thatdot.quine.graph.{CypherOpsGraph, cypher}
import com.thatdot.quine.routes._
import com.thatdot.quine.util.StringInput.filenameOrUrl

package object importers extends StrictLogging {

  /** Create (and start) and ingest stream from the configuration
    *
    * @param name the human-friendly name of the stream
    * @param settings ingest stream config
    * @param meter ingest meter
    * @param initialSwitchMode is the ingest stream initially paused or not?
    * @param graph graph into which to ingest
    * @param materializer
    * @param timeout
    * @return a valve switch to toggle the ingest stream, a shutdown function, and a termination signal
    */
  def createIngestStream(
    name: String,
    settings: IngestStreamConfiguration,
    meter: IngestMeter,
    initialSwitchMode: SwitchMode
  )(implicit
    graph: CypherOpsGraph,
    executionContext: ExecutionContext,
    materializer: Materializer
  ): IngestSrcType[QuineAppIngestControl] =
    settings match {
      case KafkaIngest(
            format,
            topics,
            parallelism,
            bootstrapServers,
            groupId,
            securityProtocol,
            autoCommitIntervalMs,
            autoOffsetReset,
            endingOffset,
            maxPerSecond
          ) =>
        val subscription = topics.fold(
          KafkaSubscriptions.topics,
          assignments =>
            KafkaSubscriptions.assignment(
              (
                for {
                  (topic, partitions) <- assignments
                  partition <- partitions
                } yield new TopicPartition(topic, partition)
              ).toSet
            )
        )
        val importFormat: KafkaImportFormat = importFormatFor(format)

        importFormat.importFromKafka(
          subscription,
          bootstrapServers,
          groupId.getOrElse(name),
          meter,
          initialSwitchMode,
          parallelism,
          securityProtocol,
          autoCommitIntervalMs,
          autoOffsetReset,
          endingOffset,
          maxPerSecond
        )

      case KinesisIngest(
            format,
            streamName,
            shardIds,
            parallelism,
            creds,
            iteratorType,
            numRetries,
            maxPerSecond
          ) =>
        import KinesisIngest.IteratorType
        val kinesisIterator = iteratorType match {
          case IteratorType.Latest => Latest
          case IteratorType.TrimHorizon => TrimHorizon
          case IteratorType.AtTimestamp(ms) => AtTimestamp(Instant.ofEpochMilli(ms))
          case IteratorType.AtSequenceNumber(_) | IteratorType.AfterSequenceNumber(_)
              if shardIds.fold(true)(_.size != 1) =>
            throw new IllegalArgumentException(
              "To use AtSequenceNumber or AfterSequenceNumber, exactly 1 shard must be specified"
            ) // will be caught as an "Invalid" (400) below
          case IteratorType.AtSequenceNumber(seqNo) => AtSequenceNumber(seqNo)
          case IteratorType.AfterSequenceNumber(seqNo) => AfterSequenceNumber(seqNo)
        }
        val kinesisStream = KinesisSourceDef(
          streamName,
          shardIds.getOrElse(Set.empty),
          importFormatFor(format),
          meter,
          initialSwitchMode,
          parallelism,
          creds,
          kinesisIterator,
          numRetries,
          maxPerSecond
        )
        kinesisStream.stream

      case ServerSentEventsIngest(format, url, parallelism, maxPerSecond) =>
        val stream = ServerSentEvents(
          url,
          importFormatFor(format),
          meter,
          initialSwitchMode,
          parallelism,
          maxPerSecond
        )
        stream.stream

      case SQSIngest(
            format,
            queueURL,
            readParallelism,
            writeParallelism,
            credentials,
            deleteReadMessages,
            maxPerSecond
          ) =>
        val sqsStream = SqsStreamDef(
          queueURL,
          importFormatFor(format),
          meter,
          initialSwitchMode,
          readParallelism,
          writeParallelism,
          credentials,
          deleteReadMessages,
          maxPerSecond
        )
        sqsStream.stream

      case FileIngest(
            format,
            path,
            encodingString,
            parallelism,
            maximumLineSize,
            startAtOffset,
            ingestLimit,
            maxPerSecond,
            fileIngestMode
          ) =>
        ingestFromSource(
          initialSwitchMode,
          format,
          NamedPipeSource.fileOrNamedPipeSource(Paths.get(path), fileIngestMode),
          encodingString,
          parallelism,
          maximumLineSize,
          startAtOffset,
          ingestLimit,
          maxPerSecond,
          meter,
          IngestSrcExecToken(s"File: $path")
        )

      case StandardInputIngest(
            format,
            encodingString,
            parallelism,
            maximumLineSize,
            maxPerSecond
          ) =>
        ingestFromSource(
          initialSwitchMode,
          format,
          StreamConverters.fromInputStream(() => System.in).mapMaterializedValue(_ => NotUsed),
          encodingString,
          parallelism,
          maximumLineSize,
          startAtOffset = 0L,
          ingestLimit = None,
          maxPerSecond,
          meter,
          IngestSrcExecToken("STDIN")
        )
    }

  private def ingestFromSource(
    initialSwitchMode: SwitchMode,
    format: FileIngestFormat,
    source: Source[ByteString, NotUsed],
    encodingString: String,
    parallelism: Int,
    maximumLineSize: Int,
    startAtOffset: Long,
    ingestLimit: Option[Long],
    maxPerSecond: Option[Int],
    meter: IngestMeter,
    execToken: IngestSrcExecToken
  )(implicit
    graph: CypherOpsGraph,
    executionContext: ExecutionContext,
    materializer: Materializer
  ): Source[IngestSrcExecToken, Future[ControlSwitches]] = {
    def throttled[A] = maxPerSecond match {
      case None => Flow[A]
      case Some(perSec) => Flow[A].throttle(perSec, 1.second)
    }

    val newLineDelimited = Framing
      .delimiter(ByteString("\n"), maximumLineSize, allowTruncation = true)
      .map(line => if (!line.isEmpty && line.last == '\r') line.dropRight(1) else line)

    def bounded[A] = ingestLimit match {
      case None => Flow[A].drop(startAtOffset)
      case Some(limit) => Flow[A].drop(startAtOffset).take(limit)
    }

    /* Extract out the character set that should be assumed, along with a possible
     * transcoding flow needed to reach that encoding. Although we want to support all character
     * sets, this is quite difficult when our framing methods are designed to work over byte
     * sequences. Thankfully, since we frame over only a small number of delimiters, we can
     * overfit to a small subset of very common encodings which:
     *
     *   - share the same single-byte representation for these delimiter characters
     *   - those single-byte representations can't occur anywhere else in the string's bytes
     *
     * For all other character sets, we first transcode to UTF-8.
     *
     * TODO: optimize ingest for other character sets (transcoding is not cheap)
     */
    val (charset, transcode) = Charset.forName(encodingString) match {
      case userCharset @ (StandardCharsets.UTF_8 | StandardCharsets.ISO_8859_1 | StandardCharsets.US_ASCII) =>
        userCharset -> Flow[ByteString]
      case otherCharset =>
        logger.warn(s"File ingest does not directly support $otherCharset - transcoding through UTF-8 first")
        StandardCharsets.UTF_8 -> TextFlow.transcoding(otherCharset, StandardCharsets.UTF_8)
    }

    def csvHeadersFlow(headerDef: Either[Boolean, List[String]]): Flow[List[ByteString], Value, NotUsed] =
      headerDef match {
        case Right(h) =>
          CsvToMap
            .withHeaders(h: _*)
            .via(bounded)
            .wireTap(bssm => meter.mark(bssm.values.map(_.length).sum))
            .map(m => cypher.Expr.Map(m.mapValues(bs => cypher.Expr.Str(bs.decodeString(charset)))))
        case Left(true) =>
          CsvToMap
            .toMap()
            .via(bounded)
            .wireTap(bssm => meter.mark(bssm.values.map(_.length).sum))
            .map(m => cypher.Expr.Map(m.mapValues(bs => cypher.Expr.Str(bs.decodeString(charset)))))
        case Left(false) =>
          Flow[List[ByteString]]
            .via(bounded)
            .wireTap((bss: Seq[ByteString]) => meter.mark(bss.map(_.length).sum))
            .map(l => cypher.Expr.List(l.map(bs => cypher.Expr.Str(bs.decodeString(charset))).toVector))
      }

    val ((cypherQuery, cypherParameterName), deserializedSource): (
      (String, String),
      Source[Value, (UniqueKillSwitch, Future[ValveSwitch])]
    ) = format match {
      case FileIngestFormat.CypherLine(query, parameterName) =>
        query -> parameterName -> source
          .via(transcode)
          .via(newLineDelimited)
          .viaMat(KillSwitches.single)(Keep.right)
          .viaMat(Valve(initialSwitchMode))(Keep.both)
          .via(bounded)
          .wireTap(bs => meter.mark(bs.length))
          .map(bs => cypher.Expr.Str(bs.decodeString(charset)))
      case FileIngestFormat.CypherJson(query, parameterName) =>
        query -> parameterName -> source
          .via(transcode)
          .via(newLineDelimited)
          .viaMat(KillSwitches.single)(Keep.right)
          .viaMat(Valve(initialSwitchMode))(Keep.both)
          .via(bounded)
          .wireTap(bs => meter.mark(bs.length))
          .map(bs => cypher.Value.fromJson(ujson.read(bs.decodeString(charset))))
      case FileIngestFormat.CypherCsv(query, parameterName, headers, delimiter, quote, escape) =>
        query -> parameterName -> source
          .via(transcode)
          .via(CsvParsing.lineScanner(delimiter.char, quote.char, escape.char, maximumLineSize))
          .via(csvHeadersFlow(headers))
          .viaMat(KillSwitches.single)(Keep.right)
          .viaMat(Valve(initialSwitchMode))(Keep.both)
    }

    // TODO: think about error handling of failed compilation
    val compiled = compiler.cypher.compile(
      cypherQuery,
      unfixedParameters = Seq(cypherParameterName)
    )
    if (!compiled.query.isIdempotent) {
      // TODO allow user to override this (see: allowAllNodeScan) and only retry when idempotency is asserted
      logger.warn(
        """Could not verify that the provided ingest query is idempotent. If timeouts occur, query
          |execution may be retried and duplicate data may be created.""".stripMargin.replace('\n', ' ')
      )
    }

    deserializedSource
      .via(throttled)
      .via(graph.ingestThrottleFlow)
      .mapAsyncUnordered(parallelism) { (value: Value) =>
        // this Source represents the work that would be needed to query over one specific `value`
        // Work does not begin until the source is `run` (after the recovery strategy is hooked up below)
        // If a recoverable error occurs, instead return a Source that will fail after a small delay
        // so that recoverWithRetries (below) can retry the query
        def cypherQuerySource: Source[Vector[Value], NotUsed] =
          try compiled
            .run(parameters = Map(cypherParameterName -> value))(graph)
            .results
          catch {
            case RetriableIngestFailure(e) =>
              // TODO arbitrary timeout delays repeated failing calls to requiredGraphIsReady in implementation of .run above
              Source.future(akka.pattern.after(100.millis)(Future.failed(e))(graph.system))
          }

        cypherQuerySource
          .recoverWithRetries(
            attempts = -1, // retry forever, relying on the relayAsk timer itself to slow down attempts
            { case RetriableIngestFailure(e) =>
              logger.info(
                s"""Suppressed ${e.getClass.getSimpleName} during execution of file ingest query, retrying now.
                   |Ingested item: $value. Query: "$cypherQuery. Suppressed exception:
                   |${e.getMessage}"""".stripMargin.replace('\n', ' ')
              )
              cypherQuerySource
            }
          )
          .runWith(Sink.ignore)
          .map(_ => execToken)
      }
      .watchTermination() { case ((a, b), c) => b.map(v => ControlSwitches(a, v, c)) }
  }

  private[this] def importFormatFor(label: StreamedRecordFormat): ImportFormat with KafkaImportFormat =
    label match {
      case StreamedRecordFormat.CypherJson(query, parameter) =>
        KafkaImportFormat.CypherJson(query, parameter)
      case StreamedRecordFormat.CypherProtobuf(query, parameter, schemaUrl, typeName) =>
        KafkaImportFormat.CypherProtobuf(query, parameter, filenameOrUrl(schemaUrl), typeName)
      case StreamedRecordFormat.CypherRaw(query, parameter) =>
        KafkaImportFormat.CypherRaw(query, parameter)
      case StreamedRecordFormat.Drop => KafkaImportFormat.Drop
    }
}
