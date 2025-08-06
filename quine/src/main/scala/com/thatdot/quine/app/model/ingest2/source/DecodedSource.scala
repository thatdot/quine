package com.thatdot.quine.app.model.ingest2.source

import java.nio.charset.Charset

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Flow, Keep, RestartSource, RetryFlow, Sink, Source, SourceWithContext}
import org.apache.pekko.{Done, NotUsed}

import cats.data.{Validated, ValidatedNel}
import cats.implicits.catsSyntaxValidatedId

import com.thatdot.common.logging.Log.{LazySafeLogging, LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.convert.Api2ToAws
import com.thatdot.data.{DataFoldableFrom, DataFolderTo}
import com.thatdot.outputs2.FoldableDestinationSteps.{WithByteEncoding, WithDataFoldable}
import com.thatdot.outputs2.NonFoldableDestinationSteps.WithRawBytes
import com.thatdot.outputs2.OutputEncoder.{JSON, Protobuf}
import com.thatdot.outputs2.destination.HttpEndpoint
import com.thatdot.outputs2.{
  BytesOutputEncoder,
  DestinationSteps,
  FoldableDestinationSteps,
  NonFoldableDestinationSteps,
  ResultDestination,
  destination,
}
import com.thatdot.quine.app.data.QuineDataFoldersTo
import com.thatdot.quine.app.model.ingest.QuineIngestSource
import com.thatdot.quine.app.model.ingest.serialization.ContentDecoder
import com.thatdot.quine.app.model.ingest2.V2IngestEntities._
import com.thatdot.quine.app.model.ingest2.codec.FrameDecoder
import com.thatdot.quine.app.model.ingest2.sources.S3Source.s3Source
import com.thatdot.quine.app.model.ingest2.sources.StandardInputSource.stdInSource
import com.thatdot.quine.app.model.ingest2.sources._
import com.thatdot.quine.app.model.ingest2.{V1ToV2, V2IngestEntities}
import com.thatdot.quine.app.model.transformation.polyglot.{
  PolyglotValueDataFoldableFrom,
  PolyglotValueDataFolderTo,
  Transformation,
}
import com.thatdot.quine.app.routes.IngestMeter
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest.RecordRetrySettings
import com.thatdot.quine.app.v2api.definitions.ingest2.{DeadLetterQueueOutput, DeadLetterQueueSettings, OutputFormat}
import com.thatdot.quine.app.{ControlSwitches, ShutdownSwitch}
import com.thatdot.quine.graph.MasterStream.IngestSrcExecToken
import com.thatdot.quine.graph.metrics.implicits.TimeFuture
import com.thatdot.quine.graph.{CypherOpsGraph, NamespaceId, cypher}
import com.thatdot.quine.serialization.{AvroSchemaCache, ProtobufSchemaCache}
import com.thatdot.quine.util.StringInput.filenameOrUrl
import com.thatdot.quine.util.{BaseError, SwitchMode, Valve, ValveSwitch}
import com.thatdot.quine.{routes => V1}

final case class DlqEnvelope[Frame, Decoded](
  /** The original input data type. */
  frame: Frame,
  /** The type of decoded data to be forwarded to the dlq. */
  decoded: Option[Decoded] = None,
  /** An optional message describing the error that occurred. */
  message: String,
)

/** A decoded source represents a source of interpreted values, that is, values that have
  * been translated from raw formats as supplied by their ingest source.
  */
// Note: The only reason the meter needs to be included here is to enable the creation of
// the quineIngestSource for v1 compatibility. If the meter is not used downstream from
// that it may not be needed here.
abstract class DecodedSource(val meter: IngestMeter) {
  type Decoded
  type Frame

  val foldableFrame: DataFoldableFrom[Frame]

  val foldable: DataFoldableFrom[Decoded]

  def content(input: Frame): Array[Byte]

  /** Stream of decoded values. This stream must already be metered. */
  def stream: Source[(() => Try[Decoded], Frame), ShutdownSwitch]

  def ack: Flow[Frame, Done, NotUsed] = Flow.fromFunction(_ => Done)

  def onTermination(): Unit = ()

  /** Converts the raw decoded value into the Cypher value that the ingest query expects */
  private def preprocessToCypherValue(
    decoded: Decoded,
    transformationOpt: Option[Transformation],
  ): Either[BaseError, cypher.Value] =
    transformationOpt match {
      // Just produce a cypher value if no transform.
      case None => Right(foldable.fold(decoded, QuineDataFoldersTo.cypherValueFolder))

      // Transform the input using provided transformation
      case Some(transformation) =>
        val polyglotInput = foldable.fold(decoded, PolyglotValueDataFolderTo)
        transformation(polyglotInput).map { polyglotOutput =>
          PolyglotValueDataFoldableFrom.fold(polyglotOutput, QuineDataFoldersTo.cypherValueFolder)
        }
    }

  /** Generate an [[QuineIngestSource]] from this decoded stream Source[(() => Try[A], Frame), ShutdownSwitch]
    * into a Source[IngestSrcExecToken,NotUsed]
    * applying
    * RestartSettings | switch | valve | throttle | writeToGraph | Error Handler | Ack | Termination Hooks |
    *
    * return this source as an instance of a source that can be ingested into a Quine graph.
    */
  def toQuineIngestSource(
    ingestName: String,
    /* A step ingesting cypher (query,parameters) => graph.*/
    ingestQuery: QuineIngestQuery,
    transformation: Option[Transformation],
    cypherGraph: CypherOpsGraph,
    initialSwitchMode: SwitchMode = SwitchMode.Open,
    parallelism: Int = 1,
    maxPerSecond: Option[Int] = None,
    onDecodeError: List[(DestinationSteps, Boolean)] = Nil,
    retrySettings: Option[RecordRetrySettings] = None,
    logRecordError: Boolean = false,
    onStreamErrorHandler: OnStreamErrorHandler = LogStreamError,
  )(implicit logConfig: LogConfig): QuineIngestSource = new QuineIngestSource {

    val name: String = ingestName
    implicit val graph: CypherOpsGraph = cypherGraph

    override val meter: IngestMeter = DecodedSource.this.meter

    /** Fully assembled stream with the following operations applied:
      *
      * - restart settings
      * - shutdown switch
      * - valve
      * - throttle
      * - write to graph
      * - ack
      * - termination hook
      */
    override def stream(
      intoNamespace: NamespaceId,
      registerTerminationHooks: Future[Done] => Unit,
    ): Source[IngestSrcExecToken, NotUsed] = {

      val token = IngestSrcExecToken(name)
      // TODO error handler should be settable from a config, e.g. DeadLetterErrorHandler
      val ingestStream =
        DecodedSource.this.stream
          .viaMat(Valve(initialSwitchMode))(Keep.both)
          .via(throttle(graph, maxPerSecond))

      implicit val ex: ExecutionContext = ExecutionContext.parasitic
      implicit val toBytesFrame: BytesOutputEncoder[Frame] = BytesOutputEncoder(content)

      val dlqSinks = DecodedSource.getDlqSinks(name, intoNamespace, onDecodeError)(
        toBytesFrame,
        foldableFrame = foldableFrame,
        foldable = foldable,
        logConfig = logConfig,
      )

      val src: Source[IngestSrcExecToken, Unit] =
        SourceWithContext
          .fromTuples(ingestStream)
          .asSource
          .via(DecodedSource.optionallyRetryDecodeStep[Frame, Decoded](logRecordError, retrySettings))
          // TODO this is slower than mapAsyncUnordered and is only necessary for Kafka acking case
          .mapAsync(parallelism) {
            case Right((t, frame)) =>
              preprocessToCypherValue(t, transformation) match {
                case Left(value) =>
                  Future.successful(Left(DlqEnvelope(frame, Some(t), value.getMessage)))
                case Right(cypherInput) =>
                  graph.metrics
                    .ingestQueryTimer(intoNamespace, name)
                    .time(ingestQuery.apply(cypherInput))
                    .map(_ => Right((t, frame)))
              }

            case other => Future.successful(other)
          }
          .alsoToAll(
            dlqSinks.map { sink =>
              Flow[Either[DlqEnvelope[Frame, Decoded], (Decoded, Frame)]]
                .collect { case Left(env) => env }
                .to {
                  sink
                }
            }: _*,
          )
          .map {
            case Right((_, frame)) => frame
            case Left(env) => env.frame
          }
          .via(ack)
          .map(_ => token)
          .watchTermination() { case ((a: ShutdownSwitch, b: Future[ValveSwitch]), c: Future[Done]) =>
            c.onComplete(_ => onTermination())
            b.map(v => ControlSwitches(a, v, c))
          }
          .mapMaterializedValue(c => setControl(c, initialSwitchMode, registerTerminationHooks))
          .named(name)

      onStreamErrorHandler match {
        case RetryStreamError(retryCount) =>
          RestartSource.onFailuresWithBackoff(
            // TODO: Actually lift these
            // described in IngestSrcDef or expose these settings at the api level.
            restartSettings.withMaxRestarts(retryCount, restartSettings.maxRestartsWithin),
          ) { () =>
            src.mapMaterializedValue(_ => NotUsed)
          }
        case V2IngestEntities.LogStreamError =>
          src.mapMaterializedValue(_ => NotUsed)
      }

    }

  }

  private def outputFormatToDestinationBytes(outputFormat: OutputFormat, bytesDestination: ResultDestination.Bytes)(
    implicit protobufSchemaCache: ProtobufSchemaCache,
  ): (DestinationSteps, Boolean) =
    outputFormat match {
      case OutputFormat.Bytes =>
        (WithRawBytes(bytesDestination), false)
      case OutputFormat.JSON(withMetaData) =>
        (WithByteEncoding(JSON(), bytesDestination), withMetaData)
      case OutputFormat.Protobuf(schemaUrl, typeName, withMetaData) =>
        val messageDescriptor = Await.result(
          protobufSchemaCache.getMessageDescriptor(filenameOrUrl(schemaUrl), typeName, flushOnFail = true),
          10.seconds,
        )
        (
          WithByteEncoding(Protobuf(schemaUrl, typeName, messageDescriptor), bytesDestination),
          withMetaData,
        )
    }

  def getDeadLetterQueues(
    dlq: DeadLetterQueueSettings,
  )(implicit protobufSchemaCache: ProtobufSchemaCache, system: ActorSystem): List[(DestinationSteps, Boolean)] =
    dlq.destinations.map {

      case DeadLetterQueueOutput.HttpEndpoint(url, parallelism, OutputFormat.JSON(withMetaData)) =>
        (WithDataFoldable(HttpEndpoint(url, parallelism)), withMetaData)

      case DeadLetterQueueOutput.File(path) =>
        // Update this when non-JSON outputs are supported for File (or to support including the info envelope)
        (WithByteEncoding(JSON(), destination.File(path)), false)

      case DeadLetterQueueOutput.Kafka(topic, bootstrapServers, kafkaProperties, outputFormat) =>
        val kafkaDestination = destination.Kafka(topic, bootstrapServers, kafkaProperties)
        outputFormatToDestinationBytes(outputFormat = outputFormat, bytesDestination = kafkaDestination)

      case DeadLetterQueueOutput.Kinesis(
            credentials,
            region,
            streamName,
            kinesisParallelism,
            kinesisMaxBatchSize,
            kinesisMaxRecordsPerSecond,
            kinesisMaxBytesPerSecond,
            outputFormat,
          ) =>
        val kinesisDestination = destination.Kinesis(
          credentials = credentials.map(Api2ToAws.apply),
          region = region.map(Api2ToAws.apply),
          streamName = streamName,
          kinesisParallelism = kinesisParallelism,
          kinesisMaxBatchSize = kinesisMaxBatchSize,
          kinesisMaxRecordsPerSecond = kinesisMaxRecordsPerSecond,
          kinesisMaxBytesPerSecond = kinesisMaxBytesPerSecond,
        )
        outputFormatToDestinationBytes(outputFormat = outputFormat, bytesDestination = kinesisDestination)

      case DeadLetterQueueOutput.ReactiveStream(address, port, outputFormat) =>
        val bytesDestination = destination.ReactiveStream(address, port)
        outputFormatToDestinationBytes(outputFormat = outputFormat, bytesDestination = bytesDestination)

      case DeadLetterQueueOutput.SNS(credentials, region, topic, outputFormat) =>
        val bytesDestination =
          destination.SNS(
            credentials = credentials.map(Api2ToAws.apply),
            region = region.map(Api2ToAws.apply),
            topic = topic,
          )
        outputFormatToDestinationBytes(outputFormat = outputFormat, bytesDestination = bytesDestination)
      case DeadLetterQueueOutput.StandardOut(_, _, outputFormat) =>
        val bytesDestination = destination.StandardOut(
          logLevel = destination.StandardOut.LogLevel.Info,
          logMode = destination.StandardOut.LogMode.Complete,
        )
        outputFormatToDestinationBytes(outputFormat = outputFormat, bytesDestination = bytesDestination)
    }

}

object DecodedSource extends LazySafeLogging {

  def dlqFold[Frame, Decoded](implicit
    foldableFrame: DataFoldableFrom[Frame],
    foldable: DataFoldableFrom[Decoded],
  ): DataFoldableFrom[DlqEnvelope[Frame, Decoded]] = new DataFoldableFrom[DlqEnvelope[Frame, Decoded]] {
    def fold[B](value: DlqEnvelope[Frame, Decoded], folder: DataFolderTo[B]): B = {
      val builder = folder.mapBuilder()
      builder.add("frame", foldableFrame.fold(value.frame, folder))
      value.decoded.foreach(decoded => builder.add("decoded", foldable.fold(decoded, folder)))
      builder.add("message", folder.string(value.message))
      builder.finish()
    }
  }

  def getDlqSinks[Frame: BytesOutputEncoder, Decoded](
    name: String,
    intoNamespace: NamespaceId,
    onDecodeError: List[(DestinationSteps, Boolean)],
  )(implicit
    foldableFrame: DataFoldableFrom[Frame],
    foldable: DataFoldableFrom[Decoded],
    logConfig: LogConfig,
  ): List[Sink[DlqEnvelope[Frame, Decoded], NotUsed]] =
    onDecodeError.map {
      case (steps: FoldableDestinationSteps, true) =>
        Flow[DlqEnvelope[Frame, Decoded]]
          .to(
            steps.sink(
              s"$name-errors",
              intoNamespace,
            )(DecodedSource.dlqFold(foldableFrame, foldable), logConfig),
          )
      case (steps: FoldableDestinationSteps, false) =>
        Flow[DlqEnvelope[Frame, Decoded]]
          .map(_.frame)
          .to(
            steps.sink(
              s"$name-errors",
              intoNamespace,
            )(foldableFrame, logConfig),
          )
      case (sink: NonFoldableDestinationSteps, _) =>
        Flow[DlqEnvelope[Frame, Decoded]]
          .map(_.frame)
          .to(
            sink.sink(
              s"$name-errors",
              intoNamespace,
            ),
          )
    }

  private def decodedFlow[Frame, Decoded](
    logRecord: Boolean,
  ): Flow[(() => Try[Decoded], Frame), Either[DlqEnvelope[Frame, Decoded], (Decoded, Frame)], NotUsed] =
    Flow[(() => Try[Decoded], Frame)].map { case (decoded, frame) =>
      decoded() match {
        case Success(d) => Right((d, frame))
        case Failure(ex) =>
          if (logRecord) {
            logger.warn(safe"error decoding: ${Safe(ex.getMessage)}")
          }
          Left(DlqEnvelope.apply[Frame, Decoded](frame, None, ex.getMessage))
      }
    }

  def optionallyRetryDecodeStep[Frame, Decoded](
    logRecord: Boolean,
    retrySettings: Option[RecordRetrySettings],
  ): Flow[(() => Try[Decoded], Frame), Either[DlqEnvelope[Frame, Decoded], (Decoded, Frame)], NotUsed] =
    retrySettings match {
      case Some(settings) =>
        RetryFlow
          .withBackoff(
            minBackoff = settings.minBackoff.millis,
            maxBackoff = settings.maxBackoff.seconds,
            randomFactor = settings.randomFactor,
            maxRetries = settings.maxRetries,
            decodedFlow[Frame, Decoded](logRecord),
          ) {
            case (in @ (_, _), Left(_)) => Some(in)
            case _ => None
          }
      case None => decodedFlow[Frame, Decoded](logRecord)
    }

  /** Convenience to extract parallelism from v1 configuration types w/o altering v1 configurations */
  def parallelism(config: V1.IngestStreamConfiguration): Int = config match {
    case k: V1.KafkaIngest => k.parallelism
    case k: V1.KinesisIngest => k.parallelism
    case s: V1.ServerSentEventsIngest => s.parallelism
    case s: V1.SQSIngest => s.writeParallelism
    case w: V1.WebsocketSimpleStartupIngest => w.parallelism
    case f: V1.FileIngest => f.parallelism
    case s: V1.S3Ingest => s.parallelism
    case s: V1.StandardInputIngest => s.parallelism
    case n: V1.NumberIteratorIngest => n.parallelism
    case other => throw new NoSuchElementException(s"Ingest type $other not supported")

  }

  // build from v1 configuration
  def apply(
    name: String,
    config: V1.IngestStreamConfiguration,
    meter: IngestMeter,
    system: ActorSystem,
  )(implicit
    protobufCache: ProtobufSchemaCache,
    logConfig: LogConfig,
  ): ValidatedNel[BaseError, DecodedSource] = {
    config match {
      case V1.KafkaIngest(
            format,
            topics,
            _,
            bootstrapServers,
            groupId,
            securityProtocol,
            maybeExplicitCommit,
            autoOffsetReset,
            kafkaProperties,
            endingOffset,
            _,
            recordDecoders,
          ) =>
        KafkaSource(
          topics,
          bootstrapServers,
          groupId.getOrElse(name),
          securityProtocol,
          maybeExplicitCommit,
          autoOffsetReset,
          kafkaProperties,
          endingOffset,
          recordDecoders.map(ContentDecoder(_)),
          meter,
          system,
        ).framedSource.map(_.toDecoded(FrameDecoder(format)))

      case V1.FileIngest(
            format,
            path,
            encoding,
            _,
            maximumLineSize,
            startAtOffset,
            ingestLimit,
            _,
            fileIngestMode,
          ) =>
        FileSource.decodedSourceFromFileStream(
          FileSource.srcFromIngest(path, fileIngestMode),
          FileFormat(format),
          Charset.forName(encoding),
          maximumLineSize,
          IngestBounds(startAtOffset, ingestLimit),
          meter,
          Seq(), // V1 file ingest does not define recordDecoders
        )

      case V1.S3Ingest(
            format,
            bucketName,
            key,
            encoding,
            _,
            credsOpt,
            maxLineSize,
            startAtOffset,
            ingestLimit,
            _,
          ) =>
        S3Source(
          FileFormat(format),
          bucketName,
          key,
          credsOpt,
          maxLineSize,
          Charset.forName(encoding),
          IngestBounds(startAtOffset, ingestLimit),
          meter,
          Seq(), // There is no compression support in the v1 configuration object.
        )(system).decodedSource

      case V1.StandardInputIngest(
            format,
            encoding,
            _,
            maximumLineSize,
            _,
          ) =>
        StandardInputSource(
          FileFormat(format),
          maximumLineSize,
          Charset.forName(encoding),
          meter,
          Seq(),
        ).decodedSource

      case V1.KinesisIngest(
            streamedRecordFormat,
            streamName,
            shardIds,
            _,
            creds,
            region,
            iteratorType,
            numRetries,
            _,
            recordEncodings,
          ) =>
        KinesisSource(
          streamName,
          shardIds,
          creds,
          region,
          iteratorType,
          numRetries, // TODO not currently supported
          meter,
          recordEncodings.map(ContentDecoder(_)),
        )(system.getDispatcher).framedSource.map(_.toDecoded(FrameDecoder(streamedRecordFormat)))

      case V1.KinesisKCLIngest(
            format,
            applicationName,
            kinesisStreamName,
            _,
            credentials,
            region,
            initialPosition,
            numRetries,
            _,
            recordDecoders,
            schedulerSourceSettings,
            checkpointSettings,
            advancedSettings,
          ) =>
        KinesisKclSrc(
          kinesisStreamName = kinesisStreamName,
          applicationName = applicationName,
          meter = meter,
          credentialsOpt = credentials,
          regionOpt = region,
          initialPosition = V1ToV2(initialPosition),
          numRetries = numRetries,
          decoders = recordDecoders.map(ContentDecoder(_)),
          schedulerSettings = V1ToV2(schedulerSourceSettings),
          checkpointSettings = V1ToV2(checkpointSettings),
          advancedSettings = V1ToV2(advancedSettings),
        )(ExecutionContext.parasitic).framedSource.map(_.toDecoded(FrameDecoder(format)))

      case V1.NumberIteratorIngest(_, startAtOffset, ingestLimit, _, _) =>
        Validated.valid(NumberIteratorSource(IngestBounds(startAtOffset, ingestLimit), meter).decodedSource)

      case V1.SQSIngest(
            format,
            queueURL,
            readParallelism,
            _,
            credentialsOpt,
            regionOpt,
            deleteReadMessages,
            _,
            recordEncodings,
          ) =>
        SqsSource(
          queueURL,
          readParallelism,
          credentialsOpt,
          regionOpt,
          deleteReadMessages,
          meter,
          recordEncodings.map(ContentDecoder(_)),
        ).framedSource
          .map(_.toDecoded(FrameDecoder(format)))

      case V1.ServerSentEventsIngest(
            format,
            url,
            _,
            _,
            recordEncodings,
          ) =>
        ServerSentEventSource(url, meter, recordEncodings.map(ContentDecoder(_)))(system).framedSource
          .map(_.toDecoded(FrameDecoder(format)))

      case V1.WebsocketSimpleStartupIngest(
            format,
            wsUrl,
            initMessages,
            keepAliveProtocol,
            _,
            encoding,
          ) =>
        WebsocketSource(wsUrl, initMessages, keepAliveProtocol, Charset.forName(encoding), meter)(system).framedSource
          .map(_.toDecoded(FrameDecoder(format)))
    }
  }

  //V2 configuration
  def apply(src: FramedSource, format: IngestFormat)(implicit
    protobufCache: ProtobufSchemaCache,
    avroCache: AvroSchemaCache,
  ): DecodedSource =
    src.toDecoded(FrameDecoder(format))

  // build from v2 configuration
  def apply(
    name: String,
    config: V2IngestConfiguration,
    meter: IngestMeter,
    system: ActorSystem,
  )(implicit
    protobufCache: ProtobufSchemaCache,
    avroCache: AvroSchemaCache,
    logConfig: LogConfig,
  ): ValidatedNel[BaseError, DecodedSource] =
    config.source match {
      case FileIngest(format, path, mode, maximumLineSize, startOffset, limit, charset, recordDecoders) =>
        FileSource.decodedSourceFromFileStream(
          FileSource.srcFromIngest(path, mode),
          format,
          charset,
          maximumLineSize.getOrElse(1000000), //TODO - To optional
          IngestBounds(startOffset, limit),
          meter,
          recordDecoders.map(ContentDecoder(_)),
        )

      case StdInputIngest(format, maximumLineSize, charset) =>
        FileSource.decodedSourceFromFileStream(
          stdInSource,
          format,
          charset,
          maximumLineSize.getOrElse(1000000), //TODO
          IngestBounds(),
          meter,
          Seq(),
        )

      case S3Ingest(format, bucketName, key, creds, maximumLineSize, startOffset, limit, charset, recordDecoders) =>
        FileSource.decodedSourceFromFileStream(
          s3Source(bucketName, key, creds)(system),
          format,
          charset,
          maximumLineSize.getOrElse(1000000), //TODO
          IngestBounds(startOffset, limit),
          meter,
          recordDecoders.map(ContentDecoder(_)),
        )

      case NumberIteratorIngest(_, startAtOffset, ingestLimit) =>
        NumberIteratorSource(IngestBounds(startAtOffset, ingestLimit), meter).decodedSource.valid

      case WebsocketIngest(format, wsUrl, initMessages, keepAliveProtocol, charset) =>
        WebsocketSource(wsUrl, initMessages, keepAliveProtocol, charset, meter)(system).framedSource
          .map(_.toDecoded(FrameDecoder(format)))

      case KinesisIngest(format, streamName, shardIds, creds, region, iteratorType, numRetries, recordDecoders) =>
        KinesisSource(
          streamName,
          shardIds,
          creds,
          region,
          iteratorType,
          numRetries, //TODO not currently supported
          meter,
          recordDecoders.map(ContentDecoder(_)),
        )(ExecutionContext.parasitic).framedSource.map(_.toDecoded(FrameDecoder(format)))

      case KinesisKclIngest(
            kinesisStreamName,
            applicationName,
            format,
            credentialsOpt,
            regionOpt,
            iteratorType,
            numRetries,
            recordDecoders,
            schedulerSourceSettings,
            checkpointSettings,
            advancedSettings,
          ) =>
        KinesisKclSrc(
          kinesisStreamName = kinesisStreamName,
          applicationName = applicationName,
          meter = meter,
          credentialsOpt = credentialsOpt,
          regionOpt = regionOpt,
          initialPosition = iteratorType,
          numRetries = numRetries,
          decoders = recordDecoders.map(ContentDecoder(_)),
          schedulerSettings = schedulerSourceSettings,
          checkpointSettings = checkpointSettings,
          advancedSettings = advancedSettings,
        )(ExecutionContext.parasitic).framedSource.map(_.toDecoded(FrameDecoder(format)))

      case ServerSentEventIngest(format, url, recordDecoders) =>
        ServerSentEventSource(url, meter, recordDecoders.map(ContentDecoder(_)))(system).framedSource
          .map(_.toDecoded(FrameDecoder(format)))

      case SQSIngest(
            format,
            queueUrl,
            readParallelism,
            credentialsOpt,
            regionOpt,
            deleteReadMessages,
            recordDecoders,
          ) =>
        SqsSource(
          queueUrl,
          readParallelism,
          credentialsOpt,
          regionOpt,
          deleteReadMessages,
          meter,
          recordDecoders.map(ContentDecoder(_)),
        ).framedSource.map(_.toDecoded(FrameDecoder(format)))
      case KafkaIngest(
            format,
            topics,
            bootstrapServers,
            groupId,
            securityProtocol,
            maybeExplicitCommit,
            autoOffsetReset,
            kafkaProperties,
            endingOffset,
            recordDecoders,
          ) =>
        KafkaSource(
          topics,
          bootstrapServers,
          groupId.getOrElse(name),
          securityProtocol,
          maybeExplicitCommit,
          autoOffsetReset,
          kafkaProperties,
          endingOffset,
          recordDecoders.map(ContentDecoder(_)),
          meter,
          system,
        ).framedSource.map(_.toDecoded(FrameDecoder(format)))
      case ReactiveStreamIngest(format, url, port) =>
        ReactiveSource(url, port, meter).framedSource.map(_.toDecoded(FrameDecoder(format)))
    }

}
