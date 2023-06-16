package com.thatdot.quine.app.ingest

import java.nio.charset.{Charset, StandardCharsets}
import java.nio.file.Paths
import java.time.Duration

import scala.compat.ExecutionContexts
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

import akka.actor.ActorSystem
import akka.stream.alpakka.kinesis.KinesisSchedulerCheckpointSettings
import akka.stream.alpakka.text.scaladsl.TextFlow
import akka.stream.scaladsl.{Flow, Keep, RestartSource, Source, StreamConverters}
import akka.stream.{KillSwitches, RestartSettings}
import akka.util.ByteString
import akka.{Done, NotUsed}

import cats.data.ValidatedNel
import cats.effect.IO
import cats.implicits.catsSyntaxValidatedId
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.KafkaException

import com.thatdot.quine.app.ingest.serialization._
import com.thatdot.quine.app.routes.{IngestMeter, IngestMetered}
import com.thatdot.quine.app.{AkkaKillSwitch, ControlSwitches, QuineAppIngestControl, ShutdownSwitch}
import com.thatdot.quine.graph.CypherOpsGraph
import com.thatdot.quine.graph.MasterStream.IngestSrcExecToken
import com.thatdot.quine.graph.cypher.{Value => CypherValue}
import com.thatdot.quine.routes._
import com.thatdot.quine.util.StringInput.filenameOrUrl
import com.thatdot.quine.util.{SwitchMode, Valve, ValveSwitch}

/** Definition of an ingest that performs the actions
  *    sourceWithShutdown -> throttle -> writeToGraph -> ack
  *    @see [[stream]]
  *
  * Because some libraries define a source as simply a flow of raw values,
  * and some (e.g. Kafka, Pulsar) define sources with other functionality
  * already applied (source of values and a control switch), there are 2 places
  * provided to extend with additional ingest types:
  *
  * [[IngestSrcDef]] builds a stream from sourceWithShutdown: Source[TryDeserialized, ShutdownSwitch]
  * This requires a source of deserialized values. The source is responsible for
  * defining metering, since that requires access to the original values.
  *
  * [[RawValuesIngestSrcDef]] builds from source of raw values: Source[InputType, NotUsed].
  * That is, defined by a stream of uninterpreted inputs. The RawValues ingest
  * is responsible for defining how results will be deserialized from raw bytes.
  */
abstract class IngestSrcDef(
  format: ImportFormat,
  initialSwitchMode: SwitchMode,
  parallelism: Int,
  maxPerSecond: Option[Int],
  val name: String
)(implicit graph: CypherOpsGraph)
    extends LazyLogging {
  implicit val system: ActorSystem = graph.system
  val isSingleHost: Boolean = graph.isSingleHost
  val meter: IngestMeter = IngestMetered.ingestMeter(name)

  /** The type of a single value to be ingested. Data sources will be defined
    * as suppliers of this type.
    */
  type InputType

  /** A base type that is carried through streams that includes both the
    * (possibly) deserialized value as well as the original input.
    * The original input is carried through for later ack-ing or other
    * reference.
    */
  type TryDeserialized = (Try[CypherValue], InputType)

  /** A source of deserialized values along with a control. Ingest types
    * that provide a source of raw types should extend [[RawValuesIngestSrcDef]]
    * instead of this class.
    */
  def sourceWithShutdown(): Source[TryDeserialized, ShutdownSwitch]

  /** Default no-op implementation */
  val ack: Flow[TryDeserialized, Done, NotUsed] = Flow[TryDeserialized].map(_ => Done)

  /** MaxPerSecond rate limiting. */
  def throttle[A](): Flow[A, A, NotUsed] =
    Flow[A]
      .via(IngestSrcDef.throttled(maxPerSecond))
      .via(graph.ingestThrottleFlow)

  /** Extend for by-instance naming (e.g. to include url) */
  def ingestToken: IngestSrcExecToken = IngestSrcExecToken(name)

  /** Write successful values to the graph. */
  protected val writeSuccessValues: TryDeserialized => Future[TryDeserialized] = { t: TryDeserialized =>
    t._1 match {
      case Success(deserialized) =>
        format.writeValueToGraph(graph, deserialized).map(_ => t)(ExecutionContexts.parasitic)
      case Failure(err) =>
        logger.info(s"Deserialization failure {} {}", name, err)
        Future.failed(err)
    }
  }

  /** If the input value is properly deserialized, insert into the graph, otherwise
    * propagate the error.
    */
  val writeToGraph: Flow[TryDeserialized, TryDeserialized, NotUsed] =
    Flow[TryDeserialized].mapAsyncUnordered(parallelism)(writeSuccessValues)

  val restartSettings: RestartSettings =
    RestartSettings(minBackoff = 10.seconds, maxBackoff = 10.seconds, 2.0)
      .withMaxRestarts(3, 31.seconds)
      .withRestartOn {
        case _: KafkaException => true
        case _ => false
      }

  /** Assembled stream definition.
    */
  def stream(): Source[IngestSrcExecToken, NotUsed] = stream(_ => ())
  def stream(
    registerTerminationHooks: Future[Done] => Unit
  ): Source[IngestSrcExecToken, NotUsed] =
    RestartSource.onFailuresWithBackoff(restartSettings) { () =>
      sourceWithShutdown()
        .viaMat(Valve(initialSwitchMode))(Keep.both)
        .via(throttle())
        .via(writeToGraph)
        .via(ack)
        .map(_ => ingestToken)
        .watchTermination() { case ((a: ShutdownSwitch, b: Future[ValveSwitch]), c: Future[Done]) =>
          b.map(v => ControlSwitches(a, v, c))(ExecutionContexts.parasitic)
        }
        .mapMaterializedValue(c => setControl(c, initialSwitchMode, registerTerminationHooks))
        .named(name)
    }

  private var ingestControl: Option[Future[QuineAppIngestControl]] = None
  private val controlPromise: Promise[QuineAppIngestControl] = Promise()

  private def setControl(
    control: Future[QuineAppIngestControl],
    desiredSwitchMode: SwitchMode,
    registerTerminationHooks: Future[Done] => Unit
  ): Unit = {

    // Ensure valve is opened if required and termination hooks are registered
    control.foreach(c => c.valveHandle.flip(desiredSwitchMode))(graph.nodeDispatcherEC)
    control.map(c => registerTerminationHooks(c.termSignal))(graph.nodeDispatcherEC)

    // Set the appropriate ref and deferred ingest control
    controlPromise.completeWith(control)
    ingestControl = Some(control)
  }

  val getControl: IO[QuineAppIngestControl] =
    for {
      ctrl <- IO(ingestControl)
      result <- ctrl match {
        case Some(c) => IO.fromFuture(IO.pure(c))
        case None => IO.fromFuture(IO.pure(controlPromise.future))
      }
    } yield result

}

/** Define an ingest from a the definition of a Source of InputType. */
abstract class RawValuesIngestSrcDef(
  format: ImportFormat,
  initialSwitchMode: SwitchMode,
  parallelism: Int,
  maxPerSecond: Option[Int],
  decoders: Seq[ContentDecoder],
  name: String
)(implicit graph: CypherOpsGraph)
    extends IngestSrcDef(format, initialSwitchMode, parallelism, maxPerSecond, name) {

  /** Try to deserialize a value of InputType into a CypherValue.  This method
    * also meters the raw byte length of the input.
    */
  val deserializeAndMeter: Flow[InputType, TryDeserialized, NotUsed] =
    Flow[InputType].map { input: InputType =>
      val bytes = rawBytes(input)
      meter.mark(bytes.length)
      val decoded = ContentDecoder.decode(decoders, bytes)
      (format.importMessageSafeBytes(decoded, graph.isSingleHost), input)
    }

  /** Define a way to extract raw bytes from a single input event */
  def rawBytes(value: InputType): Array[Byte]

  /** Define a data source */
  def source(): Source[InputType, NotUsed]

  /**  Default value source is defined as a combination of the raw source and kill switch.
    *  IngestSrcDef types  that need to alter this behavior should extend [[IngestSrcDef]].
    */
  def sourceWithShutdown(): Source[TryDeserialized, ShutdownSwitch] =
    source()
      .viaMat(KillSwitches.single)(Keep.right)
      .mapMaterializedValue(ks => AkkaKillSwitch(ks))
      .via(deserializeAndMeter)

}

object IngestSrcDef extends LazyLogging {

  private def importFormatFor(label: StreamedRecordFormat): ImportFormat =
    label match {
      case StreamedRecordFormat.CypherJson(query, parameter) =>
        new CypherJsonInputFormat(query, parameter)
      case StreamedRecordFormat.CypherProtobuf(query, parameter, schemaUrl, typeName) =>
        new ProtobufInputFormat(query, parameter, filenameOrUrl(schemaUrl), typeName)
      case StreamedRecordFormat.CypherRaw(query, parameter) =>
        new CypherRawInputFormat(query, parameter)
      case StreamedRecordFormat.Drop => new TestOnlyDrop()
    }

  /* Identify by name the character set that should be assumed, along with a possible
   * transcoding flow needed to reach that encoding. Although we want to support all character
   * sets, this is quite difficult when our framing methods are designed to work over byte
   * sequences. Thankfully, for content-delimited formats, since we frame over only a small
   * number of delimiters, we can overfit to a small subset of very common encodings which:
   *
   *   - share the same single-byte representation for these delimiter characters
   *   - those single-byte representations can't occur anywhere else in the string's bytes
   *
   * For all other character sets, we first transcode to UTF-8.
   *
   * TODO: optimize ingest for other character sets (transcoding is not cheap)
   */
  def getTranscoder(charsetName: String): (Charset, Flow[ByteString, ByteString, NotUsed]) =
    Charset.forName(charsetName) match {
      case userCharset @ (StandardCharsets.UTF_8 | StandardCharsets.ISO_8859_1 | StandardCharsets.US_ASCII) =>
        userCharset -> Flow[ByteString]
      case otherCharset =>
        logger.warn(
          s"Charset-sensitive ingest does not directly support $otherCharset - transcoding through UTF-8 first"
        )
        StandardCharsets.UTF_8 -> TextFlow.transcoding(otherCharset, StandardCharsets.UTF_8)
    }

  private def throttled[A](maxPerSecond: Option[Int]): Flow[A, A, NotUsed] = maxPerSecond match {
    case None => Flow[A]
    case Some(perSec) => Flow[A].throttle(perSec, 1.second)
  }

  def createIngestSrcDef(
    name: String,
    settings: IngestStreamConfiguration,
    initialSwitchMode: SwitchMode
  )(implicit
    graph: CypherOpsGraph
  ): ValidatedNel[String, IngestSrcDef] = settings match {
    case KafkaIngest(
          format,
          topics,
          parallelism,
          bootstrapServers,
          groupId,
          securityProtocol,
          autoCommitIntervalMs,
          autoOffsetReset,
          kafkaProperties,
          endingOffset,
          maxPerSecond,
          recordEncodings
        ) =>
      KafkaSrcDef(
        name,
        topics,
        bootstrapServers,
        groupId.getOrElse(name),
        importFormatFor(format),
        initialSwitchMode,
        parallelism,
        securityProtocol,
        autoCommitIntervalMs,
        autoOffsetReset,
        kafkaProperties,
        endingOffset,
        maxPerSecond,
        recordEncodings.map(ContentDecoder.apply)
      )

    case KinesisIngest(
          format: StreamedRecordFormat,
          streamName,
          shardIds,
          parallelism,
          creds,
          region,
          iteratorType,
          numRetries,
          maxPerSecond,
          recordEncodings,
          checkpointSettings
        ) =>
      checkpointSettings match {
        case None =>
          KinesisSrcDef(
            name,
            streamName,
            shardIds,
            importFormatFor(format),
            initialSwitchMode,
            parallelism,
            creds,
            region,
            iteratorType,
            numRetries,
            maxPerSecond,
            recordEncodings.map(ContentDecoder.apply)
          ).valid
        case Some(settings) =>
          KinesisCheckpointSrcDef(
            name,
            streamName,
            //shardIds,
            importFormatFor(format),
            initialSwitchMode,
            parallelism,
            creds,
            region,
            numRetries,
            //iteratorType,
            maxPerSecond,
            recordEncodings.map(ContentDecoder.apply),
            KinesisSchedulerCheckpointSettings.create(settings.maxBatchSize, Duration.ofMillis(settings.maxBatchWait))
          ).valid

      }

    case PulsarIngest(
          format,
          topics,
          serviceUrl,
          subscriptionName,
          subscriptionType,
          parallelism,
          maximumPerSecond,
          recordEncodings
        ) =>
      PulsarSrcDef(
        name,
        serviceUrl,
        topics,
        subscriptionName,
        subscriptionType,
        importFormatFor(format),
        initialSwitchMode,
        parallelism,
        maximumPerSecond,
        recordEncodings.map(ContentDecoder.apply)
      ).valid

    case ServerSentEventsIngest(format, url, parallelism, maxPerSecond, recordEncodings) =>
      ServerSentEventsSrcDef(
        name,
        url,
        importFormatFor(format),
        initialSwitchMode,
        parallelism,
        maxPerSecond,
        recordEncodings.map(ContentDecoder.apply)
      ).valid

    case SQSIngest(
          format,
          queueURL,
          readParallelism,
          writeParallelism,
          credentialsOpt,
          regionOpt,
          deleteReadMessages,
          maxPerSecond,
          recordEncodings
        ) =>
      SqsStreamSrcDef(
        name,
        queueURL,
        importFormatFor(format),
        initialSwitchMode,
        readParallelism,
        writeParallelism,
        credentialsOpt,
        regionOpt,
        deleteReadMessages,
        maxPerSecond,
        recordEncodings.map(ContentDecoder.apply)
      ).valid

    case WebsocketSimpleStartupIngest(
          format,
          wsUrl,
          initMessages,
          keepAliveProtocol,
          parallelism,
          encoding
        ) =>
      WebsocketSimpleStartupSrcDef(
        name,
        importFormatFor(format),
        wsUrl,
        initMessages,
        keepAliveProtocol,
        parallelism,
        encoding,
        initialSwitchMode
      ).valid

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
      ContentDelimitedIngestSrcDef
        .apply(
          initialSwitchMode,
          format,
          NamedPipeSource.fileOrNamedPipeSource(Paths.get(path), fileIngestMode),
          encodingString,
          parallelism,
          maximumLineSize,
          startAtOffset,
          ingestLimit,
          maxPerSecond,
          name
        )
        .valid

    case StandardInputIngest(
          format,
          encodingString,
          parallelism,
          maximumLineSize,
          maxPerSecond
        ) =>
      ContentDelimitedIngestSrcDef
        .apply(
          initialSwitchMode,
          format,
          StreamConverters.fromInputStream(() => System.in).mapMaterializedValue(_ => NotUsed),
          encodingString,
          parallelism,
          maximumLineSize,
          startAtOffset = 0L,
          ingestLimit = None,
          maxPerSecond,
          name
        )
        .valid

    case NumberIteratorIngest(format, startAt, ingestLimit, throttlePerSecond, parallelism) =>
      ContentDelimitedIngestSrcDef
        .apply(
          initialSwitchMode,
          format,
          Source.unfold(startAt)(l => Some(l + 1 -> ByteString(l.toString + "\n"))),
          StandardCharsets.UTF_8.name(),
          parallelism,
          1000,
          0,
          ingestLimit,
          throttlePerSecond,
          name
        )
        .valid
  }

}
