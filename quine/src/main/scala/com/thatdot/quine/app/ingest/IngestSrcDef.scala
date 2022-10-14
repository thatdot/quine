package com.thatdot.quine.app.ingest

import java.nio.charset.{Charset, StandardCharsets}
import java.nio.file.Paths

import scala.compat.ExecutionContexts
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

import akka.actor.ActorSystem
import akka.stream.alpakka.text.scaladsl.TextFlow
import akka.stream.contrib.{SwitchMode, Valve, ValveSwitch}
import akka.stream.scaladsl.{Flow, Keep, Source, StreamConverters}
import akka.stream.{KillSwitches, Materializer, UniqueKillSwitch}
import akka.util.ByteString
import akka.{Done, NotUsed}

import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.app.ingest.serialization._
import com.thatdot.quine.app.routes.{IngestMeter, IngestMetered}
import com.thatdot.quine.app.{AkkaKillSwitch, ControlSwitches, QuineAppIngestControl, ShutdownSwitch}
import com.thatdot.quine.graph.CypherOpsGraph
import com.thatdot.quine.graph.MasterStream.{IngestSrcExecToken, IngestSrcType}
import com.thatdot.quine.graph.cypher.{Value => CypherValue}
import com.thatdot.quine.routes._
import com.thatdot.quine.util.StringInput.filenameOrUrl

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
  implicit val ec: ExecutionContext = graph.system.dispatcher
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

  type Switches = (UniqueKillSwitch, Future[ValveSwitch])

  /**  A source of deserialized values along with a control. Ingest types
    *  that provide a source of raw types should extend [[RawValuesIngestSrcDef]]
    *  instead of this class.
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
      case Success(deserialized) => format.writeValueToGraph(graph, deserialized).flatMap(_ => Future.successful(t))
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

  /** Assembled stream definition.
    */
  def stream(): Source[IngestSrcExecToken, Future[QuineAppIngestControl]] =
    sourceWithShutdown()
      .viaMat(Valve(initialSwitchMode))(Keep.both)
      .via(throttle())
      .via(writeToGraph)
      .via(ack)
      .map(_ => ingestToken)
      .watchTermination() { case ((a: ShutdownSwitch, b: Future[ValveSwitch]), c: Future[Done]) =>
        b.map(v => ControlSwitches(a, v, c))(ExecutionContexts.parasitic)
      }
      .named(name)
}

/** Define an ingest from a the definition of a Source of InputType. */
abstract class RawValuesIngestSrcDef(
  format: ImportFormat,
  initialSwitchMode: SwitchMode,
  parallelism: Int,
  maxPerSecond: Option[Int],
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
      (format.importMessageSafeBytes(bytes, graph.isSingleHost), input)
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

  def importFormatFor(label: StreamedRecordFormat): ImportFormat =
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

  def throttled[A](maxPerSecond: Option[Int]): Flow[A, A, NotUsed] = maxPerSecond match {
    case None => Flow[A]
    case Some(perSec) => Flow[A].throttle(perSec, 1.second)
  }

  def createIngestSrcDef(
    name: String,
    settings: IngestStreamConfiguration,
    initialSwitchMode: SwitchMode
  )(implicit
    graph: CypherOpsGraph
  ): IngestSrcDef = settings match {
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
        endingOffset,
        maxPerSecond
      )

    case KinesisIngest(
          format: StreamedRecordFormat,
          streamName,
          shardIds,
          parallelism,
          creds,
          iteratorType,
          numRetries,
          maxPerSecond
        ) =>
      KinesisSrcDef(
        name,
        streamName,
        shardIds,
        importFormatFor(format),
        initialSwitchMode,
        parallelism,
        creds,
        iteratorType,
        numRetries,
        maxPerSecond
      )
    case PulsarIngest(
          format,
          topics,
          serviceUrl,
          subscriptionName,
          subscriptionType,
          parallelism,
          maximumPerSecond
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
        maximumPerSecond
      )

    case ServerSentEventsIngest(format, url, parallelism, maxPerSecond) =>
      ServerSentEventsSrcDef(
        name,
        url,
        importFormatFor(format),
        initialSwitchMode,
        parallelism,
        maxPerSecond
      )

    case SQSIngest(
          format,
          queueURL,
          readParallelism,
          writeParallelism,
          credentials,
          deleteReadMessages,
          maxPerSecond
        ) =>
      SqsStreamSrcDef(
        name,
        queueURL,
        importFormatFor(format),
        initialSwitchMode,
        readParallelism,
        writeParallelism,
        credentials,
        deleteReadMessages,
        maxPerSecond
      )

    case WebsocketSimpleStartupIngest(format, wsUrl, initMessages, keepAliveProtocol, parallelism, encoding) =>
      WebsocketSimpleStartupSrcDef(
        name,
        importFormatFor(format),
        wsUrl,
        initMessages,
        keepAliveProtocol,
        parallelism,
        encoding,
        initialSwitchMode
      )

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
  }

  /** Create (and start) and ingest stream from the configuration
    *
    * @param name              the human-friendly name of the stream
    * @param settings          ingest stream config
    * @param initialSwitchMode is the ingest stream initially paused or not?
    * @param graph             graph into which to ingest
    * @param materializer
    * @return a valve switch to toggle the ingest stream, a shutdown function, and a termination signal
    */
  def createIngestStream(
    name: String,
    settings: IngestStreamConfiguration,
    initialSwitchMode: SwitchMode
  )(implicit
    graph: CypherOpsGraph,
    materializer: Materializer
  ): IngestSrcType[QuineAppIngestControl] =
    createIngestSrcDef(name, settings, initialSwitchMode).stream()

}
