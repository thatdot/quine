package com.thatdot.quine.app.ingest

import scala.compat.ExecutionContexts
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

import akka.NotUsed
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws._
import akka.http.scaladsl.settings.ClientConnectionSettings
import akka.stream.scaladsl.{Flow, Keep, Source}
import akka.util.ByteString

import com.thatdot.quine.app.ingest.WebsocketSimpleStartupSrcDef.UpgradeFailedException
import com.thatdot.quine.app.ingest.serialization.ImportFormat
import com.thatdot.quine.graph.CypherOpsGraph
import com.thatdot.quine.graph.MasterStream.IngestSrcExecToken
import com.thatdot.quine.routes.WebsocketSimpleStartupIngest
import com.thatdot.quine.routes.WebsocketSimpleStartupIngest.KeepaliveProtocol
import com.thatdot.quine.util.SwitchMode

object WebsocketSimpleStartupSrcDef {
  class UpgradeFailedException(cause: Throwable)
      extends RuntimeException("Unable to upgrade to websocket connection", cause) {

    def this(cause: String) = this(new Throwable(cause))
  }
}

final case class WebsocketSimpleStartupSrcDef(
  override val name: String,
  format: ImportFormat,
  wsUrl: String,
  initMessages: Seq[String],
  keepaliveProtocol: KeepaliveProtocol,
  parallelism: Int,
  encoding: String,
  initialSwitchMode: SwitchMode
)(implicit graph: CypherOpsGraph)
    extends RawValuesIngestSrcDef(format, initialSwitchMode, parallelism, None, Seq(), s"$name (WS ingest)") {

  type InputType = ByteString

  val (charset, _) = IngestSrcDef.getTranscoder(encoding)

  val baseHttpClientSettings: ClientConnectionSettings = ClientConnectionSettings(system)

  override def ingestToken: IngestSrcExecToken = IngestSrcExecToken(s"$name $wsUrl")

  /** placeholder for compile; unused */
  override def rawBytes(value: ByteString): Array[Byte] = value.toArray

  // Copy (and potentially tweak) baseHttpClientSettings for websockets usage
  val httpClientSettings: ClientConnectionSettings = keepaliveProtocol match {
    case WebsocketSimpleStartupIngest.PingPongInterval(intervalMillis) =>
      baseHttpClientSettings.withWebsocketSettings(
        baseHttpClientSettings.websocketSettings.withPeriodicKeepAliveMaxIdle(intervalMillis.millis)
      )
    case WebsocketSimpleStartupIngest.SendMessageInterval(message, intervalMillis) =>
      baseHttpClientSettings.withWebsocketSettings(
        baseHttpClientSettings.websocketSettings
          .withPeriodicKeepAliveMaxIdle(intervalMillis.millis)
          .withPeriodicKeepAliveData(() => ByteString(message, charset))
      )
    case WebsocketSimpleStartupIngest.NoKeepalive => baseHttpClientSettings
  }

  // NB Instead of killing this source with the downstream KillSwitch, we could switch this Source.never to a
  // Source.maybe, completing it with None to kill the connection -- this is closer to the docs for
  // webSocketClientFlow
  val outboundMessages: Source[TextMessage.Strict, NotUsed] = Source
    .fromIterator(() => initMessages.iterator)
    .map(TextMessage(_))
    .concat(Source.never)
    .named("websocket-ingest-outbound-messages")

  val wsFlow: Flow[Message, Message, Future[WebSocketUpgradeResponse]] = Http()
    .webSocketClientFlow(
      WebSocketRequest(wsUrl),
      settings = httpClientSettings
    )
    .named("websocket-ingest-client")

  val (websocketUpgraded: Future[WebSocketUpgradeResponse], websocketSource: Source[Message, NotUsed]) =
    outboundMessages
      .viaMat(wsFlow)(Keep.right)
      .preMaterialize()

  val v: Source[ByteString, NotUsed] = websocketSource.flatMapConcat {
    case textMessage: TextMessage =>
      textMessage.textStream
        .fold("")(_ + _)
        .map(ByteString.fromString(_, charset))
    case m: BinaryMessage => m.dataStream.fold(ByteString.empty)(_ concat _)
  }

  def source(): Source[ByteString, NotUsed] = Source
    .futureSource(websocketUpgraded.transform {
      // if the websocket upgrade fails, return an already-failed Source
      case Success(InvalidUpgradeResponse(_, cause)) => Failure(new UpgradeFailedException(cause))
      case Failure(ex) => Failure(new UpgradeFailedException(ex))
      // the websocket upgrade succeeded: proceed with setting up the ingest stream source
      case Success(ValidUpgrade(_, _)) => Success(v)
    }(ExecutionContexts.parasitic))
    .mapMaterializedValue(_ => NotUsed) // TBD .mapMaterializedValue(_.flatten)

}
