package com.thatdot.quine.app.ingest2.sources

import java.nio.charset.Charset

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.ws._
import org.apache.pekko.http.scaladsl.settings.ClientConnectionSettings
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Source}
import org.apache.pekko.util.ByteString

import com.thatdot.quine.app.ingest.WebsocketSimpleStartupSrcDef.UpgradeFailedException
import com.thatdot.quine.app.ingest2.source.FramedSource
import com.thatdot.quine.app.routes.IngestMeter
import com.thatdot.quine.routes.WebsocketSimpleStartupIngest
import com.thatdot.quine.routes.WebsocketSimpleStartupIngest.KeepaliveProtocol

case class WebsocketSource(
  wsUrl: String,
  initMessages: Seq[String],
  keepaliveProtocol: KeepaliveProtocol,
  charset: Charset = DEFAULT_CHARSET,
  meter: IngestMeter
)(implicit system: ActorSystem) {

  val baseHttpClientSettings: ClientConnectionSettings = ClientConnectionSettings(system)

  def framedSource: FramedSource = {

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

    val source: Source[ByteString, NotUsed] = Source
      .futureSource(websocketUpgraded.transform {
        // if the websocket upgrade fails, return an already-failed Source
        case Success(InvalidUpgradeResponse(_, cause)) => Failure(new UpgradeFailedException(cause))
        case Failure(ex) => Failure(new UpgradeFailedException(ex))
        // the websocket upgrade succeeded: proceed with setting up the ingest stream source
        case Success(ValidUpgrade(_, _)) => Success(v)
      }(ExecutionContext.parasitic))
      .mapMaterializedValue(_ => NotUsed) // TBD .mapMaterializedValue(_.flatten)
      .via(metered[ByteString](meter, bs => bs.length))

    FramedSource[ByteString](
      withKillSwitches(source.via(transcodingFlow(charset))),
      meter,
      bs => bs.toArrayUnsafe()
    )
  }

}
