package com.thatdot.quine.app.routes.websocketquinepattern

import scala.concurrent.{Future, Promise}

import org.apache.pekko.actor.Status.{Failure, Success}
import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import org.apache.pekko.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage}
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.{RequestContext, RouteResult}
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.apache.pekko.stream.{CompletionStrategy, OverflowStrategy}
import org.apache.pekko.util.ByteString
import org.apache.pekko.{Done, NotUsed}

import endpoints4s.pekkohttp.server
import sttp.ws.WebSocketFrame

/** Installs a Pekko [WebSocket handler](https://pekko.apache.org/docs/pekko-http/current/server-side/websocket-support.html) for the `QuineLanguageServer`.
  *
  * The WebSocket handler creates a Flow by combining a sink and source. JSON-RPC Messages flow in
  * from the client, and into the `LSPActor` ActorRef sink. The incarnation of LSPActor
  * creates an instance of `LSPLauncher` with our `QuineLanguageServer`, to which we stream in
  * the JSON-RPC, and the `LSPLauncher` can stream out notifications/diagnostics back out from the
  * actor, to the `Source.actorRef`, and back to the client.
  */
class WebSocketQuinePatternServer(val system: ActorSystem) extends server.Endpoints {
  val route: Path[Unit] = path / "api" / "v1" / "lsp"

  val messagesFlow: Flow[Message, Message, _] = Flow.fromMaterializer { (mat, _) =>
    import mat.executionContext

    val actorPromise = Promise[ActorRef]()
    val sink: Sink[Message, Future[Done]] = Sink.foreachAsync(1) { msg =>
      actorPromise.future.map { actorRef =>
        actorRef ! msg
      }
    }

    val sourceCompletionMatcher: PartialFunction[Any, CompletionStrategy] = { case Success(_) =>
      CompletionStrategy.draining
    }
    val sourceFailureMatcher: PartialFunction[Any, Throwable] = { case Failure(ex) =>
      ex
    }
    // `Source.actorRef` cannot backpressure, so on overflow the only safe choice is to fail the
    // stream: dropping messages (dropHead/dropTail) would silently lose a JSON-RPC response and
    // leave the client awaiting an id that never returns. Failing closes the socket, and the
    // client reconnects (with backoff) and re-initializes a clean session. 64 comfortably covers
    // the small, infrequent server→client traffic (responses, diagnostics, tokens) of a query bar.
    val source: Source[Message, ActorRef] = Source.actorRef[Message](
      completionMatcher = sourceCompletionMatcher,
      failureMatcher = sourceFailureMatcher,
      bufferSize = 64,
      overflowStrategy = OverflowStrategy.fail,
    )

    // Couple the two halves so whichever side finishes first tears the other down: a client
    // disconnect completes the inbound sink, an outbound overflow-fail completes the source.
    // Watching the inbound completion lets us stop the LSPActor once the stream ends — its postStop
    // cancels the lsp4j listener thread and closes the pipes, which otherwise leak per disconnect.
    val flow: Flow[Message, Message, _] =
      Flow.fromSinkAndSourceCoupledMat(sink, source)(Keep.both).mapMaterializedValue {
        case (inboundDone, outgoingActorRef) =>
          val actor: ActorRef = mat.system.actorOf(Props(LSPActor(outgoingActorRef)))
          actorPromise.success(actor)
          inboundDone.onComplete(_ => mat.system.stop(actor))
          NotUsed
      }

    flow
  }

  /** [[messagesFlow]] adapted to Tapir's WebSocket-frame level for the V2 endpoint (`/api/v2/lsp`).
    *
    * Text and binary frames are bridged to the corresponding strict Pekko WS messages with
    * payloads passed through unchanged, so the wire format is identical to the V1 route: each
    * frame carries one raw JSON-RPC message with no Content-Length header. The V2 endpoint
    * concatenates fragmented frames before this flow sees them, and the LSP server only emits
    * strict messages, so the strict-only bridging is lossless. Control frames (close/ping/pong)
    * are handled by the endpoint's WebSocket options and dropped here.
    */
  val framesFlow: Flow[WebSocketFrame, WebSocketFrame, NotUsed] =
    Flow[WebSocketFrame]
      .collect[Message] {
        case WebSocketFrame.Text(payload, _, _) => TextMessage.Strict(payload)
        case WebSocketFrame.Binary(payload, _, _) => BinaryMessage.Strict(ByteString(payload))
      }
      .via(messagesFlow)
      .collect {
        case TextMessage.Strict(text) => WebSocketFrame.text(text)
        case BinaryMessage.Strict(data) => WebSocketFrame.binary(data.toArray)
      }

  val languageServerWebsocketRoute: RequestContext => Future[RouteResult] =
    route.directive(_ => handleWebSocketMessages(messagesFlow))
}
