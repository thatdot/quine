package com.thatdot.quine.app.routes.websocketquinepattern

import scala.concurrent.{Future, Promise}

import org.apache.pekko.actor.Status.{Failure, Success}
import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import org.apache.pekko.http.scaladsl.model.ws.Message
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.{RequestContext, RouteResult}
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.apache.pekko.stream.{CompletionStrategy, OverflowStrategy}

import endpoints4s.pekkohttp.server

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
    val sink: Sink[Message, _] = Sink.foreachAsync(1) { msg =>
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
    val source: Source[Message, ActorRef] = Source.actorRef[Message](
      completionMatcher = sourceCompletionMatcher,
      failureMatcher = sourceFailureMatcher,
      bufferSize = 64,
      overflowStrategy = OverflowStrategy.fail,
    )

    val flow: Flow[Message, Message, _] =
      Flow.fromSinkAndSourceMat(sink, source)(Keep.right).mapMaterializedValue { outgoingActorRef =>
        val actor: ActorRef = mat.system.actorOf(Props(LSPActor(outgoingActorRef)))
        actorPromise.success(actor)
      }

    flow
  }

  val languageServerWebsocketRoute: RequestContext => Future[RouteResult] =
    route.directive(_ => handleWebSocketMessages(messagesFlow))
}
