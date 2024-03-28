package com.thatdot.quine.app.routes.websocketquinepattern

import scala.concurrent.Future
import scala.concurrent.duration._

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.http.scaladsl.model.ws.Message
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.{RequestContext, RouteResult}
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.util.Timeout

import endpoints4s.pekkohttp.server

class WebSocketQuinePatternServer(val system: ActorSystem) extends server.Endpoints {
  implicit val languageServerTimeout: Timeout = Timeout(5.seconds)
  val route: Path[Unit] = path / "api" / "v1" / "lsp"

  val messagesFlow: Flow[Message, Message, NotUsed] = {
    val actor = system.actorOf(Props[QuineLanguageServerActor]())
    Flow[Message].ask[Message](parallelism = 2)(actor)
  }

  val languageServerWebsocketRoute: RequestContext => Future[RouteResult] =
    route.directive(_ => handleWebSocketMessages(messagesFlow))
}
