package com.thatdot.quine.app.v2api.endpoints

import scala.concurrent.Future

import sttp.capabilities.WebSockets
import sttp.capabilities.pekko.PekkoStreams
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.{Endpoint, webSocketBodyRaw}
import sttp.ws.WebSocketFrame

import com.thatdot.api.v2.ErrorResponse.ServerError
import com.thatdot.api.v2.ErrorResponseHelpers.serverError
import com.thatdot.api.v2.V2EndpointDefinitions
import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.routes.QuinePatternSettings
import com.thatdot.quine.app.routes.websocketquinepattern.WebSocketQuinePatternServer
import com.thatdot.quine.app.v2api.definitions.QuineApiMethods

/** V2 Tapir WebSocket endpoint for the Quine language server (LSP over WebSocket).
  *
  * Each WebSocket text frame carries one raw JSON-RPC message — WebSocket framing provides
  * message boundaries, so there is no Content-Length header prefix. Fragmented frames are
  * concatenated before they reach the LSP flow so large LSP payloads arrive whole.
  */
trait V2LspWebSocketEndpoints extends V2EndpointDefinitions {
  val appMethods: QuineApiMethods
  implicit protected def logConfig: LogConfig

  val v2LspWebSocket: Endpoint[
    Unit,
    Unit,
    ServerError,
    PekkoStreams.Pipe[WebSocketFrame, WebSocketFrame],
    WebSockets with PekkoStreams,
  ] = rawEndpoint("lsp")
    .tag("Query WebSocket")
    .name("lsp-websocket")
    .summary("Language Server WebSocket")
    .description(
      "WebSocket endpoint for the Quine language server, providing diagnostics, completions, " +
      "and semantic tokens for the query editor. Frames carry raw JSON-RPC messages.",
    )
    .get
    .out(webSocketBodyRaw(PekkoStreams).autoPongOnPing(true).concatenateFragmentedFrames(true))
    .errorOut(serverError())

  /** Lazy so that endpoint construction (e.g. for OpenAPI docs generation) never touches
    * `appMethods`; the server is only needed once a connection's logic runs.
    */
  private lazy val webSocketQuinePatternServer = new WebSocketQuinePatternServer(appMethods.graph.system)

  protected[endpoints] val v2LspWebSocketLogic: Unit => Future[
    Either[ServerError, PekkoStreams.Pipe[WebSocketFrame, WebSocketFrame]],
  ] = _ => Future.successful(Right(webSocketQuinePatternServer.framesFlow))

  val lspWebSocketEndpoints: List[ServerEndpoint[PekkoStreams with WebSockets, Future]] =
    if (QuinePatternSettings.isEnabled)
      List(v2LspWebSocket.serverLogic(v2LspWebSocketLogic))
    else Nil
}
