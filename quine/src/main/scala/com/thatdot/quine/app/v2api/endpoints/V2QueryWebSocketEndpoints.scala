package com.thatdot.quine.app.v2api.endpoints

import scala.concurrent.Future

import org.apache.pekko.stream.Materializer

import sttp.capabilities.WebSockets
import sttp.capabilities.pekko.PekkoStreams
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.{Endpoint, webSocketBodyRaw}
import sttp.ws.WebSocketFrame

import com.thatdot.api.v2.ErrorResponse.ServerError
import com.thatdot.api.v2.ErrorResponseHelpers.serverError
import com.thatdot.api.v2.V2EndpointDefinitions
import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.v2api.definitions.{
  CommonParameters,
  OSSQueryExecutor,
  QuineApiMethods,
  V2QueryWebSocketFlow,
}
import com.thatdot.quine.routes.exts.NamespaceParameter

/** V2 Tapir WebSocket endpoint for the explorer UI query protocol (OSS version, no auth). */
trait V2QueryWebSocketEndpoints extends V2EndpointDefinitions with CommonParameters {
  val appMethods: QuineApiMethods
  implicit protected def logConfig: LogConfig

  private val queryWsBase = rawEndpoint("query")
    .tag("Query WebSocket")
    .description("WebSocket endpoint for streaming query execution with the explorer UI.")

  private val v2QueryWebSocket: Endpoint[
    Unit,
    Option[NamespaceParameter],
    ServerError,
    PekkoStreams.Pipe[WebSocketFrame, WebSocketFrame],
    WebSockets with PekkoStreams,
  ] = queryWsBase
    .name("query-websocket")
    .summary("Query WebSocket")
    .in("ws")
    .in(namespaceParameter)
    .get
    .out(webSocketBodyRaw(PekkoStreams).autoPongOnPing(true))
    .errorOut(serverError())

  private val v2QueryWebSocketLogic: Option[NamespaceParameter] => Future[
    Either[ServerError, PekkoStreams.Pipe[WebSocketFrame, WebSocketFrame]],
  ] = namespaceParam => {
    val namespaceId = namespaceFromParam(namespaceParam)
    implicit val materializer: Materializer = appMethods.graph.materializer
    val executor = new OSSQueryExecutor(appMethods.graph, namespaceId)
    val flow = V2QueryWebSocketFlow.buildFlow(executor, authorizeMessage = None)
    Future.successful(Right(flow))
  }

  val queryWebSocketEndpoints: List[ServerEndpoint[PekkoStreams with WebSockets, Future]] = List(
    v2QueryWebSocket.serverLogic(v2QueryWebSocketLogic),
  )
}
