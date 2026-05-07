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
  GraphScopedEndpoints,
  OSSQueryExecutor,
  QuineApiMethods,
  V2QueryWebSocketFlow,
}
import com.thatdot.quine.graph.NamespaceId

/** V2 Tapir WebSocket endpoint for the explorer UI query protocol (OSS version, no auth). */
trait V2QueryWebSocketEndpoints extends V2EndpointDefinitions with CommonParameters with GraphScopedEndpoints {
  val appMethods: QuineApiMethods
  implicit protected def logConfig: LogConfig

  val v2QueryWebSocket: Endpoint[
    Unit,
    NamespaceId,
    ServerError,
    PekkoStreams.Pipe[WebSocketFrame, WebSocketFrame],
    WebSockets with PekkoStreams,
  ] = graphScopedEndpoint("query", "ws")
    .tag("Query WebSocket")
    .description("WebSocket endpoint for streaming query execution with the explorer UI.")
    .name("query-websocket")
    .summary("Query WebSocket")
    .get
    .out(webSocketBodyRaw(PekkoStreams).autoPongOnPing(true))
    .errorOut(serverError())

  private val v2QueryWebSocketLogic: NamespaceId => Future[
    Either[ServerError, PekkoStreams.Pipe[WebSocketFrame, WebSocketFrame]],
  ] = namespaceId => {
    implicit val materializer: Materializer = appMethods.graph.materializer
    val executor = new OSSQueryExecutor(appMethods.graph, namespaceId)
    val flow = V2QueryWebSocketFlow.buildFlow(executor, authorizeMessage = None)
    Future.successful(Right(flow))
  }

  val queryWebSocketEndpoints: List[ServerEndpoint[PekkoStreams with WebSockets, Future]] = List(
    v2QueryWebSocket.serverLogic(v2QueryWebSocketLogic),
  )
}
