package com.thatdot.quine.v2api

import scala.concurrent.duration.DurationInt

import org.apache.pekko.http.scaladsl.model.ws.TextMessage
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest, WSProbe}
import org.apache.pekko.testkit.TestDuration
import org.apache.pekko.util.Timeout

import io.circe.parser.decode
import io.circe.syntax.EncoderOps
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.app.config.{FileAccessPolicy, QuineConfig, ResolutionMode}
import com.thatdot.quine.app.model.outputs2.query.standing.LocalTapBus
import com.thatdot.quine.app.routes.websocketquinepattern.{
  Capabilities,
  InitializeParams,
  InitializeResult,
  JsonRpcRequest,
  JsonRpcResponse,
}
import com.thatdot.quine.app.v2api.{OssApiMethods, V2OssRoutes}
import com.thatdot.quine.app.{IngestTestGraph, QuineApp}
import com.thatdot.quine.util.TestLogging._

/** Route-level coverage for the V2 LSP WebSocket endpoint (`/api/v2/lsp`).
  *
  * The language server is the QuinePattern frontend, so the endpoint is served only when
  * `qp.enabled` is set. Each case builds its own routes (not the shared
  * [[EndpointValidationSupport]]) because the gate is read at route construction.
  *
  * The wire format matches the V1 route: each WebSocket text frame carries one raw JSON-RPC
  * message with no Content-Length header prefix.
  */
class V2LspWebSocketEndpointsSpec extends AnyFlatSpec with Matchers with ScalatestRouteTest with BeforeAndAfterAll {

  private var originalQpEnabled: Option[String] = None

  override def beforeAll(): Unit = {
    originalQpEnabled = Option(System.getProperty("qp.enabled"))
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    originalQpEnabled match {
      case Some(value) => System.setProperty("qp.enabled", value)
      case None => System.clearProperty("qp.enabled")
    }
    super.afterAll()
  }

  // The graph/app are independent of qp.enabled; only the routes read it (when constructed).
  private lazy val app: OssApiMethods = {
    val graph = IngestTestGraph.makeGraph("lsp-endpoint-test")
    val quineApp =
      new QuineApp(graph, false, FileAccessPolicy(List.empty, ResolutionMode.Dynamic), new LocalTapBus)
    new OssApiMethods(graph, quineApp, QuineConfig(), Timeout(5.seconds))
  }

  /** Build a fresh set of V2 routes, reading the current `qp.enabled` for the LSP gating. */
  private def routesWithQpEnabled(enabled: Boolean): Route = {
    System.setProperty("qp.enabled", enabled.toString)
    new V2OssRoutes(app).v2Routes(ingestOnly = false)
  }

  "GET /api/v2/lsp" should "upgrade to a WebSocket and answer a raw-JSON initialize request when QuinePattern is enabled" in {
    implicit val timeout: RouteTestTimeout = RouteTestTimeout(10.seconds.dilated)
    val routes = routesWithQpEnabled(enabled = true)
    val wsClient = WSProbe()

    val initializeRequest =
      JsonRpcRequest(
        jsonrpc = "2.0",
        id = 1,
        method = "initialize",
        params = InitializeParams(capabilities = Capabilities()),
      ).asJson.noSpaces

    WS("/api/v2/lsp", wsClient.flow) ~> routes ~> check {
      isWebSocketUpgrade shouldEqual true
      wsClient.sendMessage(initializeRequest)
      wsClient.expectMessage() match {
        case TextMessage.Strict(payload) =>
          // Raw JSON-RPC: WebSocket framing provides message boundaries, no LSP base-protocol header.
          payload should not startWith "Content-Length"
          decode[JsonRpcResponse](payload) match {
            case Right(JsonRpcResponse(_, id, result)) =>
              id shouldEqual 1
              result shouldBe an[InitializeResult]
            case Left(error) => fail(s"Initialize response did not parse as raw JSON-RPC: $error")
          }
        case other => fail(s"Expected a strict text message, got: $other")
      }
    }
  }

  it should "not be served when QuinePattern is disabled" in {
    val routes = routesWithQpEnabled(enabled = false)
    val wsClient = WSProbe()
    WS("/api/v2/lsp", wsClient.flow) ~> routes ~> check {
      handled shouldEqual false
    }
  }
}
