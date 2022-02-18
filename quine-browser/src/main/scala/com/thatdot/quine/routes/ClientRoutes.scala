package com.thatdot.quine.routes

import scala.scalajs.js

import endpoints4s.xhr.EndpointsSettings
import org.scalajs.dom.{WebSocket, window}

/** Client for calling Quine's API server endpoints
  *
  * @param baseUrl host for REST API calls (useful for remote Quine server)
  */
class ClientRoutes(baseUrl: js.UndefOr[String])
    extends QueryUiRoutes
    with exts.ClientQuineEndpoints
    with QueryUiConfigurationRoutes
    with AdministrationRoutes
    with LiteralRoutes
    with endpoints4s.xhr.JsonEntitiesFromSchemas
    with endpoints4s.xhr.future.Endpoints
    with endpoints4s.ujson.JsonSchemas
    with exts.NoopEntitiesWithExamples
    with exts.UjsonAnySchema {

  protected val baseUrlOpt: Option[String] = baseUrl.toOption
  protected val baseWsUrl: String = baseUrlOpt
    .getOrElse(window.location.origin.get) // websocket URLs must be absolute... :/
    .replaceFirst("^http", "ws") // turns `http` into `ws` and `https` into `wss`

  val settings: EndpointsSettings = EndpointsSettings().withBaseUri(baseUrlOpt)

  lazy val csvRequest: RequestEntity[List[List[String]]] = (body, xhr) => {
    xhr.setRequestHeader("Content-type", "text/plain; charset=utf8")
    renderCsv(body)
  }

  def queryProtocolClient(): WebSocketQueryClient =
    new WebSocketQueryClient(new WebSocket(s"$baseWsUrl/api/v1/query"))

  def ServiceUnavailable: StatusCode = 503
}
