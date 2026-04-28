package com.thatdot.quine.routes

import scala.scalajs.js

import endpoints4s.Codec
import endpoints4s.xhr.EndpointsSettings
import io.circe
import org.scalajs.dom.{WebSocket, XMLHttpRequest, window}

import com.thatdot.quine.v2api.routes.{V2MetricsRoutes, V2QueryUiConfigurationRoutes, V2QueryUiRoutes}

/** Client for calling Quine's API server endpoints
  *
  * @param baseUrl host for REST API calls (useful for remote Quine server)
  */
class ClientRoutes(baseUrl: js.UndefOr[String])
    extends QueryUiRoutes
    with V2QueryUiRoutes
    with exts.ClientQuineEndpoints
    with QueryUiConfigurationRoutes
    with V2QueryUiConfigurationRoutes
    with AdministrationRoutes
    with V2MetricsRoutes
    with DebugOpsRoutes
    with AlgorithmRoutes
    with endpoints4s.circe.JsonSchemas
    with exts.NoopEntitiesWithExamples
    with exts.CirceJsonAnySchema
    with endpoints4s.xhr.JsonEntitiesFromCodecs {

  def stringCodec[A](implicit codec: JsonCodec[A]): Codec[String, A] =
    Codec.fromEncoderAndDecoder[String, A](a => codec.encoder(a).noSpaces)(s =>
      endpoints4s.Validated.fromEither(
        circe.parser.decodeAccumulating(s)(codec.decoder).leftMap(_.toList.map(circe.Error.showError.show)).toEither,
      ),
    )

  // Treat an empty string the same as undefined — the startup script passes `serverUrl: ""`
  // to mean "same origin", which would otherwise leave the base URL blank and cause every
  // request to be resolved relative to the current page path (broken on sub-routes like
  // `/streams`, where relative URLs get a `/streams/` prefix).
  val baseUrlOpt: Option[String] = baseUrl.toOption.filter(_.nonEmpty)
  private val effectiveBaseUrl: String = baseUrlOpt.getOrElse(window.location.origin)
  protected val baseWsUrl: String = effectiveBaseUrl
    .replaceFirst("^http", "ws") // turns `http` into `ws` and `https` into `wss`

  val settings: EndpointsSettings = EndpointsSettings().withBaseUri(Some(effectiveBaseUrl))

  lazy val csvRequest: RequestEntity[List[List[String]]] = (body, xhr) => {
    xhr.setRequestHeader("Content-type", "text/plain; charset=utf8")
    renderCsv(body)
  }

  def yamlRequest[A](implicit codec: JsonCodec[A]): RequestEntity[A] =
    (a: A, xhr: XMLHttpRequest) => {
      xhr.setRequestHeader("Content-Type", "application/yaml")
      stringCodec(codec).encode(a)
    }

  def queryProtocolClient(namespace: Option[String] = None): WebSocketQueryClient = {
    val nsParam = namespace.fold("")(ns => s"?namespace=$ns")
    new WebSocketQueryClient(new WebSocket(s"$baseWsUrl/api/v1/query$nsParam"))
  }

  def queryProtocolClientV2(namespace: Option[String] = None): V2WebSocketQueryClient = {
    val nsParam = namespace.fold("")(ns => s"?namespace=$ns")
    new V2WebSocketQueryClient(new WebSocket(s"$baseWsUrl/api/v2/query/ws$nsParam"))
  }

}
