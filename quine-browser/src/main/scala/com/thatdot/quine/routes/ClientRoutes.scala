package com.thatdot.quine.routes

import scala.scalajs.js

import endpoints4s.Codec
import endpoints4s.xhr.EndpointsSettings
import io.circe
import org.scalajs.dom.{WebSocket, XMLHttpRequest, window}

/** Client for calling Quine's API server endpoints
  *
  * @param baseUrl host for REST API calls (useful for remote Quine server)
  */
class ClientRoutes(baseUrl: js.UndefOr[String])
    extends QueryUiRoutes
    with exts.ClientQuineEndpoints
    with QueryUiConfigurationRoutes
    with AdministrationRoutes
    with DebugOpsRoutes
    with AlgorithmRoutes
    with endpoints4s.xhr.future.Endpoints
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

  protected val baseUrlOpt: Option[String] = baseUrl.toOption
  protected val baseWsUrl: String = baseUrlOpt
    .getOrElse(window.location.origin) // websocket URLs must be absolute... :/
    .replaceFirst("^http", "ws") // turns `http` into `ws` and `https` into `wss`

  val settings: EndpointsSettings = EndpointsSettings().withBaseUri(baseUrlOpt)

  lazy val csvRequest: RequestEntity[List[List[String]]] = (body, xhr) => {
    xhr.setRequestHeader("Content-type", "text/plain; charset=utf8")
    renderCsv(body)
  }

  def yamlRequest[A](implicit codec: JsonCodec[A]): RequestEntity[A] =
    (a: A, xhr: XMLHttpRequest) => {
      xhr.setRequestHeader("Content-Type", "application/yaml")
      stringCodec(codec).encode(a)
    }

  def queryProtocolClient(): WebSocketQueryClient =
    new WebSocketQueryClient(new WebSocket(s"$baseWsUrl/api/v1/query"))

  val ServiceUnavailable: StatusCode = 503

}
