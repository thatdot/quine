package com.thatdot.quine.app.v2api.definitions

import scala.concurrent.{ExecutionContext, Future}

import org.apache.pekko.event.Logging
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.server.directives.DebuggingDirectives

import com.github.pjfanning.pekkohttpcirce.FailFastCirceSupport
import io.circe.generic.extras.auto._
import io.circe.syntax._
import sttp.apispec.openapi.OpenAPI
import sttp.apispec.openapi.circe._
import sttp.tapir.docs.openapi.OpenAPIDocsInterpreter
import sttp.tapir.generic.auto._
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.model.ValuedEndpointOutput
import sttp.tapir.server.pekkohttp.{PekkoHttpServerInterpreter, PekkoHttpServerOptions}

import com.thatdot.quine.app.v2api.endpoints.V2IngestApiSchemas

/** Definitions wrapping Tapir endpoints into akka-http routes.
  */
abstract class TapirRoutes extends FailFastCirceSupport with V2IngestApiSchemas {
  protected val apiEndpoints: List[ServerEndpoint[Any, Future]]
  protected val ingestEndpoints: List[ServerEndpoint[Any, Future]]

  /** List of endpoints that should not appear in api docs. */
  protected val hiddenEndpoints: Set[ServerEndpoint[Any, Future]]

  val appMethods: ApplicationApiMethods

  private def openApiSpec(ingestOnly: Boolean): OpenAPI = OpenAPIDocsInterpreter()
    .toOpenAPI(
      (if (ingestOnly) ingestEndpoints else apiEndpoints).filterNot(hiddenEndpoints.contains).map(_.endpoint),
      "thatdot-api-v2",
      "1.0.0",
    )

  private def v2DocsRoute(ingestOnly: Boolean): Route =
    pathPrefix("api" / "v2" / "openapi.json") {
      get {
        complete(200, openApiSpec(ingestOnly).asJson)
      }
    }

  private def decodeFailureResponse(m: String): ValuedEndpointOutput[_] =
    ValuedEndpointOutput(jsonBody[ErrorResponse.BadRequest], ErrorResponse.BadRequest(m))

  private def serverOptions(implicit ec: ExecutionContext): PekkoHttpServerOptions =
    PekkoHttpServerOptions.customiseInterceptors
      .defaultHandlers(decodeFailureResponse)
      .options

  private def v2ApiRoutes(ingestOnly: Boolean)(implicit ec: ExecutionContext): Route =
    DebuggingDirectives.logRequestResult(("HTTP", Logging.DebugLevel))(
      PekkoHttpServerInterpreter(serverOptions)(ec).toRoute(if (ingestOnly) ingestEndpoints else apiEndpoints),
    )

  def v2Routes(ingestOnly: Boolean)(implicit ec: ExecutionContext): Route =
    v2ApiRoutes(ingestOnly)(ec) ~ v2DocsRoute(ingestOnly)

}
