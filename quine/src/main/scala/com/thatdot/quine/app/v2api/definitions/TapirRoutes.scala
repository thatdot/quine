package com.thatdot.quine.app.v2api.definitions

import scala.concurrent.{ExecutionContext, Future}

import org.apache.pekko.event.Logging
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.server.directives.DebuggingDirectives

import com.github.pjfanning.pekkohttpcirce.FailFastCirceSupport
import io.circe.syntax._
import sttp.apispec.openapi.circe._
import sttp.apispec.openapi.{Info, OpenAPI}
import sttp.capabilities.WebSockets
import sttp.capabilities.pekko.PekkoStreams
import sttp.tapir.docs.openapi.OpenAPIDocsInterpreter
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.pekkohttp.{PekkoHttpServerInterpreter, PekkoHttpServerOptions}

import com.thatdot.api.v2.schema.TypeDiscriminatorConfig
import com.thatdot.quine.app.v2api.endpoints.Visibility

/** Definitions wrapping Tapir endpoints into akka-http routes.
  */
abstract class TapirRoutes extends FailFastCirceSupport with TypeDiscriminatorConfig with TapirDecodeErrorHandler {
  import TapirRoutes.Requirements
  protected val apiEndpoints: List[ServerEndpoint[Requirements, Future]]
  protected val ingestEndpoints: List[ServerEndpoint[Requirements, Future]]

  val appMethods: ApplicationApiMethods

  val apiInfo: Info

  protected def openApiSpec(ingestOnly: Boolean): OpenAPI = OpenAPIDocsInterpreter()
    .toOpenAPI(
      (if (ingestOnly) ingestEndpoints else apiEndpoints)
        .filterNot(_.attribute(Visibility.attributeKey).contains(Visibility.Hidden))
        .map(_.endpoint),
      apiInfo,
    )

  protected def v2DocsRoute(ingestOnly: Boolean): Route =
    pathPrefix("api" / "v2" / "openapi.json") {
      get {
        complete(200, openApiSpec(ingestOnly).asJson)
      }
    }

  /** Uses a custom decode failure handler, [[customHandler]] that we define in order to capture special cases, like
    * YAML, and augment errors messages with help text in hard to understand cases, `type` has a wrong value.
    */
  private def serverOptions(implicit ec: ExecutionContext): PekkoHttpServerOptions =
    PekkoHttpServerOptions.customiseInterceptors
      .decodeFailureHandler(customHandler)
      .exceptionHandler(customExceptionHandler)
      .options

  protected def v2ApiRoutes(ingestOnly: Boolean)(implicit ec: ExecutionContext): Route =
    DebuggingDirectives.logRequestResult(("HTTP", Logging.DebugLevel))(
      PekkoHttpServerInterpreter(serverOptions)(ec).toRoute(if (ingestOnly) ingestEndpoints else apiEndpoints),
    )

  def v2Routes(ingestOnly: Boolean)(implicit ec: ExecutionContext): Route =
    v2ApiRoutes(ingestOnly)(ec) ~ v2DocsRoute(ingestOnly)

}

object TapirRoutes {
  type Requirements = PekkoStreams with WebSockets
}
