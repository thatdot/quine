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
import sttp.tapir.docs.openapi.OpenAPIDocsInterpreter
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.pekkohttp.{PekkoHttpServerInterpreter, PekkoHttpServerOptions}

import com.thatdot.quine.app.v2api.endpoints.V2IngestApiSchemas

/** Definitions wrapping Tapir endpoints into akka-http routes.
  */
abstract class TapirRoutes extends FailFastCirceSupport with V2IngestApiSchemas with TapirDecodeErrorHandler {
  protected val apiEndpoints: List[ServerEndpoint[Any, Future]]
  protected val ingestEndpoints: List[ServerEndpoint[Any, Future]]

  /** List of endpoints that should not appear in api docs. */
  protected val hiddenEndpoints: Set[ServerEndpoint[Any, Future]]

  val appMethods: ApplicationApiMethods

  private val apiInfo: Info = Info(
    title = "thatdot-api-v2",
    version = "1.0.0",
    description = Some(appMethods.productVersion match {
      case ProductVersion.Novelty =>
        """|This is a complete reference for the public REST API.
           |
           |Please refer to the Novelty documentation at https://docs.thatdot.com/novelty for more information about how to use the Novelty.
           |""".stripMargin
      case ProductVersion.Oss =>
        """|The following is autogenerated from the OpenAPI specification openapi.json and is included in Quine as fully interactive documentation. When running Quine, you can issue API calls directly from the embedded documentation pages.
           |
           |For docs, guides, and tutorials, please visit https://quine.io
           |""".stripMargin
      case ProductVersion.Enterprise =>
        """|The following is autogenerated from the OpenAPI specification openapi.json and is included in thatDot Streaming Graph as fully interactive documentation. When running Quine, you can issue API calls directly from the embedded documentation pages.
           |
           |For docs, guides, and tutorials, please visit https://docs.thatdot.com/quine-enterprise
           |""".stripMargin
    }),
  )

  private def openApiSpec(ingestOnly: Boolean): OpenAPI = OpenAPIDocsInterpreter()
    .toOpenAPI(
      (if (ingestOnly) ingestEndpoints else apiEndpoints).filterNot(hiddenEndpoints.contains).map(_.endpoint),
      apiInfo,
    )

  private def v2DocsRoute(ingestOnly: Boolean): Route =
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
      .options

  private def v2ApiRoutes(ingestOnly: Boolean)(implicit ec: ExecutionContext): Route =
    DebuggingDirectives.logRequestResult(("HTTP", Logging.DebugLevel))(
      PekkoHttpServerInterpreter(serverOptions)(ec).toRoute(if (ingestOnly) ingestEndpoints else apiEndpoints),
    )

  def v2Routes(ingestOnly: Boolean)(implicit ec: ExecutionContext): Route =
    v2ApiRoutes(ingestOnly)(ec) ~ v2DocsRoute(ingestOnly)

}
