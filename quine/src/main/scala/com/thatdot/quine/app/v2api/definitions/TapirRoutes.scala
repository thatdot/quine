package com.thatdot.quine.app.v2api.definitions

import scala.concurrent.{ExecutionContext, Future}

import org.apache.pekko.event.Logging
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.server.directives.DebuggingDirectives

import sttp.tapir.redoc.RedocUIOptions
import sttp.tapir.redoc.bundle.RedocInterpreter
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.pekkohttp.{PekkoHttpServerInterpreter, PekkoHttpServerOptions}

/** Definitions wrapping Tapir endpoints into akka-http routes.
  */
abstract class TapirRoutes {
  protected val apiEndpoints: List[ServerEndpoint[Any, Future]]
  protected val ingestEndpoints: List[ServerEndpoint[Any, Future]]

  /** List of endpoints that should not appear in api docs. */
  protected val hiddenEndpoints: Set[ServerEndpoint[Any, Future]]

  val appMethods: ApplicationApiMethods
  private def docEndpoints(ingestOnly: Boolean): Seq[ServerEndpoint[Any, Future]] =
    RedocInterpreter(
      customiseDocsModel = openAPI => openAPI.openapi("3.0.3"), // set OpenAPI spec version
      redocUIOptions = RedocUIOptions.default.copy(pathPrefix = List("v2docs")),
    )
      .fromServerEndpoints[Future](
        (if (ingestOnly) ingestEndpoints else apiEndpoints).filterNot(hiddenEndpoints.contains(_)),
        "thatdot-api-v2",
        "1.0.0",
      )

  private def serverOptions(implicit ec: ExecutionContext): PekkoHttpServerOptions = PekkoHttpServerOptions.default

  private def v2ApiRoutes(ingestOnly: Boolean)(implicit ec: ExecutionContext): Route =
    DebuggingDirectives.logRequestResult(("HTTP", Logging.DebugLevel))(
      PekkoHttpServerInterpreter(serverOptions)(ec).toRoute(if (ingestOnly) ingestEndpoints else apiEndpoints),
    )
  private def v2DocsRoute(ingestOnly: Boolean)(implicit ec: ExecutionContext): Route =
    PekkoHttpServerInterpreter(serverOptions)(ec).toRoute(docEndpoints(ingestOnly).toList)

  def v2Routes(ingestOnly: Boolean)(implicit ec: ExecutionContext): Route =
    v2ApiRoutes(ingestOnly)(ec) ~ v2DocsRoute(ingestOnly)(ec)

}
