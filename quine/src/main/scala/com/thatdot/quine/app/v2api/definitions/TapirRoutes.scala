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

  /** List of endpoints that should not appear in api docs. */
  protected val hiddenEndpoints: Set[ServerEndpoint[Any, Future]]

  val app: ApplicationApiInterface
  private def docEndpoints: Seq[ServerEndpoint[Any, Future]] =
    RedocInterpreter(redocUIOptions = RedocUIOptions.default.copy(pathPrefix = List("v2docs")))
      .fromServerEndpoints[Future](apiEndpoints.filterNot(hiddenEndpoints.contains(_)), "thatdot-api-v2", "1.0.0")

  private def serverOptions(implicit ec: ExecutionContext): PekkoHttpServerOptions = PekkoHttpServerOptions.default

  private def v2ApiRoutes(implicit ec: ExecutionContext): Route =
    DebuggingDirectives.logRequestResult(("HTTP", Logging.DebugLevel))(
      PekkoHttpServerInterpreter(serverOptions)(ec).toRoute(apiEndpoints),
    )
  private def v2DocsRoute(implicit ec: ExecutionContext): Route =
    PekkoHttpServerInterpreter(serverOptions)(ec).toRoute(docEndpoints.toList)

  def v2Routes(implicit ec: ExecutionContext): Route = v2ApiRoutes(ec) ~ v2DocsRoute(ec)

}
