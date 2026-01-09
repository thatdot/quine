package com.thatdot.quine.app.routes

import scala.concurrent.{ExecutionContext, Future}

import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.util.Timeout

import sttp.apispec.openapi.Info
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.pekkohttp.PekkoHttpServerInterpreter
import sttp.tapir.{EndpointInput, query}

import com.thatdot.common.logging.Log.{LazySafeLogging, LogConfig}
import com.thatdot.quine.app.QuineApp
import com.thatdot.quine.app.config.BaseConfig
import com.thatdot.quine.app.v2api.OssApiMethods
import com.thatdot.quine.app.v2api.definitions.TapirRoutes
import com.thatdot.quine.app.v2api.endpoints.V2QuineAdministrationEndpoints
import com.thatdot.quine.graph.GraphService

/** Health endpoint routes for Quine
  *
  * Exposes only the liveness and readiness endpoints on a separate binding.
  * These endpoints are used for orchestration health checks (e.g., Kubernetes probes).
  *
  * @param graph underlying graph
  * @param quineApp quine application state
  * @param appConfig current application config
  * @param timeout timeout
  */
class HealthAppRoutes(
  val graph: GraphService,
  val quineApp: QuineApp,
  appConfig: BaseConfig,
  val timeout: Timeout,
)(implicit val ec: ExecutionContext, protected val logConfig: LogConfig)
    extends BaseAppRoutes
    with V2QuineAdministrationEndpoints
    with LazySafeLogging {

  implicit val system: org.apache.pekko.actor.ActorSystem = graph.system

  override lazy val idProvider = graph.idProvider

  val appMethods = new OssApiMethods(graph, quineApp, appConfig, timeout)

  val ingestEndpoints: List[ServerEndpoint[TapirRoutes.Requirements, Future]] = List.empty

  // Expose only the liveness and readiness endpoints for health checks
  val apiEndpoints: List[ServerEndpoint[TapirRoutes.Requirements, Future]] = List(
    livenessServerEndpoint,
    readinessServerEndpoint,
  )

  val apiInfo: Info = Info(
    title = "health",
    version = "1.0.0",
    description = Some("Health check endpoints"),
  )

  override def memberIdxParameter: EndpointInput[Option[Int]] =
    query[Option[Int]]("memberIdx").schema(_.hidden(true))

  override def namespaceParameter: EndpointInput[Option[String]] =
    query[Option[String]]("namespace").schema(_.hidden(true))

  override lazy val staticFilesRoute: Route = reject

  override lazy val openApiRoute: Route = reject

  override lazy val apiRoute: Route =
    PekkoHttpServerInterpreter().toRoute(apiEndpoints)
}
