package com.thatdot.connect.routes

import akka.http.scaladsl.server.Route

import endpoints4s.openapi.model._

import com.thatdot.connect.BuildInfo
import com.thatdot.quine.graph.BaseGraph
import com.thatdot.quine.model.QuineIdProvider
import com.thatdot.quine.routes._

/** The OpenAPI docs for our API
  *
  * @param idProvider the Quine ID provider (relevant for serialization of IDs and examples)
  */
final class ConnectOpenApiDocs(val idProvider: QuineIdProvider)
    extends LiteralRoutes
    with AdministrationRoutes
    with QueryUiRoutes
    with QueryUiConfigurationRoutes
    with IngestRoutes
    with StandingQueryRoutes
    with endpoints4s.openapi.Endpoints
    with endpoints4s.openapi.JsonEntitiesFromSchemas
    with exts.ServerQuineEndpoints
    with com.thatdot.quine.routes.exts.OpenApiEntitiesWithExamples
    with com.thatdot.quine.routes.exts.OpenApiAnySchema {

  private[this] val connectEndpoints = List(
    buildInfo,
    config,
    readinessProbe,
    livenessProbe,
    shutdown,
    shardSizes,
    literalGet,
    // literalMergeNodes, // QU-353
    literalPost,
    literalDelete,
    literalDebug,
    literalEdgesGet,
    literalEdgePut,
    literalEdgeDelete,
    literalHalfEdgesGet,
    literalPropertyGet,
    literalPropertyPut,
    literalPropertyDelete,
    cypherPost,
    cypherNodesPost,
    cypherEdgesPost,
    gremlinPost,
    gremlinNodesPost,
    gremlinEdgesPost,
    queryUiSampleQueries,
    updateQueryUiSampleQueries,
    queryUiQuickQueries,
    updateQueryUiQuickQueries,
    queryUiAppearance,
    updateQueryUiAppearance,
    updateQueryUiAppearance,
    ingestStreamList,
    ingestStreamStart,
    ingestStreamStop,
    ingestStreamLookup,
    ingestStreamPause,
    ingestStreamUnpause,
    standingList,
    standingIssue,
    standingAddOut,
    standingRemoveOut,
    standingCancel,
    standingGet,
    standingList,
    standingPropagate
  )

  val api: OpenApi =
    openApi(
      Info(title = "thatDot Connect API", version = BuildInfo.version).withDescription(
        Some(
          """This is a complete reference for the public REST API. For non-reference information
            |such as tutorials, please refer to <https://docs.thatdot.com>""".stripMargin
        )
      )
    )(
      connectEndpoints: _*
    )

}

/** The Akka HTTP implementation of routes serving up the OpenAPI specification
  * of our API
  *
  * @param graph the Quine graph
  */
final case class ConnectOpenApiDocsRoutes(graph: BaseGraph)
    extends endpoints4s.akkahttp.server.Endpoints
    with endpoints4s.akkahttp.server.JsonEntitiesFromEncodersAndDecoders {

  val doc = new ConnectOpenApiDocs(graph.idProvider)

  final val route: Route = {
    val docEndpoint = endpoint(
      get(path / "docs" / "openapi.json"),
      ok(jsonResponse[endpoints4s.openapi.model.OpenApi])
    )

    docEndpoint.implementedBy(_ => doc.api)
  }
}
