package com.thatdot.quine.v2api.routes

import com.thatdot.quine.routes._
import com.thatdot.quine.routes.exts.EndpointsWithCustomErrorText

trait V2QueryUiConfigurationRoutes
    extends QueryUiConfigurationSchemas
    with V2QuerySchemas
    with EndpointsWithCustomErrorText
    with endpoints4s.algebra.JsonEntitiesFromSchemas
    with exts.QuineEndpoints {

  private val v2Api = path / "api" / "v2"
  private val v2QueryUi = v2Api / "query-ui"
  private val v2SampleQueries = v2QueryUi / "sample-queries"
  private val v2NodeAppearances = v2QueryUi / "node-appearances"
  private val v2QuickQueries = v2QueryUi / "quick-queries"

  private[this] val v2QueryUiTag = endpoints4s.algebra
    .Tag("UI Styling V2")
    .withDescription(
      Some(
        """Operations for customizing parts of the Query UI using API v2. These options are generally useful
          |for tailoring the UI to a particular domain or data model (eg. to customize the
          |icon, color, size, context-menu queries, etc. for nodes based on their contents).
          |""".stripMargin,
      ),
    )

  final val queryUiSampleQueriesV2: Endpoint[Unit, Vector[SampleQuery]] =
    endpoint(
      request = get(
        url = v2SampleQueries,
      ),
      response = ok(
        jsonResponse[V2SuccessResponse[Vector[SampleQuery]]],
      ).xmap(response => response.content)(content => V2SuccessResponse(content)),
      docs = EndpointDocs()
        .withSummary(Some("List Sample Queries V2"))
        .withDescription(
          Some(
            """Queries provided here will be available via a drop-down menu from the Quine UI search bar.""".stripMargin,
          ),
        )
        .withTags(List(v2QueryUiTag)),
    )

  final val updateQueryUiSampleQueriesV2: Endpoint[Vector[SampleQuery], Unit] =
    endpoint(
      request = put(
        url = v2SampleQueries,
        entity = jsonOrYamlRequestWithExample[Vector[SampleQuery]](SampleQuery.defaults),
      ),
      response = ok(
        jsonResponse[V2SuccessResponse[Unit]],
      ).xmap(response => response.content)(_ => V2SuccessResponse(())),
      docs = EndpointDocs()
        .withSummary(Some("Replace Sample Queries V2"))
        .withDescription(
          Some(
            """Queries provided here will be available via a drop-down menu from the Quine UI search bar.
              |
              |Queries applied here will replace any currently existing sample queries.""".stripMargin,
          ),
        )
        .withTags(List(v2QueryUiTag)),
    )

  final val queryUiAppearanceV2: Endpoint[Unit, Vector[UiNodeAppearance]] =
    endpoint(
      request = get(
        url = v2NodeAppearances,
      ),
      response = ok(
        jsonResponse[V2SuccessResponse[Vector[UiNodeAppearance]]],
      ).xmap(response => response.content)(content => V2SuccessResponse(content)),
      docs = EndpointDocs()
        .withSummary(Some("List Node Appearances V2"))
        .withDescription(
          Some(
            "When rendering a node in the UI, a node's style is decided by " +
            "picking the first style in this list whose `predicate` matches " +
            "the node.",
          ),
        )
        .withTags(List(v2QueryUiTag)),
    )

  final val updateQueryUiAppearanceV2: Endpoint[Vector[UiNodeAppearance], Unit] =
    endpoint(
      request = put(
        url = v2NodeAppearances,
        entity = jsonOrYamlRequestWithExample[Vector[UiNodeAppearance]](UiNodeAppearance.defaults),
      ),
      response = ok(
        jsonResponse[V2SuccessResponse[Unit]],
      ).xmap(response => response.content)(_ => V2SuccessResponse(())),
      docs = EndpointDocs()
        .withSummary(Some("Replace Node Appearances V2"))
        .withDescription(
          Some(
            "For a list of icon names, refer to [this page](https://ionicons.com/v2/cheatsheet.html)",
          ),
        )
        .withTags(List(v2QueryUiTag)),
    )

  final val queryUiQuickQueriesV2: Endpoint[Unit, Vector[UiNodeQuickQuery]] =
    endpoint(
      request = get(
        url = v2QuickQueries,
      ),
      response = ok(
        jsonResponse[V2SuccessResponse[Vector[UiNodeQuickQuery]]],
      ).xmap(response => response.content)(content => V2SuccessResponse(content)),
      docs = EndpointDocs()
        .withSummary(Some("List Quick Queries V2"))
        .withDescription(
          Some("""Quick queries are queries that appear when right-clicking
                 |a node in the UI.
                 |Nodes will only display quick queries that satisfy any
                 |provided predicates.""".stripMargin),
        )
        .withTags(List(v2QueryUiTag)),
    )

  final val updateQueryUiQuickQueriesV2: Endpoint[Vector[UiNodeQuickQuery], Unit] =
    endpoint(
      request = put(
        url = v2QuickQueries,
        entity = jsonOrYamlRequestWithExample[Vector[UiNodeQuickQuery]](UiNodeQuickQuery.defaults),
      ),
      response = ok(
        jsonResponse[V2SuccessResponse[Unit]],
      ).xmap(response => response.content)(_ => V2SuccessResponse(())),
      docs = EndpointDocs()
        .withSummary(Some("Replace Quick Queries V2"))
        .withDescription(Some("""Quick queries are queries that appear when right-clicking
            |a node in the UI.
            |Queries applied here will replace any currently existing quick queries.
            |""".stripMargin))
        .withTags(List(v2QueryUiTag)),
    )
}
