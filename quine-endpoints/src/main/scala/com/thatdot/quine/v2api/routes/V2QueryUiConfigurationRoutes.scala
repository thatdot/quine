package com.thatdot.quine.v2api.routes

import endpoints4s.generic.JsonSchemas
import io.circe.Json

import com.thatdot.quine.routes._
import com.thatdot.quine.routes.exts.AnySchema
import com.thatdot.quine.v2api.routes.V2QuerySort.{Node, Text}

final case class V2UiNodePredicate(
  propertyKeys: Vector[String],
  knownValues: Map[
    String,
    Json,
  ],
  dbLabel: Option[String],
)
sealed abstract class V2QuerySort
object V2QuerySort {
  case object Node extends V2QuerySort
  case object Text extends V2QuerySort
}
final case class V2QuickQuery(name: String, querySuffix: String, sort: V2QuerySort, edgeLabel: Option[String])
final case class V2UiNodeQuickQuery(predicate: V2UiNodePredicate, quickQuery: V2QuickQuery)

trait V2QueryUiConfigurationRoutesConverters {
  def convertToV1UiNodeQuickQuery(v2: V2SuccessResponse[Vector[V2UiNodeQuickQuery]]): Vector[UiNodeQuickQuery] =
    v2.content.map(v1NodeQuickQuery =>
      UiNodeQuickQuery(
        predicate = UiNodePredicate(
          propertyKeys = v1NodeQuickQuery.predicate.propertyKeys,
          knownValues = v1NodeQuickQuery.predicate.knownValues,
          dbLabel = v1NodeQuickQuery.predicate.dbLabel,
        ),
        quickQuery = QuickQuery(
          name = v1NodeQuickQuery.quickQuery.name,
          querySuffix = v1NodeQuickQuery.quickQuery.querySuffix,
          queryLanguage = QueryLanguage.Cypher,
          sort = v1NodeQuickQuery.quickQuery.sort match {
            case Node => QuerySort.Node
            case Text => QuerySort.Text
          },
          edgeLabel = v1NodeQuickQuery.quickQuery.edgeLabel,
        ),
      ),
    )
}

trait V2QueryUiConfigurationRoutesSchemas extends AnySchema with JsonSchemas {
  implicit lazy val v2JsonSchema: JsonSchema[Json] = anySchema(None)
  implicit lazy val v2UiNodePredicateSchema: JsonSchema[V2UiNodePredicate] = genericRecord
  implicit lazy val v2QuerySort: JsonSchema[V2QuerySort] = genericTagged
  implicit lazy val v2QuickQuerySchema: JsonSchema[V2QuickQuery] = genericRecord
  implicit lazy val v2UiNodeQuickQuerySchema: JsonSchema[V2UiNodeQuickQuery] = genericRecord
}

trait V2QueryUiConfigurationRoutes
    extends QueryUiConfigurationSchemas
    with V2QueryUiConfigurationRoutesSchemas
    with V2SuccessResponseSchema
    with V2QueryUiConfigurationRoutesConverters
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
        jsonResponse[V2SuccessResponse[Vector[V2UiNodeQuickQuery]]],
      ).xmap[Vector[UiNodeQuickQuery]](convertToV1UiNodeQuickQuery)(_ =>
        throw new UnsupportedOperationException(
          "Client-endpoint only, not needed",
        ),
      ),
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
