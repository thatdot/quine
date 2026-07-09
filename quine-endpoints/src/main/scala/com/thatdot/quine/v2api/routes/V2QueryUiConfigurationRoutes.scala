package com.thatdot.quine.v2api.routes

import endpoints4s.generic.JsonSchemas
import io.circe.Json

import com.thatdot.quine.routes._
import com.thatdot.quine.routes.exts.AnySchema

final case class V2UiNodePredicate(
  propertyKeys: Vector[String],
  knownValues: Map[
    String,
    Json,
  ],
  dbLabel: Option[String],
) {
  def matches(node: UiNode[String]): Boolean = {
    def hasRightLabel = dbLabel.forall(_ == node.label)
    def hasRightKeys = propertyKeys.forall(node.properties.contains)
    def hasRightValues = knownValues.forall { case (k, v) =>
      node.properties.get(k).fold(false)(nodeV => v == nodeV)
    }
    hasRightLabel && hasRightKeys && hasRightValues
  }
}

object V2UiNodePredicate {
  val every: V2UiNodePredicate = V2UiNodePredicate(Vector.empty, Map.empty, None)
}

sealed abstract class V2QuerySort
object V2QuerySort {
  case object Node extends V2QuerySort
  case object Text extends V2QuerySort
}

final case class V2QuickQuery(name: String, querySuffix: String, sort: V2QuerySort, edgeLabel: Option[String]) {
  def fullQuery(startingIds: Seq[String]): String = {
    val simpleNumberId = startingIds.forall(_ matches "-?\\d+")
    val idOrStrIds = startingIds
      .map(id => if (simpleNumberId) id else ujson.Str(id).toString)
      .mkString(", ")
    if (startingIds.length == 1)
      s"MATCH (n) WHERE ${if (simpleNumberId) "id" else "strId"}(n) = $idOrStrIds $querySuffix"
    else
      s"UNWIND [$idOrStrIds] AS nId MATCH (n) WHERE ${if (simpleNumberId) "id" else "strId"}(n) = nId $querySuffix"
  }
}

object V2QuickQuery {
  val defaults: Vector[V2UiNodeQuickQuery] = Vector(
    V2UiNodeQuickQuery(
      V2UiNodePredicate.every,
      V2QuickQuery("Adjacent Nodes", "MATCH (n)--(m) RETURN DISTINCT m", V2QuerySort.Node, None),
    ),
    V2UiNodeQuickQuery(
      V2UiNodePredicate.every,
      V2QuickQuery("Refresh", "RETURN n", V2QuerySort.Node, None),
    ),
  )
}

final case class V2UiNodeQuickQuery(predicate: V2UiNodePredicate, quickQuery: V2QuickQuery)

trait V2QueryUiConfigurationRoutesSchemas extends AnySchema with JsonSchemas {
  implicit lazy val v2JsonSchema: JsonSchema[Json] = anySchema(None)
  implicit lazy val v2UiNodePredicateSchema: JsonSchema[V2UiNodePredicate] = genericRecord
  // AIP-126 wire format: flat SCREAMING_SNAKE_CASE string (e.g. `"NODE"`), not a tagged object.
  implicit lazy val v2QuerySort: Enum[V2QuerySort] =
    stringEnumeration[V2QuerySort](Seq(V2QuerySort.Node, V2QuerySort.Text))(_.toString.toUpperCase)
  implicit lazy val v2QuickQuerySchema: JsonSchema[V2QuickQuery] = genericRecord
  implicit lazy val v2UiNodeQuickQuerySchema: JsonSchema[V2UiNodeQuickQuery] = genericRecord
}

trait V2QueryUiConfigurationRoutes
    extends QueryUiConfigurationSchemas
    with V2QueryUiConfigurationRoutesSchemas
    with endpoints4s.algebra.JsonEntitiesFromSchemas
    with exts.QuineEndpoints {

  private val v2Api = path / "api" / "v2"
  private val v2QueryUi = v2Api / "queryUi"
  private val v2SampleQueries = v2QueryUi / "sampleQueries"
  private val v2NodeAppearances = v2QueryUi / "nodeAppearances"
  private val v2QuickQueries = v2QueryUi / "quickQueries"

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
      response = ok(jsonResponse[Vector[SampleQuery]]),
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
      response = noContent(),
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
      response = ok(jsonResponse[Vector[UiNodeAppearance]]),
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
      response = noContent(),
      docs = EndpointDocs()
        .withSummary(Some("Replace Node Appearances V2"))
        .withDescription(
          Some(
            "For a list of icon names, refer to [this page](https://ionicons.com/v2/cheatsheet.html)",
          ),
        )
        .withTags(List(v2QueryUiTag)),
    )

  final val queryUiQuickQueriesV2: Endpoint[Unit, Vector[V2UiNodeQuickQuery]] =
    endpoint(
      request = get(
        url = v2QuickQueries,
      ),
      response = ok(jsonResponse[Vector[V2UiNodeQuickQuery]]),
    )

  final val updateQueryUiQuickQueriesV2: Endpoint[Vector[V2UiNodeQuickQuery], Unit] =
    endpoint(
      request = put(
        url = v2QuickQueries,
        entity = jsonOrYamlRequestWithExample[Vector[V2UiNodeQuickQuery]](V2QuickQuery.defaults),
      ),
      response = noContent(),
      docs = EndpointDocs()
        .withSummary(Some("Replace Quick Queries V2"))
        .withDescription(Some("""Quick queries are queries that appear when right-clicking
            |a node in the UI.
            |Queries applied here will replace any currently existing quick queries.
            |""".stripMargin))
        .withTags(List(v2QueryUiTag)),
    )

}
