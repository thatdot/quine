package com.thatdot.quine.routes

import endpoints4s.generic.{docs, title, unnamed}

@title("Sample Query")
@docs("A query that appears as an option in the dropdown under the query bar.")
final case class SampleQuery(
  @docs("text description of the query") name: String,
  @docs("Cypher or Gremlin query") query: String
)
object SampleQuery {
  def recentNodes: SampleQuery = SampleQuery(
    name = "Get a few recent nodes",
    query = "CALL recentNodes(10)"
  )

  def getNodesById: SampleQuery = SampleQuery(
    name = "Get nodes by their ID(s)",
    query = "MATCH (n) WHERE id(n) = 0 RETURN n"
  )

  val defaults: Vector[SampleQuery] = Vector(recentNodes, getNodesById)
}

/** Abstract predicate for filtering nodes */
@title("UI Node Predicate")
@docs("Predicate by which nodes may be filtered")
@unnamed()
final case class UiNodePredicate(
  @docs("properties the node must have") propertyKeys: Vector[String],
  @docs("properties with known constant values the node must have") knownValues: Map[
    String,
    ujson.Value
  ],
  @docs("label the node must have") dbLabel: Option[String]
) {
  def matches(node: UiNode[String]): Boolean = {
    def hasRightLabel = dbLabel.forall(_ == node.label)
    def hasRightKeys = propertyKeys.forall(node.properties.contains(_))
    def hasRightValues = knownValues.forall { case (k, v) =>
      node.properties.get(k).fold(false)(v == _)
    }
    hasRightLabel && hasRightKeys && hasRightValues
  }
}
object UiNodePredicate {
  val every: UiNodePredicate = UiNodePredicate(Vector.empty, Map.empty, None)
}

@title("UI Node Appearance")
@docs("Instructions for how to style the appearance of a node.")
final case class UiNodeAppearance(
  predicate: UiNodePredicate,
  size: Option[Double],
  icon: Option[String],
  color: Option[String],
  label: Option[UiNodeLabel]
)

object UiNodeAppearance {

  def apply(
    predicate: UiNodePredicate,
    size: Option[Double] = None,
    icon: Option[String] = None,
    color: Option[String] = None,
    label: Option[UiNodeLabel] = None
  ) = new UiNodeAppearance(predicate, size, icon, color, label)

  val person: UiNodeAppearance = UiNodeAppearance(
    predicate = UiNodePredicate(Vector.empty, Map.empty, Some("Person")),
    label = Some(UiNodeLabel.Property("name", None)),
    icon = Some("\uf47e")
  )
  val file: UiNodeAppearance = UiNodeAppearance(
    predicate = UiNodePredicate(Vector.empty, Map.empty, Some("File")),
    label = Some(UiNodeLabel.Property("path", Some("File path: "))),
    icon = Some("\uf381")
  )
  val defaults: Vector[UiNodeAppearance] = Vector(person, file)
}

@title("UI Node Label")
@docs("Instructions for how to label a node in the UI.")
sealed abstract class UiNodeLabel
object UiNodeLabel {

  @title("Fixed Label")
  @docs("Use a specified, fixed value as a label.")
  @unnamed()
  final case class Constant(
    value: String
  ) extends UiNodeLabel

  @title("Property Value Label")
  @docs("Use the value of a property as a label, with an optional prefix.")
  @unnamed()
  final case class Property(
    key: String,
    prefix: Option[String]
  ) extends UiNodeLabel
}

@title("Quick Query")
@docs("A query that can show up in the context menu brought up by right-clicking a node.")
final case class UiNodeQuickQuery(
  @docs("condition that a node must satisfy for this query to be in the context menu")
  predicate: UiNodePredicate,
  @docs("query to run when the context menu entry gets clicked")
  quickQuery: QuickQuery
)
object UiNodeQuickQuery {
  def every(query: QuickQuery): UiNodeQuickQuery = UiNodeQuickQuery(UiNodePredicate.every, query)

  val defaults: Vector[UiNodeQuickQuery] = Vector(
    UiNodeQuickQuery.every(QuickQuery.adjacentNodes(QueryLanguage.Cypher)),
    UiNodeQuickQuery.every(QuickQuery.refreshNode(QueryLanguage.Cypher)),
    UiNodeQuickQuery.every(QuickQuery.getProperties(QueryLanguage.Cypher))
  )
}

trait QueryUiConfigurationSchemas extends endpoints4s.generic.JsonSchemas with exts.AnySchema {

  implicit final lazy val querySortSchema: JsonSchema[QuerySort] =
    stringEnumeration[QuerySort](Seq(QuerySort.Text, QuerySort.Node))(_.toString)
  implicit final lazy val queryLanguageSchema: JsonSchema[QueryLanguage] =
    stringEnumeration[QueryLanguage](Seq(QueryLanguage.Gremlin, QueryLanguage.Cypher))(_.toString)
  implicit final lazy val quickQuerySchema: JsonSchema[QuickQuery] =
    genericJsonSchema[QuickQuery].withExample(QuickQuery.adjacentNodes(QueryLanguage.Cypher))
  implicit final lazy val sampleQuerySchema: JsonSchema[SampleQuery] =
    genericJsonSchema[SampleQuery]

  implicit final lazy val uiNodePredicateSchema: JsonSchema[UiNodePredicate] = {
    implicit lazy val uiNodePredicateValueSchema: JsonSchema[ujson.Value] = anySchema(None)
    genericJsonSchema[UiNodePredicate]
  }
  implicit final lazy val uiNodeLabelSchema: JsonSchema[UiNodeLabel] =
    genericJsonSchema[UiNodeLabel]
  implicit final lazy val uiNodeAppearanceSchema: JsonSchema[UiNodeAppearance] =
    genericJsonSchema[UiNodeAppearance]
  implicit final lazy val uiNodeQuickQuerySchema: JsonSchema[UiNodeQuickQuery] =
    genericJsonSchema[UiNodeQuickQuery]
}

trait QueryUiConfigurationRoutes
    extends QueryUiConfigurationSchemas
    with endpoints4s.algebra.Endpoints
    with endpoints4s.algebra.JsonEntitiesFromSchemas
    with exts.QuineEndpoints {

  private val api = path / "api" / "v1"
  private val queryui = api / "query-ui"
  private val sampleQueries = queryui / "sample-queries"
  private val nodeAppearances = queryui / "node-appearances"
  private val quickQueries = queryui / "quick-queries"

  private[this] val queryUiTag = endpoints4s.algebra
    .Tag("UI Styling")
    .withDescription(
      Some(
        """Operations for customizing parts of the Query UI. These options are generally useful
          |for tailoring the UI to a particular domain or data model (eg. to customize the
          |icon, color, size, context-menu queries, etc. for nodes based on their contents).
          |""".stripMargin
      )
    )

  final val queryUiSampleQueries: Endpoint[Unit, Vector[SampleQuery]] =
    endpoint(
      request = get(
        url = sampleQueries
      ),
      response = ok(jsonResponseWithExample[Vector[SampleQuery]](SampleQuery.defaults)),
      docs = EndpointDocs()
        .withSummary(Some("starting queries suggested in the query bar"))
        .withDescription(
          Some(
            "These are the queries that are suggested (via `datalist`) when " +
            "clicking on the query bar in the UI."
          )
        )
        .withTags(List(queryUiTag))
    )

  final val updateQueryUiSampleQueries: Endpoint[Vector[SampleQuery], Unit] =
    endpoint(
      request = put(
        url = sampleQueries,
        entity = jsonRequestWithExample[Vector[SampleQuery]](SampleQuery.defaults)
      ),
      response = noContent(),
      docs = EndpointDocs()
        .withSummary(Some("update collection of starting queries suggested in the query bar"))
        .withTags(List(queryUiTag))
    )

  final val queryUiAppearance: Endpoint[Unit, Vector[UiNodeAppearance]] =
    endpoint(
      request = get(
        url = nodeAppearances
      ),
      response = ok(
        jsonResponseWithExample[Vector[UiNodeAppearance]](UiNodeAppearance.defaults)
      ),
      docs = EndpointDocs()
        .withSummary(Some("ranked list of ways of styling nodes"))
        .withDescription(
          Some(
            "When rendering a node in the UI, a node's style is decided by " +
            "picking the first style in this list whose `predicate` matches " +
            "the node."
          )
        )
        .withTags(List(queryUiTag))
    )

  final val updateQueryUiAppearance: Endpoint[Vector[UiNodeAppearance], Unit] =
    endpoint(
      request = put(
        url = nodeAppearances,
        entity = jsonRequestWithExample[Vector[UiNodeAppearance]](UiNodeAppearance.defaults)
      ),
      response = noContent(),
      docs = EndpointDocs()
        .withSummary(Some("update list of ways of styling nodes"))
        .withDescription(
          Some(
            "For a list of icon names, refer to [this page](https://ionicons.com/v2/cheatsheet.html)"
          )
        )
        .withTags(List(queryUiTag))
    )

  final val queryUiQuickQueries: Endpoint[Unit, Vector[UiNodeQuickQuery]] =
    endpoint(
      request = get(
        url = quickQueries
      ),
      response = ok(jsonResponseWithExample[Vector[UiNodeQuickQuery]](UiNodeQuickQuery.defaults)),
      docs = EndpointDocs()
        .withSummary(Some("ranked list of possible quick queries"))
        .withDescription(
          Some(
            "When right-clicking on a node in the UI, the list of quick " +
            "queries to show in the context menu is populated by filtering " +
            "this full list and keeping only queries whose `predicate` field " +
            "matches the node that was right-clicked."
          )
        )
        .withTags(List(queryUiTag))
    )

  final val updateQueryUiQuickQueries: Endpoint[Vector[UiNodeQuickQuery], Unit] =
    endpoint(
      request = put(
        url = quickQueries,
        entity = jsonRequestWithExample[Vector[UiNodeQuickQuery]](UiNodeQuickQuery.defaults)
      ),
      response = noContent(),
      docs = EndpointDocs()
        .withSummary(Some("update list of possible quick queries"))
        .withTags(List(queryUiTag))
    )
}
