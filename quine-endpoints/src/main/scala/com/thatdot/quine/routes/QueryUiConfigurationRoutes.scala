package com.thatdot.quine.routes

import endpoints4s.generic.{docs, title, unnamed}

import com.thatdot.quine.routes.exts.EndpointsWithCustomErrorText

@title("Sample Query")
@docs("A query that appears as an option in the dropdown under the query bar.")
final case class SampleQuery(
  @docs("A descriptive label for the query.") name: String,
  @docs("The Cypher or Gremlin query to be run on selection.") query: String
)
object SampleQuery {
  def recentNodes: SampleQuery = SampleQuery(
    name = "Get a few recent nodes",
    query = "CALL recentNodes(10)"
  )

  def getNodesById: SampleQuery = SampleQuery(
    name = "Get nodes by their ID(s)",
    query = "MATCH (n) WHERE id(n) = idFrom(0) RETURN n"
  )

  val defaults: Vector[SampleQuery] = Vector(recentNodes, getNodesById)
}

/** Abstract predicate for filtering nodes */
@title("UI Node Predicate")
@docs("Predicate by which nodes to apply this style to may be filtered")
@unnamed()
final case class UiNodePredicate(
  @docs("Properties the node must have to apply this style") propertyKeys: Vector[String],
  @docs("Properties with known constant values the node must have to apply this style") knownValues: Map[
    String,
    ujson.Value
  ],
  @docs("Label the node must have to apply this style") dbLabel: Option[String]
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
  @docs("(Optional) size of this icon in pixels")
  size: Option[Double],
  @docs(
    "(Optional) name of the icon character to use. For a list of icon names, refer to [this page](https://ionicons.com/v2/cheatsheet.html)"
  )
  icon: Option[String],
  @docs("(Optional) color to use, specified as a hex value")
  color: Option[String],
  @docs("(Optional) node label to use")
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

@unnamed
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
  @docs("Use the value of a property as a label, with an optional prefix")
  @unnamed()
  final case class Property(
    key: String,
    prefix: Option[String]
  ) extends UiNodeLabel
}

@title("Quick Query")
@unnamed
@docs("A query that can show up in the context menu brought up by right-clicking a node")
final case class UiNodeQuickQuery(
  @docs("Condition that a node must satisfy for this query to be in the context menu")
  @unnamed
  predicate: UiNodePredicate,
  @docs("Query to run when the context menu entry is selected")
  @unnamed
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
    with EndpointsWithCustomErrorText
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
        .withSummary(Some("List Sample Queries"))
        .withDescription(
          Some(
            """Queries provided here will be available via a drop-down menu from the Quine UI search bar.""".stripMargin
          )
        )
        .withTags(List(queryUiTag))
    )

  final val updateQueryUiSampleQueries: Endpoint[Vector[SampleQuery], Unit] =
    endpoint(
      request = put(
        url = sampleQueries,
        entity = jsonOrYamlRequestWithExample[Vector[SampleQuery]](SampleQuery.defaults)
      ),
      response = noContent(),
      docs = EndpointDocs()
        .withSummary(Some("Replace Sample Queries"))
        .withDescription(
          Some(
            """Queries provided here will be available via a drop-down menu from the Quine UI search bar.
              |
              |Queries applied here will replace any currently existing sample queries.""".stripMargin
          )
        )
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
        .withSummary(Some("List Node Appearances"))
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
        entity = jsonOrYamlRequestWithExample[Vector[UiNodeAppearance]](UiNodeAppearance.defaults)
      ),
      response = noContent(),
      docs = EndpointDocs()
        .withSummary(Some("Replace Node Appearances"))
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
        .withSummary(Some("List Quick Queries"))
        .withDescription(
          Some("""Quick queries are queries that appear when right-clicking
                 |a node in the UI.
                 |Nodes will only display quick queries that satisfy any
                 |provided predicates.""".stripMargin)
        )
        .withTags(List(queryUiTag))
    )

  final val updateQueryUiQuickQueries: Endpoint[Vector[UiNodeQuickQuery], Unit] =
    endpoint(
      request = put(
        url = quickQueries,
        entity = jsonOrYamlRequestWithExample[Vector[UiNodeQuickQuery]](UiNodeQuickQuery.defaults)
      ),
      response = noContent(),
      docs = EndpointDocs()
        .withSummary(Some("Replace Quick Queries"))
        .withDescription(Some("""Quick queries are queries that appear when right-clicking
            |a node in the UI.
            |Queries applied here will replace any currently existing quick queries.
            |""".stripMargin))
        .withTags(List(queryUiTag))
    )
}
