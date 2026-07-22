package com.thatdot.quine.app.v2api.definitions

import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.{Decoder, Encoder, Json}
import sttp.tapir.Schema
import sttp.tapir.Schema.annotations.{description, title}

import com.thatdot.api.v2.TypeDiscriminatorConfig.instances.{circeConfig, tapirConfig}
import com.thatdot.api.v2.codec.ScreamingSnakeEnum

object ApiUiStyling {
  import com.thatdot.quine.app.util.StringOps.syntax._
  import com.thatdot.api.v2.schema.ThirdPartySchemas.circe._

  /** Enumeration for the kinds of queries we can issue */
  sealed abstract class QuerySort
  object QuerySort {
    case object Node extends QuerySort
    case object Text extends QuerySort

    val values: Seq[QuerySort] = Seq(Node, Text)

    implicit val encoder: Encoder[QuerySort] = ScreamingSnakeEnum.encoder
    implicit val decoder: Decoder[QuerySort] = ScreamingSnakeEnum.decoder(values)
    implicit lazy val schema: Schema[QuerySort] = ScreamingSnakeEnum.schema(values)
  }

  /** Queries like the ones that show up when right-clicking nodes
    *
    * TODO: use query parameters (challenge is how to render these nicely in the exploration UI)
    *
    * @param name human-readable title for the query
    * @param querySuffix query suffix
    * @param sort what should be done with query results?
    * @param edgeLabel virtual edge label (only relevant on node queries)
    */
  @title("Quick Query Action")
  @description("Query that gets executed starting at some node (e.g. by double-clicking or right-clicking).")
  final case class QuickQuery(
    @description("Name of the quick query. This is the name that will appear in the node drop-down menu.")
    name: String,
    @description(
      "Suffix of a traversal query (e.g. `.values('someKey')` for Gremlin or `RETURN n.someKey` for Cypher).",
    )
    querySuffix: String,
    @description("Whether the query returns node or text results.")
    sort: QuerySort,
    @description(
      """If this label is set and the query is configured to return nodes, each of the nodes returned
        |will have an additional dotted edge which connect to the source node of the quick query""".asOneLine,
    )
    edgeLabel: Option[String],
  ) {

    def fullQuery(startingIds: Seq[String]): String = {
      val simpleNumberId = startingIds.forall(_ matches "-?\\d+")
      val idOrStrIds = startingIds
        .map { (startingId: String) =>
          if (simpleNumberId) startingId else ujson.Str(startingId).toString
        }
        .mkString(", ")
      if (startingIds.length == 1) {
        s"MATCH (n) WHERE ${if (simpleNumberId) "id" else "strId"}(n) = $idOrStrIds $querySuffix"
      } else {
        s"UNWIND [$idOrStrIds] AS nId MATCH (n) WHERE ${if (simpleNumberId) "id" else "strId"}(n) = nId $querySuffix"
      }
    }
  }

  object QuickQuery {
    implicit val encoder: Encoder[QuickQuery] = deriveConfiguredEncoder
    implicit val decoder: Decoder[QuickQuery] = deriveConfiguredDecoder
    implicit lazy val schema: Schema[QuickQuery] = Schema.derived

    /** Open up adjacent nodes */
    def adjacentNodes: QuickQuery = {
      val querySuffix = "MATCH (n)--(m) RETURN DISTINCT m"

      QuickQuery(
        name = "Adjacent Nodes",
        querySuffix,
        sort = QuerySort.Node,
        edgeLabel = None,
      )
    }

    /** Refresh the current node */
    def refreshNode: QuickQuery = {
      val querySuffix =
        "RETURN n"

      QuickQuery(
        name = "Refresh",
        querySuffix,
        sort = QuerySort.Node,
        edgeLabel = None,
      )
    }
  }

  @title("Graph Node")
  @description("Information needed by the Query UI to display a node in the graph.")
  final case class UiNode[Id](
    @description("Node ID.") id: Id,
    @description("Index of the cluster host responsible for this node.") hostIndex: Int,
    @description("Categorical classification.") label: String,
    @description("Properties on the node.") properties: Map[String, Json],
  )

  @title("Graph Edge")
  @description("Information needed by the Query UI to display an edge in the graph.")
  final case class UiEdge[Id](
    @description("Node at the start of the edge.") from: Id,
    @description("Name of the edge.") edgeType: String,
    @description("Node at the end of the edge.") to: Id,
    @description("Whether the edge is directed or undirected.") isDirected: Boolean = true,
  )

  @title("Cypher Query Result")
  @description(
    """Cypher queries are designed to return data in a table format.
      |This gets encoded into JSON with `columns` as the header row and each element in `results` being another row
      |of results. Consequently, every array element in `results` will have the same length, and all will have the
      |same length as the `columns` array.""".asOneLine,
  )
  final case class CypherQueryResult(
    @description("Return values of the Cypher query.") columns: Seq[String],
    @description("Rows of results.") results: Seq[Seq[Json]],
  )

  @title("Cypher Query")
  final case class CypherQuery(
    @description("Text of the query to execute.") text: String,
    @description("Parameters the query expects, if any.") parameters: Map[String, Json] = Map.empty,
  )

  @title("Gremlin Query")
  final case class GremlinQuery(
    @description("Text of the query to execute.") text: String,
    @description("Parameters the query expects, if any.") parameters: Map[String, Json] = Map.empty,
  )

  @title("Sample Query")
  @description("A query that appears as an option in the dropdown under the query bar.")
  final case class SampleQuery(
    @description("A descriptive label for the query.") name: String,
    @description("The Cypher or Gremlin query to be run on selection.") query: String,
  )

  object SampleQuery {
    implicit val encoder: Encoder[SampleQuery] = deriveConfiguredEncoder
    implicit val decoder: Decoder[SampleQuery] = deriveConfiguredDecoder
    implicit lazy val schema: Schema[SampleQuery] = Schema.derived

    def recentNodes: SampleQuery = SampleQuery(
      name = "Get a few recent nodes",
      query = "CALL recentNodes(10)",
    )

    def getNodesById: SampleQuery = SampleQuery(
      name = "Get nodes by their ID(s)",
      query = "MATCH (n) WHERE id(n) = idFrom(0) RETURN n",
    )

    val defaults: Vector[SampleQuery] = Vector(recentNodes, getNodesById)
  }

  /** Abstract predicate for filtering nodes */
  @title("UI Node Predicate")
  @description("Predicate by which nodes to apply this style to may be filtered.")
  final case class UiNodePredicate(
    @description("Properties the node must have to apply this style.") propertyKeys: Vector[String],
    @description("Properties with known constant values the node must have to apply this style.") knownValues: Map[
      String,
      Json,
    ],
    @description("Label the node must have to apply this style.") dbLabel: Option[String],
  ) {
    def matches(node: UiNode[String]): Boolean = {
      def hasRightLabel = dbLabel.forall(_ == node.label)

      def hasRightKeys = propertyKeys.forall(node.properties.contains)

      def hasRightValues = knownValues.forall { case (k, v) =>
        node.properties.get(k).fold(false)(v == _)
      }

      hasRightLabel && hasRightKeys && hasRightValues
    }
  }

  object UiNodePredicate {
    implicit val encoder: Encoder[UiNodePredicate] = deriveConfiguredEncoder
    implicit val decoder: Decoder[UiNodePredicate] = deriveConfiguredDecoder
    implicit lazy val schema: Schema[UiNodePredicate] = Schema.derived

    val every: UiNodePredicate = UiNodePredicate(Vector.empty, Map.empty, None)
  }

  @title("UI Node Appearance")
  @description("Instructions for how to style the appearance of a node.")
  final case class UiNodeAppearance(
    predicate: UiNodePredicate,
    @description("Size of this icon in pixels.")
    size: Option[Double],
    @description(
      "Name of the icon character to use. For a list of icon names, refer to [this page](https://ionicons.com/v2/cheatsheet.html).",
    )
    icon: Option[String],
    @description("The color to use, specified as a hex value.")
    color: Option[String],
    @description("The node label to use.")
    label: Option[UiNodeLabel],
  )

  object UiNodeAppearance {
    implicit val encoder: Encoder[UiNodeAppearance] = deriveConfiguredEncoder
    implicit val decoder: Decoder[UiNodeAppearance] = deriveConfiguredDecoder
    implicit lazy val schema: Schema[UiNodeAppearance] = Schema.derived

    def apply(
      predicate: UiNodePredicate,
      size: Option[Double] = None,
      icon: Option[String] = None,
      color: Option[String] = None,
      label: Option[UiNodeLabel] = None,
    ) = new UiNodeAppearance(predicate, size, icon, color, label)

    val named: UiNodeAppearance = UiNodeAppearance(
      predicate = UiNodePredicate(Vector.empty, Map.empty, None),
      label = Some(UiNodeLabel.Property("name", None)),
      icon = Some("\uf47e"),
    )
    val defaults: Vector[UiNodeAppearance] = Vector(named)
  }

  @title("UI Node Label")
  @description("Instructions for how to label a node in the UI.")
  sealed abstract class UiNodeLabel

  object UiNodeLabel {
    implicit val encoder: Encoder[UiNodeLabel] = deriveConfiguredEncoder
    implicit val decoder: Decoder[UiNodeLabel] = deriveConfiguredDecoder
    implicit lazy val schema: Schema[UiNodeLabel] = Schema.derived

    @title("Fixed Label")
    @description("Use a specified, fixed value as a label.")
    final case class Constant(
      value: String,
    ) extends UiNodeLabel

    @title("Property Value Label")
    @description("Use the value of a property as a label, with an optional prefix.")
    final case class Property(
      key: String,
      prefix: Option[String],
    ) extends UiNodeLabel
  }

  @title("Quick Query")
  @description("A query that can show up in the context menu brought up by right-clicking a node.")
  final case class UiNodeQuickQuery(
    @description("Condition that a node must satisfy for this query to be in the context menu.")
    predicate: UiNodePredicate,
    @description("Query to run when the context menu entry is selected.")
    quickQuery: QuickQuery,
  )

  object UiNodeQuickQuery {
    implicit val encoder: Encoder[UiNodeQuickQuery] = deriveConfiguredEncoder
    implicit val decoder: Decoder[UiNodeQuickQuery] = deriveConfiguredDecoder
    implicit lazy val schema: Schema[UiNodeQuickQuery] = Schema.derived

    def every(query: QuickQuery): UiNodeQuickQuery = UiNodeQuickQuery(UiNodePredicate.every, query)

    val defaults: Vector[UiNodeQuickQuery] = Vector(
      UiNodeQuickQuery.every(QuickQuery.adjacentNodes),
      UiNodeQuickQuery.every(QuickQuery.refreshNode),
    )
  }

  /** Direction of a synthetic edge added to query results.
    *
    *  - `Out`        : edge points from the `fromNode` to the `toNode`.
    *  - `In`         : edge points from the `toNode` to the `fromNode`.
    *  - `Undirected` : edge is drawn without an arrow.
    */
  sealed trait SyntheticEdgeDirection
  object SyntheticEdgeDirection {
    case object Out extends SyntheticEdgeDirection
    case object In extends SyntheticEdgeDirection
    case object Undirected extends SyntheticEdgeDirection

    implicit val encoder: Encoder[SyntheticEdgeDirection] = Encoder.encodeString.contramap {
      case Out => "OUT"
      case In => "IN"
      case Undirected => "UNDIRECTED"
    }
    implicit val decoder: Decoder[SyntheticEdgeDirection] = Decoder.decodeString.emap {
      case "OUT" => Right(Out)
      case "IN" => Right(In)
      case "UNDIRECTED" => Right(Undirected)
      case other => Left(s"Unknown SyntheticEdgeDirection: $other (expected OUT, IN, or UNDIRECTED)")
    }
    implicit lazy val schema: Schema[SyntheticEdgeDirection] = Schema.string[SyntheticEdgeDirection]
  }

  /** Where the UI should look up `fromNode` / `toNode` to find the synthetic edge's
    * endpoint node IDs.
    *
    *  - `WiretapMessage`: read dot-paths into the wiretap message JSON envelope, e.g.
    *    `data.even1`. Currently the only supported source.
    *
    * Sealed for future expansion (e.g. `QueryResult` to read columns from the tap
    * query's own result, or `AuxiliaryQuery` for a separately-configured lookup query).
    */
  sealed trait NodeIdsSource
  object NodeIdsSource {
    case object WiretapMessage extends NodeIdsSource

    implicit val encoder: Encoder[NodeIdsSource] = Encoder.encodeString.contramap { case WiretapMessage =>
      "WIRETAP_MESSAGE"
    }
    implicit val decoder: Decoder[NodeIdsSource] = Decoder.decodeString.emap {
      case "WIRETAP_MESSAGE" => Right(WiretapMessage)
      case other => Left(s"Unknown NodeIdsSource: $other (expected WIRETAP_MESSAGE)")
    }
    implicit lazy val schema: Schema[NodeIdsSource] = Schema.string[NodeIdsSource]
  }

  /** A virtual edge to draw between two nodes referenced by a tap query's data source.
    *
    * For each fire of the tap query, the UI extracts node IDs at the `fromNode` and `toNode`
    * paths/columns of the source named by `nodeIdsFrom`, and adds a visual-only edge between
    * them with the configured label and direction. The edge is not persisted in the graph.
    *
    * @param fromNode     Path/column where the edge's first endpoint node ID is found.
    * @param toNode       Path/column where the edge's second endpoint node ID is found.
    * @param label        Label shown on the synthetic edge.
    * @param direction    Edge direction (Out / In / Undirected).
    * @param nodeIdsFrom  Which data source to read endpoint IDs from (defaults to `WIRETAP_MESSAGE`).
    */
  @title("Synthetic Edge")
  @description("A visual edge added between two node IDs read from a tap query's data source.")
  final case class SyntheticEdge(
    @description("Path/column where the edge's first endpoint node ID is found.") fromNode: String,
    @description("Path/column where the edge's second endpoint node ID is found.") toNode: String,
    @description("Label shown on the synthetic edge.") label: String,
    @description("Edge direction.") direction: SyntheticEdgeDirection,
    @description("Data source to read endpoint IDs from.")
    nodeIdsFrom: NodeIdsSource = NodeIdsSource.WiretapMessage,
  )

  object SyntheticEdge {
    implicit val encoder: Encoder[SyntheticEdge] = deriveConfiguredEncoder
    implicit val decoder: Decoder[SyntheticEdge] = deriveConfiguredDecoder
    implicit lazy val schema: Schema[SyntheticEdge] = Schema.derived
  }

  /** A saved tap query — a wiretap paired with a Cypher query.
    *
    * Each tap query identifies a wiretap (a standing query, optionally a specific output)
    * and a Cypher query that the UI runs against each incoming message — the result is
    * displayed in the explorer just as if the user had typed it manually.
    *
    * If `syntheticEdges` are configured, the UI reads the endpoint node IDs from the wiretap
    * message JSON (the default `nodeIdsFrom = WIRETAP_MESSAGE`). `fromNode` / `toNode`
    * are dot-paths into that message (e.g. `"data.even1"`). For each fire of the tap query,
    * a visual-only edge is drawn between the resolved nodes.
    *
    * @param name              Display name shown in Explorer Settings.
    * @param description       Optional longer-form description of what this tap query does.
    * @param standingQueryName Name of the standing query to tap.
    * @param outputName        If set, tap that output's stream; otherwise tap raw results.
    * @param preEnrichment     When `outputName` is set, whether to tap the output's pre-enrichment
    *                          stream (before its enrichment query runs) instead of the
    *                          post-enrichment stream. Ignored when `outputName` is unset.
    * @param query             Cypher query run on each incoming message. SQ result fields are available
    *                          as Cypher variables (e.g. backtick-quoted column names from the SQ).
    * @param syntheticEdges    Optional virtual edges drawn between paired node-returning columns.
    */
  @title("TapQuery")
  @description("A saved wiretap + Cypher query pair, listed in Explorer Settings.")
  final case class TapQuery(
    @description("Display name for this tap query.") name: String,
    @description("Optional longer description of what this tap query does.") description: Option[String],
    @description("Name of the standing query to tap.") standingQueryName: String,
    @description("If set, tap this output's stream; otherwise tap raw results.")
    outputName: Option[String],
    @description(
      "When outputName is set, tap the output's pre-enrichment stream instead of post-enrichment. " +
      "Ignored when outputName is unset.",
    )
    preEnrichment: Boolean = false,
    @description("Cypher query run against each incoming wiretap message.") query: String,
    @description("Optional virtual edges drawn between paired node-returning result columns.")
    syntheticEdges: Vector[SyntheticEdge] = Vector.empty,
  )

  object TapQuery {
    implicit val encoder: Encoder[TapQuery] = deriveConfiguredEncoder
    implicit val decoder: Decoder[TapQuery] = deriveConfiguredDecoder
    implicit lazy val schema: Schema[TapQuery] = Schema.derived

    val defaults: Vector[TapQuery] = Vector.empty
  }
}
