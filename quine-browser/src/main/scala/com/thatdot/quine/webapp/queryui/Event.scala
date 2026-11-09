package com.thatdot.quine.webapp.queryui

import cats.data.ValidatedNel
import io.circe.parser.decodeAccumulating
import io.circe.{Error, Json}

import com.thatdot.quine.routes.{UiEdge, UiNode, exts}
import com.thatdot.quine.webapp.History

/** The events that can occur in the Query UI */
sealed abstract class QueryUiEvent {

  /** event that should negate the effects of the current event */
  def invert: QueryUiEvent
}
object QueryUiEvent {

  type Node = UiNode[String]
  type Edge = UiEdge[String]
  final case class NodePosition(x: Double, y: Double, fixed: Boolean)

  /** Add nodes and edges to the graph
    *
    * @param nodes new nodes to add to the graph
    * @param edges new edges to add to the graph
    * @param updateNodes nodes already on the graph, but which could be updated
    * @param syntheticEdges purple edges
    * @param explodeFromId new nodes should "pop out" of the node with this ID
    */
  final case class Add(
    nodes: Seq[Node],
    edges: Seq[Edge],
    updatedNodes: Seq[Node],
    syntheticEdges: Seq[Edge],
    explodeFromId: Option[String],
  ) extends QueryUiEvent {
    def invert: Remove = Remove(nodes, edges, updatedNodes, syntheticEdges, explodeFromId)

    def nonEmpty: Boolean = nodes.nonEmpty || edges.nonEmpty || updatedNodes.nonEmpty || syntheticEdges.nonEmpty
  }

  /** Remove nodes and edges from the graph */
  final case class Remove(
    nodes: Seq[Node],
    edges: Seq[Edge],
    updatedNodes: Seq[Node],
    syntheticEdges: Seq[Edge],
    explodeFromId: Option[String],
  ) extends QueryUiEvent {
    def invert: Add = Add(nodes, edges, updatedNodes, syntheticEdges, explodeFromId)
  }

  /** Collapse some nodes into a cluster */
  final case class Collapse(
    nodeIds: Seq[String],
    clusterId: String,
    name: String,
  ) extends QueryUiEvent {
    def invert: Expand = Expand(nodeIds, clusterId, name)
  }

  /** Expand some nodes out of a cluster */
  final case class Expand(
    nodeIds: Seq[String],
    clusterId: String,
    name: String,
  ) extends QueryUiEvent {
    def invert: Collapse = Collapse(nodeIds, clusterId, name)
  }

  /** Set some layout positions. Recorded alongside a [[Checkpoint]] marker at
    * creation (so checkpoint navigation can restore node positions) and
    * prepended transiently to downloaded history files; never observed on its
    * own into live history.
    */
  final case class Layout(positions: Map[String, NodePosition]) extends QueryUiEvent {
    def invert: Layout = this
  }

  /** Position marker for a named checkpoint. Applying it does nothing; plain
    * undo/redo slides over markers (and the [[Layout]] recorded under them)
    * without costing a click, while the Previous/Next Checkpoint menu actions
    * walk the stream to them.
    */
  final case class Checkpoint(name: String) extends QueryUiEvent {
    def invert: Checkpoint = this
  }
}

/** Serialization format for history */
object HistoryJsonSchema extends endpoints4s.generic.JsonSchemas with exts.CirceJsonAnySchema {
  implicit val anyJson: JsonSchema[Json] = anySchema(None)
  implicit val uiNodeSchema: Record[UiNode[String]] = genericRecord[UiNode[String]]
  implicit val uiEdgeSchema: Record[UiEdge[String]] = genericRecord[UiEdge[String]]
  implicit val nodePositionsSchema: Record[QueryUiEvent.NodePosition] =
    genericRecord[QueryUiEvent.NodePosition]
  implicit val queryUiEventSchema: Tagged[QueryUiEvent] = genericTagged[QueryUiEvent]
  implicit val historySchema: Record[History[QueryUiEvent]] =
    genericRecord[History[QueryUiEvent]]

  def encode(history: History[QueryUiEvent]): String =
    historySchema.encoder(history).noSpaces

  def decode(jsonStr: String): ValidatedNel[Error, History[QueryUiEvent]] =
    decodeAccumulating(jsonStr)(historySchema.decoder)
}

/** This is what we actually store in the `vis` mutable node set. We have
  * to cast nodes coming out of the network into this before being able to use
  * these fields
  *
  * @param uiNode original node data
  */
trait QueryUiVisNodeExt extends com.thatdot.visnetwork.Node {
  val uiNode: UiNode[String]
}

/** This is what we actually store in the `vis` mutable edge set. We have
  * to cast edges coming out of the network into this before being able to use
  * these fields
  *
  * @param uiEdge original edge data
  */
trait QueryUiVisEdgeExt extends com.thatdot.visnetwork.Edge {
  val uiEdge: UiEdge[String]
  val isSyntheticEdge: Boolean
}

/** Encodes the displayed graph as a JSON-LD 1.1 document.
  *
  * Each node becomes a node object in `@graph`:
  *  - its ID becomes the IRI `urn:quine:node:<percent-encoded id>`
  *  - its display label becomes its `rdfs:label`, a human-readable name rather than a class,
  *    since the display label is free-form text (joined node labels, an `ID: …` placeholder,
  *    or a product-specific caption) that can't be reliably split back into node labels
  *  - each property and each outgoing edge becomes a key, expanded against [[Vocab]]
  *
  * Property keys and edge types are percent-encoded, so names containing `:`, `@`,
  * spaces, etc. still expand to a single vocabulary IRI rather than being read as keywords
  * or absolute IRIs. A property and an edge type with the same name share one predicate.
  *
  * Undirected edges are emitted in both directions, since RDF has no undirected edges.
  * Property values that are scalars become plain literals, arrays of scalars become ordered
  * `@list`s, and anything else (objects, nested arrays, `null`) becomes a `@json` literal, so
  * nothing is dropped or reinterpreted as graph structure on expansion.
  */
object GraphJsonLdSchema {
  import scala.scalajs.js.URIUtils.encodeURIComponent

  val Vocab = "https://quine.io/ns#"
  val NodeIriPrefix = "urn:quine:node:"
  val RdfsNamespace = "http://www.w3.org/2000/01/rdf-schema#"

  def nodeIri(id: String): String = NodeIriPrefix + encodeURIComponent(id)

  private def term(name: String): String = encodeURIComponent(name)

  private def isPlainScalar(json: Json): Boolean = json.isString || json.isNumber || json.isBoolean

  private def encodeValue(json: Json): Json =
    if (isPlainScalar(json)) json
    else
      json.asArray match {
        case Some(elems) if elems.forall(isPlainScalar) => Json.obj("@list" -> Json.fromValues(elems))
        case _ => Json.obj("@value" -> json, "@type" -> Json.fromString("@json"))
      }

  def encodeAsJsonLd(nodes: Seq[UiNode[String]], edges: Seq[UiEdge[String]]): String = {
    val outgoing: Map[String, Seq[(String, String)]] = edges
      .flatMap { edge =>
        val forward = edge.from -> (edge.edgeType -> edge.to)
        if (edge.isDirected) Seq(forward) else Seq(forward, edge.to -> (edge.edgeType -> edge.from))
      }
      .distinct
      .groupMap(_._1)(_._2)

    def nodeObject(id: String, node: Option[UiNode[String]]): Json = {
      val propertyEntries = node.toSeq.flatMap(_.properties.toSeq.map { case (k, v) => term(k) -> encodeValue(v) })
      val edgeEntries = outgoing.getOrElse(id, Seq.empty).map { case (edgeType, to) =>
        term(edgeType) -> Json.obj("@id" -> Json.fromString(nodeIri(to)))
      }
      val predicates = (propertyEntries ++ edgeEntries).groupMap(_._1)(_._2).toSeq.sortBy(_._1).map {
        case (key, Seq(single)) => key -> single
        case (key, values) => key -> Json.fromValues(values)
      }
      // Percent-encoded user keys can never contain the `:` of this compact IRI, so it can't collide
      val labelEntry = node.map(_.label).filter(_.nonEmpty).map(label => "rdfs:label" -> Json.fromString(label))
      Json.fromFields(Seq("@id" -> Json.fromString(nodeIri(id))) ++ labelEntry ++ predicates)
    }

    val nodesById = nodes.map(n => n.id -> n).toMap
    // Edge sources missing from the node set still need a subject to hang their edges on
    val edgeOnlySources = outgoing.keys.filterNot(nodesById.contains).toSeq.sorted
    val graph = nodes.map(n => nodeObject(n.id, Some(n))) ++ edgeOnlySources.map(nodeObject(_, None))

    Json
      .obj(
        "@context" -> Json.obj(
          "@version" -> Json.fromDoubleOrNull(1.1),
          "@vocab" -> Json.fromString(Vocab),
          "rdfs" -> Json.fromString(RdfsNamespace),
        ),
        "@graph" -> Json.fromValues(graph),
      )
      .spaces2
  }
}

object DownloadUtils {
  import scala.scalajs.js
  import org.scalajs.dom
  import org.scalajs.dom.document
  import com.thatdot.{visnetwork => vis}

  def downloadFile(content: String, fileName: String, mimeType: String): Unit = {
    val blob = new dom.Blob(
      js.Array(content),
      new dom.BlobPropertyBag { `type` = mimeType },
    )

    val a = document.createElement("a").asInstanceOf[dom.HTMLAnchorElement]
    a.setAttribute("download", fileName)
    a.setAttribute("href", dom.URL.createObjectURL(blob))
    a.setAttribute("target", "_blank")
    a.click()
  }

  /** Download graph data as JSON-LD from vis DataSets
    *
    * @param nodeSet the vis DataSet containing nodes (must contain QueryUiVisNodeExt instances)
    * @param edgeSet the vis DataSet containing edges (must contain QueryUiVisEdgeExt instances)
    */
  def downloadGraphJsonLd(
    nodeSet: vis.DataSet[vis.Node],
    edgeSet: vis.DataSet[vis.Edge],
  ): Unit = {
    val nodes: Seq[UiNode[String]] = nodeSet
      .get()
      .toSeq
      .map(_.asInstanceOf[QueryUiVisNodeExt].uiNode)

    val edges: Seq[UiEdge[String]] = edgeSet
      .get()
      .toSeq
      .map(_.asInstanceOf[QueryUiVisEdgeExt].uiEdge)

    downloadFile(
      GraphJsonLdSchema.encodeAsJsonLd(nodes, edges),
      "graph.jsonld",
      "application/ld+json",
    )
  }
}
