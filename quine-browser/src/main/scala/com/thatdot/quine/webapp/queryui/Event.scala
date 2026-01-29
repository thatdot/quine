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

  /** Set some layout positions */
  final case class Layout(positions: Map[String, NodePosition]) extends QueryUiEvent {
    def invert: Layout = this
  }

  /** Checkpoint */
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

object GraphJsonLdSchema {
  import io.circe.syntax._

  def encodeAsJsonLd(nodes: Seq[UiNode[String]], edges: Seq[UiEdge[String]]): String = {
    val nodesJson = nodes.map { node =>
      Json.obj(
        "id" -> node.id.asJson,
        "label" -> node.label.asJson,
        "properties" -> node.properties.asJson,
      )
    }

    val edgesJson = edges.map { edge =>
      Json.obj(
        "from" -> edge.from.asJson,
        "to" -> edge.to.asJson,
        "edgeType" -> edge.edgeType.asJson,
        "isDirected" -> edge.isDirected.asJson,
      )
    }

    Json
      .obj(
        "nodes" -> nodesJson.asJson,
        "edges" -> edgesJson.asJson,
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
