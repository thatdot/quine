package com.thatdot.quine.webapp.queryui

import endpoints4s.Validated
import ujson.Value

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
    explodeFromId: Option[String]
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
    explodeFromId: Option[String]
  ) extends QueryUiEvent {
    def invert: Add = Add(nodes, edges, updatedNodes, syntheticEdges, explodeFromId)
  }

  /** Collapse some nodes into a cluster */
  final case class Collapse(
    nodeIds: Seq[String],
    clusterId: String,
    name: String
  ) extends QueryUiEvent {
    def invert: Expand = Expand(nodeIds, clusterId, name)
  }

  /** Expand some nodes out of a cluster */
  final case class Expand(
    nodeIds: Seq[String],
    clusterId: String,
    name: String
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
object HistoryJsonSchema extends endpoints4s.generic.JsonSchemas with exts.UjsonAnySchema {
  implicit val anyJson: JsonSchema[Value] = anySchema(None)
  implicit val uiNodeSchema: Record[UiNode[String]] = genericRecord[UiNode[String]]
  implicit val uiEdgeSchema: Record[UiEdge[String]] = genericRecord[UiEdge[String]]
  implicit val nodePositionsSchema: Record[QueryUiEvent.NodePosition] =
    genericRecord[QueryUiEvent.NodePosition]
  implicit val queryUiEventSchema: Tagged[QueryUiEvent] = genericTagged[QueryUiEvent]
  implicit val historySchema: Record[History[QueryUiEvent]] =
    genericRecord[History[QueryUiEvent]]

  def encode(history: History[QueryUiEvent]): String =
    ujson.write(historySchema.encoder.encode(history))

  def decode(jsonStr: String): Validated[History[QueryUiEvent]] =
    historySchema.decoder.decode(ujson.read(jsonStr))
}
