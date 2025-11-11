package com.thatdot.quine.compiler.cypher

import scala.concurrent.Future

import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.Timeout

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.LiteralOpsGraph
import com.thatdot.quine.graph.cypher._
import com.thatdot.quine.model.{EdgeDirection, HalfEdge}

/** Get edges from a node, filtered by edge type, direction, and/or allowed destination nodes.
  *
  * This procedure is particularly useful for optimizing queries that need to fetch edges
  * connecting to a specific set of nodes, as it filters at the HalfEdge level before
  * following edges, avoiding unnecessary traversals.
  *
  * @example {{{
  * // Get all outgoing KNOWS edges to specific nodes
  * CALL getFilteredEdges(n, ["KNOWS"], ["outgoing"], [node1, node2])
  *
  * // Get all edges (any type, any direction) to specific nodes
  * CALL getFilteredEdges(n, [], [], [node1, node2])
  *
  * // Get all FRIEND or COLLEAGUE edges in any direction
  * CALL getFilteredEdges(n, ["FRIEND", "COLLEAGUE"], [], [])
  * }}}
  */
object GetFilteredEdges extends UserDefinedProcedure {
  val name = "getFilteredEdges"
  val canContainUpdates = false
  val isIdempotent = true
  val canContainAllNodeScan = false

  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector(
      "node" -> Type.Anything, // A Node object or a String for the QuineID
      "edgeTypes" -> Type.List(Type.Str),
      "directions" -> Type.List(Type.Str), // A Single direction, or an empty list for no constraint
      "allowedNodes" -> Type.ListOfAnything, // A list of Node objects or a List of Strings that are QuineIDs
    ),
    outputs = Vector("edge" -> Type.Relationship),
    description = "Get edges from a node filtered by edge type, direction, and/or allowed destination nodes",
  )

  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation,
  )(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig,
  ): Source[Vector[Value], _] = {

    val graph = LiteralOpsGraph.getOrThrow(s"`$name` procedure", location.graph)

    // Extract and validate arguments
    val (nodeId, edgeTypeStrs, directionStrs, allowedNodeIds) = arguments match {
      case Seq(node, Expr.List(edgeTypes), Expr.List(directions), Expr.List(allowedNodes)) =>
        val qid = UserDefinedProcedure
          .extractQuineId(node)(location.idProvider)
          .getOrElse(throw wrongSignature(arguments))

        val edgeTypeStrings = edgeTypes.collect { case Expr.Str(s) => s }
        val directionStrings = directions.collect { case Expr.Str(s) => s }

        val allowedNodeIds = allowedNodes.map(n =>
          UserDefinedProcedure
            .extractQuineId(n)(location.idProvider)
            .getOrElse(throw wrongSignature(arguments)),
        )

        (qid, edgeTypeStrings, directionStrings, allowedNodeIds)

      case other => throw wrongSignature(other)
    }

    // Parse filters into Sets (empty Set = no filter)
    val edgeTypeFilter: Set[Symbol] = edgeTypeStrs.map(Symbol(_)).toSet

    val directionFilter: Set[EdgeDirection] = directionStrs
      .map(_.toLowerCase)
      .map {
        case "outgoing" => EdgeDirection.Outgoing
        case "incoming" => EdgeDirection.Incoming
        case "undirected" => EdgeDirection.Undirected
        case other =>
          throw CypherException.Runtime(
            s"`$name` procedure: Invalid direction '$other'. Must be 'outgoing', 'incoming', or 'undirected'",
          )
      }
      .toSet

    val allowedNodesFilter: Set[QuineId] = allowedNodeIds.toSet

    val halfEdgesFuture = graph
      .literalOps(location.namespace)
      .getHalfEdgesFiltered(
        nodeId,
        edgeTypes = edgeTypeFilter,
        directions = directionFilter,
        otherIds = allowedNodesFilter,
        atTime = location.atTime,
      )

    val resultFuture = halfEdgesFuture.flatMap { (halfEdges: Set[HalfEdge]) =>
      // Group half edges by their `other` node to batch validation queries
      val edgesByOther: Map[QuineId, Set[HalfEdge]] = halfEdges.groupBy(_.other)
      val validatedEdgeSetsF = Future.traverse(edgesByOther.toVector) { case (other, edges) =>
        // Reflect the edges to send to other for validating
        val reflectedEdges = edges.map(_.reflect(nodeId))
        graph
          .literalOps(location.namespace)
          .validateAndReturnMissingHalfEdges(other, reflectedEdges, location.atTime)
          // TODO: It would be slightly faster to do the validation call from the `other` node.
          .map { missingReflected =>
            // missingReflected contains reflected edges that are NOT on the other node
            // We want to keep edges whose reflections ARE on the other node
            val missingOriginal = missingReflected.map(_.reflect(other))
            edges.diff(missingOriginal)
          }(graph.nodeDispatcherEC)
      }(implicitly, graph.nodeDispatcherEC)

      validatedEdgeSetsF.map { validatedEdgeSets =>
        validatedEdgeSets.flatten
      }(graph.nodeDispatcherEC)
    }(graph.nodeDispatcherEC)

    Source.future(resultFuture).mapConcat(identity).map { halfEdge =>
      val relationship: Expr.Relationship = halfEdge.direction match {
        case EdgeDirection.Outgoing =>
          Expr.Relationship(nodeId, halfEdge.edgeType, Map.empty, halfEdge.other)
        case EdgeDirection.Incoming =>
          Expr.Relationship(halfEdge.other, halfEdge.edgeType, Map.empty, nodeId)
        case EdgeDirection.Undirected => // This is wrong, but Cypher doesn't have Undirected edges.
          Expr.Relationship(nodeId, halfEdge.edgeType, Map.empty, halfEdge.other)
      }
      Vector(relationship)
    }
  }
}
