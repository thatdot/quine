package com.thatdot.quine.graph.cypher.quinepattern.procedures

import scala.concurrent.{ExecutionContext, Future}

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.language.ast.Value
import com.thatdot.quine.model.{EdgeDirection, HalfEdge}

/** QuinePattern implementation of getFilteredEdges procedure.
  *
  * Gets edges from a node, filtered by edge type, direction, and/or
  * allowed destination nodes. This procedure is useful for optimizing
  * queries that need to fetch edges connecting to a specific set of nodes.
  *
  * Arguments:
  *   1. node: NodeId or Text (the node to get edges from)
  *   2. edgeTypes: List[Text] (edge labels to filter, empty = all)
  *   3. directions: List[Text] ("outgoing", "incoming", "undirected", empty = all)
  *   4. allowedNodes: List[NodeId | Text] (destination nodes to filter, empty = all)
  *
  * Yields:
  *   edge: Relationship
  */
object GetFilteredEdgesProcedure extends QuinePatternProcedure {

  val name: String = "getFilteredEdges"

  val signature: ProcedureSignature = ProcedureSignature(
    outputs = Vector(ProcedureOutput("edge")),
    description = "Get edges from a node filtered by edge type, direction, and/or allowed destination nodes",
  )

  def execute(
    arguments: Seq[Value],
    context: ProcedureContext,
  )(implicit ec: ExecutionContext): Future[Seq[Map[String, Value]]] = {

    // Extract and validate arguments
    val (nodeId, edgeTypeFilter, directionFilter, allowedNodesFilter) = parseArguments(arguments, context)

    implicit val timeout: org.apache.pekko.util.Timeout = context.timeout

    // Get half-edges with filtering
    val halfEdgesFuture = context.graph
      .literalOps(context.namespace)
      .getHalfEdgesFiltered(
        nodeId,
        edgeTypes = edgeTypeFilter,
        directions = directionFilter,
        otherIds = allowedNodesFilter,
        atTime = context.atTime,
      )

    // Validate edges bidirectionally and convert to results
    halfEdgesFuture.flatMap { halfEdges =>
      validateAndConvert(nodeId, halfEdges, context)
    }
  }

  /** Parse procedure arguments into typed filters.
    *
    * @return (nodeId, edgeTypeFilter, directionFilter, allowedNodesFilter)
    */
  private def parseArguments(
    arguments: Seq[Value],
    context: ProcedureContext,
  ): (QuineId, Set[Symbol], Set[EdgeDirection], Set[QuineId]) = {

    if (arguments.length != 4) {
      throw new IllegalArgumentException(
        s"$name requires 4 arguments (node, edgeTypes, directions, allowedNodes), got ${arguments.length}",
      )
    }

    // Argument 1: node (NodeId or Text)
    val nodeId: QuineId = arguments(0) match {
      case Value.NodeId(qid) => qid
      case Value.Text(s) =>
        context.graph.idProvider
          .qidFromPrettyString(s)
          .getOrElse(
            throw new IllegalArgumentException(s"$name: Cannot parse node ID from string: $s"),
          )
      case other =>
        throw new IllegalArgumentException(s"$name: Invalid node argument type: ${other.getClass.getSimpleName}")
    }

    // Argument 2: edgeTypes (List[Text], empty = no filter)
    val edgeTypeFilter: Set[Symbol] = arguments(1) match {
      case Value.List(elements) =>
        elements.collect { case Value.Text(s) => Symbol(s) }.toSet
      case other =>
        throw new IllegalArgumentException(s"$name: edgeTypes must be a list, got: ${other.getClass.getSimpleName}")
    }

    // Argument 3: directions (List[Text], empty = no filter)
    val directionFilter: Set[EdgeDirection] = arguments(2) match {
      case Value.List(elements) =>
        elements.collect { case Value.Text(s) =>
          s.toLowerCase match {
            case "outgoing" => EdgeDirection.Outgoing
            case "incoming" => EdgeDirection.Incoming
            case "undirected" => EdgeDirection.Undirected
            case other =>
              throw new IllegalArgumentException(
                s"$name: Invalid direction '$other'. Must be 'outgoing', 'incoming', or 'undirected'",
              )
          }
        }.toSet
      case other =>
        throw new IllegalArgumentException(s"$name: directions must be a list, got: ${other.getClass.getSimpleName}")
    }

    // Argument 4: allowedNodes (List[NodeId | Text], empty = no filter)
    val allowedNodesFilter: Set[QuineId] = arguments(3) match {
      case Value.List(elements) =>
        elements.flatMap {
          case Value.NodeId(qid) => Some(qid)
          case Value.Text(s) =>
            context.graph.idProvider.qidFromPrettyString(s).toOption
          case _ => None // Skip invalid elements
        }.toSet
      case other =>
        throw new IllegalArgumentException(s"$name: allowedNodes must be a list, got: ${other.getClass.getSimpleName}")
    }

    (nodeId, edgeTypeFilter, directionFilter, allowedNodesFilter)
  }

  /** Validate edges bidirectionally and convert to result maps.
    *
    * For each half-edge, we check that its reflection exists on the other node.
    * Only edges that are valid on both sides are returned.
    */
  private def validateAndConvert(
    nodeId: QuineId,
    halfEdges: Set[HalfEdge],
    context: ProcedureContext,
  )(implicit ec: ExecutionContext): Future[Seq[Map[String, Value]]] = {

    implicit val timeout: org.apache.pekko.util.Timeout = context.timeout

    // Group half-edges by their other node for batch validation
    val edgesByOther: Map[QuineId, Set[HalfEdge]] = halfEdges.groupBy(_.other)

    // Validate each group
    val validatedEdgeSetsF = Future.traverse(edgesByOther.toSeq) { case (other, edges) =>
      // Reflect edges to check on the other node
      val reflectedEdges = edges.map(_.reflect(nodeId))

      context.graph
        .literalOps(context.namespace)
        .validateAndReturnMissingHalfEdges(other, reflectedEdges, context.atTime)
        .map { missingReflected =>
          // Keep edges whose reflections ARE on the other node
          val missingOriginal = missingReflected.map(_.reflect(other))
          edges.diff(missingOriginal)
        }
    }

    // Convert validated edges to result maps
    validatedEdgeSetsF.map { validatedEdgeSets =>
      validatedEdgeSets.flatten.map { halfEdge =>
        val relationship: Value.Relationship = halfEdge.direction match {
          case EdgeDirection.Outgoing =>
            Value.Relationship(nodeId, halfEdge.edgeType, scala.collection.immutable.Map.empty, halfEdge.other)
          case EdgeDirection.Incoming =>
            Value.Relationship(halfEdge.other, halfEdge.edgeType, scala.collection.immutable.Map.empty, nodeId)
          case EdgeDirection.Undirected =>
            // Note: Cypher doesn't have undirected edges, treating as outgoing
            Value.Relationship(nodeId, halfEdge.edgeType, scala.collection.immutable.Map.empty, halfEdge.other)
        }
        Map("edge" -> relationship)
      }
    }
  }
}
