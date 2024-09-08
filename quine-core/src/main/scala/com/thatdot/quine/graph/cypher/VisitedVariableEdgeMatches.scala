package com.thatdot.quine.graph.cypher

import com.thatdot.quine.model.{EdgeDirection, HalfEdge, QuineId}

/** Tracks which edges have been visited so far in a variable length edge
  * traversal, along with the index of that match
  *
  * @param visited first part of key is lexicographically smaller endpoint, value is index
  */
final case class VisitedVariableEdgeMatches private (
  visited: Map[(QuineId, HalfEdge), Int],
) {

  /** Number of edges in the set */
  def size: Int = visited.size

  /** Check if there are no edges in the set */
  def isEmpty: Boolean = visited.isEmpty

  /** Add an edge to the set
    *
    * @param endpoint one endpoint
    * @param halfEdge edge and other endpoint
    * @return the set with the edge added
    */
  def addEdge(endpoint: QuineId, halfEdge: HalfEdge): VisitedVariableEdgeMatches = {
    val thisIndex = visited.size
    if (endpoint < halfEdge.other) {
      VisitedVariableEdgeMatches(visited + ((endpoint -> halfEdge) -> thisIndex))
    } else {
      VisitedVariableEdgeMatches(visited + ((halfEdge.other -> halfEdge.reflect(endpoint)) -> thisIndex))
    }
  }

  /** Check if an edge is in the set
    *
    * @param endpoint one endpoint
    * @param halfEdge edge and other endpoint
    * @return whether the edge is in the set
    */
  def contains(endpoint: QuineId, halfEdge: HalfEdge): Boolean =
    if (endpoint < halfEdge.other) {
      visited.contains(endpoint -> halfEdge)
    } else {
      visited.contains(halfEdge.other -> halfEdge.reflect(endpoint))
    }

  /** Recover the ordered list of relationships */
  def relationships: Vector[Expr.Relationship] =
    visited.toVector
      .sortBy(_._2)
      .map { case ((q1, HalfEdge(typ, dir, q2)), _) =>
        if (dir == EdgeDirection.Incoming) Expr.Relationship(q1, typ, Map.empty, q2)
        else Expr.Relationship(q2, typ, Map.empty, q1)
      }
      .toVector
}
object VisitedVariableEdgeMatches {

  val empty: VisitedVariableEdgeMatches = VisitedVariableEdgeMatches(Map.empty)
}
