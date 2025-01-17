package com.thatdot.quine.model

import com.thatdot.common.logging.Pretty.PrettyHelper
import com.thatdot.common.quineid.QuineId

/** Half of an edge in Quine
  *
  * An edge in Quine exists iff there exist two reciprocal half edges on the two
  * nodes that make up the edge. A half edge is stored (or referred to) in the
  * context of a node, which is why only the _other_ endpoint is stored on the
  * half edge.
  *
  * @param edgeType label on the edge
  * @param direction which way (if any) is the edge pointing
  * @param other other endpoint of the edge
  */
final case class HalfEdge(
  edgeType: Symbol,
  direction: EdgeDirection,
  other: QuineId,
) {

  /** Make a reciprocal half edge
    */
  def reflect(thisNode: QuineId): HalfEdge = HalfEdge(edgeType, direction.reverse, thisNode)

  def pretty(implicit idProvider: QuineIdProvider): String =
    s"${this.getClass.getSimpleName}(${edgeType.name}, $direction, ${other.pretty})"
}
