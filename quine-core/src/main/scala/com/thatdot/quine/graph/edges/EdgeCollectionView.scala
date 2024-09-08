package com.thatdot.quine.graph.edges

import com.thatdot.quine.model.{DomainEdge, EdgeDirection, GenericEdge, HalfEdge, QuineId}

/** Similar to [[EdgeCollection]], but does not allow any modifications */
abstract class EdgeCollectionView {

  def size: Int

  /** Matches the direction of iterator returned by [[matching]] methods
    *
    * @return An iterator in the same direction as those returned by [[matching]]
    */
  def all: Iterator[HalfEdge]

  def nonEmpty: Boolean

  def matching(edgeType: Symbol): Iterator[HalfEdge]

  def matching(edgeType: Symbol, direction: EdgeDirection): Iterator[HalfEdge]

  def matching(edgeType: Symbol, id: QuineId): Iterator[HalfEdge]

  def matching(edgeType: Symbol, direction: EdgeDirection, id: QuineId): Iterator[HalfEdge]

  def matching(direction: EdgeDirection): Iterator[HalfEdge]

  def matching(direction: EdgeDirection, id: QuineId): Iterator[HalfEdge]

  def matching(id: QuineId): Iterator[HalfEdge]

  def matching(genEdge: GenericEdge): Iterator[HalfEdge]

  def matching(
    domainEdges: List[DomainEdge],
    thisQid: QuineId,
  ): Map[DomainEdge, Set[HalfEdge]] = domainEdges
    .map(de =>
      de -> matching(de.edge.edgeType, de.edge.direction)
        .filter(he => de.circularMatchAllowed || he.other != thisQid)
        .toSet,
    )
    .toMap

  def contains(edge: HalfEdge): Boolean

  /** Test for the presence of all required half-edges, without allowing one existing half-edge to match
    * more than one required edge.
    *
    * Returns true if for all non-circular [[GenericEdge]] in the input set, the total HalfEdges in this edge collection
    * can contain the values in the input set.
    *
    * - We count domainEdges marked constraints.min > 0 and circularMatchAllowed == false.
    * - If there are additional edges marked circularMatchAllowed, we count those as well, up to the number of allowed
    *   circular matches for that [[GenericEdge]]
    * - If there is not, and a circular edge is detected, we discount this disallowed half-edge before evaluating
    *   whether we have enough matching edges to satisfy the provided [[GenericEdge]]s
    */
  def hasUniqueGenEdges(requiredEdges: Set[DomainEdge], thisQid: QuineId): Boolean

}
