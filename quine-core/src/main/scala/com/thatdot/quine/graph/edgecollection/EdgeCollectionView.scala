package com.thatdot.quine.graph.edgecollection

import com.thatdot.quine.model.{DomainEdge, EdgeDirection, ExecInstruction, GenericEdge, HalfEdge, QuineId, Test}

/** Similar to [[EdgeCollection]], but does not allow any modifications */
abstract class EdgeCollectionView {

  def size: Int

  /** Matches the direction of iterator returned by [[matching]] methods
    *
    * @return An iterator in the same direction as those returned by [[matching]]
    */
  def all: Iterator[HalfEdge]

  def toSet: Set[HalfEdge]

  protected[graph] def toSerialize: Iterable[HalfEdge]

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
    domainEdges: List[DomainEdge[_ <: ExecInstruction]],
    thisQid: QuineId
  ): Map[DomainEdge[_ <: ExecInstruction], Set[HalfEdge]] = domainEdges
    .map(de =>
      de -> matching(de.edge.edgeType, de.edge.direction)
        .filter(he => de.circularMatchAllowed || he.other != thisQid)
        .toSet
    )
    .toMap

  def contains(edge: HalfEdge): Boolean

  // Test for the presence of all required edges, without allowing one existing edge to match more than one required edge.
  def hasUniqueGenEdges(requiredEdges: Set[DomainEdge[Test]], thisQid: QuineId): Boolean

}
