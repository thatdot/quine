package com.thatdot.quine.graph.edgecollection

import scala.collection.compat._

import com.thatdot.quine.model.{DomainEdge, EdgeDirection, GenericEdge, HalfEdge, QuineId}
import com.thatdot.quine.util.ReversibleLinkedHashSet

/** Conceptually, this is a mutable `ReversibleLinkedHashSet[HalfEdge]`.
  * Under the hood, it gets implemented with some auxiliary collections because we want to be able to
  * efficiently query for subsets which have some particular edge types, directions, or ids. For
  * more on that, see the various `matching` methods. Additionally, we want to maintain a consistent
  * ordering over edges (the current implementation maintains the ordering according to reverse
  * order of creation -- that is, newest to oldest).
  * Under the hood, it gets implemented with some maps and sets because we want to be able to
  * efficiently query for subsets which have some particular edge types, directions, or ids. For
  * more on that, see the various `matching` methods.
  *
  * Not concurrent.
  */
final class ReverseOrderedEdgeCollection extends EdgeCollection {

  private val edges: ReversibleLinkedHashSet[HalfEdge] = ReversibleLinkedHashSet.empty
  private val typeIndex: EdgeIndex[Symbol] = new EdgeIndex(_.edgeType)
  private val otherIndex: EdgeIndex[QuineId] = new EdgeIndex(_.other)
  private val directionIndex = new DirectionEdgeIndex

  override def toString: String = s"ReverseOrderedEdgeCollection(${edges.mkString(", ")})"

  override def size: Int = edges.size

  override def +=(edge: HalfEdge): this.type = {
    edges += edge
    typeIndex += edge
    directionIndex += edge
    otherIndex += edge
    this
  }

  override def -=(edge: HalfEdge): this.type = {
    edges -= edge
    typeIndex -= edge
    directionIndex -= edge
    otherIndex -= edge
    this
  }

  override def clear(): Unit = {
    edges.clear()
    typeIndex.clear()
    directionIndex.clear()
    otherIndex.clear()
  }

  protected[graph] def toSerialize: Iterable[HalfEdge] = edges

  /** Matches the direction of iterator returned by [[matching]] methods
    * @return An iterator in the same direction as those returned by [[matching]]
    */
  override def all: Iterator[HalfEdge] = edges.reverseIterator
  override def toSet: Set[HalfEdge] = edges.toSet
  override def nonEmpty: Boolean = edges.nonEmpty

  override def matching(edgeType: Symbol): Iterator[HalfEdge] =
    typeIndex(edgeType).reverseIterator

  override def matching(edgeType: Symbol, direction: EdgeDirection): Iterator[HalfEdge] =
    (typeIndex(edgeType) intersect directionIndex(direction)).reverseIterator

  override def matching(edgeType: Symbol, id: QuineId): Iterator[HalfEdge] =
    (typeIndex(edgeType) intersect otherIndex(id)).reverseIterator

  override def matching(edgeType: Symbol, direction: EdgeDirection, id: QuineId): Iterator[HalfEdge] = {
    val edge = HalfEdge(edgeType, direction, id)
    if (contains(edge))
      Iterator.single(edge)
    else
      Iterator.empty
  }

  override def matching(direction: EdgeDirection): Iterator[HalfEdge] =
    directionIndex(direction).reverseIterator

  override def matching(direction: EdgeDirection, id: QuineId): Iterator[HalfEdge] =
    (directionIndex(direction) intersect otherIndex(id)).reverseIterator

  override def matching(id: QuineId): Iterator[HalfEdge] =
    otherIndex(id).reverseIterator

  override def matching(genEdge: GenericEdge): Iterator[HalfEdge] =
    matching(genEdge.edgeType, genEdge.direction)

  override def contains(edge: HalfEdge): Boolean = edges contains edge

  // Test for the presence of all required edges, without allowing one existing edge to match more than one required edge.
  override def hasUniqueGenEdges(requiredEdges: Set[DomainEdge], thisQid: QuineId): Boolean = {
    val (circAlloweds, circDisalloweds) = requiredEdges.filter(_.constraints.min > 0).partition(_.circularMatchAllowed)
    val circAllowed = circAlloweds.groupMapReduce(_.edge)(_ => 1)(_ + _)
    val circDisallowed = circDisalloweds.groupMapReduce(_.edge)(_ => 1)(_ + _)

    circDisallowed.forall { case (genEdge, count) =>
      val otherSet = typeIndex(genEdge.edgeType) intersect directionIndex(genEdge.direction)
      // if circularAllowed is non-zero, then we can ignore the distinction about thisQid being present and simply compare the size with the combined total
      val ca = circAllowed.getOrElse(
        genEdge,
        if (otherSet contains genEdge.toHalfEdge(thisQid)) 1 else 0
      )
      otherSet.sizeIs >= count + ca
    }
  }

}
