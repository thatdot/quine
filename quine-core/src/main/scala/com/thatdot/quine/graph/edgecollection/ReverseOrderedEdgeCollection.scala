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

    // keys are edge specifications, values are how many edges matching that specification are necessary.
    val circAllowed =
      circAlloweds.groupMapReduce(_.edge)(_ => 1)(_ + _) // how many edge requirements allow circularity?
    val circDisallowed = circDisalloweds.groupMapReduce(_.edge)(_ => 1)(_ + _)

    // For each required (non-circular) edge, check if we have half-edges satisfying the requirement.
    // NB circular edges have already been checked by this point, so we are only concerned with them insofar as they
    // interfere with counting noncircular half-edges
    circDisallowed.forall { case (genEdge, requiredNoncircularCount) =>
      // the set of half-edges matching this edge requirement, potentially including circular half-edges
      val edgesMatchingRequirement = typeIndex(genEdge.edgeType) intersect directionIndex(genEdge.direction)

      // number of circular edges allowed to count towards this edge requirement. If no entry exists in [[circAllowed]] for this
      // requirement, 0 edges may
      val numberOfCircularEdgesPermitted = circAllowed.getOrElse(genEdge, 0)

      /** NB a half-edge is (type, direction, remoteQid) == ((type, direction), qid) == (GenericEdge, qid)
        * Because of this, for each requirement and qid, there is either 0 or 1 half-edge that matches the requirement.
        * In particular, there is either 0 or 1 *circular* half-edge that matches the requirement
        */
      lazy val oneOfTheMatchingEdgesIsCircular = edgesMatchingRequirement.contains(genEdge.toHalfEdge(thisQid))
      if (numberOfCircularEdgesPermitted == 0) {
        if (oneOfTheMatchingEdgesIsCircular)
          // No circular edges allowed, but 1 is circular: discount that 1 from [[edgesMatchingRequirement]] before
          // comparing to the count requirement.
          edgesMatchingRequirement.size - 1 >= requiredNoncircularCount
        else
          // No circular edges allowed, and none are circular: We satisfy this requirement by the natural condition
          // against the count requirement
          edgesMatchingRequirement.size >= requiredNoncircularCount
      } else
        // Some number of circular edges are allowed -- we must have at least enough edges matching the requirement to
        // cover both the circular and noncircular requirements
        edgesMatchingRequirement.size >= requiredNoncircularCount + numberOfCircularEdgesPermitted
    }
  }

}
