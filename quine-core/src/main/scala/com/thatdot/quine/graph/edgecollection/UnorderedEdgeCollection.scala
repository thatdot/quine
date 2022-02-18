package com.thatdot.quine.graph.edgecollection

import scala.collection.AbstractIterable
import scala.collection.mutable.{Map => MutableMap, Set => MutableSet}

import com.thatdot.quine.model._

/** Conceptually, this is a mutable `Set[HalfEdge]`.
  *
  * Under the hood, it gets implemented with some maps and sets because we want to be able to
  * efficiently query for subsets which have some particular edge types, directions, or ids. For
  * more on that, see the various `matching` methods.
  *
  * Not concurrent.
  */
final class UnorderedEdgeCollection extends EdgeCollection {
  private val edgeMap: MutableMap[Symbol, MutableMap[EdgeDirection, MutableSet[QuineId]]] = MutableMap.empty
  private var totalSize: Int = 0

  // TODO: consider lazily populating other maps (which represent different views into the same data). Example:
//  private val idMap: MutableMap[QuineId, MutableSet[GenericEdge]] = MutableMap.empty  // TODO: consider this for fast edge lookups by ID.

  override def toString: String = s"EdgeCollection(${edgeMap.mkString(", ")})"

  override def size: Int = totalSize

  override def +=(edge: HalfEdge): this.type = {
    val edgeDirMap = edgeMap.getOrElseUpdate(edge.edgeType, MutableMap.empty)
    val quineIdSet = edgeDirMap.getOrElseUpdate(edge.direction, MutableSet.empty)
    val didAddQuineId = quineIdSet.add(edge.other)

    // Only if something new was added does size need to be updated
    if (didAddQuineId) totalSize += 1

    this
  }

  override def -=(edge: HalfEdge): this.type = {
    for {
      edgeDirMap <- edgeMap.get(edge.edgeType)
      quineIdSet <- edgeDirMap.get(edge.direction)
    } {
      val didRemoveQuineId = quineIdSet.remove(edge.other)

      if (didRemoveQuineId) {
        // Only if something new was removed does size need to be updated
        totalSize -= 1

        // Also, we delete maps and sets that are now empty
        if (quineIdSet.isEmpty) {
          edgeDirMap -= edge.direction
          if (edgeDirMap.isEmpty)
            edgeMap -= edge.edgeType
        }
      }
    }

    this
  }

  override def clear(): Unit = {
    edgeMap.clear()
    totalSize = 0
  }

  override def all: Iterator[HalfEdge] = for {
    (edgeTyp, dirMap) <- edgeMap.iterator
    (dir, qids) <- dirMap.iterator
    qid <- qids.iterator
  } yield HalfEdge(edgeTyp, dir, qid)

  override def toSerialize: Iterable[HalfEdge] = new AbstractIterable[HalfEdge] {
    def iterator: Iterator[HalfEdge] = all
  }

  def matching(edgeType: Symbol): Iterator[HalfEdge] =
    for {
      dirMap <- edgeMap.get(edgeType).iterator
      (dir, qids) <- dirMap.iterator
      qid <- qids.iterator
    } yield HalfEdge(edgeType, dir, qid)

  def matching(edgeType: Symbol, direction: EdgeDirection): Iterator[HalfEdge] =
    for {
      dirMap <- edgeMap.get(edgeType).iterator
      qids <- dirMap.get(direction).iterator
      qid <- qids.iterator
    } yield HalfEdge(edgeType, direction, qid)

  def matching(edgeType: Symbol, id: QuineId): Iterator[HalfEdge] =
    for {
      dirMap <- edgeMap.get(edgeType).iterator
      (dir, qids) <- dirMap.iterator
      if qids.contains(id)
    } yield HalfEdge(edgeType, dir, id)

  def matching(edgeType: Symbol, direction: EdgeDirection, id: QuineId): Iterator[HalfEdge] =
    for {
      dirMap <- edgeMap.get(edgeType).iterator
      qids <- dirMap.get(direction).iterator
      if qids.contains(id)
    } yield HalfEdge(edgeType, direction, id)

  def matching(direction: EdgeDirection): Iterator[HalfEdge] =
    for {
      (edgeTyp, dirMap) <- edgeMap.iterator
      qids <- dirMap.get(direction).iterator
      qid <- qids.iterator
    } yield HalfEdge(edgeTyp, direction, qid)

  def matching(direction: EdgeDirection, id: QuineId): Iterator[HalfEdge] =
    for {
      (edgeTyp, dirMap) <- edgeMap.iterator
      qids <- dirMap.get(direction).iterator
      if qids.contains(id)
    } yield HalfEdge(edgeTyp, direction, id)

  def matching(id: QuineId): Iterator[HalfEdge] =
    for {
      (edgeTyp, dirMap) <- edgeMap.iterator
      (dir, qids) <- dirMap.iterator
      if qids.contains(id)
    } yield HalfEdge(edgeTyp, dir, id)

  def matching(genEdge: GenericEdge): Iterator[HalfEdge] =
    matching(genEdge.edgeType, genEdge.direction)

  override def contains(edge: HalfEdge): Boolean = edgeMap
    .getOrElse(edge.edgeType, MutableMap.empty)
    .getOrElse(edge.direction, MutableSet.empty)
    .contains(edge.other)

  // Test for the presence of all required edges, without allowing one existing edge to match more than one required edge.
  def hasUniqueGenEdges(requiredEdges: Set[DomainEdge[Test]], thisQid: QuineId): Boolean = {
    val circAllowed = collection.mutable.Map.empty[GenericEdge, Int]
    val circDisallowed = collection.mutable.Map.empty[GenericEdge, Int]
    requiredEdges.foreach { e =>
      if (e.constraints.min > 0) {
        val which = if (e.circularMatchAllowed) circAllowed else circDisallowed
        which(e.edge) = which.getOrElse(e.edge, 0) + 1
      }
    }

    circDisallowed.forall { case (genEdge, count) =>
      val otherSet = edgeMap
        .getOrElse(genEdge.edgeType, MutableMap.empty)
        .getOrElse(genEdge.direction, MutableSet.empty)
      (circAllowed.getOrElse(genEdge, 0), otherSet.contains(thisQid)) match {
        case (0, false) => otherSet.size >= count
        case (0, true) => otherSet.size >= count + 1
        case (ca, _) =>
          otherSet.size >= count + ca // if circularAllowed is non-zero, then we can ignore the distinction about thisQid being present and simply compare the size with the combined total
      }
    }
  }

  override def toSet: Set[HalfEdge] = all.toSet

  override def nonEmpty: IsDirected = edgeMap.nonEmpty
}
