package com.thatdot.quine.graph.edgecollection

import scala.collection.mutable

import com.thatdot.quine.model.{EdgeDirection, HalfEdge}
import com.thatdot.quine.util.ReversibleLinkedHashSet

/** A wrapper for interacting with ordered sets by key ([[ReversibleLinkedHashSet]]s).
  * @tparam K The type of the key for the [[HalfEdge]] index (a field in the HalfEdge)
  */
abstract class AbstractEdgeIndex[K] {

  /** Adds an edge to the appropriate internal [[ReversibleLinkedHashSet]]
    * @param edge the edge to add
    * @return the collection the element was added to
    */
  def +=(edge: HalfEdge): ReversibleLinkedHashSet[HalfEdge]

  def -=(edge: HalfEdge): ReversibleLinkedHashSet[HalfEdge]

  /** Returns the [[ReversibleLinkedHashSet]] associated with a given key
    * @param key the lookup key
    * @return the collection at that key
    */
  def apply(key: K): ReversibleLinkedHashSet[HalfEdge]

  def clear(): Unit
}

final class EdgeIndex[K](
  keyFn: HalfEdge => K,
  index: mutable.Map[K, ReversibleLinkedHashSet[HalfEdge]] = mutable.Map.empty[K, ReversibleLinkedHashSet[HalfEdge]]
) extends AbstractEdgeIndex[K] {

  override def toString: String = s"EdgeIndex($index)"

  override def +=(edge: HalfEdge): ReversibleLinkedHashSet[HalfEdge] = {
    val key = keyFn(edge)
    index.getOrElseUpdate(key, ReversibleLinkedHashSet.empty) += edge
  }

  override def -=(edge: HalfEdge): ReversibleLinkedHashSet[HalfEdge] = {
    val key = keyFn(edge)
    val updatedEntry = index(key) -= edge
    // Delete entries that are now empty from the Map
    if (updatedEntry.isEmpty) index -= key
    updatedEntry
  }

  override def apply(key: K): ReversibleLinkedHashSet[HalfEdge] =
    index.getOrElse(key, ReversibleLinkedHashSet.empty)

  override def clear(): Unit =
    index.clear()

}

final class DirectionEdgeIndex extends AbstractEdgeIndex[EdgeDirection] {
  private[this] val outgoingEdges = ReversibleLinkedHashSet.empty[HalfEdge]
  private[this] val incomingEdges = ReversibleLinkedHashSet.empty[HalfEdge]
  private[this] val undirectedEdges = ReversibleLinkedHashSet.empty[HalfEdge]

  override def toString: String =
    s"DirectionEdgeIndex(outgoingEdges = $outgoingEdges, incomingEdges = $incomingEdges, undirectedEdges = $undirectedEdges)"
  @inline
  private[this] def collectionForDirection(direction: EdgeDirection): ReversibleLinkedHashSet[HalfEdge] =
    direction match {
      case EdgeDirection.Outgoing => outgoingEdges
      case EdgeDirection.Incoming => incomingEdges
      case EdgeDirection.Undirected => undirectedEdges
    }

  override def +=(edge: HalfEdge): ReversibleLinkedHashSet[HalfEdge] = collectionForDirection(edge.direction) += edge

  override def -=(edge: HalfEdge): ReversibleLinkedHashSet[HalfEdge] = collectionForDirection(edge.direction) -= edge

  override def apply(key: EdgeDirection): ReversibleLinkedHashSet[HalfEdge] = collectionForDirection(key)

  override def clear(): Unit = {
    outgoingEdges.clear()
    incomingEdges.clear()
    undirectedEdges.clear()
  }
}
