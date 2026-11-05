package com.thatdot.quine.graph.edges

import scala.collection.mutable

import com.thatdot.quine.model.HalfEdge
import com.thatdot.quine.util.ReversibleLinkedHashSet

/** Behaves like a mutable set of [[HalfEdge]], but optimized for fetching all edges with a given key.
  *
  * For example, in computing results for `MATCH (n)-[:KNOWS]->(m)`, node `n` needs to fetch all of its
  * half edges that have the type "KNOWS" and are _outgoing_. The `K` in this example could be
  * `(Symbol, EdgeDirection)` (it's actually [[com.thatdot.quine.model.GenericEdge]]).
  */
final class EdgeIndex[K](keyFn: HalfEdge => K) {

  private val index: mutable.Map[K, ReversibleLinkedHashSet[HalfEdge]] = mutable.Map.empty

  override def toString: String = s"EdgeIndex($index)"

  /** Adds the given half edge to this collection. No-op if this already contains the half edge. */
  def +=(edge: HalfEdge): Unit = {
    val key = keyFn(edge)
    index.getOrElseUpdate(key, ReversibleLinkedHashSet.empty) += edge
    ()
  }

  /** Removes the given half edge from this collection. No-op if the half edge is already missing. */
  def -=(edge: HalfEdge): Unit = {
    val key = keyFn(edge)
    index.get(key) match {
      case Some(entry) =>
        val updatedEntry = entry -= edge
        if (updatedEntry.isEmpty) index -= key
        ()
      case None => ()
    }
  }

  /** Returns the [[ReversibleLinkedHashSet]] of half edges associated with the given key. */
  def apply(key: K): ReversibleLinkedHashSet[HalfEdge] = index.getOrElse(key, ReversibleLinkedHashSet.empty)

  /** Removes all half edges from this collection. */
  def clear(): Unit = index.clear()

}
