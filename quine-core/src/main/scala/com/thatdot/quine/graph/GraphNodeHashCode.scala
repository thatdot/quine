package com.thatdot.quine.graph

import scala.jdk.CollectionConverters._

import com.google.common.hash.Hashing.combineUnordered
import com.google.common.hash.{HashCode, Hasher, Hashing}

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.messaging.QuineMessage
import com.thatdot.quine.model.{EdgeDirection, HalfEdge, PropertyValue}

case class GraphNodeHashCode(value: Long) extends QuineMessage

/** Computes and returns the node hash code, which is determined from:
  *
  * - This node's ID
  * - Node properties (keys and values)
  * - Node edges (types, directions, and related node IDs)
  *
  * @note A node without any properties or edges by definition has hash code 0.
  *       This is so nodes  that exist in the future are not erroneously
  *       included in historical hash code calculation.
  */
object GraphNodeHashCode {
  def apply(qid: QuineId, properties: Map[Symbol, PropertyValue], edges: Iterable[HalfEdge]): GraphNodeHashCode =
    if (properties.isEmpty && edges.isEmpty) {
      // nodes with [a quineid but] neither properties nor edges are deliberately hashed to the same value: they may
      // have interesting histories, or they may not, but their current materialized state is definitely uninteresting.
      // The value is 0 because the graph-level implementation of getGraphHashCode combines node hashes by summing them.
      GraphNodeHashCode(0L)
    } else {
      // hash function implementing the 128-bit murmur3 algorithm
      def newHasher() = Hashing.murmur3_128.newHasher()

      // The hash code is computed with data from the node ID,
      val resultHashCode = newHasher().putBytes(qid.array)

      // node property keys and values,
      putUnordered[(Symbol, PropertyValue)](
        properties,
        resultHashCode,
        { case (k, v) =>
          val h = newHasher()
          h.putUnencodedChars(k.name)
          putPropertyValue(v, h)
          h.hash()
        },
      )

      // and node half edges.
      putUnordered[HalfEdge](
        edges,
        resultHashCode,
        { case HalfEdge(edgeType, direction, other) =>
          val h = newHasher()
          h.putUnencodedChars(edgeType.name)
          h.putInt(direction match {
            case EdgeDirection.Outgoing => 1
            case EdgeDirection.Incoming => 2
            case EdgeDirection.Undirected => 3
          })
          h.putBytes(other.array)
          h.hash()
        },
      )

      GraphNodeHashCode(resultHashCode.hash().asLong)
    }

  // TODO refactor to eliminate duplicated code below and in DomainGraphNode.scala
  private def putPropertyValue(v: PropertyValue, h: Hasher): Hasher =
    h.putBytes(v.serialized) // serialized is stable within a Quine version because serialization is stable + versioned

  private def putUnordered[T](iter: Iterable[T], into: Hasher, putElement: T => HashCode): Hasher = {
    val seq = iter.toList
    val size = seq.size
    into.putInt(size)
    if (size > 0) into.putBytes(combineUnordered(seq.map(putElement).asJava).asBytes)
    into
  }
}
