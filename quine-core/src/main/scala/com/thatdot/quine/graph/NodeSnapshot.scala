package com.thatdot.quine.graph

import scala.collection.mutable.{Map => MutableMap}

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.behavior.DomainNodeIndexBehavior
import com.thatdot.quine.graph.behavior.DomainNodeIndexBehavior.SubscribersToThisNodeUtil
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.model.{HalfEdge, PropertyValue}
import com.thatdot.quine.persistor.codecs.{AbstractSnapshotCodec, UnsupportedExtension}

abstract class AbstractNodeSnapshot {
  def time: EventTime
  def properties: Map[Symbol, PropertyValue]
  def edges: Iterable[HalfEdge]
  def subscribersToThisNode: MutableMap[
    DomainGraphNodeId,
    DomainNodeIndexBehavior.SubscribersToThisNodeUtil.DistinctIdSubscription,
  ]
  def domainNodeIndex: MutableMap[
    QuineId,
    MutableMap[DomainGraphNodeId, Option[Boolean]],
  ]
}
// Convenience class to define which NodeActor fields to close over (sometimes mutable!) for the sake of immediately serializing it.
// Don't pass instances of this class around!
final case class NodeSnapshot(
  time: EventTime,
  properties: Map[Symbol, PropertyValue],
  edges: Iterable[HalfEdge],
  subscribersToThisNode: MutableMap[
    DomainGraphNodeId,
    DomainNodeIndexBehavior.SubscribersToThisNodeUtil.DistinctIdSubscription,
  ],
  domainNodeIndex: MutableMap[
    QuineId,
    MutableMap[DomainGraphNodeId, Option[Boolean]],
  ],
) extends AbstractNodeSnapshot

object NodeSnapshot {
  implicit val snapshotCodec: AbstractSnapshotCodec[NodeSnapshot] = new AbstractSnapshotCodec[NodeSnapshot] {
    def determineReserved(snapshot: NodeSnapshot): Boolean = false

    def constructDeserialized(
      time: EventTime,
      properties: Map[Symbol, PropertyValue],
      edges: Iterable[HalfEdge],
      subscribersToThisNode: MutableMap[DomainGraphNodeId, SubscribersToThisNodeUtil.DistinctIdSubscription],
      domainNodeIndex: MutableMap[QuineId, MutableMap[DomainGraphNodeId, Option[Boolean]]],
      reserved: Boolean,
    ): NodeSnapshot = {
      if (reserved) { // must be false in Quine
        throw new UnsupportedExtension(
          """Node snapshot indicates that restoring this node requires a Quine system
          |extension not available in the running application.""".stripMargin.replace('\n', ' '),
        )
      }

      NodeSnapshot(
        time,
        properties,
        edges,
        subscribersToThisNode,
        domainNodeIndex,
      )
    }
  }

}
