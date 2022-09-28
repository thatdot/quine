package com.thatdot.quine.graph

import scala.collection.mutable.{Map => MutableMap}

import com.thatdot.quine.graph.behavior.DomainNodeIndexBehavior
import com.thatdot.quine.model.{DomainGraphBranch, HalfEdge, PropertyValue, QuineId}

// Convenience class to define which NodeActor fields to close over (sometimes mutable!) for the sake of immediately serializing it.
// Don't pass instances of this class around!
final case class NodeSnapshot(
  properties: Map[Symbol, PropertyValue],
  edges: Iterable[HalfEdge],
  forwardTo: Option[QuineId],
  mergedIntoHere: Set[QuineId],
  subscribersToThisNode: MutableMap[
    (DomainGraphBranch, AssumedDomainEdge),
    DomainNodeIndexBehavior.SubscribersToThisNodeUtil.Subscription
  ],
  domainNodeIndex: MutableMap[
    QuineId,
    MutableMap[(DomainGraphBranch, AssumedDomainEdge), Option[Boolean]]
  ]
)
