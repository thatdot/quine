package com.thatdot.quine.graph

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.model.{HalfEdge, PropertyValue}

sealed trait NodeEvent

object NodeEvent {

  /** Event along with the time it occurs at
    *
    * @param event what happened to the node?
    * @param atTime when did it happen?
    */
  final case class WithTime[+E <: NodeEvent](
    event: E,
    atTime: EventTime,
  )
}

/** Event which affects the local node state (properties or edges)
  *
  * Storing node state as a series of time-indexed events (aka. event sourcing),
  * it becomes straightforward to:
  *
  *   - re-create node state for any timestamp by applying or unapplying events
  *     until the desired timestamp is reached (eg. for historical queries)
  *
  *   - design event-driven systems for triggering on changes to node state (eg.
  *     standing queries)
  *
  *   - persist the changes to durable storage without necessarily needing
  *     expensive updates (append often suffices)
  */
sealed abstract class NodeChangeEvent extends NodeEvent
sealed abstract class PropertyEvent extends NodeChangeEvent {
  val key: Symbol
}
object PropertyEvent {
  final case class PropertySet(key: Symbol, value: PropertyValue) extends PropertyEvent

  final case class PropertyRemoved(key: Symbol, previousValue: PropertyValue) extends PropertyEvent

}
sealed abstract class EdgeEvent extends NodeChangeEvent {
  val edge: HalfEdge
}
object EdgeEvent {
  final case class EdgeAdded(edge: HalfEdge) extends EdgeEvent
  final case class EdgeRemoved(edge: HalfEdge) extends EdgeEvent

}

sealed trait DomainIndexEvent extends NodeEvent {
  val dgnId: DomainGraphNodeId
}

object DomainIndexEvent {

  /** This is the internal node-to-node subscription (dual to: [[CancelDomainNodeSubscription]]) */
  final case class CreateDomainNodeSubscription(
    dgnId: DomainGraphNodeId,
    replyTo: QuineId,
    relatedQueries: Set[StandingQueryId],
  ) extends DomainIndexEvent

  /** This is the outer-most subscriber for a Standing Query.
    * Dual of: [[CancelDomainStandingQuerySubscription]]
    */
  final case class CreateDomainStandingQuerySubscription(
    dgnId: DomainGraphNodeId,
    replyTo: StandingQueryId,
    relatedQueries: Set[StandingQueryId],
  ) extends DomainIndexEvent

  /** This retires the outer-most subscriber for a Standing Query (dual to:
    * [[CreateDomainStandingQuerySubscription]]).
    *
    * A cancelled query is gone from the graph, so a node could once read the cancellation off its absence. That
    * says *whether* it happened but not *when*, and when is what a replay needs: the same events with the teardown
    * before or after them leave the node in different states. Nor can one graph-wide time answer it, because each
    * node applies the teardown at its own moment -- awake nodes when the cancellation reaches them, sleeping ones
    * at their next wake. So the node that discovers the query is gone records it here, in its own journal, at the
    * point it retires the subscription. That is the one ordering a replay of this node needs, and it needs no
    * clock to establish.
    *
    * Which answers went with it needs no recording: each answer records the queries it is kept for, and a replay
    * knows from this node's own history which of those were still live at this point, so applying this drops the
    * answers left with no live query needing them -- the same rule the node applied when it wrote this. See
    * `retireSubscription`.
    */
  final case class CancelDomainStandingQuerySubscription(
    dgnId: DomainGraphNodeId,
    alreadyCancelledSubscriber: StandingQueryId,
  ) extends DomainIndexEvent

  final case class DomainNodeSubscriptionResult(
    from: QuineId,
    dgnId: DomainGraphNodeId,
    result: Boolean,
  ) extends DomainIndexEvent

  /** This cancels internal subscriptions (dual to: [[CreateDomainNodeSubscription]]) */
  final case class CancelDomainNodeSubscription(
    dgnId: DomainGraphNodeId,
    alreadyCancelledSubscriber: QuineId,
  ) extends DomainIndexEvent
}
