package com.thatdot.quine.graph

import scala.collection.mutable

import com.thatdot.quine.graph.EdgeEvent.{EdgeAdded, EdgeRemoved}
import com.thatdot.quine.graph.PropertyEvent.{PropertyRemoved, PropertySet}
import com.thatdot.quine.graph.StandingQueryWatchableEventIndex.EventSubscriber
import com.thatdot.quine.graph.cypher.MultipleValuesStandingQueryState
import com.thatdot.quine.graph.edges.EdgeCollectionView
import com.thatdot.quine.model
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.model.{And, DomainGraphBranch, Mu, MuVar, Not, Or, PropertyValue, SingleBranch}

/** Local events a standing query may want to watch
  *
  * NB The more fine-grained this type hierarchy is, the more complex the state we must
  * store on a node becomes, but the more efficient we become at delivering just
  * the right event to the right stage.
  */
sealed abstract class WatchableEventType
object WatchableEventType {
  final case class EdgeChange(labelConstraint: Option[Symbol]) extends WatchableEventType
  final case class PropertyChange(propertyKey: Symbol) extends WatchableEventType
  final case object AnyPropertyChange extends WatchableEventType

  /** traverse a DomainGraphBranch (standing query) and extract the set of StandingQueryLocalEvents relevant to that
    * branch at its root node.
    *
    * @example Given a branch watching for a pattern like (n{foo: "bar"})-[:relates_to]->(m{fizz: "buzz"}), rooted at n:
    *          - StandingQueryLocalEvents.Property("foo") will be extracted -- a change in that property may update the
    *            Standing Query
    *          - StandingQueryLocalEvents.Edge("relates_to") will be extracted -- a change in that edge may update the
    *            Standing Query
    *          - StandingQueryLocalEvents.Property("fizz") will _not_ be extracted -- that property will never be
    *            checkable by n (the root node) and so n cannot update the standing Standing Query on it
    *          - StandingQueryLocalEvents.Edge("not_in_pattern") will _not_ be extracted -- that edge is not relevant to
    *            the branch provided
    */
  def extractWatchableEvents(
    branch: DomainGraphBranch
  ): Set[WatchableEventType] = {

    /** Recursive helper to extract the StandingQueryLocalEvents as described.
      *
      * For performance, all intermediate states are Seqs (or, where possible, SeqViews),
      * only converting to a Set at the end (so that, for example, uniqueness doesn't need to be checked at each
      * `map` and `++`)
      */
    def extractWatchables(
      branch: DomainGraphBranch,
      acc: Seq[WatchableEventType] = Nil
    ): Seq[WatchableEventType] = branch match {
      case SingleBranch(
            model.DomainNodeEquiv(_, localProps, circularEdges),
            id @ _,
            nextBranches,
            _
          ) =>
        (
          localProps.keys.view.map(WatchableEventType.PropertyChange) ++
          // circular edges
          circularEdges.view.map { case (name, _) => WatchableEventType.EdgeChange(Some(name)) } ++
          // non-circular edges -- note that we do NOT traverse into domainEdge.branch, as that branch is the
          // responsibility of another node
          nextBranches.view.map(domainEdge => WatchableEventType.EdgeChange(Some(domainEdge.edge.edgeType))) ++
          // previously collected watchables
          acc.view
        ).toSeq // optimization: combine SeqViews only once
      case Or(disjuncts) => disjuncts.foldLeft(acc)((nextAcc, nextBranch) => extractWatchables(nextBranch, nextAcc))
      case And(conjuncts) => conjuncts.foldLeft(acc)((nextAcc, nextBranch) => extractWatchables(nextBranch, nextAcc))
      case Not(negated) => extractWatchables(negated, acc)
      case MuVar(_) =>
        // MuVars should only occur as a child of both a Mu and a SingleBranch -- so the SingleBranch
        // terminal case should always be hit before this case is reached.
        Nil
      case Mu(_, repeatsBranch) =>
        // Mu only affects the scope of an SQ by binding a MuVar -- because we'll never hit a MuVar case,
        // Mu is just a passthru
        extractWatchables(repeatsBranch, acc)
    }

    extractWatchables(branch).toSet
  }
}

/** Index for efficiently determining which standing queries care to be notified
  * about any given event
  *
  * Plan: use `MultiDict` after dropping 2.12 support
  *
  * @note `watchingForEdge.keySet` and `watchingForAnyEdge` must be disjoint
  *
  * @param watchingForProperty mapping of property key to interested SQs
  * @param watchingForEdge mapping of edge key to interested SQs
  * @param watchingForAnyEdge set of SQs interested in any edge
  */

final case class StandingQueryWatchableEventIndex(
  watchingForProperty: mutable.Map[Symbol, mutable.Set[EventSubscriber]],
  watchingForEdge: mutable.Map[Symbol, mutable.Set[EventSubscriber]],
  watchingForAnyEdge: mutable.Set[EventSubscriber],
  watchingForAnyProperty: mutable.Set[EventSubscriber]
) {

  /** Register a new SQ as being interested in a given event type and return an event to represent the  initial
    * state if there is any.
    *
    * TODO: return iterable and avoid `toSeq`
    *
    * @param subscriber standing query which is interested in certain node events
    * @param eventType  watchable event relevant to the subscriber
    * @param properties the current node's collection of properties used to produce the initial set of NodeChangeEvents
    * @param edges      the current node's collection of edges used to produce the initial set of NodeChangeEvents
    * @return an iterator of initial node events (from the existing node state)
    */
  def registerStandingQuery(
    subscriber: EventSubscriber,
    eventType: WatchableEventType,
    properties: Map[Symbol, PropertyValue],
    edges: EdgeCollectionView
  ): Seq[NodeChangeEvent] =
    eventType match {
      case WatchableEventType.PropertyChange(key) =>
        watchingForProperty.getOrElseUpdate(key, mutable.Set.empty) += subscriber
        properties.get(key).toSeq.map { propVal =>
          PropertySet(key, propVal)
        }

      case WatchableEventType.AnyPropertyChange =>
        watchingForAnyProperty.add(subscriber)
        properties.map { case (k, v) =>
          PropertySet(k, v)
        }.toSeq

      case WatchableEventType.EdgeChange(Some(key)) =>
        watchingForEdge.getOrElseUpdate(key, mutable.Set.empty) += subscriber
        edges.matching(key).toSeq.map { halfEdge =>
          EdgeAdded(halfEdge)
        }

      case WatchableEventType.EdgeChange(None) =>
        watchingForAnyEdge.add(subscriber)
        edges.all.toSeq.map { halfEdge =>
          EdgeAdded(halfEdge)
        }

    }

  /** Unregister a SQ as being interested in a given event */
  def unregisterStandingQuery(handler: EventSubscriber, event: WatchableEventType): Unit =
    event match {
      case WatchableEventType.PropertyChange(key) =>
        for (set <- watchingForProperty.get(key)) {
          set -= handler
          if (set.isEmpty) watchingForProperty -= key
        }

      case WatchableEventType.AnyPropertyChange =>
        watchingForAnyProperty -= handler

      case WatchableEventType.EdgeChange(Some(key)) =>
        for (set <- watchingForEdge.get(key)) {
          set -= handler
          if (set.isEmpty) watchingForProperty -= key
        }

      case WatchableEventType.EdgeChange(None) =>
        watchingForAnyEdge -= handler
    }

  /** Invokes [[retainSubscriberPredicate]] with subscribers interested in a given node event.
    * Callback [[removeSubscriberPredicate]] returns true to indicate the record is invalid and should be removed.
    */
  def standingQueriesWatchingNodeEvent(
    event: NodeChangeEvent,
    removeSubscriberPredicate: EventSubscriber => Boolean
  ): Unit = event match {
    case EdgeAdded(halfEdge) =>
      watchingForEdge
        .get(halfEdge.edgeType)
        .foreach(index => index.filter(removeSubscriberPredicate).foreach(index.remove))
      watchingForAnyEdge.filter(removeSubscriberPredicate).foreach(watchingForAnyEdge.remove)
    case EdgeRemoved(halfEdge) =>
      watchingForEdge
        .get(halfEdge.edgeType)
        .foreach(index => index.filter(removeSubscriberPredicate).foreach(index.remove))
      watchingForAnyEdge.filter(removeSubscriberPredicate).foreach(watchingForAnyEdge.remove)
    case PropertySet(propKey, _) =>
      watchingForProperty.get(propKey).foreach(index => index.filter(removeSubscriberPredicate).foreach(index.remove))
      watchingForAnyProperty.filter(removeSubscriberPredicate).foreach(watchingForAnyProperty.remove)
    case PropertyRemoved(propKey, _) =>
      watchingForProperty.get(propKey).foreach(index => index.filter(removeSubscriberPredicate).foreach(index.remove))
      watchingForAnyProperty.filter(removeSubscriberPredicate).foreach(watchingForAnyProperty.remove)
    case _ => ()
  }
}
object StandingQueryWatchableEventIndex {

  /** EventSubscribers are the recipients of the event types classifiable by [[WatchableEventType]]
    * See the concrete implementations for more detail
    */
  sealed trait EventSubscriber
  object EventSubscriber {
    def apply(sqIdTuple: (StandingQueryId, MultipleValuesStandingQueryPartId)): StandingQueryWithId =
      StandingQueryWithId(sqIdTuple._1, sqIdTuple._2)
    def apply(
      dgnId: DomainGraphNodeId
    ): DomainNodeIndexSubscription =
      DomainNodeIndexSubscription(dgnId)
  }

  /** A single SQv4 standing query part -- this handles events by passing them to the SQ's state's "onNodeEvents" hook
    *
    * @param queryId
    * @param partId
    * @see [[behavior.MultipleValuesStandingQueryBehavior.multipleValuesStandingQueries]]
    * @see [[behavior.MultipleValuesStandingQueryBehavior.updateMultipleValuesSqs]]
    */
  final case class StandingQueryWithId(queryId: StandingQueryId, partId: MultipleValuesStandingQueryPartId)
      extends EventSubscriber

  /** A DGB subscription -- this handles events by routing them to multiple other nodes, ie, [[Notifiable]]s
    * @param branch
    * @see [[behavior.DomainNodeIndexBehavior.domainGraphSubscribers]]
    * @see [[behavior.DomainNodeIndexBehavior.SubscribersToThisNode.updateAnswerAndNotifySubscribers]]
    */
  final case class DomainNodeIndexSubscription(dgnId: DomainGraphNodeId) extends EventSubscriber

  def empty: StandingQueryWatchableEventIndex = StandingQueryWatchableEventIndex(
    mutable.Map.empty[Symbol, mutable.Set[EventSubscriber]],
    mutable.Map.empty[Symbol, mutable.Set[EventSubscriber]],
    mutable.Set.empty[EventSubscriber],
    mutable.Set.empty[EventSubscriber]
  )

  /** Rebuild the part of the event index based on the provided query states and subscribers
    *
    * @param dgnSubscribers
    * @param multipleValuesStandingQueryStates currently set states
    * @return tuple containing rebuilt index and [[DomainGraphNodeId]]s that are not in the registry
    */
  def from(
    dgnRegistry: DomainGraphNodeRegistry,
    dgnSubscribers: Iterator[DomainGraphNodeId],
    multipleValuesStandingQueryStates: Iterator[
      ((StandingQueryId, MultipleValuesStandingQueryPartId), MultipleValuesStandingQueryState)
    ]
  ): (StandingQueryWatchableEventIndex, Iterable[DomainGraphNodeId]) = {
    val toReturn = StandingQueryWatchableEventIndex.empty
    val removed = Iterable.newBuilder[DomainGraphNodeId]
    val dgnEvents = for {
      dgnId <- dgnSubscribers
      branch <- dgnRegistry.getDomainGraphBranch(dgnId) match {
        case Some(b) => Set(b)
        case None =>
          removed += dgnId
          Set.empty
      }
      event <- WatchableEventType.extractWatchableEvents(branch)
    } yield event -> EventSubscriber(dgnId)
    val sqStateEvents = for {
      (sqIdAndPartId, queryState) <- multipleValuesStandingQueryStates
      event <- queryState.relevantEventTypes
    } yield event -> EventSubscriber(sqIdAndPartId)

    (dgnEvents ++ sqStateEvents).foreach { case (event, handler) =>
      event match {
        case WatchableEventType.PropertyChange(key) =>
          toReturn.watchingForProperty.getOrElseUpdate(key, mutable.Set.empty) += handler

        case WatchableEventType.AnyPropertyChange =>
          toReturn.watchingForAnyProperty += handler

        case WatchableEventType.EdgeChange(Some(key)) =>
          toReturn.watchingForEdge.getOrElseUpdate(key, mutable.Set.empty) += handler

        case WatchableEventType.EdgeChange(None) =>
          toReturn.watchingForAnyEdge += handler
      }
    }

    (toReturn, removed.result())
  }
}
