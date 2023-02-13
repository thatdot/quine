package com.thatdot.quine.graph

import scala.collection.mutable

import com.thatdot.quine.graph.EdgeEvent.{EdgeAdded, EdgeRemoved}
import com.thatdot.quine.graph.PropertyEvent.{PropertyRemoved, PropertySet}
import com.thatdot.quine.graph.StandingQueryLocalEventIndex.EventSubscriber
import com.thatdot.quine.graph.cypher.MultipleValuesStandingQueryState
import com.thatdot.quine.graph.edgecollection.EdgeCollectionView
import com.thatdot.quine.model
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.model.{And, DomainGraphBranch, Mu, MuVar, Not, Or, PropertyValue, SingleBranch}

/** Local events a standing query may want to watch
  *
  * NB The more fine-grained this type hierarchy is, the more complex the state we must
  * store on a node becomes, but the more efficient we become at delivering just
  * the right event to the right stage.
  */
sealed abstract class StandingQueryLocalEvents
object StandingQueryLocalEvents {
  final case class Edge(labelConstraint: Option[Symbol]) extends StandingQueryLocalEvents
  final case class Property(propertyKey: Symbol) extends StandingQueryLocalEvents

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
  ): Set[StandingQueryLocalEvents] = {

    /** Recursive helper to extract the StandingQueryLocalEvents as described.
      *
      * For performance, all intermediate states are Seqs (or, where possible, SeqViews),
      * only converting to a Set at the end (so that, for example, uniqueness doesn't need to be checked at each
      * `map` and `++`)
      */
    def extractWatchables(
      branch: DomainGraphBranch,
      acc: Seq[StandingQueryLocalEvents] = Nil
    ): Seq[StandingQueryLocalEvents] = branch match {
      case SingleBranch(
            model.DomainNodeEquiv(_, localProps, circularEdges),
            id @ _,
            nextBranches,
            _
          ) =>
        (
          localProps.keys.view.map(StandingQueryLocalEvents.Property) ++
          // circular edges
          circularEdges.view.map { case (name, _) => StandingQueryLocalEvents.Edge(Some(name)) } ++
          // non-circular edges -- note that we do NOT traverse into domainEdge.branch, as that branch is the
          // responsibility of another node
          nextBranches.view.map(domainEdge => StandingQueryLocalEvents.Edge(Some(domainEdge.edge.edgeType))) ++
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

final case class StandingQueryLocalEventIndex(
  watchingForProperty: mutable.Map[Symbol, mutable.Set[EventSubscriber]],
  watchingForEdge: mutable.Map[Symbol, mutable.Set[EventSubscriber]],
  watchingForAnyEdge: mutable.Set[EventSubscriber]
) {

  /** Register a new SQ as being interested in a given event and return some
    * initial state if there is any
    *
    * TODO: return iterable and avoid `toSeq`
    *
    * @param handler standing query which is interested in certain node events
    * @param event sort of event it is interested in
    * @param node current node state
    * @return an iterator of initial node events (from the existing node state)
    */
  def registerStandingQuery(
    handler: EventSubscriber,
    event: StandingQueryLocalEvents,
    properties: Map[Symbol, PropertyValue],
    edges: EdgeCollectionView
  ): Seq[NodeChangeEvent] =
    event match {
      case StandingQueryLocalEvents.Property(key) =>
        watchingForProperty.getOrElseUpdate(key, mutable.Set.empty) += handler
        properties.get(key).toSeq.map { propVal =>
          PropertySet(key, propVal)
        }

      case StandingQueryLocalEvents.Edge(Some(key)) =>
        watchingForEdge.getOrElseUpdate(key, mutable.Set.empty) += handler
        edges.matching(key).toSeq.map { halfEdge =>
          EdgeAdded(halfEdge)
        }

      case StandingQueryLocalEvents.Edge(None) =>
        watchingForAnyEdge.add(handler)
        edges.all.toSeq.map { halfEdge =>
          EdgeAdded(halfEdge)
        }
    }

  /** Unregister a SQ as being interested in a given event */
  def unregisterStandingQuery(handler: EventSubscriber, event: StandingQueryLocalEvents): Unit =
    event match {
      case StandingQueryLocalEvents.Property(key) =>
        for (set <- watchingForProperty.get(key)) {
          set -= handler
          if (set.isEmpty) watchingForProperty -= key
        }

      case StandingQueryLocalEvents.Edge(Some(key)) =>
        for (set <- watchingForEdge.get(key)) {
          set -= handler
          if (set.isEmpty) watchingForProperty -= key
        }

      case StandingQueryLocalEvents.Edge(None) =>
        watchingForAnyEdge -= handler
    }

  /** Invokes [[retainSubscriberPredicate]] with subscribers interested in a given node event.
    * Callback [[retainSubscriberPredicate]] returns false to indicate the record is invalid and should be removed.
    */
  def standingQueriesWatchingNodeEvent(
    event: NodeChangeEvent,
    retainSubscriberPredicate: EventSubscriber => Boolean
  ): Unit = event match {
    case EdgeAdded(halfEdge) =>
      watchingForEdge
        .get(halfEdge.edgeType)
        .foreach(index => index.filter(retainSubscriberPredicate).foreach(index.remove))
      watchingForAnyEdge.filter(retainSubscriberPredicate).foreach(watchingForAnyEdge.remove)
    case EdgeRemoved(halfEdge) =>
      watchingForEdge
        .get(halfEdge.edgeType)
        .foreach(index => index.filter(retainSubscriberPredicate).foreach(index.remove))
      watchingForAnyEdge.filter(retainSubscriberPredicate).foreach(watchingForAnyEdge.remove)
    case PropertySet(propKey, _) =>
      watchingForProperty.get(propKey).foreach(index => index.filter(retainSubscriberPredicate).foreach(index.remove))
    case PropertyRemoved(propKey, _) =>
      watchingForProperty.get(propKey).foreach(index => index.filter(retainSubscriberPredicate).foreach(index.remove))
    case _ => ()
  }
}
object StandingQueryLocalEventIndex {

  /** EventSubscribers are the recipients of the event types classifiable by [[StandingQueryLocalEvents]]
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

  def empty: StandingQueryLocalEventIndex = StandingQueryLocalEventIndex(
    mutable.Map.empty[Symbol, mutable.Set[EventSubscriber]],
    mutable.Map.empty[Symbol, mutable.Set[EventSubscriber]],
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
  ): (StandingQueryLocalEventIndex, Iterable[DomainGraphNodeId]) = {
    val toReturn = StandingQueryLocalEventIndex.empty
    val removed = Iterable.newBuilder[DomainGraphNodeId]
    val dgnEvents = for {
      dgnId <- dgnSubscribers
      branch <- dgnRegistry.getDomainGraphBranch(dgnId) match {
        case Some(b) => Set(b)
        case None =>
          removed += dgnId
          Set.empty
      }
      event <- StandingQueryLocalEvents.extractWatchableEvents(branch)
    } yield event -> EventSubscriber(dgnId)
    val sqStateEvents = for {
      (sqIdAndPartId, queryState) <- multipleValuesStandingQueryStates
      event <- queryState.relevantEvents
    } yield event -> EventSubscriber(sqIdAndPartId)

    (dgnEvents ++ sqStateEvents).foreach { case (event, handler) =>
      event match {
        case StandingQueryLocalEvents.Property(key) =>
          toReturn.watchingForProperty.getOrElseUpdate(key, mutable.Set.empty) += handler

        case StandingQueryLocalEvents.Edge(Some(key)) =>
          toReturn.watchingForEdge.getOrElseUpdate(key, mutable.Set.empty) += handler

        case StandingQueryLocalEvents.Edge(None) =>
          toReturn.watchingForAnyEdge += handler
      }
    }

    (toReturn, removed.result())
  }
}
