package com.thatdot.quine.graph

import scala.collection.mutable

import com.thatdot.quine.graph.StandingQueryLocalEventIndex.EventSubscriber
import com.thatdot.quine.graph.cypher.StandingQueryState
import com.thatdot.quine.graph.edgecollection.EdgeCollectionView
import com.thatdot.quine.model
import com.thatdot.quine.model.{And, DomainGraphBranch, Mu, MuVar, Not, Or, PropertyValue, SingleBranch, Test}

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
  def extractWatchableEvents(branch: DomainGraphBranch[Test]): Set[StandingQueryLocalEvents] = {

    /** Recursive helper to extract the StandingQueryLocalEvents as described.
      *
      * For performance, all intermediate states are Seqs (or, where possible, SeqViews),
      * only converting to a Set at the end (so that, for example, uniqueness doesn't need to be checked at each
      * `map` and `++`)
      */
    def extractWatchables(
      branch: DomainGraphBranch[model.Test],
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

    // optimization: only convert Seq => Set once, to save time checking uniqueness over intermediate states
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
          NodeChangeEvent.PropertySet(key, propVal)
        }

      case StandingQueryLocalEvents.Edge(Some(key)) =>
        watchingForEdge.getOrElseUpdate(key, mutable.Set.empty) += handler
        edges.matching(key).toSeq.map { halfEdge =>
          NodeChangeEvent.EdgeAdded(halfEdge)
        }

      case StandingQueryLocalEvents.Edge(None) =>
        watchingForAnyEdge.add(handler)
        edges.all.toSeq.map { halfEdge =>
          NodeChangeEvent.EdgeAdded(halfEdge)
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

  /** Get the subscribers interested in a given node event */
  def standingQueriesWatchingNodeEvent(event: NodeChangeEvent): Iterator[EventSubscriber] =
    event match {
      case NodeChangeEvent.EdgeAdded(halfEdge) =>
        watchingForEdge
          .getOrElse(halfEdge.edgeType, List.empty)
          .iterator
          .++(watchingForAnyEdge.iterator)

      case NodeChangeEvent.EdgeRemoved(halfEdge) =>
        watchingForEdge
          .getOrElse(halfEdge.edgeType, List.empty)
          .iterator
          .++(watchingForAnyEdge.iterator)

      case NodeChangeEvent.PropertySet(propKey, _) =>
        watchingForProperty
          .getOrElse(propKey, List.empty)
          .iterator

      case NodeChangeEvent.PropertyRemoved(propKey, _) =>
        watchingForProperty
          .getOrElse(propKey, List.empty)
          .iterator

      case _ => Iterator.empty
    }
}
object StandingQueryLocalEventIndex {

  /** EventSubscribers are the recipients of the event types classifiable by [[StandingQueryLocalEvents]]
    * See the concrete implementations for more detail
    */
  sealed trait EventSubscriber
  object EventSubscriber {
    def apply(sqIdTuple: (StandingQueryId, StandingQueryPartId)): StandingQueryWithId =
      StandingQueryWithId(sqIdTuple._1, sqIdTuple._2)
    def apply(
      branchSubscriptionTuple: (DomainGraphBranch[model.Test], AssumedDomainEdge)
    ): DomainNodeIndexSubscription =
      DomainNodeIndexSubscription(branchSubscriptionTuple._1, branchSubscriptionTuple._2)
  }

  /** A single SQv4 standing query part -- this handles events by passing them to the SQ's state's "onNodeEvents" hook
    * @param queryId
    * @param partId
    * @see [[behavior.CypherStandingBehavior.standingQueries]]
    * @see [[behavior.CypherStandingBehavior.updateCypherSq]]
    */
  final case class StandingQueryWithId(queryId: StandingQueryId, partId: StandingQueryPartId) extends EventSubscriber

  /** A DGB subscription -- this handles events by routing them to multiple other nodes, ie, [[Notifiable]]s
    * @param branch
    * @param assumedEdge
    * @see [[behavior.DomainNodeIndexBehavior.subscribers]]
    * @see [[behavior.DomainNodeIndexBehavior.SubscribersToThisNode.updateAnswerAndNotifySubscribers]]
    */
  final case class DomainNodeIndexSubscription(branch: DomainGraphBranch[model.Test], assumedEdge: AssumedDomainEdge)
      extends EventSubscriber

  def empty: StandingQueryLocalEventIndex = StandingQueryLocalEventIndex(
    mutable.Map.empty[Symbol, mutable.Set[EventSubscriber]],
    mutable.Map.empty[Symbol, mutable.Set[EventSubscriber]],
    mutable.Set.empty[EventSubscriber]
  )

  /** Rebuild the part of the event index based on the provided query states and subscribers
    *
    * @param dgbSubscribers
    * @param standingQueryStates currently set states
    * @return rebuilt index
    */
  def from(
    dgbSubscribers: Iterator[(DomainGraphBranch[model.Test], AssumedDomainEdge)],
    standingQueryStates: Iterator[((StandingQueryId, StandingQueryPartId), StandingQueryState)]
  ): StandingQueryLocalEventIndex = {
    val toReturn = StandingQueryLocalEventIndex.empty

    val dgbEvents = for {
      handler <- dgbSubscribers
      (branch, _) = handler
      event: StandingQueryLocalEvents <- StandingQueryLocalEvents.extractWatchableEvents(branch)
    } yield event -> EventSubscriber(handler)

    val sqStateEvents = for {
      (handler, queryState) <- standingQueryStates
      event <- queryState.relevantEvents
    } yield event -> EventSubscriber(handler)

    (dgbEvents ++ sqStateEvents).map { case (event, handler) =>
      event match {
        case StandingQueryLocalEvents.Property(key) =>
          toReturn.watchingForProperty.getOrElseUpdate(key, mutable.Set.empty) += handler

        case StandingQueryLocalEvents.Edge(Some(key)) =>
          toReturn.watchingForEdge.getOrElseUpdate(key, mutable.Set.empty) += handler

        case StandingQueryLocalEvents.Edge(None) =>
          toReturn.watchingForAnyEdge += handler
      }
    }

    toReturn
  }
}
