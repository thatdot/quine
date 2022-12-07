package com.thatdot.quine.graph.behavior

import scala.annotation.nowarn
import scala.collection.compat._
import scala.collection.{immutable, mutable}
import scala.compat.ExecutionContexts
import scala.concurrent.Future

import akka.actor.{Actor, ActorLogging}
import akka.event.LoggingAdapter
import akka.stream.scaladsl.Source

import com.thatdot.quine.graph.StandingQueryLocalEventIndex.EventSubscriber
import com.thatdot.quine.graph.behavior.DomainNodeIndexBehavior.SubscribersToThisNodeUtil.DistinctIdSubscription
import com.thatdot.quine.graph.messaging.BaseMessage.Done
import com.thatdot.quine.graph.messaging.StandingQueryMessage.{
  CancelDomainNodeSubscription,
  CreateDomainNodeSubscription,
  DomainNodeSubscriptionCommand,
  DomainNodeSubscriptionResult,
  SqResultLike
}
import com.thatdot.quine.graph.messaging.{QuineIdOps, QuineRefOps}
import com.thatdot.quine.graph.{
  BaseNodeActor,
  DomainGraphNodeRegistry,
  DomainIndexEvent,
  LastNotification,
  Notifiable,
  StandingQueryId,
  StandingQueryLocalEvents,
  StandingQueryOpsGraph,
  StandingQueryPattern
}
import com.thatdot.quine.model.DomainGraphNode.{DomainGraphEdge, DomainGraphNodeId}
import com.thatdot.quine.model.{DomainGraphNode, HalfEdge, IdentifiedDomainGraphNode, QuineId, SingleBranch}

/** Conceptual note:
  * Standing queries should really be a subscription to whether the other satisfies a domain node (branch) or not,
  * with updates as that changes (until canceled). This is very similar to indexing behavior, except that indexing has
  * no specific requirement on the value--just requirement to send _the value_ on ALL changes. Standing test if domain
  * (a.k.a.: TestIfDomainNodeSubscription), would simply return a boolean. Nonetheless, it represents external
  * knowledge. This would probably be eventually consistent, but having the option to make this strongly consistent
  * (i.e. subscribers are notified of a change before the change request returns) could be incredibly powerful!
  */
object DomainNodeIndexBehavior {

  /** An index into the current state of downstream matches. Keys are remote QuineIds which may be the next step in a
    * DGB match. Values are maps where the key set is the set of DGBs the QuineId might match, and the value for each
    * such key is Some(match state) or None if this node does not know whether the downstream node matches.
    *
    * @param index   the initial state of the index (useful for restoring from snapshot)
    * @example Map(
    *           QuineId(0x02) -> Map(
    *             dgn1 -> Some(true)
    *             dgn2 -> None
    *         ))
    *         "The node at QID 0x02 last reported matching dgn1, and has not yet reported whether it matches dgn2"
    */
  final case class DomainNodeIndex(
    index: mutable.Map[
      QuineId,
      mutable.Map[DomainGraphNodeId, LastNotification]
    ] = mutable.Map.empty
  ) {

    def contains(id: QuineId): Boolean = index.contains(id)
    def contains(
      id: QuineId,
      dgnId: DomainGraphNodeId
    ): Boolean = index.get(id).exists(_.contains(dgnId))

    /** Ensure an index into the state of a downstream node at the provided node is tracked
      *
      * @param downstreamQid the node whose results this index will cache
      * @param dgnId         the downstream DGN to be queried against [[downstreamQid]]
      *                      note: dgnId will refer to a child of a DGN rooted on this node
      * @return true when the index is tracked but not yet populated,
      *         false when the index is tracked and already populated
      *         (That is, the return answers "could I use the index of `downstreamQid+dgnId` to answer queries without
      *         additional messages to downstreamQid)
      */
    def newIndex(
      downstreamQid: QuineId,
      dgnId: DomainGraphNodeId
    ): Boolean =
      if (
        !contains(downstreamQid, dgnId) // don't duplicate subscriptions
      ) {
        // add dgnId to the sub-index at downstreamQid (or initialize that sub-index if it does not exist)
        if (index.contains(downstreamQid)) index(downstreamQid) += (dgnId -> None)
        else index += (downstreamQid -> mutable.Map(dgnId -> None))
        // downstreamQid's sub-index was just initialized, so we definitely need to poll it to answer queries about it
        true
      } else {
        // we only need more information to answer queries about `downstreamQid+dgnId` if we haven't yet cached a result
        // for that pair -- that is, if the LastNotification (from downstreamQid about dgnId) isEmpty
        index(downstreamQid)(dgnId).isEmpty
      }

    /** Remove the index tracking [[testBranch]] on [[id]], if any
      *
      * @see [[newIndex]] (dual)
      * @return Some last result reported for the provided index entry, or None if the provided ID is not known to track
      *         the provided node
      *         TODO if an edge is removed, the index should be removed...
      */
    def removeIndex(
      id: QuineId,
      dgnId: DomainGraphNodeId
    ): Option[(QuineId, LastNotification)] =
      if (contains(id, dgnId)) {
        val removedIndexEntry = index(id).remove(dgnId).map(id -> _)
        if (index(id).isEmpty) {
          index.remove(id)
        }
        removedIndexEntry
      } else None

    /** Remove all indices into the state of the provided node
      *
      * Not supernode-safe: Roughly O(nk) where n is number of edges and k is number of standing queries (on this node)
      * TODO restructure [[index]] to be DGB ->> (id ->> lastNotification) instead of id ->> (DGB ->> lastNotification)
      * This change will make this O(1) without affecting performance of other functions on this object
      *
      * @return the last known state for each downstream subscription
      */
    def removeAllIndicesInefficiently(
      dgnId: DomainGraphNodeId
    ): Iterable[(QuineId, LastNotification)] = index.keys
      .flatMap { id =>
        removeIndex(id, dgnId)
      }

    /** Update (or add) an index tracking the last result of `dgnId` rooted on `fromOther`.
      *
      * @param fromOther the remote node
      * @param dgnId the node being tested by the node at `fromOther`
      * @param result the last result reported by `fromOther`
      * @param relatedQueries top-level queries that may care about `fromOther`'s match.
      *                       As an optimization, if all of these are no longer running, skip creating the index.
      */
    def updateResult(
      fromOther: QuineId,
      dgnId: DomainGraphNodeId,
      result: Boolean,
      relatedQueries: Set[StandingQueryId]
    )(implicit graph: StandingQueryOpsGraph, log: LoggingAdapter): Unit =
      if (index contains fromOther) index(fromOther)(dgnId) = Some(result)
      else {
        // if at least one related query is still active in the graph
        if (relatedQueries.exists(graph.runningStandingQuery(_).nonEmpty)) {
          index += (fromOther -> mutable.Map(dgnId -> Some(result)))
        } else {
          // intentionally ignore because this update is about [a] SQ[s] we know to be deleted
          log.info(
            s"Declining to create a DomainNodeIndex entry tracking node: ${fromOther} for a deleted Standing Query"
          )
        }
      }

    def lookup(
      id: QuineId,
      dgnId: DomainGraphNodeId
    ): Option[Boolean] =
      index.get(id).flatMap(_.get(dgnId).flatten)
  }

  object NodeParentIndex {

    /** Conservatively reconstruct the [[nodeParentIndex]] from the provided [[domainNodeIndex]] and a collection
      * of nodes rooted at this node (ie, the keys in [[DomainNodeIndexBehavior.SubscribersToThisNode]]).
      *
      * INV: The reconstructed index loaded by this function is always at least as complete as the original index.
      * In particular, the reconstructed index may contain child->parent associations for which no
      * [[DomainNodeSubscriptionResult]] will be received.
      *
      * Example in which restored and thoroughgoing indices may vary:
      *
      * Given standing queries X, Y with patterns Px, Py:
      * Px watches for ({foo: true})-->({name: "A"})
      * Py watches for ({bar: true})-->({name: "A"})
      * Name the sub-pattern ({name: "A"}) Pshared
      *
      * Suppose this node has an outgoing edge to a node 0x01 matching Pshared, and properties foo = true, bar = false
      *
      * Then, this node's subscribers will contain Px -> ({X}, true), Py -> ({Y}, false)
      * This node's DomainNodeIndex will contain (0x01 -> (Pshared -> true))
      *
      * The thoroughgoing NodeParentIndex might not contain Pshared -> Py, but the restored index will (both must
      * contain Pshared -> Px)
      *
      * @return tuple containing [[NodeParentIndex]] and [[DomainGraphNodeId]]s that are not found in the registry
      */
    private[graph] def reconstruct(
      domainNodeIndex: DomainNodeIndex,
      nodesRootedHere: Iterable[DomainGraphNodeId],
      dgnRegistry: DomainGraphNodeRegistry
    ): (NodeParentIndex, Iterable[DomainGraphNodeId]) = {
      var idx = NodeParentIndex()
      val removed = Iterable.newBuilder[DomainGraphNodeId]
      // First, find the child nodes known to this node using the domainNodeIndex.
      // These define the keys of our [[nodeParentIndex]]
      val knownChildDgnIds =
        domainNodeIndex.index.toSeq.view
          .flatMap { case (_, indexedOnPeer) => indexedOnPeer.keys }
          .view // scala 2.13 compat
          .toSeq
      // Then, iterate through the subscriptions to get the nodes this node currently monitors. For each node,
      // if that node has any children that exist in the domainNodeIndex, add a mapping to the [[nodeParentIndex]]
      nodesRootedHere.foreach { parent =>
        dgnRegistry.getDomainGraphNode(parent) match {
          case Some(dgn) =>
            dgn.children
              .filter(knownChildDgnIds.contains)
              .foreach(childDgnId => idx += ((childDgnId, parent)))
          case None =>
            removed += parent
        }
      }
      (idx, removed.result())
    }
  }

  /** An index to help route subscription notifications upstream along a DGN.
    * This helps efficiently answer questions of the form "Given a downstream DGN `x` from a
    * DomainNodeSubscriptionResult, which DGNs that are keys of [[subscribers]] are parents of `x`?
    *
    * Without this index, every time a DomainNodeSubscriptionResult is received, this node would need to re-test each
    * entry in the subscribers map to see if the key is relevant.
    *
    * This index is separate from [[subscribers]] because a single downstream DGN can be a child of multiple other DGBs.
    */
  final case class NodeParentIndex(
    knownParents: Map[DomainGraphNodeId, Set[
      DomainGraphNodeId
    ]] = Map.empty
  ) {

    // All known parent nodes of [[dgnId]], according to [[knownParents]]
    def parentNodesOf(
      dgnId: DomainGraphNodeId
    ): Set[DomainGraphNodeId] =
      knownParents.getOrElse(dgnId, Set.empty)

    def +(
      childParentTuple: (
        DomainGraphNodeId,
        DomainGraphNodeId
      )
    ): NodeParentIndex = {
      val (child, parent) = childParentTuple
      copy(knownParents = knownParents.updatedWith(child) {
        case Some(parents) => Some(parents + parent)
        case None => Some(Set(parent))
      })
    }

    /** Create a copy of this with no parents registered for `child`
      */
    def --(
      child: DomainGraphNodeId
    ): NodeParentIndex = copy(knownParents = knownParents - child)

    /** Create a copy of this with all but the specified parent registered for `child`
      */
    def -(
      childParentTuple: (
        DomainGraphNodeId,
        DomainGraphNodeId
      )
    ): NodeParentIndex = {
      val (child, parent) = childParentTuple
      val newParents = parentNodesOf(child) - parent
      if (newParents.isEmpty)
        this -- child
      else
        copy(knownParents = knownParents.updated(child, newParents))
    }

    def knownChildren: Iterable[DomainGraphNodeId] = knownParents.keys
  }

  // TODO make this the companion object of DomainNodeIndexBehavior.SubscribersToThisNode once that type is unnested
  object SubscribersToThisNodeUtil {

    /** @param subscribers the places (nodes and top-level result buffers) to which results should be reported
      *                    NB on a supernode, subscribers may be extremely large!
      * @param lastNotification the last notification sent to subscribers
      * @param relatedQueries the top-level query IDs for which this subscription may be used to calculate answers
      */
    final case class DistinctIdSubscription(
      subscribers: Set[Notifiable] = Set.empty,
      lastNotification: LastNotification = None,
      relatedQueries: Set[StandingQueryId] = Set.empty
    ) {
      def addSubscriber(subscriber: Notifiable): DistinctIdSubscription =
        copy(subscribers = subscribers + subscriber)
      def removeSubscriber(subscriber: Notifiable): DistinctIdSubscription =
        copy(subscribers = subscribers - subscriber)

      def addRelatedQueries(newRelatedQueries: Set[StandingQueryId]): DistinctIdSubscription =
        copy(relatedQueries = relatedQueries union newRelatedQueries)
      def addRelatedQuery(relatedQuery: StandingQueryId): DistinctIdSubscription =
        addRelatedQueries(Set(relatedQuery))
      def removeRelatedQuery(relatedQuery: StandingQueryId): DistinctIdSubscription =
        copy(relatedQueries = relatedQueries - relatedQuery)

      // Infix sugaring support
      def +(subscriber: Notifiable): DistinctIdSubscription = addSubscriber(subscriber)
      def -(subscriber: Notifiable): DistinctIdSubscription = removeSubscriber(subscriber)

      def ++(newRelatedQueries: Set[StandingQueryId]): DistinctIdSubscription = addRelatedQueries(newRelatedQueries)
      def +(relatedQuery: StandingQueryId): DistinctIdSubscription = addRelatedQuery(relatedQuery)
      def -(relatedQuery: StandingQueryId): DistinctIdSubscription = removeRelatedQuery(relatedQuery)

      def notified(notification: Boolean): DistinctIdSubscription = copy(lastNotification = Some(notification))
    }
  }
}

trait DomainNodeIndexBehavior
    extends Actor
    with ActorLogging
    with BaseNodeActor
    with DomainNodeTests
    with QuineIdOps
    with QuineRefOps
    with StandingQueryBehavior {
  import DomainNodeIndexBehavior._

  protected val dgnRegistry: DomainGraphNodeRegistry

  /** @see [[SubscribersToThisNode]]
    */
  protected def domainGraphSubscribers: SubscribersToThisNode

  /** @see [[DomainNodeIndex]]
    */
  protected def domainNodeIndex: DomainNodeIndex

  /** @see [[NodeParentIndex]]
    */
  protected var domainGraphNodeParentIndex: NodeParentIndex

  protected def processDomainIndexEvent(
    event: DomainIndexEvent
  ): Future[Done.type]

  /** Called once on node wakeup, this updates DistinctID SQs.
    *  - adds new DistinctID SQs not already in the subscribers
    *  - removes SQs no longer in the graph state
    */
  protected def updateDistinctIdStandingQueriesOnNode(shouldSendReplies: Boolean): Unit = {
    // Register new SQs in graph state but not in the subscribers
    // NOTE: we cannot use `+=` because if already registered we want to avoid duplicating the result
    for {
      (sqId, runningSq) <- graph.runningStandingQueries
      query <- runningSq.query.query match {
        case dgnPattern: StandingQueryPattern.DomainGraphNodeStandingQueryPattern => Some(dgnPattern.dgnId)
        case _ => None
      }
    } {
      val subscriber = Right(sqId)
      val alreadySubscribed = domainGraphSubscribers.containsSubscriber(query, subscriber, sqId)
      if (!alreadySubscribed) {
        receiveDomainNodeSubscription(subscriber, query, Set(sqId), shouldSendReplies = true)
      }
    }

    // Remove old SQs in subscribers but no longer present in graph state
    for {
      (query, DistinctIdSubscription(subscribers, _, sqIds)) <- domainGraphSubscribers.subscribersToThisNode
      if sqIds.forall(graph.runningStandingQuery(_).isEmpty)
      subscriber <- subscribers
    } cancelSubscription(query, Some(subscriber), shouldSendReplies = true)
  }

  protected def domainNodeIndexBehavior(command: DomainNodeSubscriptionCommand): Unit = {
    // Convert Akka message model to node journal model
    val event = command match {
      case CreateDomainNodeSubscription(dgnId, Left(quineId), relatedQueries) =>
        DomainIndexEvent.CreateDomainNodeSubscription(dgnId, quineId, relatedQueries)
      case CreateDomainNodeSubscription(dgnId, Right(standingQueryId), relatedQueries) =>
        DomainIndexEvent.CreateDomainStandingQuerySubscription(dgnId, standingQueryId, relatedQueries)
      case DomainNodeSubscriptionResult(from, dgnId, result) =>
        DomainIndexEvent.DomainNodeSubscriptionResult(from, dgnId, result)
      case CancelDomainNodeSubscription(dgnId, alreadyCancelledSubscriber) =>
        DomainIndexEvent.CancelDomainNodeSubscription(dgnId, alreadyCancelledSubscriber)
    }
    val _ = processDomainIndexEvent(event) // TODO Do not discard this Future returned by processEvent (QU-819)
  }

  /** Given a query, produce a set of all the edges coming off the root of the
    * query paired with a set of edges that match in the graph
    */
  private[this] def resolveDomainEdgesWithIndex(
    testDgn: DomainGraphNode.Single
  ): Seq[(DomainGraphEdge, Set[(HalfEdge, Option[Boolean])])] =
    testDgn.nextNodes.flatMap { domainEdge =>
      val edgeResults: Set[(HalfEdge, Option[Boolean])] = edges
        .matching(domainEdge.edge)
        .map { (e: HalfEdge) =>
          e -> domainNodeIndex.lookup(e.other, domainEdge.dgnId)
        }
        .toSet
      val maxAllowedMatches = domainEdge.constraints.maxMatch.getOrElse(Int.MaxValue)
      if (edgeResults.size < domainEdge.constraints.min || edgeResults.size > maxAllowedMatches) Seq.empty
      else Seq(domainEdge -> edgeResults)
    }

  protected[this] def edgesSatisfiedByIndex(
    testBranch: DomainGraphNode.Single
  ): Option[Boolean] = {
    var missingInformation = false
    // Keys are domain edges, values are all node IDs reachable via Quine [half-]edges satisfying the domain edges
    val edgeResolutions: Seq[(DomainGraphEdge, Set[QuineId])] =
      resolveDomainEdgesWithIndex(testBranch)
        .map { case (domainEdge, halfEdges) =>
          // Neighboring QuineIds that match both the [[DomainGraphEdge]] and the DGB across that edge
          val matchingQids = halfEdges.collect { case (HalfEdge(_, _, qid), Some(true)) => qid }
          // if all half edges matching this domain edge have not yet returned an answer, we are missing
          // information and will need to poll those nodes to update the DomainNodeIndex
          // TODO by corollary: If there exists a negative-answering edge and no positive-answering edge,
          //      we consider the DomainGraphEdge to necessarily *not* exist, regardless of what other half edges
          //      may be left unresolved
          if (matchingQids.isEmpty && halfEdges.forall { case (_, m) => m.isEmpty })
            missingInformation = true
          domainEdge -> matchingQids
        }
    // If all half-edges matching any domain edge have not yet returned an answer, we must poll those edges to update
    // our DomainNodeIndex
    if (missingInformation) return None
    // If no half-edges were resolved, this means either no DomainGraphEdges supplied had constraints for which we
    // could match the desired multiplicity constraints, or else no DomainGraphEdges were supplied at all.
    // TODO if we can remove edge multiplicity constraints, this should be the first case, and instead be "if the
    //      testBranch has no next edges"
    else if (edgeResolutions.isEmpty) return Some(true)

    /* Build up an iterator of the sets of nodes that match the edges. During
     * this process, we make sure that no two edges are matched by the same
     * node.
     *
     * At the end of the process, we don't really care about the sets of IDs
     * that constitute matches - just that there is more than one (using
     * `Iterator` allows us to do this somewhat efficiently)!
     */
    val matchSets: Iterator[Set[QuineId]] =
      edgeResolutions.foldLeft(Iterator(Set.empty[QuineId])) {
        case (qidSetMatches: Iterator[Set[QuineId]], (_, qidsForEdge: Set[QuineId])) =>
          for {
            qidSetMatch <- qidSetMatches
            newQid <- qidsForEdge -- qidSetMatch
          } yield (qidSetMatch + newQid)
      }

    Some(matchSets.hasNext)
  }

  /** Register a new subscriber for the node `dgnId` rooted at this node
    *
    * @param from the new subscriber to which results should be reported
    * @param dgnId the DGN against whose root this node should be compared
    * @param relatedQueries the top-level query IDs for which this subscription may be used to calculate answers
    */
  protected[this] def receiveDomainNodeSubscription(
    from: Notifiable,
    dgnId: DomainGraphNodeId,
    relatedQueries: Set[StandingQueryId],
    shouldSendReplies: Boolean
  ): Unit = {
    domainGraphSubscribers.add(from, dgnId, relatedQueries)
    val existingAnswerOpt = domainGraphSubscribers.getAnswer(dgnId)
    existingAnswerOpt match {
      case Some(result) =>
        conditionallyReplyToAll(
          Set(from),
          DomainNodeSubscriptionResult(qid, dgnId, result),
          shouldSendReplies
        )
        ()
      case None =>
        dgnRegistry.withIdentifiedDomainGraphNode(dgnId)(
          domainGraphSubscribers.updateAnswerAndNotifySubscribers(_, shouldSendReplies)
        )
        ()
    }
  }

  /** Check for any subscriptions `dgn` may need in order to answer the question: "is dgn consistent with
    * a tree rooted at this node?"
    */
  protected def ensureSubscriptionToDomainEdges(
    dgn: IdentifiedDomainGraphNode,
    relatedQueries: Set[StandingQueryId],
    shouldSendReplies: Boolean
  ): Unit = {
    val childNodes = dgn.domainGraphNode.children
    // register subscriptions in DomainNodeIndex, tracking which QIDs' entries were updated
    val indexedQidsUpdated = dgn.domainGraphNode match {
      case DomainGraphNode.Single(_, _, nextDomainEdges, _) =>
        for {
          c <- nextDomainEdges
          downstreamDgnId = c.dgnId
          acrossEdge <- edges.matching(c.edge)
          downstreamQid = acrossEdge.other
          idxUpdated = domainNodeIndex
            .newIndex(
              downstreamQid,
              downstreamDgnId
            )
          if idxUpdated && shouldSendReplies
          _ = downstreamQid ! CreateDomainNodeSubscription(
            downstreamDgnId,
            Left(qid),
            relatedQueries
          )
        } yield downstreamQid
      // these combinators all index other local nodes
      case DomainGraphNode.And(_) | DomainGraphNode.Or(_) | DomainGraphNode.Not(_) =>
        for {
          childDgnId <- childNodes
          idxUpdated = domainNodeIndex.newIndex(qid, childDgnId)
          if idxUpdated && shouldSendReplies
          _ = self ! CreateDomainNodeSubscription(childDgnId, Left(qid), relatedQueries)
        } yield qid

      case DomainGraphNode.Mu(_, _) | DomainGraphNode.MuVar(_) => ???
    }
    if (indexedQidsUpdated.nonEmpty) {
      updateRelevantToSnapshotOccurred()
    }
    // register each new parental relationship
    for {
      childNodeDgnId <- childNodes
    } domainGraphNodeParentIndex += ((childNodeDgnId, dgn.dgnId))
  }

  protected[this] def receiveIndexUpdate(
    fromOther: QuineId,
    otherDgnId: DomainGraphNodeId,
    result: Boolean,
    shouldSendReplies: Boolean
  ): Unit = {
    val relatedQueries =
      domainGraphNodeParentIndex.parentNodesOf(otherDgnId) flatMap { dgnId =>
        domainGraphSubscribers.getRelatedQueries(dgnId)
      }
    domainNodeIndex.updateResult(fromOther, otherDgnId, result, relatedQueries)(graph, log)
    domainGraphSubscribers.updateAnswerAndPropagateToRelevantSubscribers(otherDgnId, shouldSendReplies)
    updateRelevantToSnapshotOccurred()
  }

  /** Remove state used to track `dgnId`'s completion at this node for `subscriber`. If `subscriber` is the last
    * Notifiable interested in `dgnId`, remove all state used to track `dgnId`'s completion from this node
    * and propagate the cancellation.
    *
    * State removed might include upstream subscriptions to this node (from [[domainGraphSubscribers]]), downstream subscriptions
    * from this node (from [[domainNodeIndex]]), child->parent mappings tracking children of `dgnId` (from
    * [[domainGraphNodeParentIndex]]), and local events watched by the SQ (from [[localEventIndex]])
    *
    * This always propagates "down" a standing query (ie, from the global subscriber to the node at the root of the SQ)
    */
  protected[this] def cancelSubscription(
    dgnId: DomainGraphNodeId,
    subscriber: Option[Notifiable], // TODO just move this to the caller only
    shouldSendReplies: Boolean
  ): Unit = {
    // update [[subscribers]]
    val abandoned = subscriber.map(s => domainGraphSubscribers.removeSubscriber(s, dgnId)).getOrElse(Map.empty)

    val _ = dgnRegistry.withDomainGraphNode(dgnId) { dgn =>
      val nextNodesToRemove = abandoned match {
        case empty if empty.isEmpty => // there are other subscribers to dgnId, so don't remove the local state about it
          None
        case singleton if singleton.keySet == Set(dgnId) =>
          // This was the last subscriber that cared about this node -- clean up state for dgnId and continue
          // propagating
          Some(dgn.children)

        case wrongNodesRemoved =>
          // indicates a bug in [[subscribers.remove]]: we removed more nodes than the one we intended to
          log.info(
            s"""Expected to clear a specific DGN from this node, instead started deleting multiple. Re-subscribing the
               |inadvertently removed DGNs. Expected: $dgn but found: ${wrongNodesRemoved.size} node[s]:
               |${wrongNodesRemoved.toList}""".stripMargin.replace('\n', ' ')
          )
          // re-subscribe any extra nodes removed
          (wrongNodesRemoved - dgnId).foreach {
            case (
                  resubNode,
                  SubscribersToThisNodeUtil.DistinctIdSubscription(resubSubscribers, _, relatedQueries)
                ) =>
              for {
                resubSubscriber <- resubSubscribers
              } domainGraphSubscribers.add(resubSubscriber, resubNode, relatedQueries)
          }

          // if the correct node was among those originally removed, then continue removing it despite the bug
          if (wrongNodesRemoved.contains(dgnId))
            Some(dgn.children)
          else // we removed the completely wrong set of nodes - don't continue removing state
            None
      }

      nextNodesToRemove match {
        case Some(downstreamNodes) =>
          // update [[localEventIndex]]
          dgnRegistry.withDomainGraphBranch(dgnId) {
            StandingQueryLocalEvents
              .extractWatchableEvents(_)
              .foreach(event => localEventIndex.unregisterStandingQuery(EventSubscriber(dgnId), event))
          }
          for {
            downstreamNode <- downstreamNodes
          } {
            domainGraphNodeParentIndex -= (downstreamNode -> dgnId)
            val lastDownstreamResults = domainNodeIndex.removeAllIndicesInefficiently(downstreamNode)

            // TODO: DON'T send messages to cancel subscriptions from individual nodes. This should be done from the
            //       shard exactly once to all awake nodes when the SQ is removed.
            // propagate the cancellation to any awake nodes representing potential children of this DGB
            // see [[NodeActorMailbox.shouldIgnoreWhenSleeping]]
            if (shouldSendReplies) for {
              (downstreamQid, _) <- lastDownstreamResults
            } downstreamQid ! CancelDomainNodeSubscription(downstreamNode, qid)
          }
        case None =>
        // None means don't continue clearing out state
      }
      // [[domainNodeIndex]] and [[subscribers]] are both snapshotted -- so report that they (may) have been updated
      updateRelevantToSnapshotOccurred()
    }
  }

  /** If [[shouldSendReplies]], begin asynchronously notifying all [[notifiables]] of [[msg]]
    * @return a future that completes when all notifications have been sent (though not necessarily received yet)
    */
  private[this] def conditionallyReplyToAll(
    notifiables: immutable.Iterable[Notifiable],
    msg: SqResultLike,
    shouldSendReplies: Boolean
  ): Future[Unit] =
    if (!shouldSendReplies) Future.unit
    else
      Source(notifiables)
        .runForeach {
          case Left(quineId) => quineId ! msg
          case Right(sqId) =>
            graph.reportStandingResult(sqId, msg) // TODO should this really be suppressed by shouldSendReplies?
            ()
        }
        .map(_ => ())(ExecutionContexts.parasitic)

  /** An index of upstream subscribers to this node for a given DGB. Keys are DGBs registered on this node, values are
    * the [[Notifiable]]s (eg, nodes or global SQ result queues) subscribed to this node, paired with the last result
    * sent to those [[Notifiable]]s.
    *
    * @example
    *  Map(
    *     dgn1 ->
    *       (Set(Left(QuineId(0x01))) -> Some(true))
    *     dgn2 ->
    *       (Set(Left(QuineId(0x01))) -> None)
    *  )
    *  "Concerning dgn1: this node last notified its subscribers (QID 0x01) that dgn1 matches on this node."
    *  "Concerning dgn2: this node has not yet notified its subscribers (QID 0x01) whether dgn2 matches on this node".
    */
  case class SubscribersToThisNode(
    subscribersToThisNode: mutable.Map[
      DomainGraphNodeId,
      SubscribersToThisNodeUtil.DistinctIdSubscription
    ] = mutable.Map.empty
  ) {
    import SubscribersToThisNodeUtil.DistinctIdSubscription
    def containsSubscriber(
      dgnId: DomainGraphNodeId,
      subscriber: Notifiable,
      forQuery: StandingQueryId
    ): Boolean =
      subscribersToThisNode
        .get(dgnId)
        .exists { case DistinctIdSubscription(subscribers, _, relatedQueries) =>
          subscribers.contains(subscriber) && relatedQueries.contains(forQuery)
        }

    def tracksNode(dgnId: DomainGraphNodeId): Boolean = subscribersToThisNode.contains(dgnId)

    def getAnswer(dgnId: DomainGraphNodeId): Option[Boolean] =
      subscribersToThisNode.get(dgnId).flatMap(_.lastNotification)

    def getRelatedQueries(
      dgnId: DomainGraphNodeId
    ): Set[StandingQueryId] =
      subscribersToThisNode.get(dgnId).toSeq.flatMap(_.relatedQueries).toSet

    def add(
      from: Notifiable,
      dgnId: DomainGraphNodeId,
      relatedQueries: Set[StandingQueryId]
    ): Unit =
      if (tracksNode(dgnId)) {
        val subscription = subscribersToThisNode(dgnId)
        if (!subscription.subscribers.contains(from) || !relatedQueries.subsetOf(subscription.relatedQueries)) {
          updateRelevantToSnapshotOccurred()
          subscribersToThisNode(dgnId) += from
          subscribersToThisNode(dgnId) ++= relatedQueries
          ()
        }
      } else { // [[from]] is the first subscriber to this DGB, so register the DGB and add [[from]] as a subscriber
        updateRelevantToSnapshotOccurred()
        dgnRegistry.withDomainGraphBranch(dgnId) {
          StandingQueryLocalEvents
            .extractWatchableEvents(_)
            .foreach { event =>
              localEventIndex.registerStandingQuery(EventSubscriber(dgnId), event, properties, edges)
            }
        }
        subscribersToThisNode(dgnId) =
          DistinctIdSubscription(subscribers = Set(from), lastNotification = None, relatedQueries = relatedQueries)
        ()
      }

    // Returns: the subscriptions removed from if and only if there are no other Notifiables in those subscriptions.
    private[DomainNodeIndexBehavior] def removeSubscriber(
      subscriber: Notifiable,
      dgnId: DomainGraphNodeId
    ): Map[DomainGraphNodeId, DistinctIdSubscription] =
      subscribersToThisNode
        .get(dgnId)
        .map { case subscription @ DistinctIdSubscription(notifiables, _, _) =>
          if (notifiables == Set(subscriber)) {
            subscribersToThisNode -= dgnId // remove the whole node if no more subscriptions (no one left to tell)
            Map(dgnId -> subscription)
          } else {
            subscribersToThisNode(dgnId) -= subscriber // else remove just the requested subscriber
            Map.empty[DomainGraphNodeId, DistinctIdSubscription]
          }
        }
        .getOrElse(Map.empty)

    def removeSubscribersOf(
      dgnIds: Iterable[DomainGraphNodeId]
    ): Unit = subscribersToThisNode --= dgnIds

    @deprecated(
      "Use updateAnswerAndPropagateToRelevantSubscribers for the propagation case, and the identity of the DGB for the wake-up/initial registration case",
      "Nov 2021"
    )
    private[this] def updateAnswerAndNotifySubscribersInefficiently(shouldSendReplies: Boolean): Unit =
      subscribersToThisNode.keys.foreach { dgnId =>
        dgnRegistry.getIdentifiedDomainGraphNode(dgnId) match {
          case Some(dgn) => updateAnswerAndNotifySubscribers(dgn, shouldSendReplies)
          case None => subscribersToThisNode -= dgnId
        }
      }

    def updateAnswerAndPropagateToRelevantSubscribers(
      downstreamNode: DomainGraphNodeId,
      shouldSendReplies: Boolean
    ): Unit = {
      val parentNodes = domainGraphNodeParentIndex.parentNodesOf(downstreamNode)
      // this should always be the case: we shouldn't be getting subscription results for DGBs that we don't track
      // a parent of
      if (parentNodes.nonEmpty) {
        parentNodes foreach { dgnId =>
          dgnRegistry.getIdentifiedDomainGraphNode(dgnId) match {
            case Some(dgn) => updateAnswerAndNotifySubscribers(dgn, shouldSendReplies)
            case None => domainGraphNodeParentIndex - ((downstreamNode, dgnId))
          }
        }
      } else {
        // recovery case: If this is hit, there is a bug in the protocol -- either a subscription result was received
        // for an unknown subscription, or the nodeParentIndex fell out of sync

        // attempt recovery
        val (recoveredIndex, removed) =
          NodeParentIndex.reconstruct(
            domainNodeIndex,
            domainGraphSubscribers.subscribersToThisNode.keys,
            dgnRegistry
          )
        domainGraphSubscribers.subscribersToThisNode --= removed
        val parentsAfterRecovery = recoveredIndex.parentNodesOf(downstreamNode)
        if (parentsAfterRecovery.nonEmpty) {
          // recovery succeeded -- add recovered entries to nodeParentIndex and continue, logging an INFO-level notice
          // no data was lost, but this is a bug
          log.info(
            s"""Found out-of-sync nodeParentIndex while propagating a DGN result. Previously-untracked DGN ID was:
               |$downstreamNode. Previously only tracking children: ${domainGraphNodeParentIndex.knownChildren.toList}.
               |""".stripMargin.replace('\n', ' ')
          )
          domainGraphNodeParentIndex = recoveredIndex
        } else {
          // recovery failed -- there is either data loss, or a bug in [[NodeParentIndex.reconstruct]], or the usage of
          // [[NodeParentIndex.reconstruct]] (or any combination thereof).
          if (shouldSendReplies)
            log.error(
              s"""While propagating a result of a DGN match, found no upstream subscribers that might care about
                 |an update in the provided downstream node. This may indicate a bug in the DGN registration/indexing
                 |logic. Falling back to trying all locally-tracked DGNs. Orphan (downstream) DGN ID is: $downstreamNode
                 |""".trim.stripMargin.replace('\n', ' ')
            )
          else {
            // if shouldSendReplies == false, we're probably restoring a node from sleep via journals. In this case,
            // an incomplete nodeParentIndex is not surprising
            log.debug(
              s"""While propagating a result of a DGN match, found no upstream subscribers that might care about
                 |an update in the provided downstream node. This may indicate a bug in the DGN registration/indexing
                 |logic. Falling back to trying all locally-tracked DGNs. Orphan (downstream) DGN ID is: $downstreamNode.
                 |However, this is expected during initial journal replay on a node after wake when snapshots are
                 |disabled or otherwise missing.""".stripMargin.replace('\n', ' ')
            )
          }
          updateAnswerAndNotifySubscribersInefficiently(shouldSendReplies): @nowarn
        }
      }
    }

    def updateAnswerAndNotifySubscribers(
      identifiedDomainGraphNode: IdentifiedDomainGraphNode,
      shouldSendReplies: Boolean
    ): Unit = {
      val IdentifiedDomainGraphNode(dgnId, testDgn) = identifiedDomainGraphNode
      testDgn match {
        // TODO this is the only variant used for standing queries
        case single: DomainGraphNode.Single =>
          val matchesLocal = dgnRegistry
            .withDomainGraphBranch(dgnId) {
              case sb: SingleBranch => localTestBranch(sb)
              case _ => false
            }
            .getOrElse(false)
          val edgesSatisfied = edgesSatisfiedByIndex(single)
          // if no subscribers found for the DGN, clear out expired state from other (non-`subscribers`) bookkeeping
          if (!subscribersToThisNode.contains(dgnId)) {
            cancelSubscription(dgnId, None, shouldSendReplies)
          }

          subscribersToThisNode.get(dgnId) foreach {
            case subscription @ DistinctIdSubscription(notifiables, lastNotification, relatedQueries) =>
              (matchesLocal, edgesSatisfied) match {
                // If the query doesn't locally match, don't bother issuing recursive subscriptions
                case (false, _) if !lastNotification.contains(false) && shouldSendReplies =>
                  conditionallyReplyToAll(
                    notifiables,
                    DomainNodeSubscriptionResult(qid, dgnId, result = false),
                    shouldSendReplies
                  )
                  subscribersToThisNode(dgnId) = subscription.notified(false)
                  updateRelevantToSnapshotOccurred()

                // If the query locally matches and we've already got edge results, reply with those
                case (true, Some(result)) if !lastNotification.contains(result) && shouldSendReplies =>
                  conditionallyReplyToAll(
                    notifiables,
                    DomainNodeSubscriptionResult(qid, dgnId, result),
                    shouldSendReplies
                  )
                  subscribersToThisNode(dgnId) = subscription.notified(result)
                  updateRelevantToSnapshotOccurred()

                // If the query locally matches and we don't have edge results, issue subscriptions
                case (true, None) =>
                  ensureSubscriptionToDomainEdges(identifiedDomainGraphNode, relatedQueries, shouldSendReplies)
                case _ => ()
              }
          }

        case DomainGraphNode.And(conjs) =>
          // Collect the state of recursive matches, then "AND" them together using Kleene logic
          val andMatches: Option[Boolean] = conjs
            .foldLeft[Option[Boolean]](Some(true)) { (acc, conj) =>
              val conjResult = domainNodeIndex.lookup(qid, conj)

              // Create a subscription if it isn't already created
              if (conjResult.isEmpty)
                dgnRegistry
                  .withIdentifiedDomainGraphNode(conj)(
                    ensureSubscriptionToDomainEdges(
                      _,
                      subscribersToThisNode.get(dgnId).toSeq.flatMap(_.relatedQueries).toSet,
                      shouldSendReplies
                    )
                  )

              // Kleene AND
              (acc, conjResult) match {
                case (Some(false), _) => Some(false)
                case (_, Some(false)) => Some(false)
                case (Some(true), Some(true)) => Some(true)
                case _ => None
              }
            }

          subscribersToThisNode.get(dgnId).foreach {
            case subscription @ DistinctIdSubscription(notifiables, lastNotification, _) =>
              andMatches match {
                case Some(result) if !lastNotification.contains(result) =>
                  conditionallyReplyToAll(
                    notifiables,
                    DomainNodeSubscriptionResult(qid, dgnId, result),
                    shouldSendReplies
                  )
                  subscribersToThisNode(dgnId) = subscription.notified(result)
                  updateRelevantToSnapshotOccurred()

                case _ => ()
              }
          }

        case DomainGraphNode.Or(disjs) =>
          // Collect the state of recursive matches, then "OR" them together using Kleene logic
          val orMatches: Option[Boolean] = disjs
            .foldLeft[Option[Boolean]](Some(false)) { (acc, disj) =>
              val disjResult = domainNodeIndex.lookup(qid, disj)

              // Create a subscription if it isn't already created
              if (disjResult.isEmpty)
                dgnRegistry
                  .withIdentifiedDomainGraphNode(disj)(
                    ensureSubscriptionToDomainEdges(
                      _,
                      subscribersToThisNode.get(dgnId).toSeq.flatMap(_.relatedQueries).toSet,
                      shouldSendReplies
                    )
                  )

              // Kleene OR
              (acc, disjResult) match {
                case (Some(true), _) => Some(true)
                case (_, Some(true)) => Some(true)
                case (Some(false), Some(false)) => Some(false)
                case _ => None
              }
            }

          subscribersToThisNode.get(dgnId).foreach {
            case subscription @ DistinctIdSubscription(notifiables, lastNotification, _) =>
              orMatches match {
                case Some(result) if !lastNotification.contains(result) =>
                  conditionallyReplyToAll(
                    notifiables,
                    DomainNodeSubscriptionResult(qid, dgnId, result),
                    shouldSendReplies
                  )
                  subscribersToThisNode(dgnId) = subscription.notified(result)
                  updateRelevantToSnapshotOccurred()

                case _ => ()
              }
          }

        case DomainGraphNode.Not(neg) =>
          // Collect the state of the recursive match and "NOT" it using Kleene logic
          val notMatches: Option[Boolean] = domainNodeIndex
            .lookup(qid, neg)
            .map(!_)

          // Create a subscription if it isn't already created
          if (notMatches.isEmpty)
            dgnRegistry
              .withIdentifiedDomainGraphNode(neg)(
                ensureSubscriptionToDomainEdges(
                  _,
                  subscribersToThisNode.get(dgnId).toSeq.flatMap(_.relatedQueries).toSet,
                  shouldSendReplies
                )
              )

          subscribersToThisNode.get(dgnId).foreach {
            case subscription @ DistinctIdSubscription(notifiables, lastNotification, _) =>
              notMatches match {
                case Some(result) if !lastNotification.contains(result) =>
                  conditionallyReplyToAll(
                    notifiables,
                    DomainNodeSubscriptionResult(qid, dgnId, result),
                    shouldSendReplies
                  )
                  subscribersToThisNode(dgnId) = subscription.notified(result)
                  updateRelevantToSnapshotOccurred()

                case _ => ()
              }
          }

        case mu @ (DomainGraphNode.Mu(_, _) | DomainGraphNode.MuVar(_)) =>
          log.error("Standing query test node contains illegal sub-node: {}", mu)
      }
    }
  }
}
