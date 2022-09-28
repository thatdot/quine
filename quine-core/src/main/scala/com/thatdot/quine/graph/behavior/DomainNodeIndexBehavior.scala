package com.thatdot.quine.graph.behavior

import scala.annotation.nowarn
import scala.collection.compat._
import scala.collection.mutable

import akka.actor.{Actor, ActorLogging}
import akka.event.LoggingAdapter

import com.thatdot.quine.graph.StandingQueryLocalEventIndex.EventSubscriber
import com.thatdot.quine.graph.messaging.StandingQueryMessage.{
  CancelDomainNodeSubscription,
  CreateDomainNodeSubscription,
  DomainNodeSubscriptionCommand,
  DomainNodeSubscriptionResult,
  SqResultLike
}
import com.thatdot.quine.graph.messaging.{QuineIdOps, QuineRefOps}
import com.thatdot.quine.graph.{
  AssumedDomainEdge,
  BaseNodeActor,
  LastNotification,
  Notifiable,
  StandingQueryId,
  StandingQueryLocalEvents,
  StandingQueryOpsGraph,
  StandingQueryPattern
}
import com.thatdot.quine.model
import com.thatdot.quine.model.{DomainEdge, DomainGraphBranch, HalfEdge, QuineId}

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
    *             (dgb1, assumedEdge1) -> Some(true)
    *             (dgb2, assumedEdge2) -> None
    *         ))
    *         "The node at QID 0x02 last reported matching dgb1, and has not yet reported whether it matches dgb2"
    */
  final case class DomainNodeIndex(
    index: mutable.Map[
      QuineId,
      mutable.Map[(DomainGraphBranch, AssumedDomainEdge), LastNotification]
    ] = mutable.Map.empty
  ) {

    def contains(id: QuineId): Boolean = index.contains(id)
    def contains(
      id: QuineId,
      testBranch: DomainGraphBranch,
      assumedEdge: AssumedDomainEdge
    ): Boolean = index.get(id).exists(_.contains(testBranch -> assumedEdge))

    /** Create an index into the state of a downstream branch at the provided node
      *
      * @param id          the node whose results this index will cache
      * @param testBranch  the downstream branch to be rooted at [[id]]
      * @param assumedEdge the assumed edge for the provided branch
      * @return whether an update was applied
      */
    def newIndex(
      id: QuineId,
      testBranch: DomainGraphBranch,
      assumedEdge: AssumedDomainEdge
    ): Boolean = if (
      !contains(id, testBranch, assumedEdge) // don't duplicate subscriptions
    ) {
      if (index.contains(id)) index(id) += (testBranch -> assumedEdge -> None)
      else index += (id -> mutable.Map(testBranch -> assumedEdge -> None))
      true
    } else false

    /** Remove the index tracking [[testBranch]] on [[id]], if any
      *
      * @see [[newIndex]] (dual)
      * @return Some last result reported for the provided index entry, or None if the provided ID is not known to track
      *         the provided branch
      *         TODO if an edge is removed, the index should be removed...
      */
    def removeIndex(
      id: QuineId,
      testBranch: DomainGraphBranch,
      assumedEdge: AssumedDomainEdge
    ): Option[(QuineId, LastNotification)] =
      if (contains(id, testBranch, assumedEdge)) {
        val removedIndexEntry = index(id).remove(testBranch -> assumedEdge).map(id -> _)
        if (index(id).isEmpty) {
          index.remove(id)
        }
        removedIndexEntry
      } else None

    /** Remove all indices into the state of the provided branch
      *
      * Not supernode-safe: Roughly O(nk) where n is number of edges and k is number of standing queries (on this node)
      * TODO restructure [[index]] to be DGB ->> (id ->> lastNotification) instead of id ->> (DGB ->> lastNotification)
      * This change will make this O(1) without affecting performance of other functions on this object
      *
      * @return the last known state for each downstream subscription
      */
    def removeAllIndicesInefficiently(
      testBranch: DomainGraphBranch,
      assumedEdge: AssumedDomainEdge
    ): Iterable[(QuineId, LastNotification)] = index.keys
      .flatMap { id =>
        removeIndex(id, testBranch, assumedEdge)
      }

    /** Update (or add) an index tracking the last result of (testBranch, assumedEdge) rooted on `fromOther`.
      *
      * @param fromOther the remote node
      * @param testBranch the branch being tested by the node at `fromOther`
      * @param assumedEdge the DomainEdge assumption under which testBranch is valid
      * @param result the last result reported by `fromOther`
      * @param relatedQueries top-level queries that may care about `fromOther`'s match.
      *                       As an optimization, if all of these are no longer running, skip creating the index.
      */
    def updateResult(
      fromOther: QuineId,
      testBranch: DomainGraphBranch,
      assumedEdge: AssumedDomainEdge,
      result: Boolean,
      relatedQueries: Set[StandingQueryId]
    )(implicit graph: StandingQueryOpsGraph, log: LoggingAdapter): Unit =
      if (index contains fromOther) index(fromOther)(testBranch -> assumedEdge) = Some(result)
      else {
        // if at least one related query is still active in the graph
        if (relatedQueries.exists(graph.runningStandingQuery(_).nonEmpty)) {
          index += (fromOther -> mutable.Map(testBranch -> assumedEdge -> Some(result)))
        } else {
          // intentionally ignore because this update is about [a] SQ[s] we know to be deleted
          log.info(
            s"Declining to create a DomainNodeIndex entry tracking node: ${fromOther} for a deleted Standing Query"
          )
        }
      }

    def lookup(
      id: QuineId,
      testBranch: DomainGraphBranch,
      assumedEdge: AssumedDomainEdge
    ): Option[Boolean] =
      index.get(id).flatMap(_.get(testBranch -> assumedEdge).flatten)
  }

  object BranchParentIndex {

    /** Conservatively reconstruct the [[branchParentIndex]] from the provided [[domainNodeIndex]] and a collection
      * of branches rooted at this node (ie, the keys in [[DomainNodeIndexBehavior.SubscribersToThisNode]]).
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
      * The thoroughgoing BranchParentIndex might not contain Pshared -> Py, but the restored index will (both must
      * contain Pshared -> Px)
      */
    private[graph] def reconstruct(
      domainNodeIndex: DomainNodeIndex,
      branchesRootedHere: Iterable[(DomainGraphBranch, AssumedDomainEdge)]
    ): BranchParentIndex = {
      var idx = BranchParentIndex()
      // First, find the child branches known to this node using the domainNodeIndex.
      // These define the keys of our [[branchParentIndex]]
      val knownChildBranches: Map[DomainGraphBranch, Set[AssumedDomainEdge]] =
        domainNodeIndex.index.toSeq.view
          .flatMap { case (_, indexedOnPeer) => indexedOnPeer.keys }
          .groupBy { case (branch, assumedEdge @ _) => branch }
          .view // scala 2.13 compat
          .mapValues(_.map { case (branch @ _, assumedEdge) => assumedEdge }.toSet)
          .toMap
      // Then, iterate through the subscriptions to get the branches this node currently monitors. For each branch,
      // if that branch has any children that exist in the domainNodeIndex, add a mapping to the [[branchParentIndex]]
      for {
        parent @ (branch, _) <- branchesRootedHere
        childBranch <- branch.children
        if knownChildBranches.contains(childBranch)
        childAssumedEdge <- knownChildBranches(childBranch)
      } idx += ((childBranch -> childAssumedEdge, parent))

      idx
    }
  }

  /** An index to help route subscription notifications upstream along a DGB.
    * This helps efficiently answer questions of the form "Given a downstream DGB `x` from a
    * DomainNodeSubscriptionResult, which DGBs that are keys of [[subscribers]] are parents of `x`?
    *
    * Without this index, every time a DomainNodeSubscriptionResult is received, this node would need to re-test each
    * entry in the subscribers map to see if the key is relevant.
    *
    * This index is separate from [[subscribers]] because a single downstream DGB can be a child of multiple other DGBs.
    */
  final case class BranchParentIndex(
    knownParents: Map[(DomainGraphBranch, AssumedDomainEdge), Set[
      (DomainGraphBranch, AssumedDomainEdge)
    ]] = Map.empty
  ) {

    // All known parent branches of [[testBranch]], according to [[knownParents]]
    def parentBranchesOf(
      testBranch: (DomainGraphBranch, AssumedDomainEdge)
    ): Set[(DomainGraphBranch, AssumedDomainEdge)] =
      knownParents.getOrElse(testBranch, Set.empty)

    def +(
      childParentTuple: (
        (DomainGraphBranch, AssumedDomainEdge),
        (DomainGraphBranch, AssumedDomainEdge)
      )
    ): BranchParentIndex = {
      val (child, parent) = childParentTuple
      copy(knownParents = knownParents.updatedWith(child) {
        case Some(parents) => Some(parents + parent)
        case None => Some(Set(parent))
      })
    }

    /** Create a copy of this with no parents registered for `child`
      */
    def --(
      child: (DomainGraphBranch, AssumedDomainEdge)
    ): BranchParentIndex = copy(knownParents = knownParents - child)

    /** Create a copy of this with all but the specified parent registered for `child`
      */
    def -(
      childParentTuple: (
        (DomainGraphBranch, AssumedDomainEdge),
        (DomainGraphBranch, AssumedDomainEdge)
      )
    ): BranchParentIndex = {
      val (child, parent) = childParentTuple
      val newParents = parentBranchesOf(child) - parent
      if (newParents.isEmpty)
        this -- child
      else
        copy(knownParents = knownParents.updated(child, newParents))
    }

    def knownChildren: Iterable[(DomainGraphBranch, AssumedDomainEdge)] = knownParents.keys
  }

  // TODO make this the companion object of DomainNodeIndexBehavior.SubscribersToThisNode once that type is unnested
  object SubscribersToThisNodeUtil {

    /** @param subscribers the places (nodes and top-level result buffers) to which results should be reported
      * @param lastNotification the last notification sent to subscribers
      * @param relatedQueries the top-level query IDs for which this subscription may be used to calculate answers
      */
    final case class Subscription(
      subscribers: Set[Notifiable] = Set.empty,
      lastNotification: LastNotification = None,
      relatedQueries: Set[StandingQueryId] = Set.empty
    ) {
      def addSubscriber(subscriber: Notifiable): Subscription =
        copy(subscribers = subscribers + subscriber)
      def removeSubscriber(subscriber: Notifiable): Subscription =
        copy(subscribers = subscribers - subscriber)

      def addRelatedQueries(newRelatedQueries: Set[StandingQueryId]): Subscription =
        copy(relatedQueries = relatedQueries union newRelatedQueries)
      def addRelatedQuery(relatedQuery: StandingQueryId): Subscription =
        addRelatedQueries(Set(relatedQuery))
      def removeRelatedQuery(relatedQuery: StandingQueryId): Subscription =
        copy(relatedQueries = relatedQueries - relatedQuery)

      // Infix sugaring support
      def +(subscriber: Notifiable): Subscription = addSubscriber(subscriber)
      def -(subscriber: Notifiable): Subscription = removeSubscriber(subscriber)

      def ++(newRelatedQueries: Set[StandingQueryId]): Subscription = addRelatedQueries(newRelatedQueries)
      def +(relatedQuery: StandingQueryId): Subscription = addRelatedQuery(relatedQuery)
      def -(relatedQuery: StandingQueryId): Subscription = removeRelatedQuery(relatedQuery)

      def notified(notification: Boolean): Subscription = copy(lastNotification = Some(notification))
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

  /** @see [[SubscribersToThisNode]]
    */
  protected var subscribers: SubscribersToThisNode = SubscribersToThisNode()

  /** @see [[DomainNodeIndex]]
    */
  protected var domainNodeIndex: DomainNodeIndex = DomainNodeIndex()

  /** @see [[BranchParentIndex]]
    */
  protected var branchParentIndex: BranchParentIndex = BranchParentIndex()

  /** Called once on node wakeup, this updates universal SQs.
    *
    *    - adds new universal SQs not already in the subscribers
    *    - removes SQs no longer in the graph state (must've been universal
    *      otherwise their cancellation would've involved notifying this node)
    */
  protected def updateUniversalQueriesOnWake(): Unit = {

    // Register new universal SQs in graph state but not in the subscribers
    // NOTE: we cannot use `+=` because if already registered we want to avoid
    //       duplicating the result
    for {
      (universalSqId, universalSq) <- graph.runningStandingQueries
      query <- universalSq.query.query match {
        case branchQuery: StandingQueryPattern.Branch => Some(branchQuery.branch)
        case _ => None
      }
    } {
      val subscriber = Right(universalSqId)
      val alreadySubscribed = subscribers.containsSubscriber(query, None, subscriber, universalSqId)

      if (!alreadySubscribed) {
        receiveDomainNodeSubscription(subscriber, query, None, Set(universalSqId))
      }
    }

    // Remove old SQs in subscribers but no longer present in graph state
    for {
      ((query, assumedEdge), SubscribersToThisNodeUtil.Subscription(subscribers, _, sqIds)) <-
        subscribers.subscribersToThisNode
      if sqIds.forall(graph.runningStandingQuery(_).isEmpty)
      subscriber <- subscribers
    } cancelSubscription(query, assumedEdge, subscriber)
  }

  protected def domainNodeIndexBehavior(command: DomainNodeSubscriptionCommand): Unit = command match {
    case CreateDomainNodeSubscription(testBranch, assumedEdge, subscriber, forQuery) =>
      receiveDomainNodeSubscription(subscriber, testBranch, assumedEdge, forQuery)

    case DomainNodeSubscriptionResult(from, testBranch, assumedEdge, result) =>
      receiveIndexUpdate(from, testBranch, assumedEdge, result)

    case CancelDomainNodeSubscription(testBranch, assumedEdge, fromSubscriber) =>
      cancelSubscription(
        testBranch,
        assumedEdge,
        Left(fromSubscriber): Notifiable
      )
  }

  /** Given a query, produce a set of all the edges coming off the root of the
    * query paired with a set of edges that match in the graph
    */
  private[this] def resolveDomainEdgesWithIndex(
    testBranch: model.SingleBranch,
    assumedEdge: AssumedDomainEdge
  ): List[(DomainEdge, Set[(HalfEdge, Option[Boolean])])] =
    testBranch.nextBranches.flatMap { (domainEdge: DomainEdge) =>
      val edgeResults: Set[(HalfEdge, Option[Boolean])] = edges
        .matching(domainEdge.edge)
        .map { (e: HalfEdge) =>
          e -> domainNodeIndex.lookup(e.other, domainEdge.branch, assumedEdge)
        }
        .toSet
      val maxAllowedMatches = domainEdge.constraints.maxMatch.getOrElse(Int.MaxValue)
      if (edgeResults.size < domainEdge.constraints.min || edgeResults.size > maxAllowedMatches) List.empty
      else List(domainEdge -> edgeResults)
    }

  private[this] def edgesSatisfiedByIndex(
    testBranch: model.SingleBranch,
    assumedEdge: AssumedDomainEdge
  ): Option[Boolean] = {

    /* For each edge required in `testBranch`, find all matching edges in the
     * data and collect their `QuineId`'s
     */
    var missingInformation = false
    val edgeResolutions: List[(DomainEdge, Set[QuineId])] =
      resolveDomainEdgesWithIndex(testBranch, assumedEdge)
        .map { case (domainEdge, halfEdges) =>
          val qids = halfEdges.collect { case (HalfEdge(_, _, qid), Some(true)) => qid }
          if (qids.isEmpty && halfEdges.forall { case (_, m) => m.isEmpty })
            missingInformation = true
          domainEdge -> qids
        }

    if (missingInformation) return None
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

  /** Register a new subscriber for the branch (testBranch, assumedEdge) rooted at this node
    *
    * @param from the new subscriber to which results should be reported
    * @param testBranch the DGB against whose root this node should be compared
    * @param assumedEdge the edge assumption under which the DGB should be tested
    * @param relatedQueries the top-level query IDs for which this subscription may be used to calculate answers
    */
  private[this] def receiveDomainNodeSubscription(
    from: Notifiable,
    testBranch: DomainGraphBranch,
    assumedEdge: AssumedDomainEdge,
    relatedQueries: Set[StandingQueryId]
  ): Unit = {
    subscribers.add(from, testBranch, assumedEdge, relatedQueries)
    val existingAnswerOpt = subscribers.getAnswer(testBranch, assumedEdge)
    existingAnswerOpt match {
      case Some(result) =>
        replyToAll(
          Set(from),
          DomainNodeSubscriptionResult(qid, testBranch, assumedEdge, result)
        )
      case None =>
        subscribers.updateAnswerAndNotifySubscribers(testBranch, assumedEdge)
    }
  }

  /** Check for any subscriptions `testBranch` may need in order to answer the question: "is testBranch consistent with
    * a tree rooted at this node?"
    */
  protected def ensureSubscriptionToDomainEdges(
    testBranch: DomainGraphBranch,
    assumedEdge: AssumedDomainEdge,
    relatedQueries: Set[StandingQueryId]
  ): Unit = {
    val downstreamAssumedEdge = None
    val childBranches = testBranch.children
    // register subscriptions in DomainNodeIndex, tracking which QIDs' entries were updated
    val indexedQidsUpdated = testBranch match {
      case model.SingleBranch(_, _, nextDomainEdges, _) =>
        for {
          c <- nextDomainEdges
          downstreamBranch = c.branch
          acrossEdge <- edges.matching(c.edge)
          downstreamQid = acrossEdge.other
          idxUpdated = domainNodeIndex
            .newIndex(
              downstreamQid,
              downstreamBranch,
              downstreamAssumedEdge
            )
          if idxUpdated
          _ = downstreamQid ! CreateDomainNodeSubscription(
            downstreamBranch,
            downstreamAssumedEdge,
            Left(qid),
            relatedQueries
          )
        } yield downstreamQid
      case (model.And(_) | model.Or(_) | model.Not(_)) => // these combinators all index other local branches
        for {
          childBranch <- childBranches
          idxUpdated = domainNodeIndex.newIndex(qid, childBranch, downstreamAssumedEdge)
          if idxUpdated
          _ = self ! CreateDomainNodeSubscription(childBranch, downstreamAssumedEdge, Left(qid), relatedQueries)
        } yield qid

      case model.Mu(_, _) | model.MuVar(_) => ???
    }
    if (indexedQidsUpdated.nonEmpty) {
      updateRelevantToSnapshotOccurred()
    }
    // register each new parental relationship
    for {
      childBranch <- childBranches
    } branchParentIndex += ((childBranch -> downstreamAssumedEdge, testBranch -> assumedEdge))
  }

  private[this] def receiveIndexUpdate(
    fromOther: QuineId,
    otherTestBranch: DomainGraphBranch,
    otherAssumedEdge: AssumedDomainEdge,
    result: Boolean
  ): Unit = {
    val relatedQueries =
      branchParentIndex.parentBranchesOf(otherTestBranch -> otherAssumedEdge).flatMap { case (branch, assumedEdge) =>
        subscribers.getRelatedQueries(branch, assumedEdge)
      }
    domainNodeIndex.updateResult(fromOther, otherTestBranch, otherAssumedEdge, result, relatedQueries)(graph, log)
    subscribers.updateAnswerAndPropagateToRelevantSubscribers(otherTestBranch, otherAssumedEdge)
    updateRelevantToSnapshotOccurred()
  }

  /** Remove state used to track `testBranch`'s completion at this node for `subscriber`. If `subscriber` is the last
    * Notifiable interested in `testBranch`, remove all state used to track `testBranch`'s completion from this node
    * and propagate the cancellation.
    *
    * State removed might include upstream subscriptions to this node (from [[subscribers]]), downstream subscriptions
    * from this node (from [[domainNodeIndex]]), child->parent mappings tracking children of `testBranch` (from
    * [[branchParentIndex]]), and local events watched by the SQ (from [[localEventIndex]])
    *
    * This always propagates "down" a standing query (ie, from the global subscriber to the node at the root of the SQ)
    *
    * @param testBranch a branch rooted at this node
    * @param assumedEdge
    * @param subscriber
    */
  private[this] def cancelSubscription(
    testBranch: DomainGraphBranch,
    assumedEdge: AssumedDomainEdge,
    subscriber: Notifiable
  ): Unit = {
    // update [[subscribers]]
    val abandonedBranches: Map[(DomainGraphBranch, AssumedDomainEdge), SubscribersToThisNodeUtil.Subscription] =
      subscribers.removeSubscriber(subscriber, testBranch -> assumedEdge)

    val nextBranchesToRemove = abandonedBranches match {
      case empty
          if empty.isEmpty => // there are other subscribers to testBranch, so don't remove the local state about it
        None
      case singleton if singleton.keySet == Set(testBranch -> assumedEdge) =>
        // This was the last subscriber that cared about this branch -- clean up state for testBranch and continue
        // propagating
        Some(testBranch.children)

      case wrongBranchesRemoved =>
        // indicates a bug in [[subscribers.remove]]: we removed more branches than the one we intended to
        log.info(
          s"""Expected to clear a specific DGB from this node, instead started deleting multiple. Re-subscribing the
             |inadvertently removed branches. Expected $testBranch but found ${wrongBranchesRemoved.size} branch[es]:
             |${wrongBranchesRemoved.toList}""".stripMargin.replace('\n', ' ')
        )
        // re-subscribe any extra branches removed
        (wrongBranchesRemoved - (testBranch -> assumedEdge)).foreach {
          case (
                (resubBranch, resubEdge),
                SubscribersToThisNodeUtil.Subscription(resubSubscribers, _, relatedQueries)
              ) =>
            for {
              resubSubscriber <- resubSubscribers
            } subscribers.add(resubSubscriber, resubBranch, resubEdge, relatedQueries)
        }

        // if the correct branch was among those originally removed, then continue removing it despite the bug
        if (wrongBranchesRemoved.contains(testBranch -> assumedEdge))
          Some(testBranch.children)
        else // we removed the completely wrong set of branches - don't continue removing state
          None
    }

    nextBranchesToRemove match {
      case Some(downstreamBranches) =>
        // update [[localEventIndex]]
        StandingQueryLocalEvents
          .extractWatchableEvents(testBranch)
          .foreach(event => localEventIndex.unregisterStandingQuery(EventSubscriber(testBranch -> assumedEdge), event))

        for {
          downstreamBranch <- downstreamBranches
          downstreamAssumedEdge =
            None // INV this must match [[ensureSubscriptionToDomainEdges]]'s choice of downstream assumed edge
        } {
          // update [[branchParentIndex]]
          branchParentIndex -= ((downstreamBranch -> downstreamAssumedEdge) -> (testBranch -> assumedEdge))
          // update [[domainNodeIndex]]
          val lastDownstreamResults =
            domainNodeIndex.removeAllIndicesInefficiently(downstreamBranch, downstreamAssumedEdge)
          // propagate the cancellation to any awake nodes representing potential children of this DGB
          // see [[NodeActorMailbox.shouldIgnoreWhenSleeping]]
          for {
            (downstreamNode: QuineId, _) <- lastDownstreamResults
          } downstreamNode ! CancelDomainNodeSubscription(downstreamBranch, downstreamAssumedEdge, qid)
        }
      case None =>
      // None means don't continue clearing out state
    }
    // [[domainNodeIndex]] and [[subscribers]] are both snapshotted -- so report that they (may) have been updated
    updateRelevantToSnapshotOccurred()
  }

  private[this] def replyToAll(notifiables: Iterable[Notifiable], msg: SqResultLike): Unit = notifiables.foreach {
    case Left(quineId) => quineId ! msg
    case Right(sqId) => graph.reportStandingResult(sqId, msg)
  }

  /** An index of upstream subscribers to this node for a given DGB. Keys are DGBs registered on this node, values are
    * the [[Notifiable]]s (eg, nodes or global SQ result queues) subscribed to this node, paired with the last result
    * sent to those [[Notifiable]]s.
    *
    * @example
    *  Map(
    *     (dgb1, assumedEdge1) ->
    *       (Set(Left(QuineId(0x01))) -> Some(true))
    *     (dgb2, assumedEdge2) ->
    *       (Set(Left(QuineId(0x01))) -> None)
    *  )
    *  "Concerning dgb1: this node last notified its subscribers (QID 0x01) that dgb1 matches on this node."
    *  "Concerning dgb2: this node has not yet notified its subscribers (QID 0x01) whether dgb2 matches on this node".
    */
  case class SubscribersToThisNode(
    subscribersToThisNode: mutable.Map[
      (DomainGraphBranch, AssumedDomainEdge),
      SubscribersToThisNodeUtil.Subscription
    ] = mutable.Map.empty
  ) {
    import SubscribersToThisNodeUtil.Subscription
    def containsSubscriber(
      testBranch: DomainGraphBranch,
      assumedDomainEdge: AssumedDomainEdge,
      subscriber: Notifiable,
      forQuery: StandingQueryId
    ): Boolean =
      subscribersToThisNode
        .get(testBranch -> assumedDomainEdge)
        .collect { case Subscription(subscribers, _, relatedQueries) =>
          subscribers.contains(subscriber) && relatedQueries.contains(forQuery)
        }
        .getOrElse(false)

    def tracksBranch(
      testBranch: DomainGraphBranch,
      assumedEdge: AssumedDomainEdge
    ): Boolean = subscribersToThisNode.contains(testBranch -> assumedEdge)

    def getAnswer(
      testBranch: DomainGraphBranch,
      assumedEdge: AssumedDomainEdge
    ): Option[Boolean] = subscribersToThisNode.get(testBranch -> assumedEdge).flatMap(_.lastNotification)

    def getRelatedQueries(
      testBranch: DomainGraphBranch,
      assumedEdge: AssumedDomainEdge
    ): Set[StandingQueryId] =
      subscribersToThisNode.get(testBranch -> assumedEdge).toSeq.flatMap(_.relatedQueries).toSet

    def add(
      from: Notifiable,
      testBranch: DomainGraphBranch,
      assumedEdge: AssumedDomainEdge,
      relatedQueries: Set[StandingQueryId]
    ): Unit =
      if (tracksBranch(testBranch, assumedEdge)) {
        val subscription = subscribersToThisNode(testBranch -> assumedEdge)
        if (!subscription.subscribers.contains(from) || !relatedQueries.subsetOf(subscription.relatedQueries)) {
          updateRelevantToSnapshotOccurred()
          subscribersToThisNode(testBranch -> assumedEdge) += from
          subscribersToThisNode(testBranch -> assumedEdge) ++= relatedQueries
          ()
        }
      } else { // [[from]] is the first subscriber to this DGB, so register the DGB and add [[from]] as a subscriber
        updateRelevantToSnapshotOccurred()
        val subscriptionKey = testBranch -> assumedEdge
        StandingQueryLocalEvents
          .extractWatchableEvents(testBranch)
          .foreach { event =>
            localEventIndex.registerStandingQuery(EventSubscriber(subscriptionKey), event, properties, edges)
          }

        subscribersToThisNode(subscriptionKey) =
          Subscription(subscribers = Set(from), lastNotification = None, relatedQueries = relatedQueries)
        ()
      }

    // Returns: the subscriptions removed from if and only if there are no other Notifiables in those subscriptions.
    private[DomainNodeIndexBehavior] def removeSubscriber(
      subscriber: Notifiable,
      branch: (DomainGraphBranch, AssumedDomainEdge)
    ): Map[(DomainGraphBranch, AssumedDomainEdge), Subscription] =
      subscribersToThisNode
        .get(branch)
        .map { case subscription @ Subscription(notifiables, _, _) =>
          if (notifiables == Set(subscriber)) {
            subscribersToThisNode -= branch // remove the whole branch if no more subscriptions (no one left to tell)
            Map(branch -> subscription)
          } else {
            subscribersToThisNode(branch) -= subscriber // else remove just the requested subscriber
            Map.empty[(DomainGraphBranch, AssumedDomainEdge), Subscription]
          }
        }
        .getOrElse(Map.empty)

    @deprecated(
      "Use updateAnswerAndPropagateToRelevantSubscribers for the propagation case, and the identity of the DGB for the wake-up/initial registration case",
      "Nov 2021"
    )
    private[this] def updateAnswerAndNotifySubscribersInefficiently(): Unit =
      subscribersToThisNode.keys.foreach { case (b, e) =>
        updateAnswerAndNotifySubscribers(b, e)
      }

    def updateAnswerAndPropagateToRelevantSubscribers(
      downstreamBranch: DomainGraphBranch,
      downstreamAssumedEdge: AssumedDomainEdge
    ): Unit = {
      val parentBranches = branchParentIndex.parentBranchesOf(downstreamBranch -> downstreamAssumedEdge)
      // this should always be the case: we shouldn't be getting subscription results for DGBs that we don't track
      // a parent of
      if (parentBranches.nonEmpty)
        parentBranches.foreach { case (b, e) =>
          updateAnswerAndNotifySubscribers(b, e)
        }
      else {
        // recovery case: If this is hit, there is a bug in the protocol -- either a subscription result was received
        // for an unknown subscription, or the branchParentIndex fell out of sync

        // attempt recovery
        val recoveredIndex =
          BranchParentIndex.reconstruct(domainNodeIndex, subscribers.subscribersToThisNode.keys)
        val parentsAfterRecovery = recoveredIndex.parentBranchesOf(downstreamBranch -> downstreamAssumedEdge)
        if (parentsAfterRecovery.nonEmpty) {
          // recovery succeeded -- add recovered entries to branchParentIndex and continue, logging an INFO-level notice
          // no data was lost, but this is a bug
          log.info(
            s"""Found out-of-sync branchParentIndex while propagating a DGB. Previously-untracked child was
               |$downstreamBranch. Previously only tracking children ${branchParentIndex.knownChildren.toList}.
               |""".stripMargin.replace('\n', ' ')
          )
          branchParentIndex = recoveredIndex
        } else {
          // recovery failed -- there is either data loss, or a bug in [[BranchParentIndex.reconstruct]], or both
          log.error(
            s"""While propagating a result a DGB Standing Query, found no upstream subscribers that might care about
               |an update in the provided downstream branch. This may indicate a bug in the branch registration/indexing
               |logic. Falling back to trying all branches. Orphan (downstream) branch is $downstreamBranch
               |""".stripMargin.replace('\n', ' ')
          )
          updateAnswerAndNotifySubscribersInefficiently(): @nowarn
        }
      }
    }

    def updateAnswerAndNotifySubscribers(
      testBranch: DomainGraphBranch,
      assumedEdge: AssumedDomainEdge
    ): Unit = {
      val subscriptionKey = testBranch -> assumedEdge
      testBranch match {
        // TODO this is the only variant used for standing queries
        case single: model.SingleBranch =>
          val matchesLocal = localTestBranch(single) // TODO: Consider whether to test assumedEdge in `localTestBranch`?
          val edgesSatisfied = edgesSatisfiedByIndex(single, assumedEdge)
          subscribersToThisNode.get(subscriptionKey).foreach {
            case subscription @ Subscription(notifiables, lastNotification, relatedQueries) =>
              (matchesLocal, edgesSatisfied) match {
                // If the query doesn't locally match, don't bother issuing recursive subscriptions
                case (false, _) if !lastNotification.contains(false) =>
                  replyToAll(
                    notifiables,
                    DomainNodeSubscriptionResult(qid, single, assumedEdge, result = false)
                  )
                  subscribersToThisNode(subscriptionKey) = subscription.notified(false)
                  updateRelevantToSnapshotOccurred()

                // If the query locally matches and we've already got edge results, reply with those
                case (true, Some(result)) if !lastNotification.contains(result) =>
                  replyToAll(
                    notifiables,
                    DomainNodeSubscriptionResult(qid, single, assumedEdge, result)
                  )
                  subscribersToThisNode(subscriptionKey) = subscription.notified(result)
                  updateRelevantToSnapshotOccurred()

                // If the query locally matches and we don't have edge results, issue subscriptions
                case (true, None) => ensureSubscriptionToDomainEdges(single, assumedEdge, relatedQueries)
                case _ => ()
              }
          }

        case and @ model.And(conjs) =>
          // Collect the state of recursive matches, then "AND" them together using Kleene logic
          val andMatches: Option[Boolean] = conjs
            .foldLeft[Option[Boolean]](Some(true)) { (acc, conj) =>
              val conjResult = domainNodeIndex.lookup(qid, conj, assumedEdge)

              // Create a subscription if it isn't already created
              if (conjResult.isEmpty)
                ensureSubscriptionToDomainEdges(
                  conj,
                  assumedEdge,
                  subscribersToThisNode.get(subscriptionKey).toSeq.flatMap(_.relatedQueries).toSet
                )

              // Kleene AND
              (acc, conjResult) match {
                case (Some(false), _) => Some(false)
                case (_, Some(false)) => Some(false)
                case (Some(true), Some(true)) => Some(true)
                case _ => None
              }
            }

          subscribersToThisNode.get(subscriptionKey).foreach {
            case subscription @ Subscription(notifiables, lastNotification, _) =>
              andMatches match {
                case Some(result) if !lastNotification.contains(result) =>
                  replyToAll(
                    notifiables,
                    DomainNodeSubscriptionResult(qid, and, assumedEdge, result)
                  )
                  subscribersToThisNode(subscriptionKey) = subscription.notified(result)
                  updateRelevantToSnapshotOccurred()

                case _ => ()
              }
          }

        case or @ model.Or(disjs) =>
          // Collect the state of recursive matches, then "OR" them together using Kleene logic
          val orMatches: Option[Boolean] = disjs
            .foldLeft[Option[Boolean]](Some(false)) { (acc, disj) =>
              val disjResult = domainNodeIndex.lookup(qid, disj, assumedEdge)

              // Create a subscription if it isn't already created
              if (disjResult.isEmpty)
                ensureSubscriptionToDomainEdges(
                  disj,
                  assumedEdge,
                  subscribersToThisNode.get(subscriptionKey).toSeq.flatMap(_.relatedQueries).toSet
                )

              // Kleene OR
              (acc, disjResult) match {
                case (Some(true), _) => Some(true)
                case (_, Some(true)) => Some(true)
                case (Some(false), Some(false)) => Some(false)
                case _ => None
              }
            }

          subscribersToThisNode.get(subscriptionKey).foreach {
            case subscription @ Subscription(notifiables, lastNotification, _) =>
              orMatches match {
                case Some(result) if !lastNotification.contains(result) =>
                  replyToAll(
                    notifiables,
                    DomainNodeSubscriptionResult(qid, or, assumedEdge, result)
                  )
                  subscribersToThisNode(subscriptionKey) = subscription.notified(result)
                  updateRelevantToSnapshotOccurred()

                case _ => ()
              }
          }

        case not @ model.Not(neg) =>
          // Collect the state of the recursive match and "NOT" it using Kleene logic
          val notMatches: Option[Boolean] = domainNodeIndex
            .lookup(qid, neg, assumedEdge)
            .map(!_)

          // Create a subscription if it isn't already created
          if (notMatches.isEmpty)
            ensureSubscriptionToDomainEdges(
              neg,
              assumedEdge,
              subscribersToThisNode.get(subscriptionKey).toSeq.flatMap(_.relatedQueries).toSet
            )

          subscribersToThisNode.get(subscriptionKey).foreach {
            case subscription @ Subscription(notifiables, lastNotification, _) =>
              notMatches match {
                case Some(result) if !lastNotification.contains(result) =>
                  replyToAll(
                    notifiables,
                    DomainNodeSubscriptionResult(qid, not, assumedEdge, result)
                  )
                  subscribersToThisNode(subscriptionKey) = subscription.notified(result)
                  updateRelevantToSnapshotOccurred()

                case _ => ()
              }
          }

        case mu @ (model.Mu(_, _) | model.MuVar(_)) =>
          log.error("Standing query test branch contains illegal sub-branch: {}", mu)
      }
    }
  }
}
