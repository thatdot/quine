package com.thatdot.quine.graph.behavior

import scala.annotation.nowarn
import scala.collection.{immutable, mutable}
import scala.concurrent.Future

import org.apache.pekko.actor.Actor

import com.thatdot.common.logging.Log.{
  ActorSafeLogging,
  AlwaysSafeLoggable,
  LogConfig,
  Safe,
  SafeLoggableInterpolator,
  SafeLogger,
}
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.StandingQueryWatchableEventIndex.EventSubscriber
import com.thatdot.quine.graph.messaging.BaseMessage.Done
import com.thatdot.quine.graph.messaging.StandingQueryMessage.{
  CancelDomainNodeSubscription,
  CreateDomainNodeSubscription,
  DomainNodeSubscriptionCommand,
  DomainNodeSubscriptionResult,
  SqResultLike,
}
import com.thatdot.quine.graph.messaging.{QuineIdOps, QuineRefOps}
import com.thatdot.quine.graph.{
  BaseNodeActor,
  DomainGraphNodeRegistry,
  DomainIndexEvent,
  EdgeEvent,
  LatestAnswer,
  NamespaceId,
  Notifiable,
  RunningStandingQuery,
  StandingQueryId,
  StandingQueryPattern,
  WatchableEventType,
}
import com.thatdot.quine.model.DomainGraphNode.{DomainGraphEdge, DomainGraphNodeId}
import com.thatdot.quine.model.{DomainGraphNode, HalfEdge, IdentifiedDomainGraphNode, SingleBranch}
import com.thatdot.quine.util.Log.implicits._

/** Conceptual note:
  * Standing queries should really be a subscription to whether the other satisfies a domain node (branch) or not,
  * with updates as that changes (until canceled). This is very similar to indexing behavior, except that indexing has
  * no specific requirement on the value--just requirement to send _the value_ on ALL changes. Standing test if domain
  * (a.k.a.: TestIfDomainNodeSubscription), would simply return a boolean. Nonetheless, it represents external
  * knowledge. This would probably be eventually consistent, but having the option to make this strongly consistent
  * (i.e. subscribers are notified of a change before the change request returns) could be incredibly powerful!
  */
object DomainNodeIndexBehavior {

  object DomainNodeIndex {

    /** What this node holds of one peer's answer about one child pattern.
      *
      * @param answer the peer's last answer, or None while the question is out
      * @param forQueries the standing queries this node told the peer the question was for: exactly the set
      *                   carried in the [[CreateDomainNodeSubscription]] messages that produced this entry. The
      *                   peer records the same set against this node, so both ends can retire the subscription
      *                   by the same test -- that no query in it still runs -- each at its own next wake and
      *                   without telling the other. That is what makes a cancellation lazy instead of a cascade.
      *
      *                   Grows with each further ask and never shrinks while the entry lives: a cancelled
      *                   query's id stays until every query here is cancelled. The set therefore
      *                   over-approximates who is interested, which risks keeping a subscription too long
      *                   rather than dropping an update that mattered.
      *
      *                   Empty means unknown, not nobody: a snapshot written before this field existed carries
      *                   none. See [[hasLiveQuery]].
      */
    final case class DomainIndexResult(answer: Option[Boolean], forQueries: Set[StandingQueryId]) {

      /** Whether some query this was asked for still runs. False for an empty set, which is unknown rather
        * than unwanted -- such an entry is judged instead by whether a pattern here still has it as a child,
        * see `dropAnswersNoLiveQueryNeeds`.
        */
      def hasLiveQuery(isRunning: StandingQueryId => Boolean): Boolean = forQueries.exists(isRunning)

      /** Whether this node ever recorded what it asked for. False only for an entry restored from a snapshot
        * written before the queries were recorded.
        */
      def recordsItsQueries: Boolean = forQueries.nonEmpty
    }
  }

  /** An index into the current state of downstream matches. Keys are remote QuineIds which may be the next step in a
    * DGB match. Values are maps where the key set is the set of DGBs the QuineId might match, and the value for each
    * such key is a [[DomainNodeIndex.DomainIndexResult]]: the peer's last answer, or None while the question is
    * out, and the patterns here that asked.
    *
    * @param index   the initial state of the index (useful for restoring from snapshot)
    */
  class DomainNodeIndex(
    val index: mutable.Map[QuineId, mutable.Map[DomainGraphNodeId, DomainNodeIndex.DomainIndexResult]] =
      mutable.Map.empty,
  ) {
    import DomainNodeIndex.DomainIndexResult

    def contains(id: QuineId): Boolean = index.contains(id)
    def contains(
      id: QuineId,
      dgnId: DomainGraphNodeId,
    ): Boolean = index.get(id).exists(_.contains(dgnId))

    /** Ensure an index into the state of a downstream node at the provided node is tracked
      *
      * @param downstreamQid the node whose results this index will cache
      * @param dgnId         the downstream DGN to be queried against [[downstreamQid]]
      *                      note: dgnId will refer to a child of a DGN rooted on this node
      * @param parent        the pattern here whose evaluation asks
      * @return true when the index is tracked but not yet populated,
      *         false when the index is tracked and already populated
      *         (That is, the return answers "could I use the index of `downstreamQid+dgnId` to answer queries without
      *         additional messages to downstreamQid)
      */
    def newIndex(
      downstreamQid: QuineId,
      dgnId: DomainGraphNodeId,
      forQueries: Set[StandingQueryId],
    ): Boolean =
      if (
        !contains(downstreamQid, dgnId) // don't duplicate subscriptions
      ) {
        index.getOrElseUpdate(downstreamQid, mutable.Map.empty) += (dgnId -> DomainIndexResult(None, forQueries))
        // downstreamQid's sub-index was just initialized, so we definitely need to poll it to answer queries about it
        true
      } else {
        val result = index(downstreamQid)(dgnId)
        if (!forQueries.subsetOf(result.forQueries)) {
          // Asked for on behalf of a query the peer has not heard of. Ask again, so that it maintains the
          // answer for that query too, and record it so both ends hold the same set.
          index(downstreamQid)(dgnId) = result.copy(forQueries = result.forQueries union forQueries)
          true
        } else
          // Already asked, on behalf of these queries. Waiting for the answer is the whole point of having asked;
          // asking again because it has not arrived yet would send a message per evaluation and learn nothing. An
          // answer is not dropped for a node that has gone to sleep in the meantime -- it waits in the mailbox and
          // is handled once that node has finished waking -- so an outstanding question needs no prompting.
          false
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
      dgnId: DomainGraphNodeId,
    ): Option[(QuineId, DomainIndexResult)] =
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
      * TODO restructure [[index]] to be DGB ->> (id ->> result) instead of id ->> (DGB ->> result)
      * This change will make this O(1) without affecting performance of other functions on this object
      *
      * @return the last known state for each downstream subscription
      */
    def removeAllIndicesInefficiently(
      dgnId: DomainGraphNodeId,
    ): Iterable[(QuineId, DomainIndexResult)] = index.keys
      .flatMap { id =>
        removeIndex(id, dgnId)
      }

    /** Attribute to every entry that records none of its own whatever `queriesFor` derives for its child
      * pattern; see `recordMissingQueryIds`.
      *
      * @return whether anything changed
      */
    def recordQueriesWhereUnrecorded(queriesFor: DomainGraphNodeId => Set[StandingQueryId]): Boolean = {
      var changed = false
      index.valuesIterator.foreach { byDgn =>
        byDgn.mapValuesInPlace { (childDgnId, result) =>
          if (result.recordsItsQueries) result
          else {
            val derived = queriesFor(childDgnId)
            if (derived.isEmpty) result else { changed = true; result.copy(forQueries = derived) }
          }
        }
      }
      changed
    }

    /** Forget every cancelled query from the entries that remain, so what is held names only live queries.
      *
      * Both ends of a subscription do this at their own wake, from the same list of running queries, which is how
      * they converge on the same set without telling each other. Entries recording no queries at all are left
      * alone: that is a snapshot written before the queries were recorded, and it is unknown rather than stale.
      *
      * @return whether anything changed
      */
    def pruneCancelledQueries(isRunning: StandingQueryId => Boolean): Boolean = {
      var changed = false
      index.valuesIterator.foreach { byDgn =>
        byDgn.mapValuesInPlace { (_, result) =>
          if (!result.recordsItsQueries) result
          else {
            val live = result.forQueries.filter(isRunning)
            if (live == result.forQueries) result else { changed = true; result.copy(forQueries = live) }
          }
        }
      }
      changed
    }

    /** Drop every entry `keep` rejects, by the child pattern it answers about and what it holds.
      * @return whether any was dropped
      */
    def retainResults(keep: (DomainGraphNodeId, DomainIndexResult) => Boolean): Boolean = {
      var dropped = false
      index.filterInPlace { (_, byDgn) =>
        byDgn.filterInPlace { (dgnId, result) =>
          val kept = keep(dgnId, result)
          if (!kept) dropped = true
          kept
        }
        byDgn.nonEmpty
      }
      dropped
    }

    /** Record `fromOther`'s answer about `dgnId`, only into an index entry this node created by asking: [[newIndex]]
      * creates the entry before the question is sent, so an answer with no entry is a late reply to a question this
      * node has since withdrawn, and recording it would leave an answer nobody will ever correct.
      *
      * @return whether the answer was recorded
      */
    def updateResult(
      fromOther: QuineId,
      dgnId: DomainGraphNodeId,
      result: Boolean,
    )(implicit log: SafeLogger): Boolean =
      index.get(fromOther).filter(_.contains(dgnId)) match {
        case Some(byDgn) =>
          byDgn(dgnId) = byDgn(dgnId).copy(answer = Some(result))
          true
        case None =>
          log.debug(safe"Ignoring an answer from node: $fromOther about a pattern this node no longer asks it about")
          false
      }

    /** Whether [[updateResult]] would leave this index holding something different: an answer repeating what is
      * recorded overwrites it with itself, and one about a pattern this node does not ask about is refused.
      */
    def answerWouldChange(
      fromOther: QuineId,
      dgnId: DomainGraphNodeId,
      result: Boolean,
    ): Boolean =
      index.get(fromOther).flatMap(_.get(dgnId)).exists(_.answer != Some(result))

    /** Record an answer, creating the entry it belongs to when there is none. For folding a journal only.
      *
      * A result names the peer and the child pattern, which is the whole key of an entry, so the fold can record it
      * without having re-derived the subscription that asked first. It needs to when the asking pattern belonged to
      * a cancelled query: cancelling deletes that DGN, and the fold cannot enumerate the children of a pattern it
      * can no longer resolve.
      *
      * `forQueries` is not carried by a result, so it starts empty and is filled by whichever surviving pattern
      * claims the same child -- by [[newIndex]] during the fold, or failing that by `recordMissingQueryIds` at
      * wake. An entry no pattern claims is dropped there, which is the right answer: nothing here wants it.
      */
    def recordAnswerCreatingEntry(
      fromOther: QuineId,
      dgnId: DomainGraphNodeId,
      result: Boolean,
      askedFor: Set[StandingQueryId],
    ): Unit = {
      val byDgn = index.getOrElseUpdate(fromOther, mutable.Map.empty)
      byDgn(dgnId) = byDgn.get(dgnId) match {
        case Some(existing) => existing.copy(answer = Some(result))
        case None => DomainIndexResult(Some(result), askedFor)
      }
    }

    def lookup(
      id: QuineId,
      dgnId: DomainGraphNodeId,
    ): Option[Boolean] =
      index.get(id).flatMap(_.get(dgnId)).flatMap(_.answer)
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
      dgnRegistry: DomainGraphNodeRegistry,
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
      DomainGraphNodeId,
    ]] = Map.empty,
  ) {

    // All known parent nodes of [[dgnId]], according to [[knownParents]]
    def parentNodesOf(
      dgnId: DomainGraphNodeId,
    ): Set[DomainGraphNodeId] =
      knownParents.getOrElse(dgnId, Set.empty)

    def +(
      childParentTuple: (
        DomainGraphNodeId,
        DomainGraphNodeId,
      ),
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
      child: DomainGraphNodeId,
    ): NodeParentIndex = copy(knownParents = knownParents - child)

    /** Create a copy of this with all but the specified parent registered for `child`
      */
    def -(
      childParentTuple: (
        DomainGraphNodeId,
        DomainGraphNodeId,
      ),
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

    /** This is the materialized state of [[DomainIndexEvent]] events for one pattern rooted on this node: who is
      * subscribed, which top-level queries each subscriber depends on this node for, and what they were last told.
      *
      * @param latestAnswer the last answer reported to the subscribers, or None if none has been reported yet
      * @param queriesPerSubscriber the top-level standing queries each subscriber named when it subscribed. The key
      *                             set is the subscriber set, and the union of the values is every query this
      *                             subscription may be used to answer. A subscriber is kept for as long as one of
      *                             the queries it named still runs.
      *
      *                             CAUTION: on a supernode this map may be extremely large!
      */
    final case class DistinctIdSubscription(
      latestAnswer: LatestAnswer = None,
      queriesPerSubscriber: Map[Notifiable, Set[StandingQueryId]] = Map.empty,
    ) {

      /** The places (nodes and top-level result buffers) to which results should be reported. */
      def subscribers: Set[Notifiable] = queriesPerSubscriber.keySet

      /** The top-level query IDs for which this subscription may be used to calculate answers. */
      lazy val relatedQueries: Set[StandingQueryId] =
        queriesPerSubscriber.values.foldLeft(Set.empty[StandingQueryId])(_ union _)

      /** The queries `subscriber` named, or empty if it is not subscribed here. */
      def queriesFor(subscriber: Notifiable): Set[StandingQueryId] =
        queriesPerSubscriber.getOrElse(subscriber, Set.empty)

      def hasSubscriber(subscriber: Notifiable): Boolean = queriesPerSubscriber.contains(subscriber)

      /** Whether `subscriber` is subscribed here on behalf of `query`. */
      def isSubscribedFor(subscriber: Notifiable, query: StandingQueryId): Boolean =
        queriesFor(subscriber).contains(query)

      /** Whether `subscriber` is the last one left, so removing it retires the whole subscription. */
      def isOnlySubscriber(subscriber: Notifiable): Boolean = queriesPerSubscriber.keySet == Set(subscriber)

      def isEmpty: Boolean = queriesPerSubscriber.isEmpty
      def nonEmpty: Boolean = queriesPerSubscriber.nonEmpty

      /** Register `subscriber`, adding `queries` to whatever it has already named. */
      def addSubscriber(subscriber: Notifiable, queries: Set[StandingQueryId]): DistinctIdSubscription =
        copy(queriesPerSubscriber = queriesPerSubscriber.updated(subscriber, queriesFor(subscriber) union queries))

      def removeSubscriber(subscriber: Notifiable): DistinctIdSubscription =
        copy(queriesPerSubscriber = queriesPerSubscriber - subscriber)

      // Infix sugaring support
      def +(subscription: (Notifiable, Set[StandingQueryId])): DistinctIdSubscription =
        addSubscriber(subscription._1, subscription._2)
      def -(subscriber: Notifiable): DistinctIdSubscription = removeSubscriber(subscriber)
    }
  }
}

trait DomainNodeIndexBehavior
    extends Actor
    with ActorSafeLogging
    with BaseNodeActor
    with DomainNodeTests
    with QuineIdOps
    with QuineRefOps
    with StandingQueryBehavior {

  import DomainNodeIndexBehavior._

  protected val dgnRegistry: DomainGraphNodeRegistry

  /** Which standing queries this node's history has shown to be live, as a replay walks through it.
    *
    * A replay cannot ask the graph which queries run: that is the answer for now, not for the point in the history
    * being replayed, and using it makes a fold decide a past event by a present fact. The node's own journal says
    * enough -- a subscription names the queries it is for, and a retirement says one is gone -- so the fold keeps
    * the tally itself. Seeded from whatever state a snapshot restored, since those queries were live when it was
    * written. By the end of a fold it agrees with the graph, once the wake has reconciled the cancellations that
    * happened while this node slept.
    */
  protected[this] val queriesLiveWhileReplaying: mutable.Set[StandingQueryId] = mutable.Set.empty

  /** Record an event this node has already applied to itself; see the implementation for when that is right. */
  protected[this] def journalAlreadyAppliedDomainIndexEvent(event: DomainIndexEvent): Unit

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
    event: DomainIndexEvent,
  ): Future[Done.type]

  def namespace: NamespaceId

  /** Called once on node wakeup, this updates DistinctID SQs.
    *  - adds new DistinctID SQs not already in the subscribers
    *  - removes SQs no longer in the graph state
    */
  protected def updateDistinctIdStandingQueriesOnNode()(implicit logConfig: LogConfig): Unit = {
    // Register new SQs in graph state but not in the subscribers
    // NOTE: we cannot use `+=` because if already registered we want to avoid duplicating the result
    for {
      (sqId, runningSq) <- graph
        .standingQueries(namespace) // Silently ignore absent namespace.
        .fold(Map.empty[StandingQueryId, RunningStandingQuery])(_.runningStandingQueries)
      query <- runningSq.query.queryPattern match {
        case dgnPattern: StandingQueryPattern.DomainGraphNodeStandingQueryPattern => Some(dgnPattern.dgnId)
        case _ => None
      }
    } {
      val subscriber = Right(sqId)
      val alreadySubscribed = domainGraphSubscribers.containsSubscriber(query, subscriber, sqId)
      if (!alreadySubscribed) {
        // Journaled rather than applied directly: a subscription that lives only in memory is lost when the node
        // sleeps without a snapshot, and on wake it subscribes again and reports the same match a second time.
        val _ = processDomainIndexEvent(DomainIndexEvent.CreateDomainStandingQuerySubscription(query, sqId, Set(sqId)))
      }
    }

    dropDeadDistinctIdSubscriptions()
  }

  /** Run `body` with the test for a running query, once the graph can answer it: not on a historical node, which
    * serves no standing query, and not before the graph has restored its queries or in an absent namespace, where
    * an absent query is not evidence of a cancellation.
    */
  private[this] def withRunningQueries(body: Set[StandingQueryId] => Unit): Unit =
    if (atTime.isEmpty && graph.standingQueriesRestored)
      graph.standingQueries(namespace).foreach(sqns => body(sqns.runningStandingQueries.keySet))

  /** Drop, in memory, every subscription here that no running query depends on. The propagate path, for an awake
    * node; a sleeping node does the same at its wake.
    */
  protected def dropDeadDistinctIdSubscriptions()(implicit logConfig: LogConfig): Unit =
    withRunningQueries { running =>
      dropDeadSubscriptions(running.contains)
      dropAnswersNoLiveQueryNeeds(running.contains)
    }

  /** Bring the DistinctId bookkeeping restored at wake into line with the queries the graph now runs.
    *
    * Local state only: nothing is sent and no peer is woken, which is the rule for the whole of waking and not
    * just for the fold. A node must finish waking before it handles its mailbox, and what is in that mailbox is
    * very likely the answer to the question this node asked before it slept -- a result is not dropped for a
    * sleeping node, it waits. Re-asking here would race that answer and subscribe the peer a second time for a
    * question it is already maintaining.
    *
    * In memory: a replay brings the same state back and the next wake judges it again.
    */
  protected def reconcileDistinctIdStateAtWake()(implicit logConfig: LogConfig): Unit = withRunningQueries { running =>
    recordMissingQueryIds()
    dropDeadSubscriptions(running.contains)
    dropAnswersNoLiveQueryNeeds(running.contains)
  }

  /** A query subscriber is dead once its query no longer runs. A node subscriber is dead once no query it
    * depends on this node for still runs -- judged per subscriber, from `queriesPerSubscriber`, rather than from
    * the union across all of them, which would keep one subscriber alive on another's query.
    *
    * A running query keeps the pattern registered here, and so keeps this node's own subscriptions to its peers
    * alive by this same rule. That is why what a live subscription holds stays current. Peers are not told:
    * each applies the rule to its own subscriptions, at its wake or when the cancellation reaches it awake.
    */
  /** Drop, in memory, every answer a peer gave that no running query holds any more.
    *
    * Local state only: nothing is sent and no peer is woken. The peer holds the same set of queries against this
    * node and retires its side by the same test at its own next wake, so a cancellation is cleaned up lazily at
    * each end rather than rippling outwards. Neither end can be left believing something the other does not,
    * because `updateResult` refuses an answer for an entry this node no longer has -- so a peer that has not
    * cleaned up yet, and answers anyway, is simply ignored.
    *
    * An entry restored from a snapshot written before the queries were recorded holds none, which is unknown
    * rather than dead. Those are judged the way they were before the queries were recorded: kept only while
    * some pattern subscribed here still has that child, and re-asked when one does.
    */
  private[this] def dropAnswersNoLiveQueryNeeds(isRunning: StandingQueryId => Boolean): Unit = {
    val dropped = domainNodeIndex.retainResults { (child, result) =>
      if (result.recordsItsQueries) result.hasLiveQuery(isRunning)
      else domainGraphNodeParentIndex.parentNodesOf(child).nonEmpty
    }
    // What survives keeps only the queries that still run, so that this node and the peer hold the same set.
    val pruned = domainNodeIndex.pruneCancelledQueries(isRunning)
    if (dropped || pruned) updateRelevantToSnapshotOccurred()
  }

  /** Fill in the standing queries behind every answer that records none of its own.
    *
    * Two kinds of answer arrive without them. One restored from a snapshot written before answers recorded their
    * queries has none because the field did not exist. One recorded during a replay from a peer's reply has none
    * because a reply carries no query ids. Either way the node holds an answer and cannot say who wants it.
    *
    * They are filled in from the registry: every standing query whose pattern contains that child pattern. An
    * over-approximation, since a query whose pattern merely contains the child need not be the one that asked, but
    * derived rather than guessed, and it leaves no answer unaccountable. What that buys is that an empty set now
    * means what it says -- nothing here wants this answer -- so retiring a cancelled query can drop the answers it
    * empties instead of having to leave them alone in case they were merely unrecorded.
    *
    * A subscriber that is a standing query needs no derivation: it *is* the query, so its own id is exact.
    */
  private[this] def recordMissingQueryIds(): Unit = {
    val changedSubscribers = domainGraphSubscribers.recordQuerySubscribersOwnIds()
    val changedAnswers = domainNodeIndex.recordQueriesWhereUnrecorded { child =>
      domainGraphNodeParentIndex.parentNodesOf(child).flatMap(domainGraphSubscribers.getRelatedQueries)
    }
    if (changedSubscribers || changedAnswers) updateRelevantToSnapshotOccurred()
  }

  private[this] def dropDeadSubscriptions(
    isRunning: StandingQueryId => Boolean,
  )(implicit logConfig: LogConfig): Unit = {
    for {
      (dgnId, subscription) <- domainGraphSubscribers.subscribersToThisNode.toList
      subscriber <- subscription.subscribers
      queries = subscription.queriesFor(subscriber)
      dead = subscriber match {
        case Right(sqId) => !isRunning(sqId)
        // Empty is unknown, not dead: a snapshot written before the per-subscriber queries were recorded
        // attributes nothing to a subscriber it cannot account for, and dropping those would retire every
        // subscription a restored node had.
        case Left(_) => queries.nonEmpty && !queries.exists(isRunning)
      }
      if dead
    } {
      retireSubscription(dgnId, subscriber, isRunning, shouldSendReplies = false)
      // Written where the retirement happens, not where the cancellation did: this node finds out at its own
      // moment -- awake when the cancellation reaches it, asleep until its next wake -- and a replay needs that
      // moment, since the same events with this teardown before or after them leave different state.
      journalAlreadyAppliedDomainIndexEvent(subscriber match {
        case Right(sqId) => DomainIndexEvent.CancelDomainStandingQuerySubscription(dgnId, sqId)
        case Left(quineId) => DomainIndexEvent.CancelDomainNodeSubscription(dgnId, quineId)
      })
    }
    // Whoever is left keeps only the queries that still run. A subscriber whose queries were all cancelled was
    // dropped above, so pruning never empties one.
    domainGraphSubscribers.pruneCancelledQueries(isRunning)
  }

  protected def domainNodeIndexBehavior(command: DomainNodeSubscriptionCommand): Unit = {
    // Convert Pekko message model to node journal model
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
    testDgn: DomainGraphNode.Single,
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
    testBranch: DomainGraphNode.Single,
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

  /** Whether every answer a peer gave about a child of `dgnId` is still being maintained.
    *
    * A peer was asked on behalf of the standing queries which the asking pattern served, and it goes on answering
    * only while one of those queries runs. Once the relevant SQ is cancelled, the peer has stopped maintaining
    * the result, and what is held here is the last answer it will ever send.
    *
    * This only applies to thoroughgoing present nodes, never on a historical node, which serves no standing
    * query, and not before the graph has restored its queries or in an absent namespace, where an absent query
    * is not evidence of a cancellation. Where it cannot be judged, the answer is treated as unmaintained.
    */
  private[this] def childAnswersAreMaintained(dgnId: DomainGraphNodeId): Boolean =
    atTime.isEmpty && graph.standingQueriesRestored && graph.standingQueries(namespace).exists { sqns =>
      def isRunning(sqId: StandingQueryId): Boolean = sqns.runningStandingQuery(sqId).isDefined
      childrenOf(dgnId).forall { child =>
        domainNodeIndex.index.valuesIterator.forall(_.get(child).forall(_.hasLiveQuery(isRunning)))
      }
    }

  /** Take in a subscription, either as it arrives or as the journal replays it.
    *
    * The two are different jobs and are kept apart. Arriving live, the subscriber is owed an answer now, and
    * deciding what to hand it depends on what is running now. Replaying, the journal is the source of truth and
    * there is nobody to answer: the event is recorded and what it implied is derived, nothing more.
    *
    * @param from the subscriber to which results should be reported
    * @param dgnId the DGN against whose root this node should be compared
    * @param relatedQueries the top-level query IDs for which this subscription may be used to calculate answers
    */
  protected[this] def receiveDomainNodeSubscription(
    from: Notifiable,
    dgnId: DomainGraphNodeId,
    relatedQueries: Set[StandingQueryId],
    shouldSendReplies: Boolean,
  )(implicit logConfig: LogConfig): Unit =
    if (shouldSendReplies) serveArrivingSubscriber(from, dgnId, relatedQueries)
    else recordSubscriptionFromJournal(from, dgnId, relatedQueries)

  /** Fold a subscription event back into this node.
    *
    * Recording the subscriber is the event itself; the subscriptions it implied are derived from it exactly as they
    * were the first time, by asking which edges match the pattern at this point in the node's history. An answer
    * already recorded for one of those peers is joined rather than replaced, and this event's queries are added to
    * the entry holding it. Nothing is sent, and nothing recorded is discarded.
    *
    * None of the judgement in [[serveArrivingSubscriber]] belongs here. It decides whether an answer is fit to hand
    * to a subscriber by asking which queries run *now*, and repairs an unfit one by asking the peers again -- a
    * question about the present, and a repair this cannot perform. Applied to a past event it would discard an
    * answer that was correct when the event happened, and the journal holds no second copy. What is still wanted is
    * settled once, after the fold, by `dropAnswersNoLiveQueryNeeds`.
    */
  private[this] def recordSubscriptionFromJournal(
    from: Notifiable,
    dgnId: DomainGraphNodeId,
    relatedQueries: Set[StandingQueryId],
  )(implicit logConfig: LogConfig): Unit = {
    domainGraphSubscribers.add(from, dgnId, relatedQueries)
    val _ = dgnRegistry.withIdentifiedDomainGraphNode(dgnId)(
      domainGraphSubscribers.updateAnswerAndNotifySubscribers(_, shouldSendReplies = false),
    )
  }

  /** Register a subscriber that has just arrived, and answer it.
    *
    * An answer already computed for `dgnId` is reported to the subscriber directly, but only while the peers'
    * answers it was derived from are still maintained. An answer no peer maintains can never move again, so
    * reporting it would hand a new query a result that will never be corrected: that answer is dropped and the
    * pattern re-evaluated, which asks the peers afresh.
    *
    * A subscriber may also bring a query the peers have not heard of, whose answer is nonetheless current. The
    * answers are kept and the peers are asked again, so that they maintain them for the new query as well.
    */
  private[this] def serveArrivingSubscriber(
    from: Notifiable,
    dgnId: DomainGraphNodeId,
    relatedQueries: Set[StandingQueryId],
  )(implicit logConfig: LogConfig): Unit = {
    val shouldSendReplies = true
    // Both read before `add`, which would otherwise count this subscriber's own queries as already asked for.
    val isMaintained = childAnswersAreMaintained(dgnId)
    val doesBringNewQuery = !relatedQueries.subsetOf(domainGraphSubscribers.getRelatedQueries(dgnId))
    domainGraphSubscribers.add(from, dgnId, relatedQueries)

    def evaluate(): Unit = {
      val _ = dgnRegistry.withIdentifiedDomainGraphNode(dgnId)(
        domainGraphSubscribers.updateAnswerAndNotifySubscribers(_, shouldSendReplies),
      )
    }

    domainGraphSubscribers.getAnswer(dgnId) match {
      case Some(result) if isMaintained =>
        // A peer maintains its answer only for the queries it was told about, so tell it about this one too.
        if (doesBringNewQuery) {
          val _ = dgnRegistry.withIdentifiedDomainGraphNode(dgnId)(
            // `newIndex` sends the ask exactly when the peer would learn something: the query set is no longer a
            // subset of what the entry records. That is this case, so no flag is needed to force it.
            ensureSubscriptionToDomainEdges(_, domainGraphSubscribers.getRelatedQueries(dgnId), shouldSendReplies),
          )
        }
        // Answered even for a subscriber already known here, because it may have lost the answer it was given.
        // A subscriber rebuilt from its journal drops an answer whose question it could not record -- a pattern
        // the registry has forgotten cannot be evaluated, so the ask is never made and the replayed answer is
        // refused for having no entry -- and asking again is the only way it gets the answer back.
        val _ = conditionallyReplyToAll(
          Set(from),
          DomainNodeSubscriptionResult(qid, dgnId, result),
          shouldSendReplies,
        )
      case Some(_) =>
        // No peer is maintaining this answer. Forget it and ask again.
        for {
          child <- childrenOf(dgnId)
          _ <- domainNodeIndex.removeAllIndicesInefficiently(child)
        } updateRelevantToSnapshotOccurred()
        // Forgetting the answer too makes the re-derived one count as a change, so every subscriber hears it,
        // including this one, which has been told nothing.
        domainGraphSubscribers.forgetAnswer(dgnId)
        evaluate()
      case None =>
        evaluate()
    }
  }

  /** Withdraw the subscriptions a removed edge leaves unreachable.
    *
    * An answer about a child pattern is held at a peer because some edge matching the pattern's domain edge
    * reached that peer. Once none does, this node has no use for the answer and the peer has no reason to go on
    * maintaining it, so the entry is dropped and the peer is told. Adding an edge subscribes; removing one
    * withdraws, and both happen as the edge event is applied rather than being left for a later cancellation to
    * tidy up.
    *
    * Answers a combinator holds against this node itself are left alone: those were never reached by an edge.
    */
  protected[this] def withdrawSubscriptionsUnreachableAfter(event: EdgeEvent, shouldSendReplies: Boolean): Unit =
    event match {
      case EdgeEvent.EdgeAdded(_) => ()
      case EdgeEvent.EdgeRemoved(removed) =>
        val peer = removed.other
        if (peer != qid) {
          val stillReachable: Set[DomainGraphNodeId] =
            domainGraphSubscribers.subscribersToThisNode.keys.toList.flatMap { dgnId =>
              dgnRegistry.getDomainGraphNode(dgnId).toList.flatMap {
                case DomainGraphNode.Single(_, _, nextDomainEdges, _) =>
                  nextDomainEdges.collect {
                    // A point lookup: on a supernode the edge collection is backed by the persistor.
                    case c if edges.contains(HalfEdge(c.edge.edgeType, c.edge.direction, peer)) => c.dgnId
                  }
                case DomainGraphNode.And(_) | DomainGraphNode.Or(_) | DomainGraphNode.Not(_) => Nil
                case DomainGraphNode.Mu(_, _) | DomainGraphNode.MuVar(_) => Nil
              }
            }.toSet
          for {
            byDgn <- domainNodeIndex.index.get(peer).toList
            childDgnId <- byDgn.keys.toList
            if !stillReachable.contains(childDgnId)
            _ <- domainNodeIndex.removeIndex(peer, childDgnId)
          } {
            updateRelevantToSnapshotOccurred()
            if (shouldSendReplies) peer ! CancelDomainNodeSubscription(childDgnId, qid)
          }
        }
    }

  /** Watch this node's own events for the pattern, so that a local change re-evaluates it. Registering again is
    * harmless; a watch dropped while the pattern was unregistered is what this puts back.
    */
  private[this] def watchLocalEvents(dgnId: DomainGraphNodeId): Unit = {
    val _ = dgnRegistry.withDomainGraphBranch(dgnId) {
      WatchableEventType
        .extractWatchableEvents(_)
        .foreach(event => watchableEventIndex.registerStandingQuery(EventSubscriber(dgnId), event, properties, edges))
    }
  }

  /** Check for any subscriptions `dgn` may need in order to answer the question: "is dgn consistent with
    * a tree rooted at this node?"
    */
  protected def ensureSubscriptionToDomainEdges(
    dgn: IdentifiedDomainGraphNode,
    relatedQueries: Set[StandingQueryId],
    shouldSendReplies: Boolean,
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
          idxUpdated = domainNodeIndex.newIndex(downstreamQid, downstreamDgnId, relatedQueries)
          if idxUpdated && shouldSendReplies
          _ = downstreamQid ! CreateDomainNodeSubscription(
            downstreamDgnId,
            Left(qid),
            relatedQueries,
          )
        } yield downstreamQid
      // these combinators all index other local nodes
      case DomainGraphNode.And(_) | DomainGraphNode.Or(_) | DomainGraphNode.Not(_) =>
        for {
          childDgnId <- childNodes
          idxUpdated = domainNodeIndex.newIndex(qid, childDgnId, relatedQueries)
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
    shouldSendReplies: Boolean,
  )(implicit logConfig: LogConfig): Unit = {
    // Live, an answer about a pattern this node does not ask about is refused: nothing here wants it, and the peer
    // retires its side by its own next wake. Folding a journal is the opposite case -- this node did ask, and the
    // answer is the surviving record of having asked. See `recordAnswerCreatingEntry`.
    val landed =
      if (shouldSendReplies) domainNodeIndex.updateResult(fromOther, otherDgnId, result)(log)
      else {
        // Nothing is recorded about which queries the answer is for, because nothing here knows: a reply carries
        // no query ids, and the pattern that asked is gone from the registry, so there is no way to tell which of
        // the queries this node serves wanted it. Naming all of them was tried and is wrong -- it keeps an answer
        // alive on the strength of a query that never asked, such as one whose pattern is the child itself and so
        // asks nobody. An entry recording none is judged instead by whether a pattern here has that child, which
        // is what `dropAnswersNoLiveQueryNeeds` does with it.
        domainNodeIndex.recordAnswerCreatingEntry(fromOther, otherDgnId, result, Set.empty)
        true
      }
    if (landed) {
      domainGraphSubscribers.updateAnswerAndPropagateToRelevantSubscribers(otherDgnId, shouldSendReplies)
      updateRelevantToSnapshotOccurred()
    }
  }

  /** Remove state used to track `dgnId`'s completion at this node for `subscriber`. If `subscriber` is the last
    * Notifiable interested in `dgnId`, remove all state used to track `dgnId`'s completion from this node
    * and propagate the cancellation.
    *
    * State removed might include upstream subscriptions to this node (from [[domainGraphSubscribers]]), downstream subscriptions
    * from this node (from [[domainNodeIndex]]), child->parent mappings tracking children of `dgnId` (from
    * [[domainGraphNodeParentIndex]]), and local events watched by the SQ (from [[watchableEventIndex]])
    *
    * This always propagates "down" a standing query (ie, from the global subscriber to the node at the root of the SQ)
    */
  /** The children of `dgnId`, as this node can best tell: from the registry while it still has the DGN, else from
    * the parent index, which holds every child this node subscribed to on the DGN's behalf. `cancelStandingQuery`
    * unregisters a DGN before any node hears of the cancellation, so the second source is the usual one.
    */
  protected[this] def childrenOf(dgnId: DomainGraphNodeId): Set[DomainGraphNodeId] =
    dgnRegistry.getDomainGraphNode(dgnId) match {
      case Some(dgn) => dgn.children.toSet
      case None =>
        domainGraphNodeParentIndex.knownParents.collect {
          case (child, parents) if parents.contains(dgnId) => child
        }.toSet
    }

  /** Retire one subscription and the answers it was the last to want.
    *
    * Both halves of what a node does on finding the queries behind a subscription gone, in the order it does them,
    * so that a replay of the journaled retirement reaches the same place as the node that wrote it. Which answers
    * go is settled by what each answer records about the queries it is kept for, against what the subscriptions
    * left here still depend on -- no question asked of the registry, which by now may not define the pattern.
    */
  protected[this] def retireSubscription(
    dgnId: DomainGraphNodeId,
    subscriber: Notifiable,
    isRunning: StandingQueryId => Boolean,
    shouldSendReplies: Boolean,
  )(implicit logConfig: LogConfig): Unit =
    // Retiring what is not subscribed changes nothing, and has to change nothing: whether this will have an effect
    // is answered by `domainIndexEventHasEffect` before the effect is applied, and it answers with this same test.
    if (domainGraphSubscribers.hasSubscriber(dgnId, subscriber)) {
      cancelSubscription(dgnId, Some(subscriber), shouldSendReplies)
      // One rule for which answers go with it: an answer stays while some query it is kept for still runs. Which
      // queries those are is the caller's to say, because it depends on when this is happening -- the graph's
      // running queries for something happening now, this node's own tally for a point mid-history.
      dropAnswersNoLiveQueryNeeds(isRunning)
    }

  protected[this] def cancelSubscription(
    dgnId: DomainGraphNodeId,
    // `subscriber` should only ever be a Left(QuineId). Cancellation of a Right(StandingQueryId) isn't applied on nodes
    subscriber: Option[Notifiable], // TODO just move this to the caller only
    shouldSendReplies: Boolean,
  )(implicit logConfig: LogConfig): Unit = {
    // The index results that go with the last subscriber; read before the teardown below edits the parent index.
    val children = childrenOf(dgnId)
    // update [[subscribers]]
    val abandoned = subscriber.map(s => domainGraphSubscribers.removeSubscriber(s, dgnId)).getOrElse(Map.empty)

    val nextNodesToRemove = abandoned match {
      case empty if empty.isEmpty => // there are other subscribers to dgnId, so don't remove the local state about it
        None
      case singleton if singleton.keySet == Set(dgnId) =>
        // This was the last subscriber that cared about this node -- clean up state for dgnId and continue
        // propagating
        Some(children)

      case wrongNodesRemoved =>
        // indicates a bug in [[subscribers.remove]]: we removed more nodes than the one we intended to
        log.info {
          implicit val dgnIdSafe: AlwaysSafeLoggable[DomainGraphNodeId] = _.toString
          log"""Expected to clear a specific DGN from this node, instead started deleting multiple. Re-subscribing the
             |inadvertently removed DGNs. Expected: $dgnId but found: ${Safe(wrongNodesRemoved.size)} subscription[s]:
             |$wrongNodesRemoved""".cleanLines
        }
        // re-subscribe any extra nodes removed
        (wrongNodesRemoved - dgnId).foreach {
          case (
                resubNode,
                resubscription,
              ) =>
            for {
              resubSubscriber <- resubscription.subscribers
            } domainGraphSubscribers.add(resubSubscriber, resubNode, resubscription.queriesFor(resubSubscriber))
        }

        // if the correct node was among those originally removed, then continue removing it despite the bug
        if (wrongNodesRemoved.contains(dgnId))
          Some(children)
        else // we removed the completely wrong set of nodes - don't continue removing state
          None
    }

    nextNodesToRemove match {
      case Some(downstreamNodes) =>
        // update [[watchableEventIndex]]
        dgnRegistry.withDomainGraphBranch(dgnId) {
          WatchableEventType
            .extractWatchableEvents(_)
            .foreach(event => watchableEventIndex.unregisterStandingQuery(EventSubscriber(dgnId), event))
        }
        for {
          downstreamNode <- downstreamNodes
        } {
          domainGraphNodeParentIndex -= (downstreamNode -> dgnId)
          // A child is content-addressed and shared; while another pattern subscribed here still lists it as a
          // parent, the results are that pattern's as much as this one's and stay.
          if (domainGraphNodeParentIndex.parentNodesOf(downstreamNode).isEmpty) {
            val lastDownstreamResults = domainNodeIndex.removeAllIndicesInefficiently(downstreamNode)

            // TODO: DON'T send messages to cancel subscriptions from individual nodes. This should be done from the
            //       shard exactly once to all awake nodes when the SQ is removed.
            // propagate the cancellation to any awake nodes representing potential children of this DGB
            // see [[NodeActorMailbox.shouldIgnoreWhenSleeping]]
            if (shouldSendReplies) for {
              (downstreamQid, _) <- lastDownstreamResults
            } downstreamQid ! CancelDomainNodeSubscription(downstreamNode, qid)
          }
        }
      case None =>
      // None means don't continue clearing out state
    }
    // [[domainNodeIndex]] and [[subscribers]] are both snapshotted -- so report that they (may) have been updated
    updateRelevantToSnapshotOccurred()
  }

  /** If [[shouldSendReplies]], begin asynchronously notifying all [[notifiables]] of [[msg]]
    * @return a future that completes when all notifications have been sent (though not necessarily received yet)
    */
  private[this] def conditionallyReplyToAll(
    notifiables: immutable.Iterable[Notifiable],
    msg: SqResultLike,
    shouldSendReplies: Boolean,
  ): Future[Unit] = // TODO: this doesn't need to return a `Future`
    if (!shouldSendReplies) Future.unit
    else {
      graph.standingQueries(namespace).fold(Future.unit) { sqns =>
        // Missing namespace should return `false because of `reportStandingResult` below
        notifiables.foreach {
          case Left(quineId) => quineId ! msg
          case Right(sqId) =>
            sqns.reportStandingResult(sqId, msg) // TODO should this really be suppressed by shouldSendReplies?
            ()
        }
        Future.unit
      }
    }

  /** An index of upstream subscribers to this node for a given DGB. Keys are DGBs registered on this node, values
    * are [[SubscribersToThisNodeUtil.DistinctIdSubscription]]: the [[Notifiable]]s (eg, nodes or global SQ result
    * queues) subscribed to this node, the queries each of them depends on this node for, and the last result sent
    * to them.
    *
    * @example
    *  Map(
    *     dgn1 -> DistinctIdSubscription(Some(true), Map(Left(QuineId(0x01)) -> Set(sqA)))
    *     dgn2 -> DistinctIdSubscription(None,       Map(Left(QuineId(0x01)) -> Set(sqA, sqB)))
    *  )
    *  "Concerning dgn1: this node last notified its subscribers (QID 0x01) that dgn1 matches on this node, and
    *   QID 0x01 asked on behalf of sqA alone."
    *  "Concerning dgn2: this node has not yet notified its subscribers (QID 0x01) whether dgn2 matches on this
    *   node, and QID 0x01 asked on behalf of both sqA and sqB."
    */
  case class SubscribersToThisNode(
    subscribersToThisNode: mutable.Map[
      DomainGraphNodeId,
      SubscribersToThisNodeUtil.DistinctIdSubscription,
    ] = mutable.Map.empty,
  ) {
    import SubscribersToThisNodeUtil.DistinctIdSubscription
    def containsSubscriber(
      dgnId: DomainGraphNodeId,
      subscriber: Notifiable,
      forQuery: StandingQueryId,
    ): Boolean =
      subscribersToThisNode.get(dgnId).exists(_.isSubscribedFor(subscriber, forQuery))

    def tracksNode(dgnId: DomainGraphNodeId): Boolean = subscribersToThisNode.contains(dgnId)

    def hasSubscriber(dgnId: DomainGraphNodeId, subscriber: Notifiable): Boolean =
      subscribersToThisNode.get(dgnId).exists(_.hasSubscriber(subscriber))

    /** Whether [[add]] would record anything new, by the same test `add` itself applies. A subscriber already
      * registered for queries this subscription already covers is asking again for what it has.
      */
    def subscriptionWouldChange(
      dgnId: DomainGraphNodeId,
      from: Notifiable,
      relatedQueries: Set[StandingQueryId],
    ): Boolean =
      subscribersToThisNode.get(dgnId) match {
        case None => true
        case Some(subscription) =>
          !subscription.hasSubscriber(from) || !relatedQueries.subsetOf(subscription.queriesFor(from))
      }

    def getAnswer(dgnId: DomainGraphNodeId): Option[Boolean] =
      subscribersToThisNode.get(dgnId).flatMap(_.latestAnswer)

    /** Record `result` as the answer last reported for `dgnId`. */
    def recordAnswer(dgnId: DomainGraphNodeId, result: Boolean): Unit =
      subscribersToThisNode.get(dgnId).foreach { subscription =>
        subscribersToThisNode(dgnId) = subscription.copy(latestAnswer = Some(result))
        updateRelevantToSnapshotOccurred()
      }

    /** Give a standing query subscriber that records no queries its own id, which is exactly what it depends on
      * this node for. A snapshot written before the per-subscriber queries were recorded can carry one; unlike a
      * node subscriber there is nothing to derive here, so this is exact rather than an approximation.
      *
      * @return whether anything changed
      */
    def recordQuerySubscribersOwnIds(): Boolean = {
      var changed = false
      subscribersToThisNode.mapValuesInPlace { (_, subscription) =>
        val repaired = subscription.queriesPerSubscriber.map {
          case (subscriber @ Right(sqId), queries) if queries.isEmpty =>
            changed = true
            subscriber -> Set(sqId)
          case other => other
        }
        subscription.copy(queriesPerSubscriber = repaired)
      }
      changed
    }

    /** Forget every cancelled query from each surviving subscriber; see [[DomainNodeIndex.pruneCancelledQueries]]. */
    def pruneCancelledQueries(isRunning: StandingQueryId => Boolean): Unit = {
      var changed = false
      subscribersToThisNode.mapValuesInPlace { (_, subscription) =>
        val live = subscription.queriesPerSubscriber.map { case (s, qs) => s -> qs.filter(isRunning) }
        if (live == subscription.queriesPerSubscriber) subscription
        else { changed = true; subscription.copy(queriesPerSubscriber = live) }
      }
      if (changed) updateRelevantToSnapshotOccurred()
    }

    /** Forget the answer last reported for `dgnId`, so that the next one counts as a change. */
    def forgetAnswer(dgnId: DomainGraphNodeId): Unit =
      subscribersToThisNode.get(dgnId).foreach { subscription =>
        subscribersToThisNode(dgnId) = subscription.copy(latestAnswer = None)
        updateRelevantToSnapshotOccurred()
      }

    def getRelatedQueries(
      dgnId: DomainGraphNodeId,
    ): Set[StandingQueryId] =
      subscribersToThisNode.get(dgnId).fold(Set.empty[StandingQueryId])(_.relatedQueries)

    def add(
      from: Notifiable,
      dgnId: DomainGraphNodeId,
      relatedQueries: Set[StandingQueryId],
    ): Unit =
      if (tracksNode(dgnId)) {
        val subscription = subscribersToThisNode(dgnId)
        // `hasSubscriber` is not implied by the subset test: a subscriber that names no queries at all would
        // otherwise satisfy it and never be registered.
        if (!subscription.hasSubscriber(from) || !relatedQueries.subsetOf(subscription.queriesFor(from))) {
          updateRelevantToSnapshotOccurred()
          subscribersToThisNode(dgnId) = subscription.addSubscriber(from, relatedQueries)
        }
      } else { // [[from]] is the first subscriber to this DGB, so register the DGB and add [[from]] as a subscriber
        updateRelevantToSnapshotOccurred()
        watchLocalEvents(dgnId)
        subscribersToThisNode(dgnId) = DistinctIdSubscription(
          latestAnswer = None,
          queriesPerSubscriber = Map(from -> relatedQueries),
        )
        ()
      }

    // Returns: the subscriptions removed from if and only if there are no other Notifiables in those subscriptions.
    private[DomainNodeIndexBehavior] def removeSubscriber(
      subscriber: Notifiable,
      dgnId: DomainGraphNodeId,
    ): Map[DomainGraphNodeId, DistinctIdSubscription] =
      subscribersToThisNode
        .get(dgnId)
        .map { subscription =>
          if (subscription.isOnlySubscriber(subscriber)) {
            subscribersToThisNode -= dgnId // remove the whole node if no more subscriptions (no one left to tell)
            Map(dgnId -> subscription)
          } else {
            subscribersToThisNode(dgnId) -= subscriber // else remove just the requested subscriber
            Map.empty[DomainGraphNodeId, DistinctIdSubscription]
          }
        }
        .getOrElse(Map.empty)

    @deprecated(
      "Use updateAnswerAndPropagateToRelevantSubscribers for the propagation case, and the identity of the DGB for the wake-up/initial registration case",
      "Nov 2021",
    )
    private[this] def updateAnswerAndNotifySubscribersInefficiently(
      shouldSendReplies: Boolean,
    )(implicit logConfig: LogConfig): Unit =
      subscribersToThisNode.keys.foreach { dgnId =>
        dgnRegistry.getIdentifiedDomainGraphNode(dgnId) match {
          case Some(dgn) => updateAnswerAndNotifySubscribers(dgn, shouldSendReplies)
          // Dropped at the next sync or wake, see `dropDeadDistinctIdSubscriptions`.
          case None => ()
        }
      }

    def updateAnswerAndPropagateToRelevantSubscribers(
      downstreamNode: DomainGraphNodeId,
      shouldSendReplies: Boolean,
    )(implicit logConfig: LogConfig): Unit = {
      val knownParents = domainGraphNodeParentIndex.parentNodesOf(downstreamNode)
      val parentNodes =
        if (knownParents.nonEmpty) knownParents
        else {
          // Live, a result for a child no parent here tracks means the parent index fell out of sync. On replay
          // it is routine: the index is not journaled and is rebuilt only once the whole journal has been folded.
          // Subscribed DGNs the registry lacks are reported alongside and left in place for the next sync or
          // wake to drop.
          val (recoveredIndex, _) =
            NodeParentIndex.reconstruct(
              domainNodeIndex,
              domainGraphSubscribers.subscribersToThisNode.keys,
              dgnRegistry,
            )
          val parentsAfterRecovery = recoveredIndex.parentNodesOf(downstreamNode)
          if (parentsAfterRecovery.nonEmpty) {
            if (shouldSendReplies)
              log.info(
                safe"""Found out-of-sync nodeParentIndex while propagating a DGN result. Previously-untracked DGN ID was:
                    |${Safe(downstreamNode)}. Previously only tracking children:
                    |${Safe(domainGraphNodeParentIndex.knownChildren.toList)}.
                    |""".cleanLines,
              )
            domainGraphNodeParentIndex = recoveredIndex
            parentsAfterRecovery
          } else {
            // recovery failed -- there is either data loss, or a bug in [[NodeParentIndex.reconstruct]], or the usage of
            // [[NodeParentIndex.reconstruct]] (or any combination thereof).
            if (shouldSendReplies)
              log.error(
                safe"""While propagating a result of a DGN match, found no upstream subscribers that might care about
                 |an update in the provided downstream node. This may indicate a bug in the DGN registration/indexing
                 |logic. Falling back to trying all locally-tracked DGNs. Orphan (downstream) DGN ID is:
                 |${Safe(downstreamNode)}
                 |""".cleanLines,
              )
            else {
              // if shouldSendReplies == false, we're probably restoring a node from sleep via journals. In this case,
              // an incomplete nodeParentIndex is not surprising
              log.debug(
                safe"""While propagating a result of a DGN match, found no upstream subscribers that might care about
                    |an update in the provided downstream node. This may indicate a bug in the DGN registration/indexing
                    |logic. Falling back to trying all locally-tracked DGNs. Orphan (downstream) DGN ID is:
                    |${Safe(downstreamNode)}. This is expected during initial journal replay on a node after wake when
                    |snapshots are disabled or otherwise missing.""".cleanLines,
              )
            }
            updateAnswerAndNotifySubscribersInefficiently(shouldSendReplies): @nowarn
            Set.empty[DomainGraphNodeId]
          }
        }
      parentNodes foreach { dgnId =>
        dgnRegistry.getIdentifiedDomainGraphNode(dgnId) match {
          case Some(dgn) => updateAnswerAndNotifySubscribers(dgn, shouldSendReplies)
          case None => domainGraphNodeParentIndex - ((downstreamNode, dgnId))
        }
      }
    }

    /** Report `result` to every subscriber, if it differs from the answer they were last told.
      *
      * Between them, three places decide when a subscriber hears anything at all:
      *   - the answer changed, and this reports it to everyone;
      *   - a subscriber has only just arrived, and `receiveDomainNodeSubscription` replies to it alone from the
      *     answer already held;
      *   - the answer was dropped because no peer was maintaining it, and `forgetAnswer` makes whatever is
      *     derived next count as a change, so everyone hears it again.
      *
      * Replay runs this too, with the sends suppressed by `conditionallyReplyToAll`. That is how the last answer
      * is rebuilt on a node restored from its journal, without ever being journaled itself.
      */
    private[this] def reportResultToSubscribers(
      dgnId: DomainGraphNodeId,
      subscription: DistinctIdSubscription,
      result: Boolean,
      shouldSendReplies: Boolean,
    ): Unit =
      if (!subscription.latestAnswer.contains(result)) {
        val _ = conditionallyReplyToAll(
          subscription.subscribers,
          DomainNodeSubscriptionResult(qid, dgnId, result),
          shouldSendReplies,
        )
        recordAnswer(dgnId, result)
      }

    def updateAnswerAndNotifySubscribers(
      identifiedDomainGraphNode: IdentifiedDomainGraphNode,
      shouldSendReplies: Boolean,
    )(implicit logConfig: LogConfig): Unit = {
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
          // The one place this pattern asks its peers, for either outcome below: asked before the answer is
          // computed, so a pattern that goes on to find a shared answer has still asked on its own behalf, and the
          // peer has learned the queries that now depend on it. `newIndex` sends only where the peer would learn
          // something, so asking here costs nothing when there is nothing new to say.
          if (matchesLocal) subscribersToThisNode.get(dgnId).foreach { subscription =>
            ensureSubscriptionToDomainEdges(identifiedDomainGraphNode, subscription.relatedQueries, shouldSendReplies)
          }
          val edgesSatisfied = edgesSatisfiedByIndex(single)
          // if no subscribers found for the DGN, clear out expired state from other (non-`subscribers`) bookkeeping
          if (!subscribersToThisNode.contains(dgnId)) {
            cancelSubscription(dgnId, None, shouldSendReplies)
          }

          subscribersToThisNode.get(dgnId) foreach { subscription =>
            (matchesLocal, edgesSatisfied) match {
              // If the query doesn't locally match, don't bother issuing recursive subscriptions
              case (false, _) =>
                reportResultToSubscribers(dgnId, subscription, result = false, shouldSendReplies)

              // If the query locally matches and we've already got edge results, reply with those
              case (true, Some(result)) =>
                // The results the answer came from may have been created for another pattern with the same
                // child. Record this pattern as their parent too, so that a cancellation of the other one
                // leaves them alone (see the teardown in `cancelSubscription`).
                for (child <- identifiedDomainGraphNode.domainGraphNode.children)
                  domainGraphNodeParentIndex += ((child, dgnId))
                reportResultToSubscribers(dgnId, subscription, result, shouldSendReplies)

              // Locally matching, with no answer from the edges yet. The subscriptions that will bring one were
              // issued above, before the answer was computed, and nothing since could have changed what they ask
              // for: reading the index does not touch it, and the only intervening write happens when this node
              // has no subscriber left, which is exactly when this block does not run. Asking again would rescan
              // the edges to find `newIndex` has already recorded every one. Wait for the peers instead.
              case (true, None) => ()
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
                      getRelatedQueries(dgnId),
                      shouldSendReplies,
                    ),
                  )

              // Kleene AND
              (acc, conjResult) match {
                case (Some(false), _) => Some(false)
                case (_, Some(false)) => Some(false)
                case (Some(true), Some(true)) => Some(true)
                case _ => None
              }
            }

          subscribersToThisNode.get(dgnId).foreach { subscription =>
            andMatches.foreach(result => reportResultToSubscribers(dgnId, subscription, result, shouldSendReplies))
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
                      getRelatedQueries(dgnId),
                      shouldSendReplies,
                    ),
                  )

              // Kleene OR
              (acc, disjResult) match {
                case (Some(true), _) => Some(true)
                case (_, Some(true)) => Some(true)
                case (Some(false), Some(false)) => Some(false)
                case _ => None
              }
            }

          subscribersToThisNode.get(dgnId).foreach { subscription =>
            orMatches.foreach(result => reportResultToSubscribers(dgnId, subscription, result, shouldSendReplies))
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
                  getRelatedQueries(dgnId),
                  shouldSendReplies,
                ),
              )

          subscribersToThisNode.get(dgnId).foreach { subscription =>
            notMatches.foreach(result => reportResultToSubscribers(dgnId, subscription, result, shouldSendReplies))
          }

        case mu @ (DomainGraphNode.Mu(_, _) | DomainGraphNode.MuVar(_)) =>
          // While this is a part of a query, it cannot contain PII, so it is safe to log
          log.error(safe"Standing query test node contains illegal sub-node: ${Safe(mu.toString)}")
      }
    }
  }
}
