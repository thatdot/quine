package com.thatdot.quine.graph.behavior

import scala.annotation.unused
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

import org.apache.pekko.actor.Actor

import com.thatdot.common.logging.Log.{ActorSafeLogging, LazySafeLogging, LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.StandingQueryWatchableEventIndex.{EventSubscriber, StandingQueryWithId}
import com.thatdot.quine.graph.cypher.{
  MultipleValuesInitializationEffects,
  MultipleValuesResultsReporter,
  MultipleValuesStandingQuery,
  MultipleValuesStandingQueryEffects,
  MultipleValuesStandingQueryState,
  QueryContext,
}
import com.thatdot.quine.graph.messaging.BaseMessage.Done
import com.thatdot.quine.graph.messaging.StandingQueryMessage.MultipleValuesStandingQuerySubscriber.{
  GlobalSubscriber,
  NodeSubscriber,
}
import com.thatdot.quine.graph.messaging.StandingQueryMessage.{
  CancelMultipleValuesSubscription,
  CreateMultipleValuesStandingQuerySubscription,
  MultipleValuesStandingQueryCommand,
  MultipleValuesStandingQuerySubscriber,
  NewMultipleValuesStateResult,
  UpdateStandingQueriesCommand,
  UpdateStandingQueriesNoWake,
  UpdateStandingQueriesWake,
}
import com.thatdot.quine.graph.messaging.{QuineIdOps, QuineRefOps}
import com.thatdot.quine.graph.metrics.implicits.TimeFuture
import com.thatdot.quine.graph.{
  BaseNodeActor,
  MultipleValuesStandingQueryPartId,
  NodeChangeEvent,
  RunningStandingQuery,
  StandingQueryId,
  StandingQueryPattern,
  WatchableEventType,
  cypher,
}
import com.thatdot.quine.model.{EdgeDirection, HalfEdge, PropertyValue, QuineIdProvider}
import com.thatdot.quine.persistor.codecs.MultipleValuesStandingQueryStateCodec
import com.thatdot.quine.persistor.{NamespacedPersistenceAgent, PersistenceConfig, PersistenceSchedule}
import com.thatdot.quine.util.Log.implicits._

trait MultipleValuesStandingQueryBehavior
    extends Actor
    with ActorSafeLogging
    with BaseNodeActor
    with QuineIdOps
    with QuineRefOps
    with StandingQueryBehavior {

  protected def syncStandingQueries(): Unit

  protected def persistor: NamespacedPersistenceAgent

  protected def persistenceConfig: PersistenceConfig
  implicit protected def logConfig: LogConfig

  /** Give a newly created state's per-edge bookkeeping a home other than the state's own heap, when this node has
    * one for it.
    *
    * A node small enough to hold its own edges holds this too, and changes nothing. A node whose edges live in the
    * persistor cannot afford a row per edge in memory either, and substitutes a store that keeps those rows beside
    * the edges, which answers after a round trip, and so is the node's business rather than the state's.
    *
    * Called before anything has been recorded in the state, so that the description of the node that follows
    * registration does not first fill the heap store the state was built with.
    */
  protected def relocateStandingQueryBookkeeping(
    globalId: StandingQueryId,
    partId: MultipleValuesStandingQueryPartId,
    sqState: MultipleValuesStandingQueryState,
  ): Unit = ()

  /** Bring this node's locally-tracked standing queries in sync with the current graph state. While the node is asleep,
    * no events could have occurred on the node itself, but there might have been state changes to the graph which
    * affect this node (like cancelled or unpropagated standing queries). Bring this node up to date with the graph:
    *  - Remove SQs registered on the node but not on the graph
    *  - [Re]subscribe to each SQ registered on the graph (no-op if already registered)
    */
  def updateMultipleValuesStandingQueriesOnNode(): Unit = {

    val runningStandingQueries = // Silently empty if namespace is absent.
      graph.standingQueries(namespace).fold(Map.empty[StandingQueryId, RunningStandingQuery])(_.runningStandingQueries)

    val removeParts = multipleValuesStandingQueries.filter { case ((sqId, _), _) =>
      !runningStandingQueries.contains(sqId)
    }

    removeParts.foreach { case (sqIdTuple, (_, sqState)) =>
      multipleValuesStandingQueries.remove(sqIdTuple)
      discardStandingQueryPart(sqIdTuple, sqState)
      sqState.relevantEventTypes(graph.labelsProperty).foreach { (eventType: WatchableEventType) =>
        watchableEventIndex.unregisterStandingQuery(EventSubscriber(sqIdTuple), eventType)
      }
    }

    multipleValuesResultReporters = multipleValuesResultReporters.filter { case (sqId, _) =>
      runningStandingQueries.contains(sqId)
    }

    // Register new MultipleValues SQs created since this node slept in the node's live state
    for {
      (sqId, runningSQ) <- runningStandingQueries
      query <- runningSQ.query.queryPattern match {
        case query: StandingQueryPattern.MultipleValuesQueryPattern => Some(query.compiledQuery)
        case _ => None
      }
    } {
      val subscriber = MultipleValuesStandingQuerySubscriber.GlobalSubscriber(sqId)
      // TODO for tighter consistency and possibly increased performance, consider completing this within the startup
      //      instead of as a self-tell (nontrivial)
      self ! CreateMultipleValuesStandingQuerySubscription(subscriber, query) // no-op if already registered
    }

    // A reciprocal state is subscribed to its `andThen` for as long as it exists. The invariant is described on
    // [[cypher.EdgeSubscriptionReciprocalState]]. States created through the behavior get that subscription from
    // `onInitialize`, and states decoded at wake got it from their own past, except in one case: the wake-time fold
    // can assemble a state out of rows that were never subscribed (each row's edge was absent when it was written),
    // and a fold has no way to describe this node to an `andThen` state that does not exist yet. So the link is
    // re-checked on every wake, and where it is missing the ordinary subscription path establishes it: creating
    // the `andThen` state if there is none, and answering with its current results either way, which is also why
    // the fold leaves this to happen here rather than writing a bare subscriber entry, since a bare entry would
    // relay nothing until the `andThen` next changed. Re-checking every wake is what makes the link self-healing:
    // an interruption between establishing it and persisting it converges at the next wake.
    multipleValuesStandingQueries.foreach {
      case ((sqId, partId), (_, reciprocal: cypher.EdgeSubscriptionReciprocalState)) =>
        val linked = multipleValuesStandingQueries
          .get(sqId -> reciprocal.andThenId)
          .exists { case (andThenSubscription, _) =>
            andThenSubscription.subscribers.contains(NodeSubscriber(qid, sqId, partId))
          }
        if (!linked) {
          graph
            .standingQueries(namespace)
            .flatMap(sqns => Try(sqns.getStandingQueryPart(reciprocal.andThenId)).toOption) match {
            case Some(andThenQuery) =>
              self ! CreateMultipleValuesStandingQuerySubscription(NodeSubscriber(qid, sqId, partId), andThenQuery)
            case None =>
              // The part will be resolvable once the query is fully registered; the next wake checks again.
              log.info(
                log"""A reciprocal state on node: $qid is not subscribed to its inner query part, which could not be
                     |looked up to subscribe now. Results will not flow through this node for standing query
                     |${Safe(sqId)} until a later wake finds the part.""".cleanLines,
              )
          }
        }
      case _ => ()
    }
  }

  implicit class MultipleValuesStandingQuerySubscribersOps(subs: MultipleValuesStandingQueryPartSubscription)
      extends MultipleValuesStandingQueryEffects
      with MultipleValuesInitializationEffects
      with LazySafeLogging {

    @throws[NoSuchElementException]("When a MultipleValuesStandingQueryPartId is not known to this graph")
    def lookupQuery(queryPartId: MultipleValuesStandingQueryPartId): MultipleValuesStandingQuery =
      graph.standingQueries(namespace).get.getStandingQueryPart(queryPartId)
    // TODO: Would be better to replace `.get` here ^^ but it actually works since both throw the same exception.

    def createSubscription(onNode: QuineId, query: MultipleValuesStandingQuery): Unit = {
      val subscriber =
        MultipleValuesStandingQuerySubscriber.NodeSubscriber(executingNodeId, subs.globalId, subs.forQuery)
      onNode ! CreateMultipleValuesStandingQuerySubscription(subscriber, query)
    }

    def cancelSubscription(onNode: QuineId, queryId: MultipleValuesStandingQueryPartId): Unit = {
      val subscriber =
        MultipleValuesStandingQuerySubscriber.NodeSubscriber(executingNodeId, subs.globalId, subs.forQuery)
      // optimization: only perform cancellations for running top-level queries (or to clear out local state)
      if (
        executingNodeId == onNode || graph
          .standingQueries(namespace)
          .flatMap(_.runningStandingQuery(subs.globalId))
          .isDefined
      ) {
        onNode ! CancelMultipleValuesSubscription(subscriber, queryId)
      } else {
        logger.info(
          safe"""Declining to process MultipleValues cancellation message on node: ${Safe(onNode)}
                |for deleted Standing Query with ID ${Safe(subs.globalId)}""".cleanLines,
        )
      }
    }

    def reportUpdatedResults(resultGroup: Seq[cypher.QueryContext]): Unit =
      ifStandingQueryStillExists(subs.subscribers.foreach(sendResults(_, resultGroup)))

    def reportUpdatedResultsTo(
      subscriber: MultipleValuesStandingQuerySubscriber,
      resultGroup: Seq[cypher.QueryContext],
    ): Unit =
      ifStandingQueryStillExists(sendResults(subscriber, resultGroup))

    def reportUpdatedResultsToNode(onNode: QuineId, resultGroup: Seq[cypher.QueryContext]): Unit =
      ifStandingQueryStillExists {
        subs.subscribers.foreach {
          case subscriber: NodeSubscriber if subscriber.subscribingNode == onNode =>
            sendResults(subscriber, resultGroup)
          case _ => ()
        }
      }

    def reportUpdatedResultsToRemotePart(
      onNode: QuineId,
      forPart: MultipleValuesStandingQueryPartId,
      resultGroup: Seq[cypher.QueryContext],
    ): Unit =
      // Deliberately not `ifStandingQueryStillExists`: this effect may be invoked off the node's thread, from a
      // subscriber store's stream over its rows, and the cleanup that check performs on absence mutates node state.
      // Here the absence check is read-only and a report to a query already gone is simply not sent.
      if (standingQueryStillRunning) {
        sendResults(NodeSubscriber(onNode, subs.globalId, forPart), resultGroup)
      }

    def reportUpdatedResultsToEntitledNodes(resultGroup: Seq[cypher.QueryContext], checkAtMost: Int)(
      entitled: QuineId => Boolean,
    ): Unit =
      // Checked once for the whole pass, not once per subscriber: the absence branch discards what this node
      // holds for the query, which is not something to do partway through reporting it.
      ifStandingQueryStillExists {
        cypher.MultipleValuesStandingQueryEffects
          .eachEntitledNodeSubscriber(subs.subscribers, checkAtMost, entitled)(sendResults(_, resultGroup))
      }

    def matchingEdgesTo(
      edgeName: Option[Symbol],
      edgeDirection: Option[EdgeDirection],
      other: QuineId,
    ): Option[Seq[HalfEdge]] =
      // Drained inside the `try`, not handed back lazily: on a node whose edges live in the persistor the read
      // happens as the iterator is walked, so an iterator returned from here would throw in its caller's hands.
      // Draining is affordable because every case is keyed by `other`: the answer is the edges to one node.
      try Some((edgeName, edgeDirection) match {
        case (Some(edgeType), Some(direction)) =>
          val halfEdge = HalfEdge(edgeType, direction, other)
          if (edges.contains(halfEdge)) Seq(halfEdge) else Seq.empty
        case (Some(edgeType), None) => edges.matching(edgeType, other).toSeq
        case (None, Some(direction)) => edges.matching(direction, other).toSeq
        case (None, None) => edges.matching(other).toSeq
      })
      catch {
        case NonFatal(err) =>
          log.warn(
            log"""Could not read the edges of node: $qid to node: $other while updating a standing query part. That
                 |part will neither retract a result nor cancel a subscription on the strength of an answer this
                 |node could not get, so it may hold a subscription or a reported result longer than the edges
                 |warrant.""".cleanLines withException err,
          )
          None
      }

    private[this] def sendResults(
      subscriber: MultipleValuesStandingQuerySubscriber,
      resultGroup: Seq[cypher.QueryContext],
    ): Unit = subscriber match {
      case MultipleValuesStandingQuerySubscriber.NodeSubscriber(quineId, _, upstreamPartId) =>
        quineId ! NewMultipleValuesStateResult(
          executingNodeId,
          subs.forQuery,
          subs.globalId,
          Some(upstreamPartId),
          resultGroup,
        )
      case MultipleValuesStandingQuerySubscriber.GlobalSubscriber(sqId) =>
        val reporter = multipleValuesResultReporters(sqId)
        val _ = reporter.applyAndEmitResults(resultGroup)
    }

    /** Whether the standing query behind this subscription still exists (hasn't been deleted). Read-only, and safe
      * off the node's thread.
      */
    private[this] def standingQueryStillRunning: Boolean =
      graph.standingQueries(namespace).fold(false)(ns => ns.runningStandingQuery(subs.globalId).isDefined)

    /** Verify the SQ still exists (hasn't been deleted) before reporting anything for it. */
    private[this] def ifStandingQueryStillExists(report: => Unit): Unit =
      if (standingQueryStillRunning) {
        report
      } else {
        // In this branch, the standing query or its namespace doesn't exist (SQ has been cancelled or namespace
        // deleted). This is how an awake node finds out: it goes to report, and there is nobody to report to. What
        // it was holding for that query goes now, rather than being left for some later wake to notice.
        val noLongerRunning = multipleValuesStandingQueries.filter { case ((sqId, _), _) => sqId == subs.globalId }
        noLongerRunning.foreach { case (key, (_, sqState)) => discardStandingQueryPart(key, sqState) }
        val _ = multipleValuesStandingQueries --= noLongerRunning.keys
      }

    /** The QuineId of _this_ node which has the behavior mixed in. */
    val executingNodeId: QuineId = qid

    val idProvider: QuineIdProvider = MultipleValuesStandingQueryBehavior.this.idProvider

    def currentProperties: Map[Symbol, PropertyValue] = properties
    val labelsProperty: Symbol = graph.labelsProperty
  }

  /** Locally registered & running standing queries
    *
    * The `StandingQueryId` is the global SQ ID. The `MultipleValuesStandingQueryPartId` is the incoming subscription
    * to whether the node managing this instance of `multipleValuesStandingQueries` matches the query represented by
    * that ID.
    */
  protected def multipleValuesStandingQueries: mutable.Map[
    (StandingQueryId, MultipleValuesStandingQueryPartId),
    (MultipleValuesStandingQueryPartSubscription, MultipleValuesStandingQueryState),
  ]

  /** Reporters for global subscribers to standing queries. These are used to accumulate results and send them as diffs
    */
  protected var multipleValuesResultReporters: Map[StandingQueryId, MultipleValuesResultsReporter]

  /** When running in [[com.thatdot.quine.persistor.PersistenceSchedule.OnNodeSleep]], updates
    * will be buffered here and persisted only on node sleep
    */
  final val pendingMultipleValuesWrites: mutable.Set[(StandingQueryId, MultipleValuesStandingQueryPartId)] =
    mutable.Set.empty[(StandingQueryId, MultipleValuesStandingQueryPartId)]

  /** Route a node event to exactly the stateful standing queries interested in it
    *
    * @param event new node event
    * @return future that completes once the SQ updates are saved to disk
    */
  final protected def updateMultipleValuesSqs(
    events: Seq[NodeChangeEvent],
    subscriber: StandingQueryWithId,
  )(implicit logConfig: LogConfig): Future[Unit] = {

    val persisted: Option[Future[Unit]] = for {
      tup <- multipleValuesStandingQueries.get((subscriber.queryId, subscriber.partId))
      (subscribers, sqState) = tup
      somethingChanged = sqState.onNodeEvents(events, subscribers)
      if somethingChanged
    } yield persistMultipleValuesStandingQueryState(subscriber.queryId, subscriber.partId, Some(tup))

    persisted.getOrElse(Future.unit)
  }

  /** Process a query command to create/remove a standing query or to report/invalidate a result
    *
    * @param command standing query command to process
    */
  protected def multipleValuesStandingQueryBehavior(command: MultipleValuesStandingQueryCommand): Unit = command match {
    case CreateMultipleValuesStandingQuerySubscription(subscriber, query) =>
      val combinedId = subscriber.globalId -> query.queryPartId
      val alreadyTrackingState
        : Option[(MultipleValuesStandingQueryPartSubscription, MultipleValuesStandingQueryState)] =
        multipleValuesStandingQueries.get(combinedId)

      val (subscription, sqState) = alreadyTrackingState
        .map { case tup @ (_, oldState) =>
          // Found a state already being tracked for this query. If the query is different, log a warning about the
          // collision. A state that resolves no query (an edge subscription reciprocal, which every node across a
          // matching edge subscribes to with the same constraints) has nothing to disagree with.
          if (!oldState.resolvedQuery.forall(_ == query))
            log.warn(
              safe"""While creating subscription for MultipleValues Standing Query [part] $query, detected
                    |that MultipleValuesStandingQuery part identified by $combinedId is ambiguous.
                    |Refusing to register query. Continuing to provide results for ID ${combinedId._2}
                    |to ${oldState.query: MultipleValuesStandingQuery}. New query may miss results. This is a bug in
                    |MultipleValuesStandingQueryPartId generation.
                    |""".cleanLines,
            )
          tup
        }
        .getOrElse {
          // This node is becoming aware of this SQ state for the first time, so create a state to track the query and an
          // empty subscription to that state, and have that kick off any side effects so that it will eventually produce
          // results (eg, if registering a SubscribeAcrossEdge, and there are edges matching its pattern, it should issue
          // subscriptions across the pattern-matching edges)
          val sqState = query.createState()
          relocateStandingQueryBookkeeping(subscriber.globalId, query.queryPartId, sqState)
          // NB this subscription must have an empty set of subscribers to start with. This serves two purposes:
          // - First, it avoids duplicate events being sent by onNodeEvents and the readResults-then-send later
          // - Second, it ensures that the "perform side effects necessary when adding a subscriber" if-block
          //   below gets executed
          val subscription =
            MultipleValuesStandingQueryPartSubscription(query.queryPartId, subscriber.globalId, mutable.Set.empty)
          multipleValuesStandingQueries += combinedId -> (subscription -> sqState)
          sqState.rehydrate(subscription)
          sqState.onInitialize(subscription)
          val canInform: Set[WatchableEventType] = sqState.initialEventTypes(graph.labelsProperty).toSet
          sqState.relevantEventTypes(graph.labelsProperty).foreach { (eventType: WatchableEventType) =>
            // Describe the node's current state only for the categories the state says it can learn something from,
            // and do not ask for a description otherwise: producing one is what reads the node's edges, which on
            // the nodes this matters for is the whole cost. Then deliver it a page at a time: a node with a million
            // matching edges would otherwise hold a million events at once to describe itself.
            if (canInform.contains(eventType))
              watchableEventIndex
                .registerStandingQuery(EventSubscriber(combinedId), eventType, properties, edges)
                .grouped(MultipleValuesStandingQueryBehavior.initialEventPageSize)
                .foreach(page => sqState.onNodeEvents(page, subscription))
            else watchableEventIndex.registerStandingQuery(EventSubscriber(combinedId), eventType)
          }
          subscription -> sqState
        }

      // TODO: don't ignore the persistenceEffects Future!
      def addToNodeSubscriberSet(): Future[Unit] =
        // Updates the subscribers set in-place (within `multipleValuesStandingQueries`), returning true iff
        // the subscriber is new
        if (subscription.subscribers.add(subscriber)) {
          // If the new subscriber is the end-user, shim in a MultipleValuesResultsReporter to deduplicate and manage
          // result groups for this query state
          subscriber match {
            case NodeSubscriber(_, _, queryId) => require(subscription.forQuery != queryId)
            case GlobalSubscriber(_) =>
              graph
                .standingQueries(namespace)
                .flatMap(_.runningStandingQuery(subscriber.globalId))
                .foreach { sq =>
                  if (!multipleValuesResultReporters.contains(subscriber.globalId)) {
                    multipleValuesResultReporters +=
                      subscriber.globalId -> new MultipleValuesResultsReporter(sq, Seq.empty)
                  }
                }
          }

          // Regardless of whether we were already tracking the state, give the new subscriber the
          // currently-calculable results. Only the new subscriber: the others already have this group, and for a
          // state whose answer depends on the subscriber, this group is not theirs to receive.
          val maybeResultGroup: Option[Seq[QueryContext]] = sqState.readResultsFor(subscriber, subscription)
          maybeResultGroup.foreach(subscription.reportUpdatedResultsTo(subscriber, _))

          // finally, save the updated state (including the new subscription)
          persistMultipleValuesStandingQueryState(
            subscriber.globalId,
            query.queryPartId,
            Some(subscription -> sqState),
          )
        } else Future.unit

      @unused val persistenceEffects: Future[Unit] = sqState match {
        case reciprocal: cypher.EdgeSubscriptionReciprocalState if reciprocal.subscriberStore.isDefined =>
          subscriber match {
            case nodeSubscriber @ NodeSubscriber(subscribingNode, _, forPart)
                if reciprocal.externalSubscriberForQuery.forall(_ == forPart) =>
              require(subscription.forQuery != forPart)
              val isFirstRecorded = reciprocal.externalSubscriberForQuery.isEmpty
              reciprocal.externalSubscriberForQuery = Some(forPart)
              // A blind write, with no was-it-new gate: recording a subscriber twice records the same row, and
              // answering one twice hands it the level it already has.
              reciprocal.subscriberStore.get.add(subscribingNode)
              val maybeResultGroup: Option[Seq[QueryContext]] = sqState.readResultsFor(nodeSubscriber, subscription)
              maybeResultGroup.foreach(subscription.reportUpdatedResultsTo(nodeSubscriber, _))
              // The blob records which part the recorded subscribers are answered as, set once by the first of
              // them. Every subscriber after that changes nothing the blob carries, which is the point of the
              // store.
              if (isFirstRecorded)
                persistMultipleValuesStandingQueryState(
                  subscriber.globalId,
                  query.queryPartId,
                  Some(subscription -> sqState),
                )
              else Future.unit
            case _ =>
              // A subscriber the store cannot record is held in the node's subscriber set beside the store:
              // correct, merely unshared. That means a global subscriber, or one citing a different query part than
              // the recorded ones, which is possible only for a hand-built query.
              addToNodeSubscriberSet()
          }
        case _ => addToNodeSubscriberSet()
      }

    /** This protocol is only _initiated_ when an edge is removed, causing the tree of subqueries to become selectively
      * irrelevant and worth cleaning up. Messages will either be sent from a node to itself (to expire state locally),
      * or to other nodes next in the newly irrelevant tree of subscriptions. Cancellations will continue to propagate
      * through the subtree only as long as no subscribers remain at each step.
      */
    case CancelMultipleValuesSubscription(subscriber, queryPartId) =>
      val combinedId = subscriber.globalId -> queryPartId
      multipleValuesStandingQueries.get(combinedId) match {
        case None => () // Has already been cancelled (or otherwise doesn't exist). No need to do anything.
        case Some(tup @ (subscription, sqState)) =>
          val removedFromNodeSet = subscription.subscribers.remove(subscriber)
          if (subscriber.isInstanceOf[GlobalSubscriber]) {
            multipleValuesResultReporters -= subscriber.globalId
          }
          // Only fully remove the running standing query if no subscribers remain. There might be multiple subscribers
          // to the same `combinedId` if, for example, this node is (was) at the bottom of a diamond pattern.
          def concludeCancellation(externalSubscribersRemain: Boolean): Unit =
            if (!externalSubscribersRemain && subscription.subscribers.isEmpty) {
              multipleValuesStandingQueries -= combinedId // stop managing state.
              discardStandingQueryPart(combinedId, sqState)
//            sqState.query.children.foreach(subquery => // Unsubscribe to subqueries.
//              ??? ! CancelMultipleValuesSubscription(
//                NodeSubscriber(qid, subscriber.globalId, queryPartId),
//                subquery.queryPartId
//              )
//            )
            } else if (removedFromNodeSet) {
              val _ = persistMultipleValuesStandingQueryState(subscriber.globalId, queryPartId, Some(tup))
            }
          sqState match {
            case reciprocal: cypher.EdgeSubscriptionReciprocalState if reciprocal.subscriberStore.isDefined =>
              // Some or all of this state's subscribers are recorded in the persistor, so whether any remain is
              // answered after a round trip. The store pauses the node's message processing for it, which is what
              // lets the discard decision run in the callback without anything sneaking in between.
              val store = reciprocal.subscriberStore.get
              def concludeFromStore(storeIsEmpty: Try[Boolean]): Unit = storeIsEmpty match {
                case Success(nowEmpty) => concludeCancellation(externalSubscribersRemain = !nowEmpty)
                case Failure(err) =>
                  // Kept, because the other answer is destructive and this one is not: discarding drops the rows of
                  // whatever subscribers do remain and cancels this reciprocal's subscription to its inner part,
                  // and there is nothing here that says none remain. What it costs is that nothing revisits it:
                  // the next cancellation would ask again, and the cancellation this question is about may have
                  // been the last one this part will ever see.
                  log.warn(
                    log"""Could not determine whether standing query part: ${Safe(queryPartId)} of standing query:
                         |${Safe(subscriber.globalId)} on node: $qid has any subscribers left after a cancellation,
                         |after retrying. Keeping the state, because discarding it would silence any subscriber that
                         |does remain. If that was this part's last subscriber, the state and its rows stay on this
                         |node until the standing query is cancelled.""".cleanLines withException err,
                  )
                  concludeCancellation(externalSubscribersRemain = true)
              }
              subscriber match {
                case NodeSubscriber(subscribingNode, _, forPart)
                    if !removedFromNodeSet && reciprocal.externalSubscriberForQuery.contains(forPart) =>
                  store.remove(subscribingNode)(concludeFromStore)
                case _ if subscription.subscribers.isEmpty => store.isEmpty(concludeFromStore)
                case _ => concludeCancellation(externalSubscribersRemain = true)
              }
            case _ => concludeCancellation(externalSubscribersRemain = false)
          }
      }

    case newResult @ NewMultipleValuesStateResult(
          fromQid @ _,
          queryPartId @ _,
          globalId,
          forQueryPartIdOpt,
          result @ _,
        ) =>
      val queryPartIdForResult = forQueryPartIdOpt.get // this is never `None` for node subscribers
      // Deliver the result to interested standing query state
      multipleValuesStandingQueries.get(globalId -> queryPartIdForResult) match {
        case None =>
          log.whenWarnEnabled {
            // Look up the relevant SQ part for logging purposes. If no part can be found for the provided ID,
            // assume it's been deleted prior to this message being processed.
            val relevantSqPartStr = Try(
              graph
                .standingQueries(namespace)
                .get
                .getStandingQueryPart(queryPartIdForResult),
            ).fold(_ => "deleted SQ part", part => s"$part")
            log.warn(
              log"""Got a result from: $fromQid for: ${Safe(queryPartIdForResult)},
                   |but this node does not track: ${Safe(queryPartIdForResult)} (${Safe(relevantSqPartStr)})
                   |""".cleanLines,
            )
          }
        // Possible if local shutdown happens right before a result is received
        case Some(tup @ (subscribers, sqState)) =>
          val somethingDidChange = sqState.onNewSubscriptionResult(newResult, subscribers)
          if (somethingDidChange) {
            val _ = persistMultipleValuesStandingQueryState(globalId, queryPartIdForResult, Some(tup))
            // TODO: don't ignore the returned future!
          }
      }
  }

  protected def updateStandingQueriesBehavior(command: UpdateStandingQueriesCommand): Unit = command match {
    case UpdateStandingQueriesNoWake =>
      syncStandingQueries()

    case msg: UpdateStandingQueriesWake =>
      syncStandingQueries()
      msg ?! Done
  }

  protected def persistMultipleValuesStandingQueryState(
    globalId: StandingQueryId,
    localId: MultipleValuesStandingQueryPartId,
    state: Option[(MultipleValuesStandingQueryPartSubscription, MultipleValuesStandingQueryState)],
  ): Future[Unit] =
    persistenceConfig.standingQuerySchedule match {
      case PersistenceSchedule.OnNodeUpdate =>
        val serialized = state.map(
          MultipleValuesStandingQueryStateCodec.format.write,
        )
        serialized.foreach(arr => metrics.standingQueryStateSize(namespace, globalId).update(arr.length))
        new TimeFuture(metrics.persistorSetStandingQueryStateTimer).time[Unit](
          persistor.setMultipleValuesStandingQueryState(
            globalId,
            qid,
            localId,
            serialized,
          ),
        )

      // Don't save now, but record the fact this will need to be saved on sleep
      case PersistenceSchedule.OnNodeSleep =>
        pendingMultipleValuesWrites += globalId -> localId
        updateRelevantToSnapshotOccurred()
        Future.unit

      // No-op: don't save anything!
      case PersistenceSchedule.Never =>
        Future.unit
    }

  /** Let go of a query part this node is no longer running.
    *
    * A node finds out that a standing query is gone in three different ways: a subscription is cancelled beneath
    * it, it tries to report and discovers there is nobody to report to, or it syncs itself against the graph. What
    * it should do about it is the same in all three: forget the state, forget whatever that state kept
    * somewhere other than its own blob, and delete the blob that pointed at both. Leaving any of it makes the
    * next wake responsible for tidying, which is how these came to be counted and left in the first place.
    *
    * Does not remove the state from [[multipleValuesStandingQueries]]; callers do that, because they differ in how
    * many they are removing at once.
    */
  private[this] def discardStandingQueryPart(
    key: (StandingQueryId, MultipleValuesStandingQueryPartId),
    sqState: cypher.MultipleValuesStandingQueryState,
  ): Unit = {
    sqState match {
      case reciprocal: cypher.EdgeSubscriptionReciprocalState =>
        // A reciprocal subscribes to its `andThen` once, for as long as it exists, and this is where it stops
        // existing. Left in place, that subscription is what a *re*-created reciprocal would collide with: its own
        // subscription is the same value (the part id is derived from the constraints, so it is the same id),
        // and the `andThen` skips a subscriber it already has, including the report that would have answered the
        // new state. It would then hold no result at all until the `andThen` next changed on its own account.
        if (graph.standingQueries(namespace).exists(_.runningStandingQuery(key._1).isDefined))
          self ! CancelMultipleValuesSubscription(NodeSubscriber(qid, key._1, key._2), reciprocal.andThenId)
      case _ => ()
    }
    dropRecordsHeldElsewhere(key).failed
      .foreach { err =>
        log.info(
          log"""Could not drop what query part ${Safe(key._2)} of standing query ${Safe(key._1)} recorded outside its
             |own state on node: $qid. Those rows are now unreachable rather than merely unused.""".cleanLines
          withException err,
        )
      }(context.dispatcher)
    val _ = persistMultipleValuesStandingQueryState(key._1, key._2, None)
  }

  /** Forget whatever a query part recorded outside its own blob on this node, named by its key rather than asked of
    * the state.
    *
    * By key, because the state is the one thing that cannot be relied on to know. A state's handle on where its rows
    * are is installed by the node, and there are three moments when a part is discarded before that has happened:
    * during the constructor's sync, which runs before the wake's handles are installed; when installing one fails;
    * and when a node installs no handle at all for a blob that says its rows are elsewhere. In each of those the
    * state would answer that it has nothing anywhere, and the rows would stay forever, reachable only under this
    * key, which is about to belong to nobody.
    *
    * A state that keeps everything in its own blob has nothing held elsewhere, so the default is to do nothing.
    */
  protected def dropRecordsHeldElsewhere(
    key: (StandingQueryId, MultipleValuesStandingQueryPartId),
  ): Future[Unit] = Future.unit
}

object MultipleValuesStandingQueryBehavior {

  /** How many events describing pre-existing node state are held at once while registering a standing query.
    *
    * This bounds the memory a registration takes, not the work it does: the whole replay still happens in the message
    * that registers the query. Spreading it across messages would need a cursor over the edges that can be put down
    * and picked up again, which [[com.thatdot.quine.graph.edges.EdgeCollectionView]] does not offer.
    */
  val initialEventPageSize: Int = 1000
}

/** Represents a subscription held on a specific node to the results of a query run on that node.
  * Subscribers will be added and removed over time.
  *
  * @param forQuery the query part representing what is being subscribed to. The
  * @param globalId the Standing Query ID set once each time the API call is issued.
  * @param subscribers each party interested in the results of this subscription. Each subscriber that is a
  *                    `NodeSubscriber` also has a queryPartId which corresponds to that node's bookkeeping for how
  *                    to map a delivered result back to it's own relevant query.
  *
  *                    Not always the whole record. An
  *                    [[com.thatdot.quine.graph.cypher.EdgeSubscriptionReciprocalState]] with a
  *                    [[com.thatdot.quine.graph.cypher.ReciprocalSubscriberStore]] keeps most of its subscribers
  *                    there instead, and this set holds only the ones that store cannot record. The two are
  *                    alternatives holding disjoint subscribers, never two views of the same ones, so reporting to
  *                    everyone means going through both.
  */
final case class MultipleValuesStandingQueryPartSubscription(
  forQuery: MultipleValuesStandingQueryPartId,
  globalId: StandingQueryId,
  subscribers: mutable.Set[MultipleValuesStandingQuerySubscriber],
)
