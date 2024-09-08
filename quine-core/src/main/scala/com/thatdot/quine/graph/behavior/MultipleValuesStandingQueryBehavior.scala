package com.thatdot.quine.graph.behavior

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.Try

import org.apache.pekko.actor.Actor

import com.thatdot.quine.graph.StandingQueryWatchableEventIndex.{EventSubscriber, StandingQueryWithId}
import com.thatdot.quine.graph.cypher.{
  MultipleValuesResultsReporter,
  MultipleValuesStandingQuery,
  MultipleValuesStandingQueryEffects,
  MultipleValuesStandingQueryState,
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
import com.thatdot.quine.graph.{
  BaseNodeActor,
  MultipleValuesStandingQueryPartId,
  NodeChangeEvent,
  RunningStandingQuery,
  StandingQueryId,
  StandingQueryPattern,
  TimeFuture,
  WatchableEventType,
  cypher,
}
import com.thatdot.quine.model.{PropertyValue, QuineId, QuineIdProvider}
import com.thatdot.quine.persistor.codecs.MultipleValuesStandingQueryStateCodec
import com.thatdot.quine.persistor.{NamespacedPersistenceAgent, PersistenceConfig, PersistenceSchedule}
import com.thatdot.quine.util.Log._
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

  /** Bring this node's locally-tracked standing queries in sync with the current graph state. While the node is asleep,
    * no events could have occurred on the node itself, but there might have been state changes to the graph which
    * affect this node (like cancelled or unpropagated standing queries). Bring this node up to date with the graph:
    *  - Remove SQs registered on the node but not on the graph
    *  - [Re]subscribe to each SQ registered on the graph (no-op if already registered)
    */
  def updateMultipleValuesStandingQueriesOnNode(): Unit = {

    val runningStandingQueries = // Silently empty if namespace is absent.
      graph.standingQueries(namespace).fold(Map.empty[StandingQueryId, RunningStandingQuery])(_.runningStandingQueries)

    // Remove SQs from this node if they are no longer running in the graph (updates in place)
    val _ = multipleValuesStandingQueries.filterInPlace { case ((sqId, _), _) => runningStandingQueries.contains(sqId) }
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
  }

  implicit class MultipleValuesStandingQuerySubscribersOps(subs: MultipleValuesStandingQueryPartSubscription)
      extends MultipleValuesStandingQueryEffects
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
      // Verify the SQ still exists (hasn't been deleted)
      if (graph.standingQueries(namespace).fold(false)(ns => ns.runningStandingQuery(subs.globalId).isDefined)) {
        subs.subscribers.foreach {
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
            reporter.applyAndEmitResults(resultGroup)
        }
      } else {
        // In this branch, the standing query or its namespace doesn't exist (SQ has been cancelled or namespace deleted)
        // Delete the state if the globalId has been removed. (no need to cancel results. updates in place.)
        val _ = multipleValuesStandingQueries.filterInPlace { case ((sqId, _), _) => sqId != subs.globalId }
      }

    /** The QuineId of _this_ node which has the behavior mixed in. */
    val executingNodeId: QuineId = qid

    val idProvider: QuineIdProvider = MultipleValuesStandingQueryBehavior.this.idProvider

    def currentProperties: Map[Symbol, PropertyValue] = properties - graph.labelsProperty
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
      multipleValuesStandingQueries.get(combinedId) match {
        case None =>
          val sqState = query.createState()
          val subscription =
            MultipleValuesStandingQueryPartSubscription(query.queryPartId, subscriber.globalId, mutable.Set(subscriber))
          if (subscriber.isInstanceOf[MultipleValuesStandingQuerySubscriber.GlobalSubscriber]) {
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
          multipleValuesStandingQueries += combinedId -> (subscription -> sqState)
          sqState.rehydrate(subscription)
          sqState.onInitialize(subscription)
          sqState.relevantEventTypes.foreach { (eventType: WatchableEventType) =>
            val initialEvents = watchableEventIndex.registerStandingQuery(
              EventSubscriber(combinedId),
              eventType,
              properties,
              edges,
            )

            // Notify the standing query of events for pre-existing node state
            sqState.onNodeEvents(initialEvents, subscription)
          }
          val _ = persistMultipleValuesStandingQueryState(
            subscriber.globalId,
            query.queryPartId,
            Some(subscription -> sqState),
          ) // TODO: don't ignore the returned future!

        // SQ is already running on the node
        case Some(tup @ (subscription, sqState)) =>
          // Check if this is already an existing subscriber (if so, it's a no-op)
          if (subscription.subscribers.add(subscriber)) {
            subscriber match {
              case NodeSubscriber(_, _, queryId) => require(subscription.forQuery != queryId)
              case GlobalSubscriber(sqId) =>
                graph
                  .standingQueries(namespace)
                  .flatMap(_.runningStandingQuery(sqId))
                  .foreach { sq =>
                    if (!multipleValuesResultReporters.contains(subscriber.globalId)) {
                      multipleValuesResultReporters +=
                        subscriber.globalId -> new MultipleValuesResultsReporter(sq, Seq.empty)
                    }
                  }
            }
            // Send existing results
            val existingResultGroup = sqState.readResults(properties - graph.labelsProperty)
            for (resultGroup <- existingResultGroup)
              subscriber match {
                case MultipleValuesStandingQuerySubscriber.NodeSubscriber(quineId, sqId, upstreamPartId) =>
                  quineId ! NewMultipleValuesStateResult(
                    qid,
                    query.queryPartId,
                    sqId,
                    Some(upstreamPartId),
                    resultGroup,
                  )
                case MultipleValuesStandingQuerySubscriber.GlobalSubscriber(sqId) =>
                  val reporter = multipleValuesResultReporters(sqId)
                  reporter.applyAndEmitResults(resultGroup)
              }
            val _ = persistMultipleValuesStandingQueryState(subscriber.globalId, query.queryPartId, Some(tup))
            // TODO: don't ignore the returned future!
          }
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
        case Some(tup @ (subscription, sqState @ _)) =>
          subscription.subscribers.remove(subscriber)
          if (subscriber.isInstanceOf[GlobalSubscriber]) {
            multipleValuesResultReporters -= subscriber.globalId
          }
          // Only fully remove the running standing query if no subscribers remain. There might be multiple subscribers
          // to the same `combinedId` if, for example, this node is (was) at the bottom of a diamond pattern.
          if (subscription.subscribers.isEmpty) {
            multipleValuesStandingQueries -= combinedId // stop managing state.
//            sqState.query.children.foreach(subquery => // Unsubscribe to subqueries.
//              ??? ! CancelMultipleValuesSubscription(
//                NodeSubscriber(qid, subscriber.globalId, queryPartId),
//                subquery.queryPartId
//              )
//            )
          }
          val _ = persistMultipleValuesStandingQueryState(subscriber.globalId, queryPartId, Some(tup))
        // TODO: don't ignore the returned future!
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
              log"""Got a result from: ${Safe(fromQid.pretty)} for: ${Safe(queryPartIdForResult)},
                   |but this node does not track: ${Safe(queryPartIdForResult)} ($relevantSqPartStr)
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

  private[this] def persistMultipleValuesStandingQueryState(
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
}

/** Represents a subscription held on a specific node to the results of a query run on that node.
  * Subscribers will be added and removed over time.
  *
  * @param forQuery the query part representing what is being subscribed to. The
  * @param globalId the Standing Query ID set once each time the API call is issued.
  * @param subscribers each party interested in the results of this subscription. Each subscriber that is a
  *                    `NodeSubscriber` also has a queryPartId which corresponds to that node's bookkeeping for how
  *                    to map a delivered result back to it's own relevant query.
  */
final case class MultipleValuesStandingQueryPartSubscription(
  forQuery: MultipleValuesStandingQueryPartId,
  globalId: StandingQueryId,
  subscribers: mutable.Set[MultipleValuesStandingQuerySubscriber],
)
