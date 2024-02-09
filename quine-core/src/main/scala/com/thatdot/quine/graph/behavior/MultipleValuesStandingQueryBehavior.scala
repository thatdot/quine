package com.thatdot.quine.graph.behavior

import scala.collection.mutable
import scala.concurrent.Future

import org.apache.pekko.actor.Actor

import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.graph.StandingQueryLocalEventIndex.{EventSubscriber, StandingQueryWithId}
import com.thatdot.quine.graph.cypher.{
  MultipleValuesStandingQuery,
  MultipleValuesStandingQueryEffects,
  MultipleValuesStandingQueryState
}
import com.thatdot.quine.graph.messaging.BaseMessage.Done
import com.thatdot.quine.graph.messaging.StandingQueryMessage.{
  CancelMultipleValuesResult,
  CancelMultipleValuesSubscription,
  CreateMultipleValuesStandingQuerySubscription,
  MultipleValuesStandingQueryCommand,
  MultipleValuesStandingQuerySubscriber,
  NewMultipleValuesResult,
  ResultId,
  UpdateStandingQueriesCommand,
  UpdateStandingQueriesNoWake,
  UpdateStandingQueriesWake
}
import com.thatdot.quine.graph.messaging.{QuineIdOps, QuineRefOps}
import com.thatdot.quine.graph.{
  BaseNodeActor,
  MultipleValuesStandingQueryPartId,
  NodeChangeEvent,
  StandingQueryId,
  StandingQueryLocalEvents,
  StandingQueryPattern,
  TimeFuture,
  cypher
}
import com.thatdot.quine.model.{PropertyValue, QuineId, QuineIdProvider}
import com.thatdot.quine.persistor.codecs.MultipleValuesStandingQueryStateCodec
import com.thatdot.quine.persistor.{PersistenceAgent, PersistenceConfig, PersistenceSchedule}

trait MultipleValuesStandingQueryBehavior
    extends Actor
    with BaseNodeActor
    with QuineIdOps
    with QuineRefOps
    with StandingQueryBehavior {

  protected def syncStandingQueries(): Unit

  protected def persistor: PersistenceAgent

  protected def persistenceConfig: PersistenceConfig

  /** Bring this node's locally-tracked standing queries in sync with the current graph state.
    *  - Remove SQs registered on the node but not on the graph by cancelling subscriptions to subqueries as appropriate
    *  - [Re]subscribe to each SQ registered on the graph
    */
  def updateMultipleValuesStandingQueriesOnNode(): Unit = {

    val runningStandingQueries = graph.runningStandingQueries

    // Remove old SQs no longer in graph state
    for {
      ((sqId, partId), (sqSubscribers, _)) <- multipleValuesStandingQueries
      if !runningStandingQueries.contains(sqId)
      subscriber <- sqSubscribers.subscribers
    } self ! CancelMultipleValuesSubscription(subscriber, partId)

    // Register new universal SQs in graph state
    for {
      (sqId, runningSQ) <- runningStandingQueries
      query <- runningSQ.query.query match {
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

  implicit class MultipleValuesStandingQuerySubscribersOps(subs: MultipleValuesStandingQuerySubscribers)
      extends MultipleValuesStandingQueryEffects
      with LazyLogging {

    def lookupQuery(queryPartId: MultipleValuesStandingQueryPartId): MultipleValuesStandingQuery =
      graph.getStandingQueryPart(queryPartId)

    def createSubscription(onNode: QuineId, query: MultipleValuesStandingQuery): Unit = {
      val subscriber = MultipleValuesStandingQuerySubscriber.NodeSubscriber(node, subs.globalId, subs.forQuery)
      onNode ! CreateMultipleValuesStandingQuerySubscription(subscriber, query)
    }

    def cancelSubscription(onNode: QuineId, queryId: MultipleValuesStandingQueryPartId): Unit = {
      val subscriber = MultipleValuesStandingQuerySubscriber.NodeSubscriber(node, subs.globalId, subs.forQuery)
      // optimization: only perform cancellations for running top-level queries (or to clear out local state)
      if (qid == onNode || graph.runningStandingQuery(subs.globalId).nonEmpty) {
        onNode ! CancelMultipleValuesSubscription(subscriber, queryId)
      } else {
        logger.info(
          s"Declining to process MultipleValues cancellation message on node: $onNode for deleted Standing Query ${subs.globalId}"
        )
      }
    }

    def reportNewResult(resultId: ResultId, result: cypher.QueryContext): Unit = {
      val newResult = NewMultipleValuesResult(node, subs.forQuery, subs.globalId, _, resultId, result)
      subs.subscribers.foreach {
        case MultipleValuesStandingQuerySubscriber.NodeSubscriber(quineId, _, subscriber) =>
          quineId ! newResult(Some(subscriber))
        case MultipleValuesStandingQuerySubscriber.GlobalSubscriber(sqId) =>
          graph.reportStandingResult(sqId, newResult(None))
      }
    }

    def cancelOldResult(resultId: ResultId): Unit = {
      val oldResult = CancelMultipleValuesResult(node, subs.forQuery, subs.globalId, _, resultId)
      subs.subscribers.foreach {
        case MultipleValuesStandingQuerySubscriber.NodeSubscriber(quineId, _, subscriber) =>
          quineId ! oldResult(Some(subscriber))
        case MultipleValuesStandingQuerySubscriber.GlobalSubscriber(sqId) =>
          graph.reportStandingResult(sqId, oldResult(None))
      }
    }

    val node: QuineId = qid

    val idProvider: QuineIdProvider = MultipleValuesStandingQueryBehavior.this.idProvider

    def currentProperties: Map[Symbol, PropertyValue] = properties // TODO 2.13 use .view
  }

  /** Locally registered & running standing queries */
  protected def multipleValuesStandingQueries: mutable.Map[
    (StandingQueryId, MultipleValuesStandingQueryPartId),
    (MultipleValuesStandingQuerySubscribers, MultipleValuesStandingQueryState)
  ]

  /** When running in [[PersistenceSchedule.OnNodeSleep]], updates
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
    subscriber: StandingQueryWithId
  ): Future[Unit] = {

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
      val combinedId = subscriber.globalId -> query.id
      multipleValuesStandingQueries.get(combinedId) match {
        case None =>
          val sqState = query.createState()
          val subscribers =
            MultipleValuesStandingQuerySubscribers(query.id, subscriber.globalId, mutable.Set(subscriber))
          multipleValuesStandingQueries += combinedId -> (subscribers -> sqState)
          sqState.preStart(subscribers)
          sqState.onInitialize(subscribers)
          sqState.relevantEvents.foreach { (event: StandingQueryLocalEvents) =>
            val initialEvents = localEventIndex.registerStandingQuery(
              EventSubscriber(combinedId),
              event,
              properties,
              edges
            )

            // Notify the standing query of events for pre-existing node state
            sqState.onNodeEvents(initialEvents, subscribers)
          }
          val _ = persistMultipleValuesStandingQueryState(subscriber.globalId, query.id, Some(subscribers -> sqState))

        // SQ is already running on the node
        case Some(tup @ (subscribers, sqState)) =>
          // Check if this is already an existing subscriber (if so, no-op)
          if (subscribers.subscribers.add(subscriber)) {
            // TODO: replay past results to the new subscriber, but make sure they
            // get those previous results _before_ new results

            // Replay results
            val replayed = sqState.replayResults(properties)
            for ((resultId, result) <- replayed) {
              val newResult = NewMultipleValuesResult(qid, query.id, subscriber.globalId, _, resultId, result)
              subscriber match {
                case MultipleValuesStandingQuerySubscriber.NodeSubscriber(quineId, _, subscriber) =>
                  quineId ! newResult(Some(subscriber))
                case MultipleValuesStandingQuerySubscriber.GlobalSubscriber(sqId) =>
                  graph.reportStandingResult(sqId, newResult(None))
              }
            }
            val _ = persistMultipleValuesStandingQueryState(subscriber.globalId, query.id, Some(tup))
          }
      }

    case CancelMultipleValuesSubscription(subscriber, queryId) =>
      val combinedId = subscriber.globalId -> queryId
      multipleValuesStandingQueries.get(combinedId) match {
        case None =>
        // TODO: can this happen in non-exceptional cases (ie. should we warn?)

        case Some(tup @ (subscribers, sqState)) =>
          subscribers.subscribers.remove(subscriber)

          // Only fully remove the running standing query if no subscribers remain
          if (subscribers.subscribers.isEmpty) {
            multipleValuesStandingQueries -= combinedId
            sqState.onShutdown(subscribers)
            sqState.relevantEvents.foreach { (event: StandingQueryLocalEvents) =>
              localEventIndex.unregisterStandingQuery(EventSubscriber(combinedId), event)
            }
            val _ = persistMultipleValuesStandingQueryState(subscriber.globalId, queryId, None)
          } else {
            val _ = persistMultipleValuesStandingQueryState(subscriber.globalId, queryId, Some(tup))
          }
      }

    case newResult @ NewMultipleValuesResult(_, _, globalId, forQueryIdOpt, _, _) =>
      val interestedLocalQueryId = forQueryIdOpt.get // should never be `None` for node
      // Deliver the result to interested standing query state
      multipleValuesStandingQueries.get(globalId -> interestedLocalQueryId) match {
        case None =>
        // Possible if local shutdown happens right before a result is received
        case Some(tup @ (subscribers, sqState)) =>
          val somethingChanged = sqState.onNewSubscriptionResult(newResult, subscribers)
          if (somethingChanged) {
            val _ = persistMultipleValuesStandingQueryState(globalId, interestedLocalQueryId, Some(tup))
          }
      }

    case cancelledResult @ CancelMultipleValuesResult(_, _, globalId, forQueryIdOpt, _) =>
      val interestedLocalQueryId = forQueryIdOpt.get // should never be `None` for node
      // Deliver the cancellation to interested standing query state
      multipleValuesStandingQueries.get(globalId -> interestedLocalQueryId) match {
        case None =>
        // Possible if local shutdown happens right before a result is received
        case Some(tup @ (subscribers, sqState)) =>
          val somethingChanged = sqState.onCancelledSubscriptionResult(cancelledResult, subscribers)
          if (somethingChanged) {
            val _ = persistMultipleValuesStandingQueryState(globalId, interestedLocalQueryId, Some(tup))
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
    state: Option[(MultipleValuesStandingQuerySubscribers, MultipleValuesStandingQueryState)]
  ): Future[Unit] =
    persistenceConfig.standingQuerySchedule match {
      case PersistenceSchedule.OnNodeUpdate =>
        val serialized = state.map(MultipleValuesStandingQueryStateCodec.format.write)
        serialized.foreach(arr => metrics.standingQueryStateSize(globalId).update(arr.length))
        new TimeFuture(metrics.persistorSetStandingQueryStateTimer).time[Unit](
          persistor.setMultipleValuesStandingQueryState(
            globalId,
            qid,
            localId,
            serialized
          )
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

final case class MultipleValuesStandingQuerySubscribers(
  forQuery: MultipleValuesStandingQueryPartId,
  globalId: StandingQueryId,
  subscribers: mutable.Set[MultipleValuesStandingQuerySubscriber]
)
