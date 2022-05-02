package com.thatdot.quine.graph.behavior

import scala.collection.mutable
import scala.concurrent.Future

import akka.actor.Actor

import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.graph.StandingQueryLocalEventIndex.{EventSubscriber, StandingQueryWithId}
import com.thatdot.quine.graph.messaging.BaseMessage.Done
import com.thatdot.quine.graph.messaging.StandingQueryMessage.{
  CancelCypherResult,
  CancelCypherSubscription,
  CreateCypherSubscription,
  CypherStandingQueryCommand,
  CypherSubscriber,
  NewCypherResult,
  ResultId,
  UpdateStandingQueriesCommand,
  UpdateStandingQueriesNoWake,
  UpdateStandingQueriesWake
}
import com.thatdot.quine.graph.messaging.{QuineIdOps, QuineRefOps}
import com.thatdot.quine.graph.{
  BaseNodeActor,
  NodeChangeEvent,
  StandingQueryId,
  StandingQueryLocalEvents,
  StandingQueryPartId,
  StandingQueryPattern,
  TimeFuture,
  cypher
}
import com.thatdot.quine.model.{QuineId, QuineIdProvider}
import com.thatdot.quine.persistor.PersistenceCodecs.standingQueryStateFormat
import com.thatdot.quine.persistor.{PersistenceAgent, PersistenceConfig, PersistenceSchedule}

trait CypherStandingBehavior
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
  def updateUniversalCypherQueriesOnWake(): Unit = {

    // Remove old SQs no longer in graph state
    for {
      ((sqId, partId), (sqSubscribers, _)) <- standingQueries
      if !graph.runningStandingQueries.contains(sqId)
      subscriber <- sqSubscribers.subscribers
    } self ! CancelCypherSubscription(subscriber, partId)

    // Register new universal SQs in graph state
    for {
      universalSqId <- graph.runningStandingQueries.keys
      universalSq <- graph.getStandingQuery(universalSqId)
      query <- universalSq.query.query match {
        case query: StandingQueryPattern.SqV4 => Some(query.compiledQuery)
        case _ => None
      }
    } {
      val subscriber = CypherSubscriber.GlobalSubscriber(universalSqId)
      self ! CreateCypherSubscription(subscriber, query) // no-op if already registered
    }
  }

  implicit class StandingQuerySubscribersOps(subs: StandingQuerySubscribers)
      extends cypher.StandingQueryEffects
      with LazyLogging {

    def lookupQuery(queryPartId: StandingQueryPartId): cypher.StandingQuery =
      graph.getStandingQueryPart(queryPartId)

    def createSubscription(onNode: QuineId, query: cypher.StandingQuery): Unit = {
      val subscriber = CypherSubscriber.QuerySubscriber(node, subs.globalId, subs.forQuery)
      onNode ! CreateCypherSubscription(subscriber, query)
    }

    def cancelSubscription(onNode: QuineId, queryId: StandingQueryPartId): Unit = {
      val subscriber = CypherSubscriber.QuerySubscriber(node, subs.globalId, subs.forQuery)
      // optimization: only perform cancellations for running top-level queries (or to clear out local state)
      if (qid == onNode || graph.getStandingQuery(subs.globalId).nonEmpty) {
        onNode ! CancelCypherSubscription(subscriber, queryId)
      } else {
        logger.info(
          s"Declining to process MultipleValues cancellation message on node: $onNode for deleted Standing Query ${subs.globalId}"
        )
      }
    }

    def reportNewResult(resultId: ResultId, result: cypher.QueryContext): Unit = {
      val newResult = NewCypherResult(node, subs.forQuery, subs.globalId, _, resultId, result)
      subs.subscribers.foreach {
        case CypherSubscriber.QuerySubscriber(quineId, _, subscriber) =>
          quineId ! newResult(Some(subscriber))
        case CypherSubscriber.GlobalSubscriber(sqId) =>
          graph.reportStandingResult(sqId, newResult(None))
      }
    }

    def cancelOldResult(resultId: ResultId): Unit = {
      val oldResult = CancelCypherResult(node, subs.forQuery, subs.globalId, _, resultId)
      subs.subscribers.foreach {
        case CypherSubscriber.QuerySubscriber(quineId, _, subscriber) =>
          quineId ! oldResult(Some(subscriber))
        case CypherSubscriber.GlobalSubscriber(sqId) =>
          graph.reportStandingResult(sqId, oldResult(None))
      }
    }

    val node: QuineId = qid

    val idProvider: QuineIdProvider = CypherStandingBehavior.this.idProvider
  }

  /** Locally registered & running standing queries */
  final var standingQueries: mutable.Map[
    (StandingQueryId, StandingQueryPartId),
    (StandingQuerySubscribers, cypher.StandingQueryState)
  ] =
    mutable.Map.empty[
      (StandingQueryId, StandingQueryPartId),
      (StandingQuerySubscribers, cypher.StandingQueryState)
    ]

  /** When running in [[PersistenceSchedule.OnNodeSleep]], updates
    * will be buffered here and persisted only on node sleep
    */
  final val pendingStandingQueryWrites: mutable.Set[(StandingQueryId, StandingQueryPartId)] =
    mutable.Set.empty[(StandingQueryId, StandingQueryPartId)]

  /** Route a node event to exactly the stateful standing queries interested in it
    *
    * @param event new node event
    * @return future that completes once the SQ updates are saved to disk
    */
  final protected def updateCypherSq(event: NodeChangeEvent, subscriber: StandingQueryWithId): Future[Unit] = {

    val persisted: Option[Future[Unit]] = for {
      tup <- standingQueries.get((subscriber.queryId, subscriber.partId))
      (subscribers, sqState) = tup
      somethingChanged = sqState.onNodeEvents(Seq(event), subscribers)
      if somethingChanged
    } yield persistStandingQueryState(subscriber.queryId, subscriber.partId, Some(tup))

    persisted.getOrElse(Future.unit)
  }

  /** Process a query command to create/remove a standing query or to report/invalidate a result
    *
    * @param command standing query command to process
    */
  protected def cypherStandingQueryBehavior(command: CypherStandingQueryCommand): Unit = command match {
    case CreateCypherSubscription(subscriber, query) =>
      val combinedId = subscriber.globalId -> query.id
      standingQueries.get(combinedId) match {
        case None =>
          val sqState = query.createState()
          val subscribers = StandingQuerySubscribers(query.id, subscriber.globalId, mutable.Set(subscriber))
          standingQueries += combinedId -> (subscribers -> sqState)
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
          val _ = persistStandingQueryState(subscriber.globalId, query.id, Some(subscribers -> sqState))

        // SQ is already running on the node
        case Some(tup @ (subscribers, sqState)) =>
          // Check if this is already an existing subscriber (if so, no-op)
          if (subscribers.subscribers.add(subscriber)) {
            // TODO: replay past results to the new subscriber, but make sure they
            // get those previous results _before_ new results

            // Replay results
            val replayed = sqState.replayResults(properties)
            for ((resultId, result) <- replayed) {
              val newResult = NewCypherResult(qid, query.id, subscriber.globalId, _, resultId, result)
              subscriber match {
                case CypherSubscriber.QuerySubscriber(quineId, _, subscriber) =>
                  quineId ! newResult(Some(subscriber))
                case CypherSubscriber.GlobalSubscriber(sqId) =>
                  graph.reportStandingResult(sqId, newResult(None))
              }
            }
            val _ = persistStandingQueryState(subscriber.globalId, query.id, Some(tup))
          }
      }

    case CancelCypherSubscription(subscriber, queryId) =>
      val combinedId = subscriber.globalId -> queryId
      standingQueries.get(combinedId) match {
        case None =>
        // TODO: can this happen in non-exceptional cases (ie. should we warn?)

        case Some(tup @ (subscribers, sqState)) =>
          subscribers.subscribers.remove(subscriber)

          // Only fully remove the running standing query if no subscribers remain
          if (subscribers.subscribers.isEmpty) {
            standingQueries -= combinedId
            sqState.onShutdown(subscribers)
            sqState.relevantEvents.foreach { (event: StandingQueryLocalEvents) =>
              localEventIndex.unregisterStandingQuery(EventSubscriber(combinedId), event)
            }
            val _ = persistStandingQueryState(subscriber.globalId, queryId, None)
          } else {
            val _ = persistStandingQueryState(subscriber.globalId, queryId, Some(tup))
          }
      }

    case newResult @ NewCypherResult(_, _, globalId, forQueryIdOpt, _, _) =>
      val interestedLocalQueryId = forQueryIdOpt.get // should never be `None` for node
      // Deliver the result to interested standing query state
      standingQueries.get(globalId -> interestedLocalQueryId) match {
        case None =>
        // Possible if local shutdown happens right before a result is received
        case Some(tup @ (subscribers, sqState)) =>
          val somethingChanged = sqState.onNewSubscriptionResult(newResult, subscribers)
          if (somethingChanged) {
            val _ = persistStandingQueryState(globalId, interestedLocalQueryId, Some(tup))
          }
      }

    case cancelledResult @ CancelCypherResult(_, _, globalId, forQueryIdOpt, _) =>
      val interestedLocalQueryId = forQueryIdOpt.get // should never be `None` for node
      // Deliver the cancellation to interested standing query state
      standingQueries.get(globalId -> interestedLocalQueryId) match {
        case None =>
        // Possible if local shutdown happens right before a result is received
        case Some(tup @ (subscribers, sqState)) =>
          val somethingChanged = sqState.onCancelledSubscriptionResult(cancelledResult, subscribers)
          if (somethingChanged) {
            val _ = persistStandingQueryState(globalId, interestedLocalQueryId, Some(tup))
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

  private[this] def persistStandingQueryState(
    globalId: StandingQueryId,
    localId: StandingQueryPartId,
    state: Option[(StandingQuerySubscribers, cypher.StandingQueryState)]
  ): Future[Unit] =
    persistenceConfig.standingQuerySchedule match {
      case PersistenceSchedule.OnNodeUpdate =>
        val serialized = state.map(standingQueryStateFormat.write)
        serialized.foreach(arr => metrics.standingQueryStateSize(globalId).update(arr.length))
        new TimeFuture(metrics.persistorSetStandingQueryStateTimer).time[Unit](
          persistor.setStandingQueryState(
            globalId,
            qid,
            localId,
            serialized
          )
        )

      // Don't save now, but record the fact this will need to be saved on sleep
      case PersistenceSchedule.OnNodeSleep =>
        pendingStandingQueryWrites += globalId -> localId
        updateRelevantToSnapshotOccurred()
        Future.unit

      // No-op: don't save anything!
      case PersistenceSchedule.Never =>
        Future.unit
    }
}

final case class StandingQuerySubscribers(
  forQuery: StandingQueryPartId,
  globalId: StandingQueryId,
  subscribers: mutable.Set[CypherSubscriber]
)
