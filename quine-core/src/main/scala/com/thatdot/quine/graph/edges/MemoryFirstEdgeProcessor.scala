package com.thatdot.quine.graph.edges

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

import akka.actor.ActorSystem
import akka.dispatch.MessageDispatcher

import cats.data.NonEmptyList

import com.thatdot.quine.graph.NodeEvent.WithTime
import com.thatdot.quine.graph.{BinaryHistogramCounter, CostToSleep, EdgeEvent, EventTime, NodeChangeEvent, NodeEvent}
import com.thatdot.quine.model.{QuineId, QuineIdProvider}
import com.thatdot.quine.util.QuineDispatchers

class MemoryFirstEdgeProcessor(
  edges: EdgeCollection,
  persistToJournal: NonEmptyList[NodeEvent.WithTime[EdgeEvent]] => Future[Unit],
  updateSnapshotTimestamp: () => Unit,
  runPostActions: List[NodeChangeEvent] => Unit,
  qid: QuineId,
  costToSleep: CostToSleep,
  nodeEdgesCounter: BinaryHistogramCounter
)(implicit system: ActorSystem, idProvider: QuineIdProvider)
    extends SynchronousEdgeProcessor(edges, qid, costToSleep, nodeEdgesCounter) {

  val nodeDispatcher: MessageDispatcher = new QuineDispatchers(system).nodeDispatcherEC

  protected def journalAndApplyEffects(
    effectingEvents: NonEmptyList[EdgeEvent],
    produceTimestamp: () => EventTime
  ): Future[Unit] = {
    val persistAttempts = new AtomicInteger(1)
    val effectingEventsTimestamped = effectingEvents.map(WithTime(_, produceTimestamp()))

    def persistEventsToJournal(): Future[Unit] =
      persistToJournal(effectingEventsTimestamped)
        .transform(
          _ =>
            // TODO: add a metric to report `persistAttempts`
            (),
          (e: Throwable) => {
            val attemptCount = persistAttempts.getAndIncrement()
            logger.info(
              s"Retrying persistence from node: ${qid.pretty} with events: $effectingEvents after: " +
              s"$attemptCount attempts, with error: $e"
            )
            e
          }
        )(nodeDispatcher)

    effectingEvents.toList.foreach(applyEdgeEffect)
    updateSnapshotTimestamp()
    runPostActions(effectingEvents.toList)

    akka.pattern
      .retry(
        () => persistEventsToJournal(),
        Int.MaxValue,
        1.millisecond,
        10.seconds,
        randomFactor = 0.1d
      )(nodeDispatcher, system.scheduler)
  }
}
