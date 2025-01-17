package com.thatdot.quine.graph.edges

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.dispatch.MessageDispatcher

import cats.data.NonEmptyList
import org.apache.pekko

import com.thatdot.common.logging.Log.{LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.NodeEvent.WithTime
import com.thatdot.quine.graph.metrics.BinaryHistogramCounter
import com.thatdot.quine.graph.{CostToSleep, EdgeEvent, EventTime, NodeChangeEvent, NodeEvent}
import com.thatdot.quine.model.QuineIdProvider
import com.thatdot.quine.util.Log.implicits._
import com.thatdot.quine.util.QuineDispatchers
class MemoryFirstEdgeProcessor(
  edges: SyncEdgeCollection,
  persistToJournal: NonEmptyList[NodeEvent.WithTime[EdgeEvent]] => Future[Unit],
  updateSnapshotTimestamp: () => Unit,
  runPostActions: List[NodeChangeEvent] => Unit,
  qid: QuineId,
  costToSleep: CostToSleep,
  nodeEdgesCounter: BinaryHistogramCounter,
)(implicit system: ActorSystem, idProvider: QuineIdProvider, val logConfig: LogConfig)
    extends SynchronousEdgeProcessor(edges, qid, costToSleep, nodeEdgesCounter) {

  val nodeDispatcher: MessageDispatcher = new QuineDispatchers(system).nodeDispatcherEC

  protected def journalAndApplyEffects(
    effectingEvents: NonEmptyList[EdgeEvent],
    produceTimestamp: () => EventTime,
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
              log"""Retrying persistence from node: $qid with events: $effectingEvents after:
                   |${Safe(attemptCount)} attempts""".cleanLines withException e,
            )
            e
          },
        )(nodeDispatcher)

    effectingEvents.toList.foreach(updateEdgeCollection)
    updateSnapshotTimestamp()
    runPostActions(effectingEvents.toList)

    pekko.pattern
      .retry(
        () => persistEventsToJournal(),
        Int.MaxValue,
        1.millisecond,
        10.seconds,
        randomFactor = 0.1d,
      )(nodeDispatcher, system.scheduler)
  }
}
