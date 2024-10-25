package com.thatdot.quine.graph.edges

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

import cats.data.NonEmptyList

import com.thatdot.quine.graph.NodeEvent.WithTime
import com.thatdot.quine.graph.metrics.BinaryHistogramCounter
import com.thatdot.quine.graph.{CostToSleep, EdgeEvent, EventTime, NodeChangeEvent, NodeEvent}
import com.thatdot.quine.model.{QuineId, QuineIdProvider}
import com.thatdot.quine.util.Log._
import com.thatdot.quine.util.Log.implicits._

class PersistorFirstEdgeProcessor(
  edges: SyncEdgeCollection,
  persistToJournal: NonEmptyList[NodeEvent.WithTime[EdgeEvent]] => Future[Unit],
  pauseMessageProcessingUntil: (Future[Unit], Try[Unit] => Unit, Boolean) => Future[Unit],
  updateSnapshotTimestamp: () => Unit,
  runPostActions: List[NodeChangeEvent] => Unit,
  qid: QuineId,
  costToSleep: CostToSleep,
  nodeEdgesCounter: BinaryHistogramCounter,
)(implicit idProvider: QuineIdProvider, val logConfig: LogConfig)
    extends SynchronousEdgeProcessor(edges, qid, costToSleep, nodeEdgesCounter) {

  protected def journalAndApplyEffects(
    effectingEvents: NonEmptyList[EdgeEvent],
    produceTimestamp: () => EventTime,
  ): Future[Unit] =
    pauseMessageProcessingUntil(
      persistToJournal(effectingEvents.map(e => WithTime(e, produceTimestamp()))),
      {
        case Success(_) =>
          // Instead of unwrapping the WithTimes here, maybe just take the raw EdgeEvents and () => EventTime here, and only wrap them on the line above?
          val events = effectingEvents.toList
          events.foreach(updateEdgeCollection)
          updateSnapshotTimestamp()
          runPostActions(events)
        case Failure(err) =>
          logger.error(
            log"""Persistor error occurred when writing events to journal on node: $qid Will not apply
                 |events: $effectingEvents to in-memory state. Returning failed result.""".cleanLines
            withException err,
          )
      },
      true,
    )

}
