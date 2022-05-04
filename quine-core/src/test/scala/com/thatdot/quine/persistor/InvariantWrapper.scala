package com.thatdot.quine.persistor

import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.{ExecutionContext, Future}

import akka.NotUsed
import akka.stream.scaladsl.Source

import com.thatdot.quine.graph.{EventTime, NodeChangeEvent, StandingQuery, StandingQueryId, StandingQueryPartId}
import com.thatdot.quine.model.QuineId

/** Wrapper for a persistor that checks that some invariants are upheld:
  *
  *   - for every node: every event occurs at a unique time
  *   - for every node: every snapshot occurs at a unique time
  */
class InvariantWrapper(wrapped: PersistenceAgent) extends PersistenceAgent {

  private val events = new ConcurrentHashMap[QuineId, ConcurrentHashMap[EventTime, NodeChangeEvent]]
  private val snapshots = new ConcurrentHashMap[QuineId, ConcurrentHashMap[EventTime, Array[Byte]]]

  override def emptyOfQuineData()(implicit ec: ExecutionContext): Future[Boolean] =
    if (events.isEmpty && snapshots.isEmpty) wrapped.emptyOfQuineData()
    else Future.successful(false)

  def persistEvents(id: QuineId, eventsWithTime: Seq[NodeChangeEvent.WithTime]): Future[Unit] = {
    for { NodeChangeEvent.WithTime(event, atTime) <- eventsWithTime } {
      val previous = events
        .computeIfAbsent(id, _ => new ConcurrentHashMap[EventTime, NodeChangeEvent]())
        .put(atTime, event)
      assert(
        (previous eq null) || (previous eq event),
        s"Duplicate events at node id $id and time $atTime: $event & $previous"
      )
    }
    wrapped.persistEvents(id, eventsWithTime)
  }

  def getJournal(id: QuineId, startingAt: EventTime, endingAt: EventTime): Future[Vector[NodeChangeEvent]] =
    wrapped.getJournal(id, startingAt, endingAt)

  def enumerateJournalNodeIds(): Source[QuineId, NotUsed] = wrapped.enumerateJournalNodeIds()

  def enumerateSnapshotNodeIds(): Source[QuineId, NotUsed] = wrapped.enumerateSnapshotNodeIds()

  def persistSnapshot(id: QuineId, atTime: EventTime, state: Array[Byte]): Future[Unit] = {
    val previous = snapshots
      .computeIfAbsent(id, _ => new ConcurrentHashMap[EventTime, Array[Byte]]())
      .put(atTime, state)
    assert(
      (previous eq null) || (previous eq state),
      s"Duplicate snapshots at node id $id and time $atTime: $state & $previous"
    )
    wrapped.persistSnapshot(id, atTime, state)
  }

  def getLatestSnapshot(id: QuineId, upToTime: EventTime): Future[Option[(EventTime, Array[Byte])]] =
    wrapped.getLatestSnapshot(id, upToTime)

  def persistStandingQuery(standingQuery: StandingQuery): Future[Unit] =
    wrapped.persistStandingQuery(standingQuery)

  def removeStandingQuery(standingQuery: StandingQuery): Future[Unit] = wrapped.removeStandingQuery(standingQuery)

  def getStandingQueries = wrapped.getStandingQueries

  def getStandingQueryStates(id: QuineId): Future[Map[(StandingQueryId, StandingQueryPartId), Array[Byte]]] =
    wrapped.getStandingQueryStates(id)

  def getAllMetaData(): Future[Map[String, Array[Byte]]] = wrapped.getAllMetaData()
  def getMetaData(key: String): Future[Option[Array[Byte]]] = wrapped.getMetaData(key)
  def setMetaData(key: String, newValue: Option[Array[Byte]]): Future[Unit] = wrapped.setMetaData(key, newValue)

  override def setStandingQueryState(
    standingQuery: StandingQueryId,
    id: QuineId,
    standingQueryId: StandingQueryPartId,
    state: Option[Array[Byte]]
  ): Future[Unit] = wrapped.setStandingQueryState(standingQuery, id, standingQueryId, state)

  def shutdown(): Future[Unit] = wrapped.shutdown()

  def persistenceConfig: PersistenceConfig = wrapped.persistenceConfig
}
