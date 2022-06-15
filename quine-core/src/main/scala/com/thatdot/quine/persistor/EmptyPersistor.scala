package com.thatdot.quine.persistor

import scala.concurrent.{ExecutionContext, Future}

import akka.NotUsed
import akka.stream.scaladsl.Source

import com.thatdot.quine.graph.{EventTime, NodeChangeEvent, StandingQuery, StandingQueryId, StandingQueryPartId}
import com.thatdot.quine.model.QuineId

/** Persistence agent which never saves anything
  *
  * Since Quine's bottleneck is usually disk access, this is useful for
  * benchmarking storage-unrelated issues (since it makes all storage operations
  * no-ops).
  */
class EmptyPersistor(
  val persistenceConfig: PersistenceConfig = PersistenceConfig()
) extends PersistenceAgent {

  override def emptyOfQuineData()(implicit ec: ExecutionContext): Future[Boolean] =
    Future.successful(true)

  def enumerateSnapshotNodeIds(): Source[QuineId, NotUsed] = {
    logger.warn(
      "Attempted to enumerate all node IDs on an empty persistor which never returns anything."
    )
    Source.empty[QuineId]
  }

  override def enumerateJournalNodeIds(): Source[QuineId, NotUsed] = this.enumerateSnapshotNodeIds()

  override def persistEvents(id: QuineId, events: Seq[NodeChangeEvent.WithTime]): Future[Unit] = Future.unit

  override def getJournal(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime
  ): Future[Vector[NodeChangeEvent]] = Future.successful(Vector.empty)

  override def getJournalWithTime(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime
  ): Future[Vector[NodeChangeEvent.WithTime]] = Future.successful(Vector.empty)

  def persistSnapshot(id: QuineId, atTime: EventTime, state: Array[Byte]) = Future.unit
  def getLatestSnapshot(id: QuineId, upToTime: EventTime): Future[Option[(EventTime, Array[Byte])]] =
    Future.successful(None)

  def persistStandingQuery(standingQuery: StandingQuery) = Future.unit
  def removeStandingQuery(standingQuery: StandingQuery) = Future.unit
  def getStandingQueries: Future[List[StandingQuery]] = Future.successful(List.empty)

  def getStandingQueryStates(id: QuineId): Future[Map[(StandingQueryId, StandingQueryPartId), Array[Byte]]] =
    Future.successful(Map.empty)

  def setStandingQueryState(
    standingQuery: StandingQueryId,
    id: QuineId,
    standingQueryId: StandingQueryPartId,
    state: Option[Array[Byte]]
  ): Future[Unit] = Future.unit

  def getMetaData(key: String): Future[Option[Array[Byte]]] = Future.successful(None)

  def getAllMetaData(): Future[Map[String, Array[Byte]]] = Future.successful(Map.empty)

  def setMetaData(key: String, newValue: Option[Array[Byte]]): Future[Unit] = Future.unit

  def shutdown(): Future[Unit] = Future.unit
}

object EmptyPersistor extends EmptyPersistor(PersistenceConfig())
