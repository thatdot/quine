package com.thatdot.quine.persistor
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

import akka.NotUsed
import akka.stream.scaladsl.Source

import com.thatdot.quine.graph.{EventTime, NodeChangeEvent, StandingQuery, StandingQueryId, StandingQueryPartId}
import com.thatdot.quine.model.QuineId

/** Reified version of persistor call for logging purposes
  */
sealed abstract class PersistorCall
case class PersistEvents(id: QuineId, events: Seq[NodeChangeEvent.WithTime]) extends PersistorCall
case class GetJournal(id: QuineId, startingAt: EventTime, endingAt: EventTime) extends PersistorCall
case object EnumerateJournalNodeIds extends PersistorCall
case object EnumerateSnapshotNodeIds extends PersistorCall
case class PersistSnapshot(id: QuineId, atTime: EventTime, snapshotSize: Int) extends PersistorCall
case class GetLatestSnapshot(id: QuineId, upToTime: EventTime) extends PersistorCall
case class PersistStandingQuery(standingQuery: StandingQuery) extends PersistorCall
case class RemoveStandingQuery(standingQuery: StandingQuery) extends PersistorCall
case object GetStandingQueries extends PersistorCall
case class GetStandingQueryStates(id: QuineId) extends PersistorCall
case class SetStandingQueryState(
  standingQuery: StandingQueryId,
  id: QuineId,
  standingQueryId: StandingQueryPartId,
  payloadSize: Option[Int]
) extends PersistorCall
case class SetMetaData(key: String, payloadSize: Option[Int]) extends PersistorCall
case class GetMetaData(key: String) extends PersistorCall
case object GetAllMetaData extends PersistorCall

class WrappedPersistorException(persistorCall: PersistorCall, wrapped: Throwable)
    extends Exception("Error calling " + persistorCall, wrapped)
class ExceptionWrappingPersistenceAgent(persistenceAgent: PersistenceAgent)(implicit ec: ExecutionContext)
    extends WrappedPersistenceAgent(persistenceAgent) {

  protected def leftMap[A](f: Throwable => WrappedPersistorException, future: Future[A]): Future[A] = future.transform {
    case Success(value) => Success(value)
    case Failure(exception) =>
      val wrapped = f(exception)
      logger.warn("Persistor error", wrapped)
      Failure(wrapped)
  }

  def persistEvents(id: QuineId, events: Seq[NodeChangeEvent.WithTime]): Future[Unit] = leftMap(
    new WrappedPersistorException(PersistEvents(id, events), _),
    persistenceAgent.persistEvents(id, events)
  )

  def getJournal(id: QuineId, startingAt: EventTime, endingAt: EventTime): Future[Vector[NodeChangeEvent]] = leftMap(
    new WrappedPersistorException(GetJournal(id, startingAt, endingAt), _),
    persistenceAgent.getJournal(id, startingAt, endingAt)
  )

  def enumerateJournalNodeIds(): Source[QuineId, NotUsed] = persistenceAgent.enumerateJournalNodeIds()

  def enumerateSnapshotNodeIds(): Source[QuineId, NotUsed] = persistenceAgent.enumerateSnapshotNodeIds()

  def persistSnapshot(id: QuineId, atTime: EventTime, state: Array[Byte]): Future[Unit] = leftMap(
    new WrappedPersistorException(PersistSnapshot(id, atTime, state.length), _),
    persistenceAgent.persistSnapshot(id, atTime, state)
  )

  def getLatestSnapshot(id: QuineId, upToTime: EventTime): Future[Option[(EventTime, Array[Byte])]] = leftMap(
    new WrappedPersistorException(GetLatestSnapshot(id, upToTime), _),
    persistenceAgent.getLatestSnapshot(id, upToTime)
  )

  def persistStandingQuery(standingQuery: StandingQuery): Future[Unit] = leftMap(
    new WrappedPersistorException(PersistStandingQuery(standingQuery), _),
    persistenceAgent.persistStandingQuery(standingQuery)
  )

  def removeStandingQuery(standingQuery: StandingQuery): Future[Unit] = leftMap(
    new WrappedPersistorException(RemoveStandingQuery(standingQuery), _),
    persistenceAgent.removeStandingQuery(standingQuery)
  )

  def getStandingQueries: Future[List[StandingQuery]] = leftMap(
    new WrappedPersistorException(GetStandingQueries, _),
    persistenceAgent.getStandingQueries
  )

  def getStandingQueryStates(id: QuineId): Future[Map[(StandingQueryId, StandingQueryPartId), Array[Byte]]] = leftMap(
    new WrappedPersistorException(GetStandingQueryStates(id), _),
    persistenceAgent.getStandingQueryStates(id)
  )

  def setStandingQueryState(
    standingQuery: StandingQueryId,
    id: QuineId,
    standingQueryId: StandingQueryPartId,
    state: Option[Array[Byte]]
  ): Future[Unit] = leftMap(
    new WrappedPersistorException(SetStandingQueryState(standingQuery, id, standingQueryId, state.map(_.length)), _),
    persistenceAgent.setStandingQueryState(standingQuery, id, standingQueryId, state)
  )

  def getMetaData(key: String): Future[Option[Array[Byte]]] = leftMap(
    new WrappedPersistorException(GetMetaData(key), _),
    persistenceAgent.getMetaData(key)
  )

  def getAllMetaData(): Future[Map[String, Array[Byte]]] = leftMap(
    new WrappedPersistorException(GetAllMetaData, _),
    persistenceAgent.getAllMetaData()
  )

  def setMetaData(key: String, newValue: Option[Array[Byte]]): Future[Unit] = leftMap(
    new WrappedPersistorException(SetMetaData(key, newValue.map(_.length)), _),
    persistenceAgent.setMetaData(key, newValue)
  )

  def shutdown(): Future[Unit] = persistenceAgent.shutdown()

  def persistenceConfig: PersistenceConfig = persistenceAgent.persistenceConfig
}
