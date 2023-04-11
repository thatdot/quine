package com.thatdot.quine.persistor
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success}

import akka.NotUsed
import akka.stream.scaladsl.Source

import com.thatdot.quine.graph.{
  BaseGraph,
  DomainIndexEvent,
  EventTime,
  MultipleValuesStandingQueryPartId,
  NodeChangeEvent,
  NodeEvent,
  StandingQuery,
  StandingQueryId
}
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.model.{DomainGraphNode, QuineId}

/** Reified version of persistor call for logging purposes
  */
sealed abstract class PersistorCall
case class PersistNodeChangeEvents(id: QuineId, events: Seq[NodeEvent.WithTime[NodeChangeEvent]]) extends PersistorCall
case class DeleteNodeChangeEvents(id: QuineId) extends PersistorCall
case class PersistDomainIndexEvents(id: QuineId, events: Seq[NodeEvent.WithTime[DomainIndexEvent]])
    extends PersistorCall
case class DeleteDomainIndexEvents(id: QuineId) extends PersistorCall
case class GetJournal(id: QuineId, startingAt: EventTime, endingAt: EventTime) extends PersistorCall
case object EnumerateJournalNodeIds extends PersistorCall
case object EnumerateSnapshotNodeIds extends PersistorCall
case class PersistSnapshot(id: QuineId, atTime: EventTime, snapshotSize: Int) extends PersistorCall
case class DeleteSnapshot(id: QuineId) extends PersistorCall
case class GetLatestSnapshot(id: QuineId, upToTime: EventTime) extends PersistorCall
case class PersistStandingQuery(standingQuery: StandingQuery) extends PersistorCall
case class RemoveStandingQuery(standingQuery: StandingQuery) extends PersistorCall
case object GetStandingQueries extends PersistorCall
case class GetMultipleValuesStandingQueryStates(id: QuineId) extends PersistorCall
case class DeleteMultipleValuesStandingQueryStates(id: QuineId) extends PersistorCall
case class SetStandingQueryState(
  standingQuery: StandingQueryId,
  id: QuineId,
  standingQueryId: MultipleValuesStandingQueryPartId,
  payloadSize: Option[Int]
) extends PersistorCall
case class SetMetaData(key: String, payloadSize: Option[Int]) extends PersistorCall
case class GetMetaData(key: String) extends PersistorCall
case object GetAllMetaData extends PersistorCall
case class PersistDomainGraphNodes(domainGraphNodes: Map[DomainGraphNodeId, DomainGraphNode]) extends PersistorCall
case class RemoveDomainGraphNodes(domainGraphNodeIds: Set[DomainGraphNodeId]) extends PersistorCall
case class RemoveDomainIndexEventsByDgnId(dgnId: DomainGraphNodeId) extends PersistorCall
case object GetDomainGraphNodes extends PersistorCall

class WrappedPersistorException(persistorCall: PersistorCall, wrapped: Throwable)
    extends Exception("Error calling " + persistorCall, wrapped)
    with NoStackTrace

/** @param ec EC on which to schedule error-wrapping logic (low CPU, nonblocking workload)
  */
class ExceptionWrappingPersistenceAgent(persistenceAgent: PersistenceAgent, ec: ExecutionContext)
    extends WrappedPersistenceAgent(persistenceAgent) {

  protected def leftMap[A](f: Throwable => WrappedPersistorException, future: Future[A]): Future[A] = future.transform {
    case Success(value) => Success(value)
    case Failure(exception) =>
      val wrapped = f(exception)
      logger.warn("Intercepted persistor error", wrapped)
      Failure(wrapped)
  }(ec)

  /** Persist [[NodeChangeEvent]] values. */
  def persistNodeChangeEvents(id: QuineId, events: Seq[NodeEvent.WithTime[NodeChangeEvent]]): Future[Unit] = leftMap(
    new WrappedPersistorException(PersistNodeChangeEvents(id, events), _),
    persistenceAgent.persistNodeChangeEvents(id, events)
  )

  /** Persist [[DomainIndexEvent]] values. */
  def persistDomainIndexEvents(id: QuineId, events: Seq[NodeEvent.WithTime[DomainIndexEvent]]): Future[Unit] = leftMap(
    new WrappedPersistorException(PersistDomainIndexEvents(id, events), _),
    persistenceAgent.persistDomainIndexEvents(id, events)
  )

  def getNodeChangeEventsWithTime(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime
  ): Future[Iterable[NodeEvent.WithTime[NodeChangeEvent]]] = leftMap(
    new WrappedPersistorException(GetJournal(id, startingAt, endingAt), _),
    persistenceAgent.getNodeChangeEventsWithTime(id, startingAt, endingAt)
  )

  def getDomainIndexEventsWithTime(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime
  ): Future[Iterable[NodeEvent.WithTime[DomainIndexEvent]]] = leftMap(
    new WrappedPersistorException(GetJournal(id, startingAt, endingAt), _),
    persistenceAgent.getDomainIndexEventsWithTime(id, startingAt, endingAt)
  )

  override def deleteSnapshots(qid: QuineId): Future[Unit] = leftMap(
    new WrappedPersistorException(DeleteSnapshot(qid), _),
    persistenceAgent.deleteSnapshots(qid)
  )
  override def deleteNodeChangeEvents(qid: QuineId): Future[Unit] = leftMap(
    new WrappedPersistorException(DeleteNodeChangeEvents(qid), _),
    persistenceAgent.deleteNodeChangeEvents(qid)
  )
  override def deleteDomainIndexEvents(qid: QuineId): Future[Unit] = leftMap(
    new WrappedPersistorException(DeleteDomainIndexEvents(qid), _),
    persistenceAgent.deleteDomainIndexEvents(qid)
  )
  override def deleteMultipleValuesStandingQueryStates(id: QuineId): Future[Unit] = leftMap(
    new WrappedPersistorException(DeleteMultipleValuesStandingQueryStates(id), _),
    persistenceAgent.deleteMultipleValuesStandingQueryStates(id)
  )

  // TODO: Can you catch and wrap exceptions thrown by Streams?
  def enumerateJournalNodeIds(): Source[QuineId, NotUsed] = persistenceAgent.enumerateJournalNodeIds()

  def enumerateSnapshotNodeIds(): Source[QuineId, NotUsed] = persistenceAgent.enumerateSnapshotNodeIds()

  def persistSnapshot(id: QuineId, atTime: EventTime, state: Array[Byte]): Future[Unit] = leftMap(
    new WrappedPersistorException(PersistSnapshot(id, atTime, state.length), _),
    persistenceAgent.persistSnapshot(id, atTime, state)
  )

  def getLatestSnapshot(id: QuineId, upToTime: EventTime): Future[Option[Array[Byte]]] = leftMap(
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

  def getMultipleValuesStandingQueryStates(
    id: QuineId
  ): Future[Map[(StandingQueryId, MultipleValuesStandingQueryPartId), Array[Byte]]] = leftMap(
    new WrappedPersistorException(GetMultipleValuesStandingQueryStates(id), _),
    persistenceAgent.getMultipleValuesStandingQueryStates(id)
  )

  def setMultipleValuesStandingQueryState(
    standingQuery: StandingQueryId,
    id: QuineId,
    standingQueryId: MultipleValuesStandingQueryPartId,
    state: Option[Array[Byte]]
  ): Future[Unit] = leftMap(
    new WrappedPersistorException(SetStandingQueryState(standingQuery, id, standingQueryId, state.map(_.length)), _),
    persistenceAgent.setMultipleValuesStandingQueryState(standingQuery, id, standingQueryId, state)
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

  override def ready(graph: BaseGraph): Unit = persistenceAgent.ready(graph)

  def persistDomainGraphNodes(domainGraphNodes: Map[DomainGraphNodeId, DomainGraphNode]): Future[Unit] = leftMap(
    new WrappedPersistorException(PersistDomainGraphNodes(domainGraphNodes), _),
    persistenceAgent.persistDomainGraphNodes(domainGraphNodes)
  )

  def removeDomainGraphNodes(domainGraphNodeIds: Set[DomainGraphNodeId]): Future[Unit] = leftMap(
    new WrappedPersistorException(RemoveDomainGraphNodes(domainGraphNodeIds), _),
    persistenceAgent.removeDomainGraphNodes(domainGraphNodeIds)
  )

  def getDomainGraphNodes(): Future[Map[DomainGraphNodeId, DomainGraphNode]] = leftMap(
    new WrappedPersistorException(GetDomainGraphNodes, _),
    persistenceAgent.getDomainGraphNodes()
  )

  def deleteDomainIndexEventsByDgnId(dgnId: DomainGraphNodeId): Future[Unit] = leftMap(
    new WrappedPersistorException(RemoveDomainIndexEventsByDgnId(dgnId), _),
    persistenceAgent.deleteDomainIndexEventsByDgnId(dgnId)
  )

  def shutdown(): Future[Unit] = persistenceAgent.shutdown()

  def persistenceConfig: PersistenceConfig = persistenceAgent.persistenceConfig
}
