package com.thatdot.quine.persistor
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success}

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import cats.data.NonEmptyList
import com.typesafe.scalalogging.StrictLogging

import com.thatdot.quine.graph.{
  BaseGraph,
  DomainIndexEvent,
  EventTime,
  MultipleValuesStandingQueryPartId,
  NamespaceId,
  NodeChangeEvent,
  NodeEvent,
  StandingQueryId,
  StandingQueryInfo
}
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.model.{DomainGraphNode, QuineId}

/** Reified version of persistor call for logging purposes
  */
sealed abstract class PersistorCall
case class PersistNodeChangeEvents(id: QuineId, events: NonEmptyList[NodeEvent.WithTime[NodeChangeEvent]])
    extends PersistorCall
case class DeleteNodeChangeEvents(id: QuineId) extends PersistorCall
case class PersistDomainIndexEvents(id: QuineId, events: NonEmptyList[NodeEvent.WithTime[DomainIndexEvent]])
    extends PersistorCall
case class DeleteDomainIndexEvents(id: QuineId) extends PersistorCall
case class GetJournal(id: QuineId, startingAt: EventTime, endingAt: EventTime) extends PersistorCall
case class GetDomainIndexEvents(id: QuineId, startingAt: EventTime, endingAt: EventTime) extends PersistorCall
case object EnumerateJournalNodeIds extends PersistorCall
case object EnumerateSnapshotNodeIds extends PersistorCall
case class PersistSnapshot(id: QuineId, atTime: EventTime, snapshotSize: Int) extends PersistorCall
case class DeleteSnapshot(id: QuineId) extends PersistorCall
case class GetLatestSnapshot(id: QuineId, upToTime: EventTime) extends PersistorCall
case class PersistStandingQuery(standingQuery: StandingQueryInfo) extends PersistorCall
case class RemoveStandingQuery(standingQuery: StandingQueryInfo) extends PersistorCall
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
case object GetDomainGraphNodes extends PersistorCall
case class RemoveDomainIndexEventsByDgnId(dgnId: DomainGraphNodeId) extends PersistorCall

class WrappedPersistorException(val persistorCall: PersistorCall, wrapped: Throwable)
    extends Exception("Error calling " + persistorCall, wrapped)
    with NoStackTrace

trait ExceptionWrapper extends StrictLogging {
  protected def wrapException[A](reifiedCall: PersistorCall, future: Future[A]): Future[A] =
    future.transform {
      case s: Success[A] => s
      case Failure(exception) =>
        val wrapped = new WrappedPersistorException(reifiedCall, exception)
        logger.warn("Intercepted persistor error", wrapped)
        Failure(wrapped)
    }(ExecutionContext.parasitic)
}

/** @param ec EC on which to schedule error-wrapping logic (low CPU, nonblocking workload)
  */
class ExceptionWrappingPersistenceAgent(persistenceAgent: NamespacedPersistenceAgent)
    extends WrappedPersistenceAgent(persistenceAgent)
    with ExceptionWrapper {

  val namespace: NamespaceId = persistenceAgent.namespace

  /** Persist [[NodeChangeEvent]] values. */
  def persistNodeChangeEvents(id: QuineId, events: NonEmptyList[NodeEvent.WithTime[NodeChangeEvent]]): Future[Unit] =
    wrapException(
      PersistNodeChangeEvents(id, events),
      persistenceAgent.persistNodeChangeEvents(id, events)
    )

  /** Persist [[DomainIndexEvent]] values. */
  def persistDomainIndexEvents(id: QuineId, events: NonEmptyList[NodeEvent.WithTime[DomainIndexEvent]]): Future[Unit] =
    wrapException(
      PersistDomainIndexEvents(id, events),
      persistenceAgent.persistDomainIndexEvents(id, events)
    )

  def getNodeChangeEventsWithTime(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime
  ): Future[Iterable[NodeEvent.WithTime[NodeChangeEvent]]] = wrapException(
    GetJournal(id, startingAt, endingAt),
    persistenceAgent.getNodeChangeEventsWithTime(id, startingAt, endingAt)
  )

  def getDomainIndexEventsWithTime(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime
  ): Future[Iterable[NodeEvent.WithTime[DomainIndexEvent]]] = wrapException(
    GetDomainIndexEvents(id, startingAt, endingAt),
    persistenceAgent.getDomainIndexEventsWithTime(id, startingAt, endingAt)
  )

  override def deleteSnapshots(qid: QuineId): Future[Unit] = wrapException(
    DeleteSnapshot(qid),
    persistenceAgent.deleteSnapshots(qid)
  )
  override def deleteNodeChangeEvents(qid: QuineId): Future[Unit] = wrapException(
    DeleteNodeChangeEvents(qid),
    persistenceAgent.deleteNodeChangeEvents(qid)
  )
  override def deleteDomainIndexEvents(qid: QuineId): Future[Unit] = wrapException(
    DeleteDomainIndexEvents(qid),
    persistenceAgent.deleteDomainIndexEvents(qid)
  )
  override def deleteMultipleValuesStandingQueryStates(id: QuineId): Future[Unit] = wrapException(
    DeleteMultipleValuesStandingQueryStates(id),
    persistenceAgent.deleteMultipleValuesStandingQueryStates(id)
  )

  // TODO: Can you catch and wrap exceptions thrown by Streams?
  def enumerateJournalNodeIds(): Source[QuineId, NotUsed] = persistenceAgent.enumerateJournalNodeIds()

  def enumerateSnapshotNodeIds(): Source[QuineId, NotUsed] = persistenceAgent.enumerateSnapshotNodeIds()

  def persistSnapshot(id: QuineId, atTime: EventTime, state: Array[Byte]): Future[Unit] = wrapException(
    PersistSnapshot(id, atTime, state.length),
    persistenceAgent.persistSnapshot(id, atTime, state)
  )

  def getLatestSnapshot(id: QuineId, upToTime: EventTime): Future[Option[Array[Byte]]] = wrapException(
    GetLatestSnapshot(id, upToTime),
    persistenceAgent.getLatestSnapshot(id, upToTime)
  )

  def persistStandingQuery(standingQuery: StandingQueryInfo): Future[Unit] = wrapException(
    PersistStandingQuery(standingQuery),
    persistenceAgent.persistStandingQuery(standingQuery)
  )

  def removeStandingQuery(standingQuery: StandingQueryInfo): Future[Unit] = wrapException(
    RemoveStandingQuery(standingQuery),
    persistenceAgent.removeStandingQuery(standingQuery)
  )

  def getStandingQueries: Future[List[StandingQueryInfo]] = wrapException(
    GetStandingQueries,
    persistenceAgent.getStandingQueries
  )

  def getMultipleValuesStandingQueryStates(
    id: QuineId
  ): Future[Map[(StandingQueryId, MultipleValuesStandingQueryPartId), Array[Byte]]] = wrapException(
    GetMultipleValuesStandingQueryStates(id),
    persistenceAgent.getMultipleValuesStandingQueryStates(id)
  )

  def setMultipleValuesStandingQueryState(
    standingQuery: StandingQueryId,
    id: QuineId,
    standingQueryId: MultipleValuesStandingQueryPartId,
    state: Option[Array[Byte]]
  ): Future[Unit] = wrapException(
    SetStandingQueryState(standingQuery, id, standingQueryId, state.map(_.length)),
    persistenceAgent.setMultipleValuesStandingQueryState(standingQuery, id, standingQueryId, state)
  )

  override def declareReady(graph: BaseGraph): Unit = persistenceAgent.declareReady(graph)

  def emptyOfQuineData(): Future[Boolean] = persistenceAgent.emptyOfQuineData()

  def deleteDomainIndexEventsByDgnId(dgnId: DomainGraphNodeId): Future[Unit] = wrapException(
    RemoveDomainIndexEventsByDgnId(dgnId),
    persistenceAgent.deleteDomainIndexEventsByDgnId(dgnId)
  )

  def shutdown(): Future[Unit] = persistenceAgent.shutdown()

  def delete(): Future[Unit] = persistenceAgent.delete()

  def persistenceConfig: PersistenceConfig = persistenceAgent.persistenceConfig
}
