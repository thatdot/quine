package com.thatdot.quine.persistor

import scala.concurrent.{ExecutionContext, Future}

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import cats.data.NonEmptyList

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

/** Persistence agent that multiplexes nodes across multiple underlying persistence agents
  *
  * Metadata goes (somewhat arbitrarily) entirely on the "rootAgent" persistor by default
  */
abstract class PartitionedPersistenceAgent extends PersistenceAgent {

  /** Find the persistence agent that is responsible for a given node */
  protected def getAgent(id: QuineId): PersistenceAgent

  protected def getAgents: Iterator[PersistenceAgent]

  protected def rootAgent: PersistenceAgent

  override def emptyOfQuineData(): Future[Boolean] =
    if (getAgents.isEmpty) Future.successful(true)
    else
      Future
        .traverse(getAgents)(_.emptyOfQuineData())(implicitly, ExecutionContext.parasitic)
        .map(_.reduce((leftIsClear, rightIsClear) => leftIsClear && rightIsClear))(ExecutionContext.parasitic)

  def persistNodeChangeEvents(id: QuineId, events: NonEmptyList[NodeEvent.WithTime[NodeChangeEvent]]): Future[Unit] =
    getAgent(id).persistNodeChangeEvents(id, events)

  def persistDomainIndexEvents(id: QuineId, events: NonEmptyList[NodeEvent.WithTime[DomainIndexEvent]]): Future[Unit] =
    getAgent(id).persistDomainIndexEvents(id, events)

  def getNodeChangeEventsWithTime(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime
  ): Future[Iterable[NodeEvent.WithTime[NodeChangeEvent]]] =
    getAgent(id).getNodeChangeEventsWithTime(id, startingAt, endingAt)

  def getDomainIndexEventsWithTime(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime
  ): Future[Iterable[NodeEvent.WithTime[DomainIndexEvent]]] =
    getAgent(id).getDomainIndexEventsWithTime(id, startingAt, endingAt)

  override def enumerateJournalNodeIds(): Source[QuineId, NotUsed] =
    getAgents.foldLeft(Source.empty[QuineId])(_ ++ _.enumerateJournalNodeIds())

  override def enumerateSnapshotNodeIds(): Source[QuineId, NotUsed] =
    getAgents.foldLeft(Source.empty[QuineId])(_ ++ _.enumerateSnapshotNodeIds())

  override def persistSnapshot(id: QuineId, atTime: EventTime, state: Array[Byte]): Future[Unit] =
    getAgent(id).persistSnapshot(id, atTime, state)
  override def deleteNodeChangeEvents(qid: QuineId): Future[Unit] =
    getAgent(qid).deleteNodeChangeEvents(qid)

  override def deleteDomainIndexEvents(qid: QuineId): Future[Unit] =
    getAgent(qid).deleteDomainIndexEvents(qid)

  override def deleteSnapshots(qid: QuineId): Future[Unit] =
    getAgent(qid).deleteSnapshots(qid)

  override def deleteMultipleValuesStandingQueryStates(id: QuineId): Future[Unit] =
    getAgent(id).deleteMultipleValuesStandingQueryStates(id)

  override def getLatestSnapshot(id: QuineId, upToTime: EventTime): Future[Option[Array[Byte]]] =
    getAgent(id).getLatestSnapshot(id, upToTime)

  override def persistStandingQuery(standingQuery: StandingQuery): Future[Unit] =
    rootAgent.persistStandingQuery(standingQuery)

  override def removeStandingQuery(standingQuery: StandingQuery): Future[Unit] =
    rootAgent.removeStandingQuery(standingQuery)

  override def getStandingQueries: Future[List[StandingQuery]] =
    rootAgent.getStandingQueries

  override def getMultipleValuesStandingQueryStates(
    id: QuineId
  ): Future[Map[(StandingQueryId, MultipleValuesStandingQueryPartId), Array[Byte]]] =
    getAgent(id).getMultipleValuesStandingQueryStates(id)

  override def setMultipleValuesStandingQueryState(
    standingQuery: StandingQueryId,
    id: QuineId,
    standingQueryId: MultipleValuesStandingQueryPartId,
    state: Option[Array[Byte]]
  ): Future[Unit] = getAgent(id).setMultipleValuesStandingQueryState(standingQuery, id, standingQueryId, state)

  override def getMetaData(key: String): Future[Option[Array[Byte]]] = rootAgent.getMetaData(key)

  override def getAllMetaData(): Future[Map[String, Array[Byte]]] = rootAgent.getAllMetaData()

  override def setMetaData(key: String, newValue: Option[Array[Byte]]): Future[Unit] =
    rootAgent.setMetaData(key, newValue)

  override def declareReady(graph: BaseGraph): Unit =
    getAgents.foreach(_.declareReady(graph))

  def persistDomainGraphNodes(domainGraphNodes: Map[DomainGraphNodeId, DomainGraphNode]): Future[Unit] =
    rootAgent.persistDomainGraphNodes(domainGraphNodes)

  def removeDomainGraphNodes(domainGraphNodes: Set[DomainGraphNodeId]): Future[Unit] =
    rootAgent.removeDomainGraphNodes(domainGraphNodes)

  def getDomainGraphNodes(): Future[Map[DomainGraphNodeId, DomainGraphNode]] =
    rootAgent.getDomainGraphNodes()

  def deleteDomainIndexEventsByDgnId(dgnId: DomainGraphNodeId): Future[Unit] =
    Future(getAgents.foreach(_.deleteDomainIndexEventsByDgnId(dgnId)))(ExecutionContext.parasitic)

  override def shutdown(): Future[Unit] =
    Future
      .traverse(getAgents)(_.shutdown())(implicitly, ExecutionContext.parasitic)
      .map(_ => ())(ExecutionContext.parasitic)

  override def delete(): Future[Unit] = Future
    .traverse(getAgents)(_.delete())(implicitly, ExecutionContext.parasitic)
    .map(_ => ())(ExecutionContext.parasitic)
}
