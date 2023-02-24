package com.thatdot.quine.persistor

import scala.compat.ExecutionContexts
import scala.concurrent.Future

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

/** Persistence agent that multiplexes nodes across multiple underlying persistence agents
  *
  * Metadata goes (somewhat arbitrarily) entirely on the "rootAgent" persistor by default
  */
abstract class PartitionedPersistenceAgent extends PersistenceAgent {

  /** Find the persistence agent that is responsible for a given node */
  def getAgent(id: QuineId): PersistenceAgent

  def getAgents: Iterator[PersistenceAgent]

  def rootAgent: PersistenceAgent

  override def emptyOfQuineData(): Future[Boolean] =
    if (getAgents.isEmpty) Future.successful(true)
    else
      Future
        .traverse(getAgents)(_.emptyOfQuineData())(implicitly, ExecutionContexts.parasitic)
        .map(_.reduce((leftIsClear, rightIsClear) => leftIsClear && rightIsClear))(ExecutionContexts.parasitic)

  def persistNodeChangeEvents(id: QuineId, events: Seq[NodeEvent.WithTime[NodeChangeEvent]]): Future[Unit] =
    getAgent(id).persistNodeChangeEvents(id, events)

  def persistDomainIndexEvents(id: QuineId, events: Seq[NodeEvent.WithTime[DomainIndexEvent]]): Future[Unit] =
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

  override def ready(graph: BaseGraph): Unit =
    getAgents.foreach(_.ready(graph))

  def persistDomainGraphNodes(domainGraphNodes: Map[DomainGraphNodeId, DomainGraphNode]): Future[Unit] =
    rootAgent.persistDomainGraphNodes(domainGraphNodes)

  def removeDomainGraphNodes(domainGraphNodes: Set[DomainGraphNodeId]): Future[Unit] =
    rootAgent.removeDomainGraphNodes(domainGraphNodes)

  def getDomainGraphNodes(): Future[Map[DomainGraphNodeId, DomainGraphNode]] =
    rootAgent.getDomainGraphNodes()

  def deleteDomainIndexEventsByDgnId(dgnId: DomainGraphNodeId): Future[Unit] =
    Future(getAgents.foreach(_.deleteDomainIndexEventsByDgnId(dgnId)))(ExecutionContexts.parasitic)

  override def shutdown(): Future[Unit] =
    Future
      .traverse(getAgents.toSeq)(_.shutdown())(implicitly, ExecutionContexts.parasitic)
      .map(_ => ())(ExecutionContexts.parasitic)
}
