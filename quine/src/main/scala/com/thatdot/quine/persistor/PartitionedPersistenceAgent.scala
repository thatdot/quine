package com.thatdot.quine.persistor

import scala.compat.ExecutionContexts
import scala.concurrent.{ExecutionContext, Future}

import akka.NotUsed
import akka.stream.scaladsl.Source

import com.thatdot.quine.graph.{
  BaseNodeActorView,
  EventTime,
  NodeChangeEvent,
  StandingQuery,
  StandingQueryId,
  StandingQueryPartId
}
import com.thatdot.quine.model.QuineId

/** Persistence agent that multiplexes nodes across multiple underlying persistence agents
  *
  * Metadata goes (somewhat arbitrarily) entirely on the "rootAgent" persistor by default
  */
abstract class PartitionedPersistenceAgent extends PersistenceAgent {

  /** Find the persistence agent that is responsible for a given node */
  def getAgent(id: QuineId): PersistenceAgent

  def getAgents: Iterator[PersistenceAgent]

  def rootAgent: PersistenceAgent

  override def emptyOfQuineData()(implicit ec: ExecutionContext): Future[Boolean] =
    if (getAgents.isEmpty) Future.successful(true)
    else
      Future
        .traverse(getAgents)(_.emptyOfQuineData())
        .map(_.reduce((leftIsClear, rightIsClear) => leftIsClear && rightIsClear))

  override def persistEvent(id: QuineId, atTime: EventTime, event: NodeChangeEvent): Future[Unit] =
    getAgent(id).persistEvent(id, atTime, event)

  override def getJournal(id: QuineId, startingAt: EventTime, endingAt: EventTime): Future[Vector[NodeChangeEvent]] =
    getAgent(id).getJournal(id, startingAt, endingAt)

  override def enumerateJournalNodeIds(): Source[QuineId, NotUsed] =
    getAgents.foldLeft(Source.empty[QuineId])(_ ++ _.enumerateJournalNodeIds())

  override def enumerateSnapshotNodeIds(): Source[QuineId, NotUsed] =
    getAgents.foldLeft(Source.empty[QuineId])(_ ++ _.enumerateSnapshotNodeIds())

  override def persistSnapshot(id: QuineId, atTime: EventTime, state: Array[Byte]): Future[Unit] =
    getAgent(id).persistSnapshot(id, atTime, state)

  override def getLatestSnapshot(id: QuineId, upToTime: EventTime): Future[Option[(EventTime, Array[Byte])]] =
    getAgent(id).getLatestSnapshot(id, upToTime)

  override def persistStandingQuery(standingQuery: StandingQuery): Future[Unit] =
    rootAgent.persistStandingQuery(standingQuery)

  override def removeStandingQuery(standingQuery: StandingQuery): Future[Unit] =
    rootAgent.removeStandingQuery(standingQuery)

  override def getStandingQueries: Future[List[StandingQuery]] =
    rootAgent.getStandingQueries

  override def getStandingQueryStates(id: QuineId): Future[Map[(StandingQueryId, StandingQueryPartId), Array[Byte]]] =
    getAgent(id).getStandingQueryStates(id)

  override def setStandingQueryState(
    standingQuery: StandingQueryId,
    id: QuineId,
    standingQueryId: StandingQueryPartId,
    state: Option[Array[Byte]]
  ): Future[Unit] = getAgent(id).setStandingQueryState(standingQuery, id, standingQueryId, state)

  override def getMetaData(key: String): Future[Option[Array[Byte]]] = rootAgent.getMetaData(key)

  override def getAllMetaData(): Future[Map[String, Array[Byte]]] = rootAgent.getAllMetaData()

  override def setMetaData(key: String, newValue: Option[Array[Byte]]): Future[Unit] =
    rootAgent.setMetaData(key, newValue)

  override def shutdown(): Future[Unit] = {
    implicit val context: ExecutionContext = ExecutionContexts.parasitic
    Future.traverse(getAgents.toSeq)(_.shutdown()).map(_ => ())
  }

  override def forNode(node: BaseNodeActorView): InNodePersistor = new NodePersistor(node.qid, getAgent(node.qid))

}
